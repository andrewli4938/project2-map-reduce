#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

typedef struct {
  mapper_t mapper;
  kvlist_t* input;
  kvlist_t* output;
} mapper_args_t;

typedef struct {
  reducer_t reducer;
  kvlist_t* input;
  kvlist_t* output;
} reducer_args_t;

// Allocate an array of n empty lists.
static kvlist_t** new_lists(size_t n) {
  kvlist_t** lists = malloc(n * sizeof(kvlist_t*));
  if (lists == NULL) {
    return NULL;
  }
  for (size_t i = 0; i < n; i++) {
    lists[i] = kvlist_new();
  }
  return lists;
}

// Free an array of n lists and every pair inside them.
static void free_lists(kvlist_t** lists, size_t n) {
  for (size_t i = 0; i < n; i++) {
    kvlist_free(&lists[i]);
  }
  free(lists);
}

// Run worker(&args[i]) on n threads, where each argument struct is `size`
// bytes. If a thread can't be created, that work runs on the calling thread
// instead, so the result is still correct.
static int run_threads(void* (*worker)(void*), void* args, size_t size,
                       size_t n) {
  pthread_t* threads = malloc(n * sizeof(pthread_t));
  int* created = malloc(n * sizeof(int));
  if (threads == NULL || created == NULL) {
    free(threads);
    free(created);
    return -1;
  }

  for (size_t i = 0; i < n; i++) {
    void* arg = (char*)args + i * size;
    created[i] = pthread_create(&threads[i], NULL, worker, arg) == 0;
    if (!created[i]) {
      worker(arg);
    }
  }

  for (size_t i = 0; i < n; i++) {
    if (created[i]) {
      pthread_join(threads[i], NULL);
    }
  }

  free(threads);
  free(created);
  return 0;
}

// Split phase: distribute copies of the input pairs round-robin. Copies are
// needed because mappers may modify their pairs (e.g. strtok_r on the value).
static kvlist_t** split_input(kvlist_t* input, size_t num_mapper) {
  kvlist_t** partitions = new_lists(num_mapper);
  if (partitions == NULL) {
    return NULL;
  }

  kvlist_iterator_t* iterator = kvlist_iterator_new(input);
  kvpair_t* pair;
  size_t i = 0;
  while ((pair = kvlist_iterator_next(iterator)) != NULL) {
    kvlist_append(partitions[i % num_mapper], kvpair_clone(pair));
    i++;
  }
  kvlist_iterator_free(&iterator);

  return partitions;
}

static void* mapper_worker(void* arg) {
  mapper_args_t* args = (mapper_args_t*)arg;

  // call mapper on every pair
  kvlist_iterator_t* iterator = kvlist_iterator_new(args->input);
  kvpair_t* pair;
  while ((pair = kvlist_iterator_next(iterator)) != NULL) {
    args->mapper(pair, args->output);
  }
  kvlist_iterator_free(&iterator);

  return NULL;
}

// Shuffle phase: hash each intermediate key to pick its reducer partition, so
// all pairs with the same key end up in the same partition.
static kvlist_t** shuffle(mapper_args_t* args, size_t num_mapper,
                          size_t num_reducer) {
  kvlist_t** partitions = new_lists(num_reducer);
  if (partitions == NULL) {
    return NULL;
  }

  for (size_t i = 0; i < num_mapper; i++) {
    kvlist_iterator_t* iterator = kvlist_iterator_new(args[i].output);
    kvpair_t* pair;
    while ((pair = kvlist_iterator_next(iterator)) != NULL) {
      size_t index = hash(pair->key) % num_reducer;
      kvlist_append(partitions[index], kvpair_clone(pair));
    }
    kvlist_iterator_free(&iterator);
  }

  return partitions;
}

static void* reducer_worker(void* arg) {
  reducer_args_t* args = (reducer_args_t*)arg;

  // Sort here rather than in shuffle so partitions are sorted in parallel.
  kvlist_sort(args->input);

  kvlist_iterator_t* iterator = kvlist_iterator_new(args->input);
  kvlist_t* group = kvlist_new();
  char* current_key = NULL;
  kvpair_t* pair;

  while ((pair = kvlist_iterator_next(iterator)) != NULL) {
    // A different key means the current group is complete
    if (current_key != NULL && strcmp(current_key, pair->key) != 0) {
      args->reducer(current_key, group, args->output);
      kvlist_free(&group);
      group = kvlist_new();
    }

    kvlist_append(group, kvpair_clone(pair));
    current_key = pair->key;
  }

  // Process the final group
  if (current_key != NULL) {
    args->reducer(current_key, group, args->output);
  }

  kvlist_free(&group);
  kvlist_iterator_free(&iterator);

  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  if (num_mapper == 0 || num_reducer == 0) {
    return;
  }

  // split phase
  kvlist_t** partitions = split_input(input, num_mapper);
  if (partitions == NULL) {
    return;
  }

  // map phase
  mapper_args_t* mapper_args = malloc(num_mapper * sizeof(mapper_args_t));
  kvlist_t** mapper_outputs = new_lists(num_mapper);
  if (mapper_args == NULL || mapper_outputs == NULL) {
    free(mapper_args);
    if (mapper_outputs != NULL) {
      free_lists(mapper_outputs, num_mapper);
    }
    free_lists(partitions, num_mapper);
    return;
  }
  for (size_t i = 0; i < num_mapper; i++) {
    mapper_args[i].mapper = mapper;
    mapper_args[i].input = partitions[i];
    mapper_args[i].output = mapper_outputs[i];
  }
  int map_status = run_threads(mapper_worker, mapper_args,
                               sizeof(mapper_args_t), num_mapper);
  free_lists(partitions, num_mapper);

  // shuffle phase
  kvlist_t** reducer_inputs = NULL;
  if (map_status == 0) {
    reducer_inputs = shuffle(mapper_args, num_mapper, num_reducer);
  }
  free_lists(mapper_outputs, num_mapper);
  free(mapper_args);
  if (reducer_inputs == NULL) {
    return;
  }

  // reduce phase
  reducer_args_t* reducer_args = malloc(num_reducer * sizeof(reducer_args_t));
  if (reducer_args == NULL) {
    free_lists(reducer_inputs, num_reducer);
    return;
  }
  for (size_t i = 0; i < num_reducer; i++) {
    reducer_args[i].reducer = reducer;
    reducer_args[i].input = reducer_inputs[i];
    reducer_args[i].output = kvlist_new();
  }
  int reduce_status = run_threads(reducer_worker, reducer_args,
                                  sizeof(reducer_args_t), num_reducer);

  // output phase: move reducer results into output
  for (size_t i = 0; i < num_reducer; i++) {
    if (reduce_status == 0) {
      kvlist_extend(output, reducer_args[i].output);
    }
    kvlist_free(&reducer_args[i].output);
  }

  free_lists(reducer_inputs, num_reducer);
  free(reducer_args);
}
