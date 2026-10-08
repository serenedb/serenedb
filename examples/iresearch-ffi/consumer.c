////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "iresearch_ffi.h"

#define CHECK(expr)                                                 \
  do {                                                              \
    if (!(expr)) {                                                  \
      fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, #expr, \
              irs_ffi_error());                                     \
      exit(1);                                                      \
    }                                                               \
  } while (0)

static const char* kDocs[] = {
  "the quick brown fox jumps over the lazy dog",
  "a lazy brown dog sleeps all day",
  "full text search over columnar storage",
  "search engines index documents and rank them",
};

int main(void) {
  const char* path = "./demo_index";

  irs_ffi_index* index = irs_ffi_index_create(path, strlen(path));
  if (!index) {
    printf("create failed: %s\n", irs_ffi_error());
    return 1;
  }
  for (size_t i = 0; i < sizeof(kDocs) / sizeof(*kDocs); ++i) {
    if (irs_ffi_index_add(index, kDocs[i], strlen(kDocs[i])) != 0) {
      printf("add failed: %s\n", irs_ffi_error());
      return 1;
    }
  }
  if (irs_ffi_index_commit(index) != 0) {
    printf("commit failed: %s\n", irs_ffi_error());
    return 1;
  }
  irs_ffi_index_close(index);

  irs_ffi_reader* reader = irs_ffi_reader_open(path, strlen(path));
  if (!reader) {
    printf("open failed: %s\n", irs_ffi_error());
    return 1;
  }
  printf("indexed documents: %lld\n", (long long)irs_ffi_reader_docs(reader));
  CHECK(irs_ffi_reader_docs(reader) == 4);

  const char* queries[] = {"search", "lazy", "brown AND dog", "quick*",
                           "nonexistent"};
  const size_t expected[] = {2, 2, 2, 1, 0};
  const int64_t rows[][2] = {{2, 3}, {0, 1}, {0, 1}, {0}, {0}};
  for (size_t q = 0; q < sizeof(queries) / sizeof(*queries); ++q) {
    irs_ffi_hit hits[4];
    for (size_t i = 0; i < 4; ++i) {
      hits[i].row = -1;
      hits[i].score = -1.f;
    }
    long long total =
      irs_ffi_search(reader, queries[q], strlen(queries[q]), hits, 4);
    CHECK(total == (int64_t)expected[q]);
    for (size_t i = 0; i < expected[q]; ++i) {
      size_t count = 0;
      for (size_t j = 0; j < expected[q]; ++j) {
        count += hits[j].row == rows[q][i];
      }
      CHECK(count == 1);
    }
    for (size_t i = expected[q]; i < 4; ++i) {
      CHECK(hits[i].row == -1);
      CHECK(hits[i].score == -1.f);
    }
    printf("  %-14s total=%lld", queries[q], total);
    long long shown = total < 4 ? total : 4;
    for (long long i = 0; i < shown; ++i) {
      printf("  [row=%lld score=%.3f]", (long long)hits[i].row, hits[i].score);
    }
    printf("\n");
  }
  irs_ffi_reader_close(reader);
  irs_ffi_shutdown();
  return 0;
}
