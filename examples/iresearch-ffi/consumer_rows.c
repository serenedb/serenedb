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

#include <math.h>
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

static char buf[1 << 20];
static size_t used;

static int sink(void* ctx, const void* data, size_t len) {
  (void)ctx;
  if (len > sizeof(buf) - used) {
    return 1;
  }
  memcpy(buf + used, data, len);
  used += len;
  return 0;
}

int main(void) {
  irs_ffi_index* ix = irs_ffi_index_create_memory();
  CHECK(ix);
  const int64_t rows[] = {1000, 7, 999999, 42, 5};
  const char* docs[] = {"brown fox", "lazy dog", "brown dog", "quick fox",
                        "lazy brown"};
  for (size_t i = 0; i < 5; ++i) {
    CHECK(irs_ffi_index_add_row(ix, docs[i], strlen(docs[i]), rows[i]) == 0);
    if (i == 1 || i == 3) {
      CHECK(irs_ffi_index_commit(ix) == 0);
    }
  }
  CHECK(irs_ffi_index_commit(ix) == 0);
  CHECK(irs_ffi_index_write(ix, sink, NULL) == 0);
  irs_ffi_index_close(ix);
  irs_ffi_reader* rd = irs_ffi_reader_open_bytes(buf, used);
  CHECK(rd);
  CHECK(irs_ffi_reader_docs(rd) == 5);
  const char* queries[] = {"brown", "lazy", "fox"};
  const size_t totals[] = {3, 2, 2};
  const int64_t expected[][3] = {{1000, 999999, 5}, {7, 5}, {1000, 42}};
  for (size_t q = 0; q < 3; ++q) {
    for (int scored = 0; scored <= 1; ++scored) {
      irs_ffi_hit hits[8];
      for (size_t i = 0; i < 8; ++i) {
        hits[i].row = -1;
        hits[i].score = -1.f;
      }
      CHECK(irs_ffi_search_typed(rd, IRS_FFI_MATCH_ANY, queries[q],
                                 strlen(queries[q]), 0, scored, NAN, hits,
                                 8) == (int64_t)totals[q]);
      for (size_t i = 0; i < totals[q]; ++i) {
        size_t count = 0;
        for (size_t j = 0; j < totals[q]; ++j) {
          count += hits[j].row == expected[q][i];
        }
        CHECK(count == 1);
        if (!scored) {
          CHECK(hits[i].score == 0.f);
        }
      }
      for (size_t i = totals[q]; i < 8; ++i) {
        CHECK(hits[i].row == -1);
        CHECK(hits[i].score == -1.f);
      }
    }
  }
  irs_ffi_reader_close(rd);
  puts("sparse row mapping across commits passed");
  return 0;
}
