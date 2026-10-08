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
#include <stdint.h>
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

int main(void) {
  const char* path = "./types_index";
  const char* docs[] = {"quick brown fox lazy dog",
                        "lazy brown dog",
                        "search storage",
                        "search engines",
                        "and or",
                        "CAFÉ ÉCOLE",
                        "ПРИВЕТ МИР"};
  irs_ffi_index* ix = irs_ffi_index_create(path, strlen(path));
  CHECK(ix);
  for (size_t i = 0; i < sizeof(docs) / sizeof(*docs); ++i) {
    CHECK(irs_ffi_index_add(ix, docs[i], strlen(docs[i])) == 0);
  }
  CHECK(irs_ffi_index_commit(ix) == 0);
  irs_ffi_index_close(ix);
  irs_ffi_reader* rd = irs_ffi_reader_open(path, strlen(path));
  CHECK(rd);
  struct {
    irs_ffi_search_type type;
    const char* query;
    size_t n;
    int64_t rows[3];
  } cases[] = {
    {IRS_FFI_MATCH_ANY, "lazy storage", 3, {0, 1, 2}},
    {IRS_FFI_MATCH_ALL, "lazy brown", 2, {0, 1}},
    {IRS_FFI_MATCH_ALL, "lazy storage", 0, {0}},
    {IRS_FFI_PHRASE, "brown fox", 1, {0}},
    {IRS_FFI_PHRASE, "fox brown", 0, {0}},
    {IRS_FFI_PREFIX, "SEAR", 2, {2, 3}},
    {IRS_FFI_WILDCARD, "d?g", 2, {0, 1}},
    {IRS_FFI_WILDCARD, "s*ch", 2, {2, 3}},
    {IRS_FFI_MATCH_ALL, "AND OR", 1, {4}},
    {IRS_FFI_MATCH_ANY, "AND OR", 1, {4}},
    {IRS_FFI_MATCH_ALL, "café", 1, {5}},
    {IRS_FFI_PHRASE, "café école", 1, {5}},
    {IRS_FFI_MATCH_ALL, "привет мир", 1, {6}},
    {IRS_FFI_PHRASE, "привет мир", 1, {6}},
    {IRS_FFI_PHRASE, "мир привет", 0, {0}},
  };
  for (size_t c = 0; c < sizeof(cases) / sizeof(*cases); ++c) {
    for (int scored = 0; scored <= 1; ++scored) {
      irs_ffi_hit hits[8];
      for (size_t i = 0; i < 8; ++i) {
        hits[i].row = -1;
        hits[i].score = -1.f;
      }
      CHECK(irs_ffi_search_typed(rd, cases[c].type, cases[c].query,
                                 strlen(cases[c].query), 0, scored, NAN, hits,
                                 8) == (int64_t)cases[c].n);
      for (size_t i = 0; i < cases[c].n; ++i) {
        size_t count = 0;
        for (size_t j = 0; j < cases[c].n; ++j) {
          count += hits[j].row == cases[c].rows[i];
        }
        CHECK(count == 1);
        CHECK(isfinite(hits[i].score));
        if (!scored) {
          CHECK(hits[i].score == 0.f);
        }
      }
      for (size_t i = cases[c].n; i < 8; ++i) {
        CHECK(hits[i].row == -1);
        CHECK(hits[i].score == -1.f);
      }
    }
  }
  irs_ffi_reader_close(rd);
  puts("typed query row sets passed");
  return 0;
}
