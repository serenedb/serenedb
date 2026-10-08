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

#include <float.h>
#include <math.h>
#include <roaring/roaring.h>
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

static char buf[1 << 20];
static size_t used;

static void put(char* data, uint64_t value, size_t size) {
  for (size_t i = 0; i < size; ++i) {
    data[i] = (char)(value >> (8 * i));
  }
}

static int sink(void* ctx, const void* data, size_t len) {
  (void)ctx;
  if (len > sizeof(buf) - used) {
    return 1;
  }
  memcpy(buf + used, data, len);
  used += len;
  return 0;
}

static void reset(irs_ffi_hit* hits) {
  for (size_t i = 0; i < 8; ++i) {
    hits[i].row = INT64_MIN;
    hits[i].score = -1.f;
  }
}

static void tail(const irs_ffi_hit* hits, size_t first) {
  for (size_t i = first; i < 8; ++i) {
    CHECK(hits[i].row == INT64_MIN);
    CHECK(hits[i].score == -1.f);
  }
}

int main(void) {
  irs_ffi_index* ix = irs_ffi_index_create_memory();
  CHECK(ix);
  const int64_t rows[] = {10, 20, 30, INT64_C(4294967336), 50};
  const char* docs[] = {"brown fox", "brown dog", "brown bear",
                        "lazy brown cat", "quick fox"};
  for (size_t i = 0; i < 5; ++i) {
    CHECK(irs_ffi_index_add_row(ix, docs[i], strlen(docs[i]), rows[i]) == 0);
    CHECK(irs_ffi_index_commit(ix) == 0);
  }
  CHECK(irs_ffi_index_write(ix, sink, NULL) == 0);
  irs_ffi_index_close(ix);
  irs_ffi_reader* rd = irs_ffi_reader_open_bytes(buf, used);
  CHECK(rd);

  roaring_bitmap_t* low = roaring_bitmap_create();
  roaring_bitmap_t* high = roaring_bitmap_create();
  CHECK(low && high);
  roaring_bitmap_add(low, 20);
  roaring_bitmap_add(low, 99);
  roaring_bitmap_add(high, 40);
  size_t low_size = roaring_bitmap_portable_size_in_bytes(low);
  size_t high_size = roaring_bitmap_portable_size_in_bytes(high);
  size_t n = sizeof(uint64_t) + 2 * sizeof(uint32_t) + low_size + high_size;
  char* blob = malloc(n);
  CHECK(blob);
  uint64_t maps = 2;
  uint32_t key = 0;
  size_t offset = 0;
  put(blob + offset, maps, sizeof(maps));
  offset += sizeof(maps);
  put(blob + offset, key, sizeof(key));
  offset += sizeof(key);
  CHECK(roaring_bitmap_portable_serialize(low, blob + offset) == low_size);
  offset += low_size;
  key = 1;
  put(blob + offset, key, sizeof(key));
  offset += sizeof(key);
  CHECK(roaring_bitmap_portable_serialize(high, blob + offset) == high_size);
  offset += high_size;
  CHECK(offset == n);

  irs_ffi_hit baseline[8];
  CHECK(irs_ffi_search_filtered(rd, IRS_FFI_MATCH_ANY, "brown", 5, 0, 1, NAN,
                                blob, n, baseline, 8) == 2);
  CHECK((baseline[0].row == rows[1] && baseline[1].row == rows[3]) ||
        (baseline[0].row == rows[3] && baseline[1].row == rows[1]));
  CHECK(isfinite(baseline[0].score) && isfinite(baseline[1].score));
  float thresholds[] = {NAN, -INFINITY, baseline[0].score, baseline[1].score,
                        FLT_MAX};
  for (int scored = 0; scored <= 1; ++scored) {
    for (size_t t = 0; t < sizeof(thresholds) / sizeof(*thresholds); ++t) {
      size_t expected = 0;
      for (size_t i = 0; i < 2; ++i) {
        expected += isnan(thresholds[t]) || baseline[i].score > thresholds[t];
      }
      for (size_t limit = 0; limit <= 1; ++limit) {
        for (size_t cap = 0; cap <= 3; ++cap) {
          irs_ffi_hit hits[8];
          reset(hits);
          CHECK(irs_ffi_search_filtered(rd, IRS_FFI_MATCH_ANY, "brown", 5,
                                        limit, scored, thresholds[t], blob, n,
                                        hits, cap) == (int64_t)expected);
          size_t written = expected < cap ? expected : cap;
          if (limit && written > limit) {
            written = limit;
          }
          for (size_t i = 0; i < written; ++i) {
            CHECK(hits[i].row == rows[1] || hits[i].row == rows[3]);
            if (!scored) {
              CHECK(hits[i].score == 0.f);
            } else {
              CHECK(isnan(thresholds[t]) || hits[i].score > thresholds[t]);
            }
          }
          if (written == 2) {
            CHECK(hits[0].row != hits[1].row);
          }
          tail(hits, written);
        }
      }
      CHECK(irs_ffi_search_filtered(rd, IRS_FFI_MATCH_ANY, "brown", 5, 1,
                                    scored, thresholds[t], blob, n, NULL,
                                    0) == (int64_t)expected);
    }
  }
  for (size_t len = 1; len < sizeof(uint64_t); ++len) {
    CHECK(irs_ffi_search_filtered(rd, IRS_FFI_MATCH_ANY, "brown", 5, 0, 0, NAN,
                                  blob, len, NULL, 0) == -1);
  }
  CHECK(irs_ffi_search_filtered(rd, IRS_FFI_MATCH_ANY, "brown", 5, 0, 0, NAN,
                                blob, n - 1, NULL, 0) == -1);
  uint64_t empty = 0;
  irs_ffi_hit hits[8];
  reset(hits);
  CHECK(irs_ffi_search_filtered(rd, IRS_FFI_MATCH_ANY, "brown", 5, 0, 1, NAN,
                                &empty, sizeof(empty), hits, 8) == 0);
  tail(hits, 0);
  free(blob);
  roaring_bitmap_free(low);
  roaring_bitmap_free(high);
  irs_ffi_reader_close(rd);
  puts("native Roaring64Map filtering passed");
  return 0;
}
