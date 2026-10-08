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

typedef struct {
  unsigned char* data;
  size_t len;
} buffer;

static int sink(void* ctx, const void* data, size_t len) {
  buffer* b = ctx;
  if (len > SIZE_MAX - b->len) {
    return 1;
  }
  unsigned char* next = realloc(b->data, b->len + len);
  if (!next && len) {
    return 1;
  }
  b->data = next;
  if (len) {
    memcpy(b->data + b->len, data, len);
  }
  b->len += len;
  return 0;
}

static int reject(void* ctx, const void* data, size_t len) {
  (void)ctx;
  (void)data;
  (void)len;
  return 1;
}

static const int64_t rows[] = {1000, 7, INT64_C(4294967316),
                               42,   5, INT64_C(8589934632)};
static const char* docs[] = {"brown fox",  "lazy dog",
                             "brown dog",  "quick fox",
                             "lazy brown", "brown brown brown and or"};

static void populate(irs_ffi_index* index) {
  CHECK(index);
  for (size_t i = 0; i < 6; ++i) {
    CHECK(irs_ffi_index_add_row(index, docs[i], strlen(docs[i]), rows[i]) == 0);
    if (i % 2 == 1) {
      CHECK(irs_ffi_index_commit(index) == 0);
    }
  }
  CHECK(irs_ffi_index_commit(index) == 0);
}

static void fill(irs_ffi_hit* hits, size_t n) {
  for (size_t i = 0; i < n; ++i) {
    hits[i].row = INT64_MIN;
    hits[i].score = -123.5f;
  }
}

static void untouched(const irs_ffi_hit* hits, size_t first, size_t n) {
  for (size_t i = first; i < n; ++i) {
    CHECK(hits[i].row == INT64_MIN);
    CHECK(hits[i].score == -123.5f);
  }
}

static void query(irs_ffi_reader* reader, irs_ffi_search_type type,
                  const char* text, const int64_t* expected, size_t n) {
  for (int scored = 0; scored <= 1; ++scored) {
    irs_ffi_hit hits[8];
    fill(hits, 8);
    int64_t total = irs_ffi_search_typed(reader, type, text, strlen(text), 0,
                                         scored, NAN, hits, 8);
    if (total != (int64_t)n) {
      fprintf(stderr, "type=%d query=%s scored=%d expected=%zu total=%lld\n",
              (int)type, text, scored, n, (long long)total);
    }
    CHECK(total == (int64_t)n);
    for (size_t i = 0; i < n; ++i) {
      size_t count = 0;
      for (size_t j = 0; j < n; ++j) {
        count += hits[j].row == expected[i];
      }
      CHECK(count == 1);
      CHECK(isfinite(hits[i].score));
      if (!scored) {
        CHECK(hits[i].score == 0.f);
      }
    }
    untouched(hits, n, 8);
    CHECK(irs_ffi_search_typed(reader, type, text, strlen(text), 0, scored, NAN,
                               NULL, 0) == (int64_t)n);
  }
}

static void verify(irs_ffi_reader* reader) {
  CHECK(reader);
  CHECK(irs_ffi_reader_docs(reader) == 6);
  const int64_t brown[] = {1000, INT64_C(4294967316), 5, INT64_C(8589934632)};
  const int64_t fox[] = {1000, 42};
  const int64_t dog[] = {7, INT64_C(4294967316)};
  const int64_t literal[] = {INT64_C(8589934632)};
  query(reader, IRS_FFI_MATCH_ANY, "brown", brown, 4);
  query(reader, IRS_FFI_MATCH_ALL, "brown dog", dog + 1, 1);
  query(reader, IRS_FFI_PHRASE, "brown fox", fox, 1);
  query(reader, IRS_FFI_PHRASE, "fox brown", NULL, 0);
  query(reader, IRS_FFI_PREFIX, "BRO", brown, 4);
  query(reader, IRS_FFI_WILDCARD, "b*wn", brown, 4);
  query(reader, IRS_FFI_WILDCARD, "d?g", dog, 2);
  query(reader, IRS_FFI_MATCH_ALL, "brown AND OR", literal, 1);
  query(reader, IRS_FFI_MATCH_ANY, "AND OR", literal, 1);
  query(reader, IRS_FFI_PHRASE, "AND OR", literal, 1);
  query(reader, IRS_FFI_MATCH_ALL, "brown missing", NULL, 0);

  irs_ffi_hit baseline[8];
  fill(baseline, 8);
  CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "brown", 5, 0, 1, NAN,
                             baseline, 8) == 4);
  float thresholds[] = {-INFINITY, baseline[0].score, baseline[3].score,
                        FLT_MAX, NAN};
  for (size_t t = 0; t < sizeof(thresholds) / sizeof(*thresholds); ++t) {
    size_t eligible = 0;
    for (size_t i = 0; i < 4; ++i) {
      eligible += isnan(thresholds[t]) || baseline[i].score > thresholds[t];
    }
    for (size_t limit = 0; limit <= 2; ++limit) {
      for (size_t cap = 0; cap <= 2; ++cap) {
        irs_ffi_hit hits[8];
        fill(hits, 8);
        CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "brown", 5, limit,
                                   1, thresholds[t], hits,
                                   cap) == (int64_t)eligible);
        size_t written = eligible < cap ? eligible : cap;
        if (limit && written > limit) {
          written = limit;
        }
        size_t source = 0;
        for (size_t i = 0; i < written; ++i) {
          while (!isnan(thresholds[t]) &&
                 baseline[source].score <= thresholds[t]) {
            ++source;
          }
          CHECK(hits[i].score == baseline[source++].score);
          size_t matches = 0;
          for (size_t j = 0; j < 4; ++j) {
            matches += hits[i].row == baseline[j].row &&
                       hits[i].score == baseline[j].score;
          }
          CHECK(matches == 1);
          for (size_t j = 0; j < i; ++j) {
            CHECK(hits[i].row != hits[j].row);
          }
        }
        untouched(hits, written, 8);
        fill(hits, 8);
        CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "brown", 5, limit,
                                   0, thresholds[t], hits,
                                   cap) == (int64_t)eligible);
        written = eligible < cap ? eligible : cap;
        if (limit && written > limit) {
          written = limit;
        }
        for (size_t i = 0; i < written; ++i) {
          CHECK(hits[i].score == 0.f);
          size_t matches = 0;
          for (size_t j = 0; j < 4; ++j) {
            matches +=
              hits[i].row == baseline[j].row &&
              (isnan(thresholds[t]) || baseline[j].score > thresholds[t]);
          }
          CHECK(matches == 1);
          for (size_t j = 0; j < i; ++j) {
            CHECK(hits[i].row != hits[j].row);
          }
        }
        untouched(hits, written, 8);
      }
    }
  }

  irs_ffi_hit hits[8];
  fill(hits, 8);
  CHECK(irs_ffi_search(reader, "brown AND dog", 13, hits, 8) == 1);
  CHECK(hits[0].row == rows[2]);
  untouched(hits, 1, 8);
  CHECK(irs_ffi_search(reader, "brown OR fox", 12, NULL, 0) == 5);
  CHECK(irs_ffi_search(reader, NULL, 1, hits, 8) == -1);
  CHECK(irs_ffi_search(reader, "x", (size_t)UINT32_MAX + 1, hits, 8) == -1);
  CHECK(irs_ffi_search(reader, "brown", 5, NULL, 1) == -1);
  CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, NULL, 1, 0, 0, NAN,
                             hits, 8) == -1);
  CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "x",
                             (size_t)UINT32_MAX + 1, 0, 0, NAN, hits, 8) == -1);
  CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "brown", 5, 0, 0, NAN,
                             NULL, 1) == -1);
  CHECK(irs_ffi_search_filtered(reader, IRS_FFI_MATCH_ANY, "brown", 5, 0, 0,
                                NAN, NULL, 1, hits, 8) == -1);
  CHECK(irs_ffi_search_filtered(reader, IRS_FFI_MATCH_ANY, "brown", 5, 0, 0,
                                NAN, "x", 0, hits, 8) == -1);
  CHECK(irs_ffi_search_typed(reader, (irs_ffi_search_type)99, "brown", 5, 0, 1,
                             NAN, hits, 8) == -1);
}

static uint64_t get(const unsigned char* p, size_t n) {
  uint64_t value = 0;
  for (size_t i = 0; i < n; ++i) {
    value |= (uint64_t)p[i] << (8 * i);
  }
  return value;
}

static void put(unsigned char* p, uint64_t value, size_t n) {
  for (size_t i = 0; i < n; ++i) {
    p[i] = (unsigned char)(value >> (8 * i));
  }
}

static void invalid(const void* data, size_t len) {
  irs_ffi_reader* reader = irs_ffi_reader_open_bytes(data, len);
  CHECK(reader == NULL);
  CHECK(strlen(irs_ffi_error()) != 0);
}

static void malformed(const buffer* archive) {
  CHECK(archive->len > 16);
  for (size_t len = 0; len < 16; ++len) {
    invalid(archive->data, len);
  }
  invalid(archive->data, archive->len - 1);
  unsigned char* copy = malloc(archive->len + 1);
  CHECK(copy);
  memcpy(copy, archive->data, archive->len);
  copy[archive->len] = 0;
  invalid(copy, archive->len + 1);
  copy[0] ^= 0xff;
  invalid(copy, archive->len);
  memcpy(copy, archive->data, archive->len);
  put(copy + 8, UINT64_MAX, 8);
  invalid(copy, archive->len);
  put(copy + 8, UINT64_C(1) << 61, 8);
  invalid(copy, archive->len);
  memcpy(copy, archive->data, archive->len);
  size_t entry = 16 + (size_t)get(copy + 8, 8) * 8;
  CHECK(entry + 4 < archive->len);
  size_t name_len = (size_t)get(copy + entry, 4);
  CHECK(entry + 4 + name_len + 8 <= archive->len);
  size_t length_pos = entry + 4 + name_len;
  size_t entry_size = 4 + name_len + 8 + (size_t)get(copy + length_pos, 8);
  CHECK(entry_size <= archive->len - entry);
  CHECK(name_len > 0);
  copy[entry + 4] = '/';
  invalid(copy, archive->len);
  memcpy(copy, archive->data, archive->len);
  copy[entry + 4] = '\0';
  invalid(copy, archive->len);
  memcpy(copy, archive->data, archive->len);
  put(copy + length_pos, UINT64_MAX, 8);
  invalid(copy, archive->len);
  memcpy(copy, archive->data, archive->len);
  put(copy + entry, UINT32_MAX, 4);
  invalid(copy, archive->len);
  memcpy(copy, archive->data, archive->len);
  put(copy + 4, UINT32_MAX, 4);
  invalid(copy, archive->len);
  free(copy);
  buffer duplicate = {NULL, 0};
  CHECK(sink(&duplicate, archive->data, archive->len) == 0);
  CHECK(sink(&duplicate, archive->data + entry, entry_size) == 0);
  put(duplicate.data + 4, get(archive->data + 4, 4) + 1, 4);
  invalid(duplicate.data, duplicate.len);
  free(duplicate.data);
}

static void verify_empty(irs_ffi_reader* reader) {
  CHECK(reader);
  CHECK(irs_ffi_reader_docs(reader) == 0);
  for (int scored = 0; scored <= 1; ++scored) {
    irs_ffi_hit hits[8];
    fill(hits, 8);
    CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "window", 6, 0,
                               scored, NAN, hits, 8) == 0);
    untouched(hits, 0, 8);
    CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "window", 6, 0,
                               scored, NAN, NULL, 0) == 0);
  }
  irs_ffi_hit hits[8];
  fill(hits, 8);
  CHECK(irs_ffi_search(reader, "window", 6, hits, 8) == 0);
  untouched(hits, 0, 8);
  CHECK(irs_ffi_search(reader, "window", 6, NULL, 0) == 0);
}

static void empty_roundtrip(void) {
  irs_ffi_index* index = irs_ffi_index_create_memory();
  CHECK(index);
  CHECK(irs_ffi_index_commit(index) == 0);
  buffer archive = {NULL, 0};
  CHECK(irs_ffi_index_write(index, sink, &archive) == 0);
  FILE* file = fopen("empty.archive", "wb");
  CHECK(file);
  CHECK(fwrite(archive.data, 1, archive.len, file) == archive.len);
  CHECK(fclose(file) == 0);
  irs_ffi_reader* reader = irs_ffi_reader_open_bytes(archive.data, archive.len);
  free(archive.data);
  verify_empty(reader);
  irs_ffi_reader_close(reader);
  CHECK(irs_ffi_index_add_row(index, "window", 6, 123) == 0);
  buffer pending = {NULL, 0};
  CHECK(irs_ffi_index_write(index, sink, &pending) == -1);
  CHECK(pending.len == 0);
  free(pending.data);
  CHECK(irs_ffi_index_commit(index) == 0);
  buffer committed = {NULL, 0};
  CHECK(irs_ffi_index_write(index, sink, &committed) == 0);
  irs_ffi_index_close(index);
  reader = irs_ffi_reader_open_bytes(committed.data, committed.len);
  free(committed.data);
  CHECK(reader);
  CHECK(irs_ffi_reader_docs(reader) == 1);
  const int64_t row[] = {123};
  query(reader, IRS_FFI_MATCH_ANY, "window", row, 1);
  irs_ffi_reader_close(reader);
}

static void windows(void) {
  irs_ffi_index* index = irs_ffi_index_create_memory();
  CHECK(index);
  CHECK(irs_ffi_index_add_row(index, "window", 6, -1) == -1);
  for (int64_t i = 0; i < 3000; ++i) {
    CHECK(irs_ffi_index_add_row(index, "window", 6,
                                INT64_C(4294967296) + 17 * i) == 0);
  }
  CHECK(irs_ffi_index_add_row(index, NULL, 0, 99) == 0);
  buffer pending = {NULL, 0};
  CHECK(irs_ffi_index_write(index, sink, &pending) == -1);
  CHECK(pending.len == 0);
  free(pending.data);
  CHECK(irs_ffi_index_commit(index) == 0);
  buffer archive = {NULL, 0};
  CHECK(irs_ffi_index_write(index, sink, &archive) == 0);
  irs_ffi_index_close(index);
  irs_ffi_reader* reader = irs_ffi_reader_open_bytes(archive.data, archive.len);
  free(archive.data);
  CHECK(reader);
  CHECK(irs_ffi_reader_docs(reader) == 3001);
  irs_ffi_hit* hits = malloc(3002 * sizeof(*hits));
  CHECK(hits);
  for (int scored = 0; scored <= 1; ++scored) {
    fill(hits, 3002);
    CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "window", 6, 0,
                               scored, NAN, hits, 3001) == 3000);
    unsigned char seen[3000] = {0};
    for (size_t i = 0; i < 3000; ++i) {
      int64_t delta = hits[i].row - INT64_C(4294967296);
      CHECK(delta >= 0 && delta % 17 == 0 && delta / 17 < 3000);
      CHECK(!seen[delta / 17]);
      seen[delta / 17] = 1;
      CHECK(isfinite(hits[i].score));
      if (!scored) {
        CHECK(hits[i].score == 0.f);
      }
    }
    CHECK(seen[1023] && seen[1024] && seen[2047] && seen[2048] && seen[2999]);
    untouched(hits, 3000, 3002);
    fill(hits, 3002);
    CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "window", 6, 1,
                               scored, NAN, hits, 3001) == 3000);
    untouched(hits, 1, 3002);
    fill(hits, 3002);
    CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "window", 6, 0,
                               scored, NAN, hits, 1) == 3000);
    untouched(hits, 1, 3002);
    CHECK(irs_ffi_search_typed(reader, IRS_FFI_MATCH_ANY, "window", 6, 0,
                               scored, NAN, NULL, 0) == 3000);
  }
  free(hits);
  irs_ffi_reader_close(reader);
}

int main(int argc, char** argv) {
  const char* path = "./regression_index";
  if (argc == 2 && strcmp(argv[1], "--reader-only") == 0) {
    irs_ffi_reader* fs = irs_ffi_reader_open(path, strlen(path));
    verify(fs);
    irs_ffi_reader_close(fs);
    FILE* file = fopen("regression.archive", "rb");
    CHECK(file);
    buffer archive = {NULL, 0};
    unsigned char chunk[4096];
    size_t n;
    while ((n = fread(chunk, 1, sizeof(chunk), file)) != 0) {
      CHECK(sink(&archive, chunk, n) == 0);
    }
    CHECK(!ferror(file));
    CHECK(fclose(file) == 0);
    irs_ffi_reader* reader =
      irs_ffi_reader_open_bytes(archive.data, archive.len);
    free(archive.data);
    verify(reader);
    irs_ffi_reader_close(reader);
    file = fopen("empty.archive", "rb");
    CHECK(file);
    buffer empty = {NULL, 0};
    while ((n = fread(chunk, 1, sizeof(chunk), file)) != 0) {
      CHECK(sink(&empty, chunk, n) == 0);
    }
    CHECK(!ferror(file));
    CHECK(fclose(file) == 0);
    reader = irs_ffi_reader_open_bytes(empty.data, empty.len);
    free(empty.data);
    verify_empty(reader);
    irs_ffi_reader_close(reader);
    return 0;
  }
  CHECK(argc == 1);
  CHECK(irs_ffi_index_create(NULL, 1) == NULL);
  CHECK(irs_ffi_reader_open(NULL, 1) == NULL);
  CHECK(irs_ffi_reader_open_bytes(NULL, 1) == NULL);
  CHECK(irs_ffi_reader_docs(NULL) == -1);
  CHECK(irs_ffi_index_commit(NULL) == -1);
  CHECK(irs_ffi_index_add(NULL, "x", 1) == -1);
  CHECK(irs_ffi_index_write(NULL, sink, NULL) == -1);
  CHECK(irs_ffi_search(NULL, "x", 1, NULL, 0) == -1);
  CHECK(irs_ffi_search_typed(NULL, IRS_FFI_MATCH_ANY, "x", 1, 0, 0, NAN, NULL,
                             0) == -1);
  irs_ffi_index* memory = irs_ffi_index_create_memory();
  CHECK(memory);
  CHECK(irs_ffi_index_add_row(memory, NULL, 1, 123) == -1);
  CHECK(irs_ffi_index_add_row(memory, "x", 1, -1) == -1);
  CHECK(irs_ffi_index_add(memory, "x", (size_t)UINT32_MAX + 1) == -1);
  CHECK(irs_ffi_index_write(memory, NULL, NULL) == -1);
  populate(memory);
  CHECK(irs_ffi_index_write(memory, reject, NULL) == -1);
  buffer archive = {NULL, 0};
  CHECK(irs_ffi_index_write(memory, sink, &archive) == 0);
  irs_ffi_index_close(memory);
  malformed(&archive);
  FILE* file = fopen("regression.archive", "wb");
  CHECK(file);
  CHECK(fwrite(archive.data, 1, archive.len, file) == archive.len);
  CHECK(fclose(file) == 0);
  irs_ffi_reader* reader = irs_ffi_reader_open_bytes(archive.data, archive.len);
  free(archive.data);
  verify(reader);
  irs_ffi_reader_close(reader);
  irs_ffi_index* fs = irs_ffi_index_create(path, strlen(path));
  populate(fs);
  irs_ffi_index_close(fs);
  reader = irs_ffi_reader_open(path, strlen(path));
  verify(reader);
  irs_ffi_reader_close(reader);
  empty_roundtrip();
  windows();
  puts("archive, mapping, query, validation and score regressions passed");
  return 0;
}
