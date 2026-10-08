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

#pragma once

#ifndef IRESEARCH_FFI_H
#define IRESEARCH_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define IRS_FFI_EXPORT __attribute__((visibility("default")))

typedef struct irs_ffi_index irs_ffi_index;
typedef struct irs_ffi_reader irs_ffi_reader;

typedef struct {
  int64_t row;
  float score;
} irs_ffi_hit;

typedef enum {
  IRS_FFI_MATCH_ALL = 1,
  IRS_FFI_MATCH_ANY = 2,
  IRS_FFI_PHRASE = 3,
  IRS_FFI_PREFIX = 4,
  IRS_FFI_WILDCARD = 5,
} irs_ffi_search_type;

typedef int (*irs_ffi_write_fn)(void* ctx, const void* data, size_t len);

IRS_FFI_EXPORT const char* irs_ffi_error(void);

IRS_FFI_EXPORT int irs_ffi_init(void);

IRS_FFI_EXPORT void irs_ffi_shutdown(void);

IRS_FFI_EXPORT irs_ffi_index* irs_ffi_index_create(const char* path,
                                                   size_t path_len);

IRS_FFI_EXPORT irs_ffi_index* irs_ffi_index_create_memory(void);

IRS_FFI_EXPORT int irs_ffi_index_add_row(irs_ffi_index* index, const char* text,
                                         size_t text_len, int64_t row);

IRS_FFI_EXPORT int irs_ffi_index_add(irs_ffi_index* index, const char* text,
                                     size_t text_len);

IRS_FFI_EXPORT int irs_ffi_index_commit(irs_ffi_index* index);

IRS_FFI_EXPORT int irs_ffi_index_write(irs_ffi_index* index,
                                       irs_ffi_write_fn write, void* ctx);

IRS_FFI_EXPORT void irs_ffi_index_close(irs_ffi_index* index);

IRS_FFI_EXPORT irs_ffi_reader* irs_ffi_reader_open(const char* path,
                                                   size_t path_len);

IRS_FFI_EXPORT irs_ffi_reader* irs_ffi_reader_open_bytes(const void* data,
                                                         size_t len);

IRS_FFI_EXPORT int64_t irs_ffi_reader_docs(const irs_ffi_reader* reader);

IRS_FFI_EXPORT int64_t irs_ffi_search(irs_ffi_reader* reader, const char* query,
                                      size_t query_len, irs_ffi_hit* hits,
                                      size_t hits_len);

IRS_FFI_EXPORT int64_t irs_ffi_search_typed(irs_ffi_reader* reader,
                                            irs_ffi_search_type type,
                                            const char* query, size_t query_len,
                                            size_t limit, int with_score,
                                            float min_score, irs_ffi_hit* hits,
                                            size_t hits_len);

IRS_FFI_EXPORT int64_t irs_ffi_search_filtered(
  irs_ffi_reader* reader, irs_ffi_search_type type, const char* query,
  size_t query_len, size_t limit, int with_score, float min_score,
  const void* prefilter, size_t prefilter_len, irs_ffi_hit* hits,
  size_t hits_len);

IRS_FFI_EXPORT void irs_ffi_reader_close(irs_ffi_reader* reader);

#ifdef __cplusplus
}
#endif

#endif
