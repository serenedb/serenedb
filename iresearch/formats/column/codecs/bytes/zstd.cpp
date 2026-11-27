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

#include "iresearch/formats/column/codecs/bytes/zstd.hpp"

#define ZSTD_STATIC_LINKING_ONLY

#include <zstd.h>

#include <algorithm>
#include <duckdb/common/exception.hpp>
#include <utility>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs::codecs {
namespace {

ZSTD_customMem TrackedMem(TrackedAllocator<> alloc) noexcept {
  return {
    .customAlloc =
      [](void* opaque, size_t size) {
        return TrackedMalloc(
          TrackedAllocator<>{*static_cast<LocalBudget*>(opaque)}, size);
      },
    .customFree =
      [](void* opaque, void* p) {
        TrackedFree(TrackedAllocator<>{*static_cast<LocalBudget*>(opaque)}, p);
      },
    .opaque = &alloc.Budget(),
  };
}

[[noreturn]] void ThrowOutOfMemory(const char* what) {
  throw duckdb::OutOfMemoryException("zstd: cannot allocate %s", what);
}

template<typename T>
T* Created(T* created, const char* what) {
  if (!created) {
    ThrowOutOfMemory(what);
  }
  return created;
}

size_t Checked(size_t n) {
  if (ZSTD_isError(n) && ZSTD_getErrorCode(n) == ZSTD_error_memory_allocation) {
    ThrowOutOfMemory("its working memory");
  }
  return n;
}

}  // namespace

size_t Zstd::ContextBytes(uint8_t level) noexcept {
  const auto params = ZSTD_getCParams(EffectiveLevel<Zstd>(level),
                                      kZstdFrameBytes, kMaxDictionaryBytes);
  return ZSTD_estimateCCtxSize_usingCParams(params) +
         ZSTD_estimateCDictSize_advanced(kMaxDictionaryBytes, params,
                                         ZSTD_dlm_byRef);
}

Zstd::Compressor::Compressor(TrackedAllocator<> alloc, uint8_t level)
  : DictionaryCompressor{alloc, kDictionaryCap},
    _level{EffectiveLevel<Zstd>(level)},
    _ctx{Created(ZSTD_createCCtx_advanced(TrackedMem(alloc)),
                 "a compression context")} {}

size_t Zstd::Compressor::StoredBound(size_t size) noexcept {
  return Lz4::Bound(size);
}

size_t Zstd::Compressor::CompressStored(const char* src, size_t size, char* dst,
                                        size_t capacity) noexcept {
  return static_cast<size_t>(
    std::max(0, LZ4_compress_default(src, dst, static_cast<int>(size),
                                     static_cast<int>(capacity))));
}

void Zstd::Compressor::Attach() {
  const auto dictionary = Dictionary();
  _attached.reset(Created(
    ZSTD_createCDict_advanced(
      dictionary.data(), dictionary.size(), ZSTD_dlm_byRef, ZSTD_dct_rawContent,
      ZSTD_getCParams(_level, kFrameDictionaryBytes, dictionary.size()),
      TrackedMem(Allocator())),
    "a compression dictionary"));
}

size_t Zstd::Compressor::Compress(const char* src, size_t size, char* dst,
                                  size_t capacity) {
  if (Dictionary().empty()) {
    return Plain(src, size, dst, capacity);
  }
  const auto n = Checked(ZSTD_compress_usingCDict(_ctx.get(), dst, capacity,
                                                  src, size, _attached.get()));
  SDB_ENSURE(!ZSTD_isError(n),
             "zstd compression failed: ", ZSTD_getErrorName(n));
  return n;
}

size_t Zstd::Compressor::Plain(const char* src, size_t size, char* dst,
                               size_t capacity) {
  const auto n =
    Checked(ZSTD_compressCCtx(_ctx.get(), dst, capacity, src, size, _level));
  SDB_ENSURE(!ZSTD_isError(n),
             "zstd compression failed: ", ZSTD_getErrorName(n));
  return n;
}

Zstd::Decompressor::Decompressor(TrackedAllocator<> alloc)
  : DictionaryDecompressor{alloc},
    _ctx{Created(ZSTD_createDCtx_advanced(TrackedMem(alloc)),
                 "a decompression context")} {}

bool Zstd::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                    size_t raw_size) {
  return Run(src, size, dst, raw_size, nullptr);
}

bool Zstd::Decompressor::DecompressStored(const char* src, size_t size,
                                          char* dst, size_t raw_size) noexcept {
  return Lz4::Decompressor::Decompress(src, size, dst, raw_size);
}

std::string_view Zstd::Decompressor::Inflate(const char* src, size_t size,
                                             char* dst, size_t,
                                             size_t raw_size) {
  if (!HasDictionary()) {
    return Decompress(src, size, dst, raw_size)
             ? std::string_view{dst, raw_size}
             : std::string_view{};
  }
  auto* out = FrameAfterDictionary(raw_size);
  return Run(src, size, out, raw_size, Attached())
           ? std::string_view{out, raw_size}
           : std::string_view{};
}

const ZSTD_DDict* Zstd::Decompressor::Attached() {
  const auto dictionary = Dictionary();
  if (dictionary.data() != _attached_bytes.data() ||
      dictionary.size() != _attached_bytes.size()) {
    _attached_bytes = {};
    _attached.reset(
      Created(ZSTD_createDDict_advanced(dictionary.data(), dictionary.size(),
                                        ZSTD_dlm_byRef, ZSTD_dct_rawContent,
                                        TrackedMem(Allocator())),
              "a decompression dictionary"));
    _attached_bytes = dictionary;
  }
  return _attached.get();
}

bool Zstd::Decompressor::Run(const char* src, size_t size, char* dst,
                             size_t raw_size, const ZSTD_DDict* dictionary) {
  const auto n = Checked(ZSTD_decompress_usingDDict(_ctx.get(), dst, raw_size,
                                                    src, size, dictionary));
  return !ZSTD_isError(n) && n == raw_size;
}

}  // namespace irs::codecs
