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

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"

#define LZ4_STATIC_LINKING_ONLY
#define LZ4_HC_STATIC_LINKING_ONLY

#include <lz4.h>
#include <lz4hc.h>

#include <memory>
#include <span>
#include <utility>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

size_t Lz4::Bound(size_t raw_size) noexcept {
  SDB_ASSERT(raw_size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  return static_cast<size_t>(LZ4_compressBound(static_cast<int>(raw_size)));
}

Lz4::Compressor::Compressor(LocalBudget& budget, uint8_t level)
  : DictionaryCompressor{budget, kDictionaryCap},
    _level{EffectiveLevel<Lz4>(level)} {
  if (HighCompression<Lz4>(level)) {
    _high = MakeTrackedArray<LZ4_streamHC_t>(1, budget);
    SDB_ENSURE(LZ4_initStreamHC(_high.get(), sizeof(LZ4_streamHC_t)),
               "lz4: cannot create a compression stream");
    return;
  }
  _fast = MakeTrackedArray<LZ4_stream_t>(1, budget);
  SDB_ENSURE(LZ4_initStream(_fast.get(), sizeof(LZ4_stream_t)),
             "lz4: cannot initialize a compression stream");
}

void Lz4::Compressor::Attach() {
  const auto dictionary = Dictionary();
  SDB_ASSERT(dictionary.size() <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  if (_high) {
    if (!_high_attached) {
      _high_attached = MakeTrackedArray<LZ4_streamHC_t>(1, Budget());
    }
    LZ4_setCompressionLevel(_high_attached.get(), _level);
    LZ4_favorDecompressionSpeed(_high_attached.get(), 0);
    _high_attached[0].internal_donotuse.hasWorkspace = 0;
    LZ4_loadDictHC(_high_attached.get(), dictionary.data(),
                   static_cast<int>(dictionary.size()));
    return;
  }
  if (!_fast_attached) {
    _fast_attached = MakeTrackedArray<LZ4_stream_t>(1, Budget());
  }
  LZ4_loadDict(_fast_attached.get(), dictionary.data(),
               static_cast<int>(dictionary.size()));
}

size_t Lz4::Compressor::Compress(const char* src, size_t size, char* dst,
                                 size_t capacity) {
  if (Dictionary().empty()) {
    return Plain(src, size, dst, capacity);
  }
  SDB_ASSERT(size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  SDB_ASSERT(std::in_range<int>(capacity));
  int n = 0;
  if (_high) {
    LZ4_resetStreamHC_fast(_high.get(), _level);
    LZ4_attach_HC_dictionary(_high.get(), _high_attached.get());
    n = LZ4_compress_HC_continue(_high.get(), src, dst, static_cast<int>(size),
                                 static_cast<int>(capacity));
  } else {
    LZ4_resetStream_fast(_fast.get());
    LZ4_attach_dictionary(_fast.get(), _fast_attached.get());
    n =
      LZ4_compress_fast_continue(_fast.get(), src, dst, static_cast<int>(size),
                                 static_cast<int>(capacity), 1);
  }
  SDB_ENSURE(n > 0, "lz4 compression failed");
  return static_cast<size_t>(n);
}

size_t Lz4::Compressor::Plain(const char* src, size_t size, char* dst,
                              size_t capacity) {
  SDB_ASSERT(size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  SDB_ASSERT(std::in_range<int>(capacity));
  const int n = _high ? LZ4_compress_HC_extStateHC_fastReset(
                          _high.get(), src, dst, static_cast<int>(size),
                          static_cast<int>(capacity), _level)
                      : LZ4_compress_fast_extState_fastReset(
                          _fast.get(), src, dst, static_cast<int>(size),
                          static_cast<int>(capacity), 1);
  SDB_ENSURE(n > 0, "lz4 compression failed");
  return static_cast<size_t>(n);
}

bool Lz4::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                   size_t raw_size) noexcept {
  if (!std::in_range<int>(size) || !std::in_range<int>(raw_size)) {
    return false;
  }
  const int n = LZ4_decompress_safe(src, dst, static_cast<int>(size),
                                    static_cast<int>(raw_size));
  return n == static_cast<int>(raw_size);
}

size_t Lz4::Decompressor::DecompressPrefix(const char* src, size_t size,
                                           char* dst, size_t want,
                                           size_t raw_size) noexcept {
  SDB_ASSERT(want <= raw_size);
  if (!std::in_range<int>(size) || !std::in_range<int>(raw_size)) {
    return 0;
  }
  const int n = LZ4_decompress_safe_partial(src, dst, static_cast<int>(size),
                                            static_cast<int>(want),
                                            static_cast<int>(raw_size));
  return n >= static_cast<int>(want) ? static_cast<size_t>(n) : 0;
}

std::string_view Lz4::Decompressor::Inflate(const char* src, size_t size,
                                            char* dst, size_t want,
                                            size_t raw_size) {
  SDB_ASSERT(want <= raw_size);
  if (!HasDictionary()) {
    if (want == raw_size) {
      return Decompress(src, size, dst, raw_size)
               ? std::string_view{dst, raw_size}
               : std::string_view{};
    }
    return {dst, DecompressPrefix(src, size, dst, want, raw_size)};
  }
  if (!std::in_range<int>(size) || !std::in_range<int>(raw_size)) {
    return {};
  }
  auto* out = FrameAfterDictionary(raw_size);
  const auto dictionary = Dictionary();
  const int n =
    want == raw_size
      ? LZ4_decompress_safe_usingDict(
          src, out, static_cast<int>(size), static_cast<int>(raw_size),
          dictionary.data(), static_cast<int>(dictionary.size()))
      : LZ4_decompress_safe_partial_usingDict(
          src, out, static_cast<int>(size), static_cast<int>(want),
          static_cast<int>(raw_size), dictionary.data(),
          static_cast<int>(dictionary.size()));
  return n >= static_cast<int>(want)
           ? std::string_view{out, static_cast<size_t>(n)}
           : std::string_view{};
}

}  // namespace irs::codecs
