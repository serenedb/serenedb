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

#include <zstd.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/bytes/segment_dictionary.hpp"

namespace irs::codecs {

struct ZstdFree {
  void operator()(ZSTD_CCtx* ctx) const noexcept { ZSTD_freeCCtx(ctx); }
  void operator()(ZSTD_DCtx* ctx) const noexcept { ZSTD_freeDCtx(ctx); }
  void operator()(ZSTD_CDict* dict) const noexcept { ZSTD_freeCDict(dict); }
  void operator()(ZSTD_DDict* dict) const noexcept { ZSTD_freeDDict(dict); }
};

struct Zstd {
  static constexpr ByteCodec kCodec = ByteCodec::Zstd;
  static constexpr uint8_t kDefaultLevel = 3;
  static constexpr uint8_t kMaxLevel = 22;
  static constexpr size_t kDictionaryCap = kMaxDictionaryBytes;
  static constexpr bool kInflatesPrefix = false;

  static size_t Bound(size_t raw_size) noexcept {
    return ZSTD_compressBound(raw_size);
  }

  static size_t ContextBytes(uint8_t level) noexcept;

  class Compressor final : public DictionaryCompressor {
   public:
    explicit Compressor(TrackedAllocator<> alloc, uint8_t level);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

    void Attach();
    static size_t StoredBound(size_t size) noexcept;
    static size_t CompressStored(const char* src, size_t size, char* dst,
                                 size_t capacity) noexcept;

   private:
    size_t Plain(const char* src, size_t size, char* dst, size_t capacity);

    int _level;
    std::unique_ptr<ZSTD_CCtx, ZstdFree> _ctx;
    std::unique_ptr<ZSTD_CDict, ZstdFree> _attached;
  };

  class Decompressor final : public DictionaryDecompressor {
   public:
    explicit Decompressor(TrackedAllocator<> alloc);

    bool Decompress(const char* src, size_t size, char* dst, size_t raw_size);

    static bool DecompressStored(const char* src, size_t size, char* dst,
                                 size_t raw_size) noexcept;

    std::string_view Inflate(const char* src, size_t size, char* dst, size_t,
                             size_t raw_size);

   private:
    bool Run(const char* src, size_t size, char* dst, size_t raw_size,
             const ZSTD_DDict* dictionary);
    const ZSTD_DDict* Attached();

    std::unique_ptr<ZSTD_DCtx, ZstdFree> _ctx;
    std::unique_ptr<ZSTD_DDict, ZstdFree> _attached;
    std::string_view _attached_bytes;
  };
};

}  // namespace irs::codecs
