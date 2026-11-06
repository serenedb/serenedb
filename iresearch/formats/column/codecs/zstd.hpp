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

#include "iresearch/formats/column/codecs/byte_codec.hpp"

namespace irs::codecs {

struct ZstdFree {
  void operator()(ZSTD_CCtx* ctx) const noexcept { ZSTD_freeCCtx(ctx); }
  void operator()(ZSTD_DCtx* ctx) const noexcept { ZSTD_freeDCtx(ctx); }
  void operator()(ZSTD_CDict* dict) const noexcept { ZSTD_freeCDict(dict); }
  void operator()(ZSTD_DDict* dict) const noexcept { ZSTD_freeDDict(dict); }
};

struct Zstd {
  static constexpr ByteCodec kCodec = ByteCodec::Zstd;
  static constexpr uint8_t kDefaultLevel = 1;
  static constexpr uint8_t kMaxLevel = 22;
  static constexpr uint32_t kFirstWindowed = 1;

  static size_t Bound(size_t raw_size) noexcept {
    return ZSTD_compressBound(raw_size);
  }

  class Dictionary {
   public:
    Dictionary(uint8_t level, std::string_view bytes);

    const ZSTD_CDict* Get() const noexcept { return _dictionary.get(); }

   private:
    std::unique_ptr<ZSTD_CDict, ZstdFree> _dictionary;
  };

  class Compressor {
   public:
    explicit Compressor(uint8_t level);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity,
                    const Dictionary* dictionary = nullptr);

   private:
    int _level;
    std::unique_ptr<ZSTD_CCtx, ZstdFree> _ctx;
  };

  class Decompressor {
   public:
    Decompressor();

    bool Decompress(const char* src, size_t size, char* dst, size_t raw_size,
                    std::string_view dictionary = {});

    size_t DecompressPrefix(const char* src, size_t size, char* dst, size_t,
                            size_t raw_size, const auto& dictionary) {
      return Decompress(src, size, dst, raw_size, dictionary) ? raw_size : 0;
    }

   private:
    bool Run(const char* src, size_t size, char* dst, size_t raw_size,
             const ZSTD_DDict* dictionary) noexcept;

    std::unique_ptr<ZSTD_DCtx, ZstdFree> _ctx;
    std::unique_ptr<ZSTD_DDict, ZstdFree> _raw;
    std::string_view _raw_bytes;
  };
};

void TrainDictionary(std::string_view samples, std::span<const size_t> sizes,
                     size_t capacity, std::string& dictionary);

}  // namespace irs::codecs
