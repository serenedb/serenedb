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
#include <string_view>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"

namespace irs::codecs {

struct Zstd {
  static constexpr ByteCodec kCodec = ByteCodec::Zstd;
  static constexpr uint8_t kDefaultLevel = 1;
  static constexpr uint8_t kMaxLevel = 22;
  static constexpr uint32_t kFirstWindowed = 1;

  static size_t Bound(size_t raw_size) noexcept;

  class Compressor {
   public:
    Compressor() : _ctx{ZSTD_createCCtx()} {}

    void SetLevel(uint8_t level) noexcept {
      _level = EffectiveLevel<Zstd>(level);
      _active = nullptr;
    }

    void LoadDictionary(std::string_view dictionary);
    void LoadTrained(const TrainedDictionary& dictionary);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

   private:
    uint8_t _level = kDefaultLevel;
    std::unique_ptr<ZSTD_CCtx, ZstdFree> _ctx;
    std::unique_ptr<ZSTD_CDict, ZstdFree> _cdict;
    std::unique_ptr<ZSTD_CDict, ZstdFree> _trained_cdict;
    const ZSTD_CDict* _active = nullptr;
    uint64_t _trained = 0;
    uint8_t _trained_level = 0;
  };

  class Decompressor {
   public:
    Decompressor() : _ctx{ZSTD_createDCtx()} {}

    void SetDictionary(std::string_view dictionary);
    void SetTrained(const TrainedDictionary& dictionary) noexcept {
      _active = dictionary.ZstdDictionary();
    }

    bool Decompress(const char* src, size_t size, char* dst,
                    size_t raw_size) noexcept;
    size_t DecompressPrefix(const char* src, size_t size, char* dst,
                            size_t want, size_t raw_size) noexcept;

   private:
    std::unique_ptr<ZSTD_DCtx, ZstdFree> _ctx;
    std::unique_ptr<ZSTD_DDict, ZstdFree> _ddict;
    const ZSTD_DDict* _active = nullptr;
    std::string_view _loaded;
  };
};

}  // namespace irs::codecs
