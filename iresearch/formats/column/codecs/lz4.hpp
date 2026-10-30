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

#include <lz4.h>
#include <lz4hc.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"

namespace irs::codecs {

struct Lz4Free {
  void operator()(LZ4_stream_t* stream) const noexcept {
    LZ4_freeStream(stream);
  }
  void operator()(LZ4_streamHC_t* stream) const noexcept {
    LZ4_freeStreamHC(stream);
  }
};

struct Lz4 {
  static constexpr ByteCodec kCodec = ByteCodec::Lz4;
  static constexpr uint8_t kDefaultLevel = 1;
  static constexpr uint8_t kMaxLevel = 12;
  static constexpr uint32_t kFirstWindowed = 2;

  static size_t Bound(size_t raw_size) noexcept;

  class Compressor {
   public:
    void SetLevel(uint8_t level) noexcept {
      _level = EffectiveLevel<Lz4>(level);
      _dictionary = false;
    }

    void LoadDictionary(std::string_view dictionary);
    void LoadTrained(const TrainedDictionary& dictionary);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

   private:
    uint8_t _level = kDefaultLevel;
    bool _dictionary = false;
    uint64_t _trained = 0;
    uint8_t _trained_level = 0;
    std::unique_ptr<LZ4_stream_t, Lz4Free> _dict;
    std::unique_ptr<LZ4_stream_t, Lz4Free> _work;
    std::unique_ptr<LZ4_streamHC_t, Lz4Free> _hc_dict;
    std::unique_ptr<LZ4_streamHC_t, Lz4Free> _hc_work;
  };

  class Decompressor {
   public:
    void SetDictionary(std::string_view dictionary) noexcept {
      _dictionary = dictionary;
    }

    bool Decompress(const char* src, size_t size, char* dst,
                    size_t raw_size) noexcept;
    size_t DecompressPrefix(const char* src, size_t size, char* dst,
                            size_t want, size_t raw_size) noexcept;

   private:
    std::string_view _dictionary;
  };
};

}  // namespace irs::codecs
