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

struct Lz4 {
  static constexpr ByteCodec kCodec = ByteCodec::Lz4;
  static constexpr uint8_t kDefaultLevel = 1;
  static constexpr uint8_t kMaxLevel = 12;
  static constexpr uint32_t kFirstWindowed = 2;

  static size_t Bound(size_t raw_size) noexcept;

  class Dictionary {
   public:
    Dictionary(uint8_t level, std::string_view bytes);
    Dictionary(uint8_t level, const TrainedDictionary& dictionary)
      : Dictionary{level, dictionary.Bytes()} {}

    const LZ4_stream_t* Stream() const noexcept { return _stream.get(); }

   private:
    std::unique_ptr<LZ4_stream_t> _stream;
  };

  class Compressor {
   public:
    explicit Compressor(uint8_t level);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity,
                    const Dictionary* dictionary = nullptr);

   private:
    std::unique_ptr<LZ4_stream_t> _work;
  };

  class Decompressor {
   public:
    static bool Decompress(const char* src, size_t size, char* dst,
                           size_t raw_size,
                           std::string_view dictionary = {}) noexcept;
    static size_t DecompressPrefix(const char* src, size_t size, char* dst,
                                   size_t want, size_t raw_size,
                                   std::string_view dictionary = {}) noexcept;
  };
};

struct Lz4Hc {
  static constexpr ByteCodec kCodec = ByteCodec::Lz4;
  static constexpr uint8_t kDefaultLevel = LZ4HC_CLEVEL_DEFAULT;
  static constexpr uint8_t kMaxLevel = LZ4HC_CLEVEL_MAX;

  static size_t Bound(size_t raw_size) noexcept { return Lz4::Bound(raw_size); }

  class Dictionary {
   public:
    Dictionary(uint8_t level, std::string_view bytes);
    Dictionary(uint8_t level, const TrainedDictionary& dictionary)
      : Dictionary{level, dictionary.Bytes()} {}

    const LZ4_streamHC_t* Stream() const noexcept { return _stream.get(); }

   private:
    std::unique_ptr<LZ4_streamHC_t> _stream;
  };

  class Compressor {
   public:
    explicit Compressor(uint8_t level);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity,
                    const Dictionary* dictionary = nullptr);

   private:
    int _level;
    std::unique_ptr<LZ4_streamHC_t> _work;
  };

  using Decompressor = Lz4::Decompressor;
};

}  // namespace irs::codecs
