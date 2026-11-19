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
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/bytes/segment_dictionary.hpp"

namespace irs::codecs {

struct Lz4HcFree {
  void operator()(LZ4_streamHC_t* stream) const noexcept {
    LZ4_freeStreamHC(stream);
  }
};

struct Lz4 {
  static constexpr ByteCodec kCodec = ByteCodec::Lz4;
  static constexpr uint8_t kDefaultLevel = 1;
  static constexpr uint8_t kMaxLevel = LZ4HC_CLEVEL_MAX;
  static constexpr size_t kDictionaryCap = 64 * 1024;

  static size_t Bound(size_t raw_size) noexcept;

  class Compressor final : public DictionaryCompressor {
   public:
    explicit Compressor(uint8_t level);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

    void Attach();
    static size_t StoredBound(size_t size) noexcept { return Bound(size); }
    size_t CompressStored(const char* src, size_t size, char* dst,
                          size_t capacity) {
      return Plain(src, size, dst, capacity);
    }

   private:
    size_t Plain(const char* src, size_t size, char* dst, size_t capacity);

    std::unique_ptr<LZ4_stream_t> _work;
    std::unique_ptr<LZ4_stream_t> _attached;
  };

  class Decompressor final : public DictionaryDecompressor {
   public:
    static bool Decompress(const char* src, size_t size, char* dst,
                           size_t raw_size) noexcept;
    static size_t DecompressPrefix(const char* src, size_t size, char* dst,
                                   size_t want, size_t raw_size) noexcept;
    static bool DecompressStored(const char* src, size_t size, char* dst,
                                 size_t raw_size) noexcept {
      return Decompress(src, size, dst, raw_size);
    }

    std::string_view Inflate(const char* src, size_t size, char* dst,
                             size_t want, size_t raw_size);
  };
};

struct Lz4Hc {
  static constexpr ByteCodec kCodec = ByteCodec::Lz4;
  static constexpr uint8_t kDefaultLevel = LZ4HC_CLEVEL_DEFAULT;
  static constexpr uint8_t kMaxLevel = LZ4HC_CLEVEL_MAX;
  static constexpr size_t kDictionaryCap = Lz4::kDictionaryCap;

  static size_t Bound(size_t raw_size) noexcept { return Lz4::Bound(raw_size); }

  static constexpr bool Serves(uint8_t level) noexcept {
    return EffectiveLevel<Lz4>(level) > 1;
  }

  class Compressor final : public DictionaryCompressor {
   public:
    explicit Compressor(uint8_t level);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

    void Attach();
    static size_t StoredBound(size_t size) noexcept { return Bound(size); }
    size_t CompressStored(const char* src, size_t size, char* dst,
                          size_t capacity) {
      return Plain(src, size, dst, capacity);
    }

   private:
    size_t Plain(const char* src, size_t size, char* dst, size_t capacity);

    int _level;
    std::unique_ptr<LZ4_streamHC_t, Lz4HcFree> _work;
    std::unique_ptr<LZ4_streamHC_t> _attached;
  };
};

struct Lz4AnyLevel {
  static constexpr ByteCodec kCodec = ByteCodec::Lz4;

  static size_t Bound(size_t raw_size) noexcept { return Lz4::Bound(raw_size); }

  class Compressor {
   public:
    explicit Compressor(uint8_t level) {
      if (Lz4Hc::Serves(level)) {
        _high.emplace(level);
      } else {
        _fast.emplace(level);
      }
    }

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity) {
      return _high ? _high->Compress(src, size, dst, capacity)
                   : _fast->Compress(src, size, dst, capacity);
    }

   private:
    std::optional<Lz4::Compressor> _fast;
    std::optional<Lz4Hc::Compressor> _high;
  };
};

}  // namespace irs::codecs
