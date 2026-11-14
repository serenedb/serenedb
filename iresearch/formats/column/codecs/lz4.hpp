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
#include <span>
#include <string>
#include <string_view>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/segment_dictionary.hpp"

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

  class Compressor {
   public:
    explicit Compressor(uint8_t level);

    bool Train(std::string_view frames, std::span<const size_t> sizes);
    void Use(std::string_view raw);
    void Clear() noexcept {
      _dictionary.clear();
      _stored.clear();
    }
    std::string_view Dictionary() const noexcept { return _dictionary; }
    std::string_view Stored() const noexcept { return _stored; }

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

   private:
    size_t Plain(const char* src, size_t size, char* dst, size_t capacity);
    bool Pack();
    void Attach();

    std::unique_ptr<LZ4_stream_t> _work;
    std::unique_ptr<LZ4_stream_t> _attached;
    std::string _dictionary;
    std::string _stored;
  };

  class Decompressor {
   public:
    static bool Decompress(const char* src, size_t size, char* dst,
                           size_t raw_size) noexcept;
    static size_t DecompressPrefix(const char* src, size_t size, char* dst,
                                   size_t want, size_t raw_size) noexcept;

    bool Load(std::string_view stored, size_t size);
    bool LoadFrame(const char* src, size_t size, size_t raw_size);
    std::string_view Dictionary() const noexcept {
      return {_window.data(), _dictionary};
    }

    std::string_view Inflate(const char* src, size_t size, char* dst,
                             size_t want, size_t raw_size);

   private:
    std::string _window;
    size_t _dictionary = 0;
  };
};

struct Lz4Hc {
  static constexpr ByteCodec kCodec = ByteCodec::Lz4;
  static constexpr uint8_t kDefaultLevel = LZ4HC_CLEVEL_DEFAULT;
  static constexpr uint8_t kMaxLevel = LZ4HC_CLEVEL_MAX;
  static constexpr size_t kDictionaryCap = Lz4::kDictionaryCap;

  static size_t Bound(size_t raw_size) noexcept { return Lz4::Bound(raw_size); }

  class Compressor {
   public:
    explicit Compressor(uint8_t level);

    bool Train(std::string_view frames, std::span<const size_t> sizes);
    void Use(std::string_view raw);
    void Clear() noexcept {
      _dictionary.clear();
      _stored.clear();
    }
    std::string_view Dictionary() const noexcept { return _dictionary; }
    std::string_view Stored() const noexcept { return _stored; }

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

   private:
    size_t Plain(const char* src, size_t size, char* dst, size_t capacity);
    bool Pack();
    void Attach();

    int _level;
    std::unique_ptr<LZ4_streamHC_t, Lz4HcFree> _work;
    std::unique_ptr<LZ4_streamHC_t> _attached;
    std::string _dictionary;
    std::string _stored;
  };
};

}  // namespace irs::codecs
