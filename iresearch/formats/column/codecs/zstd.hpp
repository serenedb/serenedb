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
#include "iresearch/formats/column/codecs/segment_dictionary.hpp"

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
  static constexpr size_t kDictionaryCap = 64 * 1024;

  static size_t Bound(size_t raw_size) noexcept {
    return ZSTD_compressBound(raw_size);
  }

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
    std::unique_ptr<ZSTD_CCtx, ZstdFree> _ctx;
    std::unique_ptr<ZSTD_CDict, ZstdFree> _attached;
    std::string _dictionary;
    std::string _stored;
  };

  class Decompressor {
   public:
    Decompressor();

    bool Decompress(const char* src, size_t size, char* dst, size_t raw_size);

    bool Load(std::string_view stored, size_t size);
    bool LoadFrame(const char* src, size_t size, size_t raw_size);
    std::string_view Dictionary() const noexcept {
      return {_window.data(), _dictionary};
    }

    std::string_view Inflate(const char* src, size_t size, char* dst, size_t,
                             size_t raw_size);

   private:
    bool Run(const char* src, size_t size, char* dst, size_t raw_size,
             const ZSTD_DDict* dictionary) noexcept;
    const ZSTD_DDict* Attached();

    std::unique_ptr<ZSTD_DCtx, ZstdFree> _ctx;
    std::unique_ptr<ZSTD_DDict, ZstdFree> _attached;
    std::string_view _attached_bytes;
    std::string _window;
    size_t _dictionary = 0;
  };
};

}  // namespace irs::codecs
