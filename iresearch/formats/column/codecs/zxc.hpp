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

#include <zxc_buffer.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>

#include "iresearch/formats/column/codecs/byte_codec.hpp"

namespace irs::codecs {

struct ZxcFree {
  void operator()(zxc_cctx* ctx) const noexcept { zxc_free_cctx(ctx); }
  void operator()(zxc_dctx* ctx) const noexcept { zxc_free_dctx(ctx); }
};

struct Zxc {
  static constexpr ByteCodec kCodec = ByteCodec::Zxc;
  static constexpr uint8_t kDefaultLevel = 3;
  static constexpr uint8_t kMaxLevel = 7;
  static constexpr uint32_t kFirstWindowed =
    std::numeric_limits<uint32_t>::max();

  static size_t Bound(size_t raw_size) noexcept;

  class Compressor {
   public:
    Compressor();

    void SetLevel(uint8_t level) noexcept {
      _level = EffectiveLevel<Zxc>(level);
      _dictionary = {};
    }

    void LoadDictionary(std::string_view dictionary);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

   private:
    uint8_t _level = kDefaultLevel;
    std::unique_ptr<zxc_cctx, ZxcFree> _ctx;
    std::unique_ptr<zxc_cctx, ZxcFree> _block_ctx;
    std::string_view _dictionary;
  };

  class Decompressor {
   public:
    Decompressor();

    void SetDictionary(std::string_view dictionary) noexcept;

    bool Decompress(const char* src, size_t size, char* dst,
                    size_t raw_size) noexcept;
    size_t DecompressPrefix(const char* src, size_t size, char* dst,
                            size_t want, size_t raw_size) noexcept;

   private:
    std::unique_ptr<zxc_dctx, ZxcFree> _ctx;
    std::unique_ptr<zxc_dctx, ZxcFree> _block_ctx;
    std::string_view _dictionary;
  };
};

}  // namespace irs::codecs
