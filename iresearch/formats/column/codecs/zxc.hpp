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

  class Dictionary {
   public:
    Dictionary(uint8_t level, std::string_view bytes);

    zxc_cctx* Context() const noexcept { return _ctx.get(); }

   private:
    std::unique_ptr<zxc_cctx, ZxcFree> _ctx;
  };

  class Compressor {
   public:
    explicit Compressor(uint8_t level);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity,
                    const Dictionary* dictionary = nullptr);

   private:
    std::unique_ptr<zxc_cctx, ZxcFree> _ctx;
  };

  class Decompressor {
   public:
    Decompressor();

    bool Decompress(const char* src, size_t size, char* dst, size_t raw_size,
                    std::string_view dictionary = {}) noexcept;
    size_t DecompressPrefix(const char* src, size_t size, char* dst, size_t,
                            size_t raw_size,
                            std::string_view dictionary = {}) noexcept {
      return Decompress(src, size, dst, raw_size, dictionary) ? raw_size : 0;
    }

   private:
    std::unique_ptr<zxc_dctx, ZxcFree> _ctx;
    std::unique_ptr<zxc_dctx, ZxcFree> _blocks;
  };
};

}  // namespace irs::codecs
