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
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/bytes/segment_dictionary.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs::codecs {

struct ZxcFree {
  void operator()(zxc_cctx* ctx) const noexcept { zxc_free_cctx(ctx); }
  void operator()(zxc_dctx* ctx) const noexcept { zxc_free_dctx(ctx); }
};

struct Zxc {
  static constexpr ByteCodec kCodec = ByteCodec::Zxc;
  static constexpr uint8_t kDefaultLevel = 3;
  static constexpr uint8_t kMaxLevel = 7;
  static constexpr size_t kDictionaryCap = kMaxDictionaryBytes / 2;
  static constexpr bool kInflatesPrefix = false;

  static size_t Bound(size_t raw_size) noexcept {
    return zxc_compress_bound(raw_size);
  }

  static size_t ContextBytes(uint8_t level) noexcept;

  class Compressor final : public DictionaryCompressor {
   public:
    Compressor(MemoryBudget* budget, uint8_t level);

    size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

    void Attach();
    static size_t StoredBound(size_t size) noexcept { return Bound(size); }
    size_t CompressStored(const char* src, size_t size, char* dst,
                          size_t capacity) {
      return Plain(src, size, dst, capacity);
    }

   private:
    size_t Plain(const char* src, size_t size, char* dst, size_t capacity);

    uint8_t _level;
    MemoryCharge _ctx_charge;
    std::unique_ptr<zxc_cctx, ZxcFree> _ctx;
    MemoryCharge _attached_charge;
    std::unique_ptr<zxc_cctx, ZxcFree> _attached;
  };

  class Decompressor final : public DictionaryDecompressor {
   public:
    Decompressor();

    bool Decompress(const char* src, size_t size, char* dst,
                    size_t raw_size) noexcept;
    bool DecompressStored(const char* src, size_t size, char* dst,
                          size_t raw_size) noexcept {
      return Decompress(src, size, dst, raw_size);
    }

    std::string_view Inflate(const char* src, size_t size, char* dst, size_t,
                             size_t raw_size);

   private:
    MemoryCharge _ctx_charge;
    std::unique_ptr<zxc_dctx, ZxcFree> _ctx;
    MemoryCharge _blocks_charge;
    std::unique_ptr<zxc_dctx, ZxcFree> _blocks;
  };
};

}  // namespace irs::codecs
