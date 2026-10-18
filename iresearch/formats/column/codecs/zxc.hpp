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

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "iresearch/formats/column/codecs/byte_codec.hpp"

struct zxc_cctx_s;
struct zxc_dctx_s;

namespace irs::codecs {

template<>
struct Leaf<ByteCodec::Zxc> {
  static constexpr uint8_t kDefaultLevel = 3;
  static constexpr uint8_t kMaxLevel = 7;

  static size_t Bound(size_t raw_size) noexcept;
};

template<>
class LeafCompressor<ByteCodec::Zxc> {
 public:
  LeafCompressor();
  ~LeafCompressor();

  LeafCompressor(const LeafCompressor&) = delete;
  LeafCompressor& operator=(const LeafCompressor&) = delete;

  void SetLevel(uint8_t level) noexcept {
    _level = EffectiveLevel<ByteCodec::Zxc>(level);
    _dictionary = {};
  }

  void LoadDictionary(std::string_view dictionary);

  size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

 private:
  uint8_t _level = Leaf<ByteCodec::Zxc>::kDefaultLevel;
  zxc_cctx_s* _ctx;
  zxc_cctx_s* _block_ctx = nullptr;
  std::string_view _dictionary;
};

template<>
class LeafDecompressor<ByteCodec::Zxc> {
 public:
  LeafDecompressor();
  ~LeafDecompressor();

  LeafDecompressor(const LeafDecompressor&) = delete;
  LeafDecompressor& operator=(const LeafDecompressor&) = delete;

  void SetDictionary(std::string_view dictionary) noexcept;

  bool Decompress(const char* src, size_t size, char* dst,
                  size_t raw_size) noexcept;
  size_t DecompressPrefix(const char* src, size_t size, char* dst, size_t want,
                          size_t raw_size) noexcept;

 private:
  zxc_dctx_s* _ctx;
  zxc_dctx_s* _block_ctx = nullptr;
  std::string_view _dictionary;
};

}  // namespace irs::codecs
