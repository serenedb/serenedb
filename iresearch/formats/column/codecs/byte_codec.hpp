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

#include "iresearch/utils/zstd_context.hpp"

struct zxc_cctx_s;
struct zxc_dctx_s;

namespace irs::codecs {

enum class ByteCodec : uint8_t {
  Lz4 = 0,
  Zstd = 1,
  Fsst = 2,
  Zxc = 3,
};

inline constexpr uint8_t kByteCodecCount = 4;

template<ByteCodec C>
struct Leaf;

template<>
struct Leaf<ByteCodec::Lz4> {
  static constexpr uint8_t kDefaultLevel = 1;
  static constexpr uint8_t kMaxLevel = 12;

  static size_t Bound(size_t raw_size) noexcept;
};

template<>
struct Leaf<ByteCodec::Zstd> {
  static constexpr uint8_t kDefaultLevel = 1;
  static constexpr uint8_t kMaxLevel = 22;

  static size_t Bound(size_t raw_size) noexcept;
};

template<>
struct Leaf<ByteCodec::Zxc> {
  static constexpr uint8_t kDefaultLevel = 3;
  static constexpr uint8_t kMaxLevel = 7;

  static size_t Bound(size_t raw_size) noexcept;
};

template<ByteCodec C>
constexpr uint8_t EffectiveLevel(uint8_t level) noexcept {
  if (level == 0) {
    return Leaf<C>::kDefaultLevel;
  }
  return level < Leaf<C>::kMaxLevel ? level : Leaf<C>::kMaxLevel;
}

template<ByteCodec C>
class LeafCompressor;

template<>
class LeafCompressor<ByteCodec::Lz4> {
 public:
  explicit LeafCompressor(uint8_t level) noexcept
    : _level{EffectiveLevel<ByteCodec::Lz4>(level)} {}

  void SetLevel(uint8_t level) noexcept {
    _level = EffectiveLevel<ByteCodec::Lz4>(level);
  }

  size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

 private:
  uint8_t _level;
};

template<>
class LeafCompressor<ByteCodec::Zstd> {
 public:
  explicit LeafCompressor(uint8_t level)
    : _level{EffectiveLevel<ByteCodec::Zstd>(level)},
      _ctx{utils::MakeZstdCCtx()} {}

  void SetLevel(uint8_t level) noexcept {
    _level = EffectiveLevel<ByteCodec::Zstd>(level);
  }

  size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

 private:
  uint8_t _level;
  utils::ZstdCCtxPtr _ctx;
};

template<>
class LeafCompressor<ByteCodec::Zxc> {
 public:
  explicit LeafCompressor(uint8_t level);
  ~LeafCompressor();

  LeafCompressor(const LeafCompressor&) = delete;
  LeafCompressor& operator=(const LeafCompressor&) = delete;

  void SetLevel(uint8_t level) noexcept {
    _level = EffectiveLevel<ByteCodec::Zxc>(level);
  }

  size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

 private:
  uint8_t _level;
  zxc_cctx_s* _ctx;
};

template<ByteCodec C>
class LeafDecompressor;

template<>
class LeafDecompressor<ByteCodec::Lz4> {
 public:
  bool Decompress(const char* src, size_t size, char* dst,
                  size_t raw_size) noexcept;
  size_t DecompressPrefix(const char* src, size_t size, char* dst, size_t want,
                          size_t raw_size) noexcept;
};

template<>
class LeafDecompressor<ByteCodec::Zstd> {
 public:
  LeafDecompressor() : _ctx{utils::MakeZstdDCtx()} {}

  bool Decompress(const char* src, size_t size, char* dst,
                  size_t raw_size) noexcept;
  size_t DecompressPrefix(const char* src, size_t size, char* dst, size_t want,
                          size_t raw_size) noexcept;

 private:
  utils::ZstdDCtxPtr _ctx;
};

template<>
class LeafDecompressor<ByteCodec::Zxc> {
 public:
  LeafDecompressor();
  ~LeafDecompressor();

  LeafDecompressor(const LeafDecompressor&) = delete;
  LeafDecompressor& operator=(const LeafDecompressor&) = delete;

  bool Decompress(const char* src, size_t size, char* dst,
                  size_t raw_size) noexcept;
  size_t DecompressPrefix(const char* src, size_t size, char* dst, size_t want,
                          size_t raw_size) noexcept;

 private:
  zxc_dctx_s* _ctx;
};

}  // namespace irs::codecs
