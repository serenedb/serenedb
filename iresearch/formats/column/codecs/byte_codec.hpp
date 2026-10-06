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

#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/utils/zstd_context.hpp"

union LZ4_stream_u;
union LZ4_streamHC_u;
struct ZSTD_CDict_s;
struct ZSTD_DDict_s;
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

constexpr bool Trainable(ByteCodec leaf) noexcept {
  return leaf == ByteCodec::Lz4 || leaf == ByteCodec::Zstd;
}

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
  LeafCompressor() = default;
  ~LeafCompressor();

  LeafCompressor(const LeafCompressor&) = delete;
  LeafCompressor& operator=(const LeafCompressor&) = delete;

  void SetLevel(uint8_t level) noexcept {
    _level = EffectiveLevel<ByteCodec::Lz4>(level);
    _dictionary = false;
  }

  void LoadDictionary(std::string_view dictionary);
  void LoadTrained(const TrainedDictionary& dictionary);

  size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

 private:
  uint8_t _level = Leaf<ByteCodec::Lz4>::kDefaultLevel;
  bool _dictionary = false;
  uint64_t _trained = 0;
  uint8_t _trained_level = 0;
  LZ4_stream_u* _dict = nullptr;
  LZ4_stream_u* _work = nullptr;
  LZ4_streamHC_u* _hc_dict = nullptr;
  LZ4_streamHC_u* _hc_work = nullptr;
};

template<>
class LeafCompressor<ByteCodec::Zstd> {
 public:
  LeafCompressor() : _ctx{utils::MakeZstdCCtx()} {}
  ~LeafCompressor();

  LeafCompressor(const LeafCompressor&) = delete;
  LeafCompressor& operator=(const LeafCompressor&) = delete;

  void SetLevel(uint8_t level) noexcept {
    _level = EffectiveLevel<ByteCodec::Zstd>(level);
    _active = nullptr;
  }

  void LoadDictionary(std::string_view dictionary);
  void LoadTrained(const TrainedDictionary& dictionary);

  size_t Compress(const char* src, size_t size, char* dst, size_t capacity);

 private:
  uint8_t _level = Leaf<ByteCodec::Zstd>::kDefaultLevel;
  utils::ZstdCCtxPtr _ctx;
  ZSTD_CDict_s* _cdict = nullptr;
  ZSTD_CDict_s* _trained_cdict = nullptr;
  const ZSTD_CDict_s* _active = nullptr;
  uint64_t _trained = 0;
  uint8_t _trained_level = 0;
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

template<ByteCodec C>
class LeafDecompressor;

template<>
class LeafDecompressor<ByteCodec::Lz4> {
 public:
  void SetDictionary(std::string_view dictionary) noexcept {
    _dictionary = dictionary;
  }

  bool Decompress(const char* src, size_t size, char* dst,
                  size_t raw_size) noexcept;
  size_t DecompressPrefix(const char* src, size_t size, char* dst, size_t want,
                          size_t raw_size) noexcept;

 private:
  std::string_view _dictionary;
};

template<>
class LeafDecompressor<ByteCodec::Zstd> {
 public:
  LeafDecompressor() : _ctx{utils::MakeZstdDCtx()} {}
  ~LeafDecompressor();

  LeafDecompressor(const LeafDecompressor&) = delete;
  LeafDecompressor& operator=(const LeafDecompressor&) = delete;

  void SetDictionary(std::string_view dictionary);
  void SetTrained(const TrainedDictionary& dictionary) noexcept {
    _active = dictionary.ZstdDictionary();
  }

  bool Decompress(const char* src, size_t size, char* dst,
                  size_t raw_size) noexcept;
  size_t DecompressPrefix(const char* src, size_t size, char* dst, size_t want,
                          size_t raw_size) noexcept;

 private:
  utils::ZstdDCtxPtr _ctx;
  ZSTD_DDict_s* _ddict = nullptr;
  const ZSTD_DDict_s* _active = nullptr;
  std::string_view _loaded;
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
