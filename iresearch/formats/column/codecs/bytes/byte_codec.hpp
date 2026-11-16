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

#include <algorithm>
#include <cstdint>
#include <magic_enum/magic_enum.hpp>
#include <utility>

#include "iresearch/formats/column/codecs/codec_type.hpp"

namespace irs::codecs {

enum class ByteCodec : uint8_t {
  Lz4 = 0,
  Zstd = 1,
  Fsst = 2,
  Zxc = 3,
  FsstPlus = 4,
  OnPair = 5,
  Fsst12 = 6,
};

inline constexpr uint8_t kByteCodecCount = 7;

constexpr bool Sorted(ByteCodec leaf) noexcept {
  return leaf == ByteCodec::Fsst || leaf == ByteCodec::FsstPlus ||
         leaf == ByteCodec::OnPair || leaf == ByteCodec::Fsst12;
}

template<typename Codec>
constexpr uint8_t EffectiveLevel(uint8_t level) noexcept {
  return level ? std::min(level, Codec::kMaxLevel) : Codec::kDefaultLevel;
}

enum class Shape : uint8_t {
  Dedup = 0,
  Plain = 1,
};

constexpr bool IsText(CodecType type) noexcept {
  return type >= CodecType::DictLz4 && type <= CodecType::Fsst12;
}

constexpr CodecType TextCodec(Shape shape, ByteCodec leaf) noexcept {
  return static_cast<CodecType>(std::to_underlying(CodecType::DictLz4) +
                                2 * std::to_underlying(leaf) +
                                std::to_underlying(shape));
}

constexpr Shape ShapeOf(CodecType type) noexcept {
  return static_cast<Shape>(
    (std::to_underlying(type) - std::to_underlying(CodecType::DictLz4)) % 2);
}

constexpr ByteCodec LeafOf(CodecType type) noexcept {
  return static_cast<ByteCodec>(
    (std::to_underlying(type) - std::to_underlying(CodecType::DictLz4)) / 2);
}

static_assert(TextCodec(Shape::Dedup, ByteCodec::Lz4) == CodecType::DictLz4);
static_assert(TextCodec(Shape::Plain, ByteCodec::Fsst12) == CodecType::Fsst12);
static_assert(std::to_underlying(CodecType::Fsst12) + 1 ==
              std::to_underlying(CodecType::DictLz4) + 2 * kByteCodecCount);

}  // namespace irs::codecs
