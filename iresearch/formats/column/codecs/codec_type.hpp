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

#include <bitset>
#include <cstddef>
#include <cstdint>
#include <magic_enum/magic_enum.hpp>
#include <utility>

namespace irs::codecs {

enum class CodecType : uint8_t {
  Auto,
  Uncompressed,
  Alp,
  Alprd,
  Roaring,
  Constant,
  Empty,
  Bitpacking,
  Lz4,
  Zstd,
  Fsst,
  Zxc,
  FsstPlus,
  Onpair,
  Fsst12,
  Dict,
  DictLz4,
  DictZstd,
  DictFsst,
  DictZxc,
  DictFsstPlus,
  DictOnpair,
  DictFsst12,
  Delta,
  DeltaLz4,
  DeltaZstd,
  Rle,
  RleLz4,
  RleZstd,
};

}  // namespace irs::codecs
namespace magic_enum {

template<>
consteval customize::customize_t customize::enum_name<irs::codecs::CodecType>(
  irs::codecs::CodecType value) noexcept {
  using CodecType = irs::codecs::CodecType;
  switch (value) {
    case CodecType::Auto:
      return "Auto";
    case CodecType::Uncompressed:
      return "Uncompressed";
    case CodecType::Alp:
      return "ALP";
    case CodecType::Alprd:
      return "ALPRD";
    case CodecType::Roaring:
      return "Roaring";
    case CodecType::Constant:
      return "Constant";
    case CodecType::Empty:
      return "Empty Validity";
    case CodecType::Bitpacking:
      return "BitPacking";
    case CodecType::Lz4:
      return "LZ4";
    case CodecType::Zstd:
      return "ZSTD";
    case CodecType::Fsst:
      return "FSST";
    case CodecType::Zxc:
      return "ZXC";
    case CodecType::FsstPlus:
      return "FSST_PLUS";
    case CodecType::Onpair:
      return "ONPAIR";
    case CodecType::Fsst12:
      return "FSST12";
    case CodecType::Dict:
      return "DICT";
    case CodecType::DictLz4:
      return "DICT_LZ4";
    case CodecType::DictZstd:
      return "DICT_ZSTD";
    case CodecType::DictFsst:
      return "DICT_FSST";
    case CodecType::DictZxc:
      return "DICT_ZXC";
    case CodecType::DictFsstPlus:
      return "DICT_FSST_PLUS";
    case CodecType::DictOnpair:
      return "DICT_ONPAIR";
    case CodecType::DictFsst12:
      return "DICT_FSST12";
    case CodecType::Delta:
      return "DELTA";
    case CodecType::DeltaLz4:
      return "DELTA_LZ4";
    case CodecType::DeltaZstd:
      return "DELTA_ZSTD";
    case CodecType::Rle:
      return "RLE";
    case CodecType::RleLz4:
      return "RLE_LZ4";
    case CodecType::RleZstd:
      return "RLE_ZSTD";
  }
  return invalid_tag;
}

}  // namespace magic_enum
namespace irs::codecs {

using CodecSet = std::bitset<magic_enum::enum_count<CodecType>()>;

enum class Shape : uint8_t {
  Plain,
  Dictionary,
  Delta,
  Runs,
};

inline constexpr size_t kShapeCount = magic_enum::enum_count<Shape>();

enum class ByteCodec : uint8_t {
  None,
  Lz4,
  Zstd,
  Fsst,
  Zxc,
  FsstPlus,
  OnPair,
  Fsst12,
};

inline constexpr size_t kByteCodecCount = magic_enum::enum_count<ByteCodec>();

inline constexpr size_t kTextShapeCount =
  std::to_underlying(Shape::Dictionary) + 1;
inline constexpr size_t kTextShapeForms = kTextShapeCount * kByteCodecCount;
inline constexpr size_t kNumericLeafCount =
  std::to_underlying(ByteCodec::Zstd) + 1;

constexpr bool IsForm(CodecType type) noexcept {
  return type >= CodecType::Bitpacking;
}

constexpr size_t FormIndex(CodecType type) noexcept {
  return std::to_underlying(type) - std::to_underlying(CodecType::Bitpacking);
}

constexpr CodecType CodecOf(Shape shape, ByteCodec leaf) noexcept {
  const size_t s = std::to_underlying(shape);
  const size_t l = std::to_underlying(leaf);
  const size_t index =
    s < kTextShapeCount
      ? s * kByteCodecCount + l
      : kTextShapeForms + (s - kTextShapeCount) * kNumericLeafCount + l;
  return static_cast<CodecType>(std::to_underlying(CodecType::Bitpacking) +
                                index);
}

constexpr Shape ShapeOf(CodecType type) noexcept {
  const auto index = FormIndex(type);
  return static_cast<Shape>(index < kTextShapeForms
                              ? index / kByteCodecCount
                              : kTextShapeCount + (index - kTextShapeForms) /
                                                    kNumericLeafCount);
}

constexpr ByteCodec LeafOf(CodecType type) noexcept {
  const auto index = FormIndex(type);
  return static_cast<ByteCodec>(
    index < kTextShapeForms ? index % kByteCodecCount
                            : (index - kTextShapeForms) % kNumericLeafCount);
}

constexpr bool IsText(CodecType type) noexcept {
  return IsForm(type) && FormIndex(type) < kTextShapeForms &&
         LeafOf(type) != ByteCodec::None;
}

constexpr bool IsNumeric(CodecType type) noexcept {
  return IsForm(type) && std::to_underlying(ShapeOf(type)) < kShapeCount &&
         std::to_underlying(LeafOf(type)) < kNumericLeafCount;
}

constexpr bool Sorted(ByteCodec leaf) noexcept {
  return leaf == ByteCodec::Fsst || leaf == ByteCodec::FsstPlus ||
         leaf == ByteCodec::OnPair || leaf == ByteCodec::Fsst12;
}

static_assert(CodecOf(Shape::Dictionary, ByteCodec::Fsst12) ==
              CodecType::DictFsst12);
static_assert(CodecOf(Shape::Delta, ByteCodec::None) == CodecType::Delta);
static_assert(std::to_underlying(CodecOf(Shape::Runs, ByteCodec::Zstd)) + 1 ==
              magic_enum::enum_count<CodecType>());

}  // namespace irs::codecs
