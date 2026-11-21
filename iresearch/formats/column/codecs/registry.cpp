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

#include "iresearch/formats/column/codecs/registry.hpp"

#include <absl/strings/match.h>

#include <array>
#include <iterator>
#include <magic_enum/magic_enum.hpp>
#include <utility>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/bytes/zxc.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

constexpr CodecType kLightweight[] = {
  CodecType::Uncompressed,
  CodecType::Roaring,
};

constexpr CodecType kBool[] = {
  CodecType::Constant, CodecType::Uncompressed, CodecType::Bitpacking,
  CodecType::Rle,      CodecType::Roaring,
};

constexpr CodecType kInteger[] = {
  CodecType::Constant, CodecType::Uncompressed, CodecType::Bitpacking,
  CodecType::Rle,      CodecType::Dict,         CodecType::Lz4,
  CodecType::RleLz4,   CodecType::DictLz4,      CodecType::Zstd,
  CodecType::RleZstd,  CodecType::DictZstd,
};

constexpr CodecType kFreshInteger[] = {
  CodecType::Constant, CodecType::Uncompressed, CodecType::Bitpacking,
  CodecType::Rle,      CodecType::Dict,         CodecType::Lz4,
  CodecType::RleLz4,   CodecType::DictLz4,
};

constexpr CodecType kFloat[] = {
  CodecType::Constant, CodecType::Uncompressed, CodecType::Alp,
  CodecType::Alprd,    CodecType::Bitpacking,   CodecType::Rle,
  CodecType::Dict,
};

constexpr CodecType kText[] = {
  CodecType::Uncompressed, CodecType::DictFsst, CodecType::DictLz4,
  CodecType::Fsst,         CodecType::Lz4,
};

constexpr CodecType kNamedOnly[] = {
  CodecType::Delta,
  CodecType::DeltaLz4,
  CodecType::DeltaZstd,
};

static_assert([] {
  std::array<bool, magic_enum::enum_count<CodecType>()> listed{};
  for (const std::span<const CodecType> list :
       {std::span<const CodecType>{kLightweight},
        std::span<const CodecType>{kBool}, std::span<const CodecType>{kInteger},
        std::span<const CodecType>{kFreshInteger},
        std::span<const CodecType>{kFloat},
        std::span<const CodecType>{kNamedOnly}}) {
    for (const auto type : list) {
      listed[std::to_underlying(type)] = true;
    }
  }
  for (const auto type : magic_enum::enum_values<CodecType>()) {
    const bool numeric = type != CodecType::Auto && type != CodecType::Empty &&
                         (!IsText(type) || IsNumeric(type));
    if (listed[std::to_underlying(type)] != numeric) {
      return false;
    }
  }
  return true;
}());

const Codec* TextCodecOf(CodecType type) {
  static constexpr const Codec* kText[] = {
    nullptr,           &kLz4Codec,
    &kZstdCodec,       &kFsstFrontCodec,
    &kZxcCodec,        &kFsstPlusCodec,
    &kOnPairCodec,     &kFsst12FrontCodec,
    nullptr,           &kDictLz4Codec,
    &kDictZstdCodec,   &kDictFsstFrontCodec,
    &kDictZxcCodec,    &kDictFsstPlusCodec,
    &kDictOnPairCodec, &kDictFsst12FrontCodec,
  };
  static_assert(std::size(kText) == kTextShapeForms);
  const auto index = FormIndex(type);
  if (index >= std::size(kText)) {
    return nullptr;
  }
  SDB_ASSERT(!kText[index] || kText[index]->type == type);
  return kText[index];
}

}  // namespace

const Codec* GetCodec(CodecType type, PhysicalType physical) {
  switch (type) {
    case CodecType::Auto:
      return AutoCodec(physical);
    case CodecType::Uncompressed:
      return UncompressedCodec(physical);
    case CodecType::Alp:
      return AlpCodec(physical);
    case CodecType::Alprd:
      return AlpRdCodec(physical);
    case CodecType::Roaring:
      return RoaringCodec(physical);
    case CodecType::Constant:
      return ConstantCodec(physical);
    case CodecType::Empty:
      return EmptyCodec(physical);
    default:
      break;
  }
  SDB_ASSERT(IsForm(type));
  if (physical == PhysicalType::VARCHAR) {
    return TextCodecOf(type);
  }
  return IsNumeric(type) ? NumericCodecOf(ShapeOf(type), LeafOf(type), physical)
                         : nullptr;
}

std::span<const CodecType> Candidates(PhysicalType physical,
                                      WriteTier tier) noexcept {
  switch (physical) {
    case PhysicalType::VARCHAR:
      return kText;
    case PhysicalType::FLOAT:
    case PhysicalType::DOUBLE:
      return kFloat;
    case PhysicalType::BOOL:
      return kBool;
    case PhysicalType::INT8:
    case PhysicalType::INT16:
    case PhysicalType::INT32:
    case PhysicalType::INT64:
    case PhysicalType::UINT8:
    case PhysicalType::UINT16:
    case PhysicalType::UINT32:
    case PhysicalType::UINT64:
    case PhysicalType::INT128:
    case PhysicalType::UINT128:
    case PhysicalType::LIST:
      if (tier == WriteTier::Flush) {
        return kFreshInteger;
      }
      return kInteger;
    default:
      return kLightweight;
  }
}

CodecType FindCodec(std::string_view name) {
  for (const auto type : magic_enum::enum_values<CodecType>()) {
    if (absl::EqualsIgnoreCase(magic_enum::enum_name(type), name)) {
      return type;
    }
  }
  return kNoCodec;
}

CodecType FindCodec(duckdb::CompressionType type) {
  if (type >= duckdb::CompressionType::ENUM_SIZE &&
      !duckdb::IsSereneDBCompressionType(type)) {
    return kNoCodec;
  }
  return FindCodec(duckdb::CompressionTypeToString(type));
}

uint8_t MaxLevel(CodecType type) noexcept {
  if (!IsForm(type)) {
    return 0;
  }
  switch (LeafOf(type)) {
    case ByteCodec::Lz4:
      return Lz4::kMaxLevel;
    case ByteCodec::Zstd:
      return Zstd::kMaxLevel;
    case ByteCodec::Zxc:
      return Zxc::kMaxLevel;
    case ByteCodec::None:
    case ByteCodec::Fsst:
    case ByteCodec::FsstPlus:
    case ByteCodec::OnPair:
    case ByteCodec::Fsst12:
      return 0;
  }
  return 0;
}

}  // namespace irs::codecs
