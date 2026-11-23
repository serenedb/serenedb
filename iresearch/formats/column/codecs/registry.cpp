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

#include <iterator>
#include <magic_enum/magic_enum.hpp>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/bytes/zxc.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

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

CompressionFunctions Contenders(CodecType type, PhysicalType physical) {
  if (!IsForm(type)) {
    return {};
  }
  if (physical == PhysicalType::VARCHAR) {
    switch (LeafOf(type)) {
      case ByteCodec::Lz4:
        return Lz4TextContenders(ShapeOf(type));
      case ByteCodec::Fsst:
        return FsstTextContenders(ShapeOf(type));
      default:
        return {};
    }
  }
  if (!IsNumeric(type)) {
    return {};
  }
  return NumericContenders(ShapeOf(type), LeafOf(type), physical);
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
