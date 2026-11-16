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
    nullptr,
    &kLz4Function,
    &kZstdFunction,
    &kFsstFunction,
    &kZxcFunction,
    &kFsstPlusFunction,
    &kOnPairFunction,
    &kFsst12Function,
    nullptr,
    &kDictLz4Function,
    &kDictZstdFunction,
    &kDictFsstFunction,
    &kDictZxcFunction,
    &kDictFsstPlusFunction,
    &kDictOnPairFunction,
    &kDictFsst12Function,
  };
  static_assert(std::size(kText) == kTextShapeForms);
  const auto index = FormIndex(type);
  if (index >= std::size(kText)) {
    return nullptr;
  }
  SDB_ASSERT(!kText[index] || kText[index]->type == type);
  return kText[index];
}

const Codec* EmptyCodec(PhysicalType physical) {
  return physical == PhysicalType::BIT ? &EmptyValidityFunction() : nullptr;
}

}  // namespace

const Codec* GetCodec(CodecType type, PhysicalType physical) {
  switch (type) {
    case CodecType::Auto:
      return AutoCodec(physical);
    case CodecType::Uncompressed:
      return UncompressedFunction(physical);
    case CodecType::Rle:
      return RleFunction(physical);
    case CodecType::Bitpacking:
      return BitpackingFunction(physical);
    case CodecType::Alp:
      return AlpFunction(physical);
    case CodecType::Alprd:
      return AlpRdFunction(physical);
    case CodecType::Roaring:
      return RoaringFunction(physical);
    case CodecType::Constant:
      return ConstantFunction(physical);
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
