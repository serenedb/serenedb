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

#include <magic_enum/magic_enum.hpp>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/bytes/zxc.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

template<CodecType kType, const Codec* kText = nullptr>
const Codec* FamilyCodec(PhysicalType physical) {
  if (physical == PhysicalType::VARCHAR) {
    return kText;
  }
  if constexpr (IsNumeric(kType)) {
    return NumericCodecOf(ShapeOf(kType), LeafOf(kType), physical);
  } else {
    return nullptr;
  }
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
    case CodecType::Ffor:
      return FamilyCodec<CodecType::Ffor>(physical);
    case CodecType::Lz4:
      return FamilyCodec<CodecType::Lz4, &kLz4Function>(physical);
    case CodecType::Zstd:
      return FamilyCodec<CodecType::Zstd, &kZstdFunction>(physical);
    case CodecType::Fsst:
      return FamilyCodec<CodecType::Fsst, &kFsstFunction>(physical);
    case CodecType::Zxc:
      return FamilyCodec<CodecType::Zxc, &kZxcFunction>(physical);
    case CodecType::FsstPlus:
      return FamilyCodec<CodecType::FsstPlus, &kFsstPlusFunction>(physical);
    case CodecType::Onpair:
      return FamilyCodec<CodecType::Onpair, &kOnPairFunction>(physical);
    case CodecType::Fsst12:
      return FamilyCodec<CodecType::Fsst12, &kFsst12Function>(physical);
    case CodecType::DictFfor:
      return FamilyCodec<CodecType::DictFfor>(physical);
    case CodecType::DictLz4:
      return FamilyCodec<CodecType::DictLz4, &kDictLz4Function>(physical);
    case CodecType::DictZstd:
      return FamilyCodec<CodecType::DictZstd, &kDictZstdFunction>(physical);
    case CodecType::DictFsst:
      return FamilyCodec<CodecType::DictFsst, &kDictFsstFunction>(physical);
    case CodecType::DictZxc:
      return FamilyCodec<CodecType::DictZxc, &kDictZxcFunction>(physical);
    case CodecType::DictFsstPlus:
      return FamilyCodec<CodecType::DictFsstPlus, &kDictFsstPlusFunction>(
        physical);
    case CodecType::DictOnpair:
      return FamilyCodec<CodecType::DictOnpair, &kDictOnPairFunction>(physical);
    case CodecType::DictFsst12:
      return FamilyCodec<CodecType::DictFsst12, &kDictFsst12Function>(physical);
    case CodecType::DeltaFfor:
      return FamilyCodec<CodecType::DeltaFfor>(physical);
    case CodecType::DeltaLz4:
      return FamilyCodec<CodecType::DeltaLz4>(physical);
    case CodecType::DeltaZstd:
      return FamilyCodec<CodecType::DeltaZstd>(physical);
    case CodecType::RleFfor:
      return FamilyCodec<CodecType::RleFfor>(physical);
    case CodecType::RleLz4:
      return FamilyCodec<CodecType::RleLz4>(physical);
    case CodecType::RleZstd:
      return FamilyCodec<CodecType::RleZstd>(physical);
  }
  return nullptr;
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
