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

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>

#include <cstddef>
#include <iterator>
#include <magic_enum/magic_enum.hpp>
#include <span>
#include <utility>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/bytes/zxc.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

template<const Codec& kFunction>
const Codec* Text(PhysicalType physical) {
  return physical == PhysicalType::VARCHAR ? &kFunction : nullptr;
}

template<const Codec& kText, CodecType kType>
const Codec* TextOrNumeric(PhysicalType physical) {
  return physical == PhysicalType::VARCHAR ? &kText
                                           : NumericCodecOf(kType, physical);
}

template<CodecType kType>
const Codec* Numeric(PhysicalType physical) {
  return NumericCodecOf(kType, physical);
}

const Codec* EmptyFunction(PhysicalType physical) {
  return physical == PhysicalType::BIT ? &EmptyValidityFunction() : nullptr;
}

const Codec* NoCodec(PhysicalType) { return nullptr; }

constexpr CodecInfo kCodecs[] = {
  {CodecType::Auto, "Auto", NoCodec},
  {CodecType::Uncompressed, "Uncompressed", UncompressedFunction},
  {CodecType::Rle, "RLE", RleFunction},
  {CodecType::Bitpacking, "BitPacking", BitpackingFunction},
  {CodecType::Alp, "ALP", AlpFunction},
  {CodecType::AlpRd, "ALPRD", AlpRdFunction},
  {CodecType::Roaring, "Roaring", RoaringFunction},
  {CodecType::Constant, "Constant", ConstantFunction},
  {CodecType::Empty, "Empty Validity", EmptyFunction},
  {CodecType::Ffor, "FFOR", Numeric<CodecType::Ffor>},
  {CodecType::DictLz4, "DICT_LZ4",
   TextOrNumeric<kDictLz4Function, CodecType::DictLz4>},
  {CodecType::Lz4, "LZ4", TextOrNumeric<kLz4Function, CodecType::Lz4>},
  {CodecType::DictZstd, "DICT_ZSTD",
   TextOrNumeric<kDictZstdFunction, CodecType::DictZstd>},
  {CodecType::Zstd, "ZSTD", TextOrNumeric<kZstdFunction, CodecType::Zstd>},
  {CodecType::DictFsst, "DICT_FSST", Text<kDictFsstFunction>},
  {CodecType::Fsst, "FSST", Text<kFsstFunction>},
  {CodecType::DictZxc, "DICT_ZXC", Text<kDictZxcFunction>},
  {CodecType::Zxc, "ZXC", Text<kZxcFunction>},
  {CodecType::DictFsstPlus, "DICT_FSST_PLUS", Text<kDictFsstPlusFunction>},
  {CodecType::FsstPlus, "FSST_PLUS", Text<kFsstPlusFunction>},
  {CodecType::DictOnPair, "DICT_ONPAIR", Text<kDictOnPairFunction>},
  {CodecType::OnPair, "ONPAIR", Text<kOnPairFunction>},
  {CodecType::DictFsst12, "DICT_FSST12", Text<kDictFsst12Function>},
  {CodecType::Fsst12, "FSST12", Text<kFsst12Function>},
  {CodecType::DeltaFfor, "DELTA_FFOR", Numeric<CodecType::DeltaFfor>},
  {CodecType::RleFfor, "RLE_FFOR", Numeric<CodecType::RleFfor>},
  {CodecType::DictFfor, "DICT_FFOR", Numeric<CodecType::DictFfor>},
  {CodecType::DeltaLz4, "DELTA_LZ4", Numeric<CodecType::DeltaLz4>},
  {CodecType::RleLz4, "RLE_LZ4", Numeric<CodecType::RleLz4>},
  {CodecType::DeltaZstd, "DELTA_ZSTD", Numeric<CodecType::DeltaZstd>},
  {CodecType::RleZstd, "RLE_ZSTD", Numeric<CodecType::RleZstd>},
};

static_assert(std::size(kCodecs) == magic_enum::enum_count<CodecType>());
static_assert([] {
  for (size_t i = 0; i < std::size(kCodecs); ++i) {
    if (std::to_underlying(kCodecs[i].type) != i) {
      return false;
    }
  }
  return true;
}());

}  // namespace

const Codec* GetCodec(CodecType type, PhysicalType physical) {
  const auto index = std::to_underlying(type);
  return index < std::size(kCodecs) ? kCodecs[index].function(physical)
                                    : nullptr;
}

std::string_view NameOf(CodecType type) noexcept {
  SDB_ASSERT(std::to_underlying(type) < std::size(kCodecs));
  return kCodecs[std::to_underlying(type)].name;
}

const CodecInfo* FindCodec(std::string_view name) noexcept {
  const auto it = absl::c_find_if(kCodecs, [&](const CodecInfo& codec) {
    return absl::EqualsIgnoreCase(codec.name, name);
  });
  return it == std::end(kCodecs) ? nullptr : &*it;
}

const CodecInfo* FindCodec(duckdb::CompressionType type) noexcept {
  using enum duckdb::CompressionType;
  const auto codec = [](CodecType ours) noexcept {
    return &kCodecs[std::to_underlying(ours)];
  };
  switch (type) {
    case COMPRESSION_AUTO:
      return codec(CodecType::Auto);
    case COMPRESSION_UNCOMPRESSED:
      return codec(CodecType::Uncompressed);
    case COMPRESSION_CONSTANT:
      return codec(CodecType::Constant);
    case COMPRESSION_RLE:
      return codec(CodecType::Rle);
    case COMPRESSION_BITPACKING:
      return codec(CodecType::Bitpacking);
    case COMPRESSION_FSST:
      return codec(CodecType::Fsst);
    case COMPRESSION_ALP:
      return codec(CodecType::Alp);
    case COMPRESSION_ALPRD:
      return codec(CodecType::AlpRd);
    case COMPRESSION_ZSTD:
      return codec(CodecType::Zstd);
    case COMPRESSION_ROARING:
      return codec(CodecType::Roaring);
    case COMPRESSION_EMPTY:
      return codec(CodecType::Empty);
    case COMPRESSION_DICT_FSST:
      return codec(CodecType::DictFsst);
    case COMPRESSION_DICT_LZ4:
      return codec(CodecType::DictLz4);
    case COMPRESSION_DICT_ZSTD:
      return codec(CodecType::DictZstd);
    case COMPRESSION_LZ4:
      return codec(CodecType::Lz4);
    case COMPRESSION_DICT_ZXC:
      return codec(CodecType::DictZxc);
    case COMPRESSION_ZXC:
      return codec(CodecType::Zxc);
    case COMPRESSION_COL_DICT_FSST_PLUS:
      return codec(CodecType::DictFsstPlus);
    case COMPRESSION_COL_FSST_PLUS:
      return codec(CodecType::FsstPlus);
    case COMPRESSION_COL_DICT_ONPAIR:
      return codec(CodecType::DictOnPair);
    case COMPRESSION_COL_ONPAIR:
      return codec(CodecType::OnPair);
    case COMPRESSION_COL_DICT_FSST12:
      return codec(CodecType::DictFsst12);
    case COMPRESSION_COL_FSST12:
      return codec(CodecType::Fsst12);
    case COMPRESSION_COL_FFOR:
      return codec(CodecType::Ffor);
    case COMPRESSION_COL_DELTA_FFOR:
      return codec(CodecType::DeltaFfor);
    case COMPRESSION_COL_RLE_FFOR:
      return codec(CodecType::RleFfor);
    case COMPRESSION_COL_DICT_FFOR:
      return codec(CodecType::DictFfor);
    case COMPRESSION_COL_DELTA_LZ4:
      return codec(CodecType::DeltaLz4);
    case COMPRESSION_COL_RLE_LZ4:
      return codec(CodecType::RleLz4);
    case COMPRESSION_COL_DELTA_ZSTD:
      return codec(CodecType::DeltaZstd);
    case COMPRESSION_COL_RLE_ZSTD:
      return codec(CodecType::RleZstd);
    case COMPRESSION_DICTIONARY:
    case COMPRESSION_PFOR_DELTA:
    case COMPRESSION_CHIMP:
    case COMPRESSION_PATAS:
    case ENUM_SIZE:
    case COMPRESSION_SERENEDB_COUNT:
      break;
  }
  return nullptr;
}

uint8_t MaxLevel(CodecType type) noexcept {
  for (const auto& shape : kNumericCodecs) {
    if (type == shape[std::to_underlying(NumericLeaf::Lz4)]) {
      return Lz4::kMaxLevel;
    }
    if (type == shape[std::to_underlying(NumericLeaf::Zstd)]) {
      return Zstd::kMaxLevel;
    }
  }
  if (!IsText(type)) {
    return 0;
  }
  switch (LeafOf(type)) {
    case ByteCodec::Lz4:
      return Lz4::kMaxLevel;
    case ByteCodec::Zstd:
      return Zstd::kMaxLevel;
    case ByteCodec::Zxc:
      return Zxc::kMaxLevel;
    case ByteCodec::Fsst:
    case ByteCodec::FsstPlus:
    case ByteCodec::OnPair:
    case ByteCodec::Fsst12:
      return 0;
  }
  return 0;
}

}  // namespace irs::codecs
