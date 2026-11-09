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

#include <array>
#include <iterator>
#include <vector>

#include "iresearch/formats/column/codecs/duckdb_bridge.hpp"
#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/zstd.hpp"
#include "iresearch/formats/column/codecs/zxc.hpp"

namespace irs::codecs {
namespace {

using duckdb::CompressionType;
using duckdb::PhysicalType;

struct TextCodec {
  CompressionType type;
  StringChoice choice;
  const CompressionFunction& (*function)();
};

constexpr TextCodec kTextCodecs[] = {
  {CompressionType::COMPRESSION_DICT_LZ4,
   {Shape::Dedup, ByteCodec::Lz4},
   DictLz4Function},
  {CompressionType::COMPRESSION_LZ4,
   {Shape::Plain, ByteCodec::Lz4},
   Lz4Function},
  {CompressionType::COMPRESSION_DICT_ZSTD,
   {Shape::Dedup, ByteCodec::Zstd},
   DictZstdFunction},
  {CompressionType::COMPRESSION_COL_ZSTD,
   {Shape::Plain, ByteCodec::Zstd},
   ZstdFunction},
  {CompressionType::COMPRESSION_DICT_ZXC,
   {Shape::Dedup, ByteCodec::Zxc},
   DictZxcFunction},
  {CompressionType::COMPRESSION_ZXC,
   {Shape::Plain, ByteCodec::Zxc},
   ZxcFunction},
  {CompressionType::COMPRESSION_COL_DICT_FSST,
   {Shape::Dedup, ByteCodec::Fsst},
   DictFsstFunction},
  {CompressionType::COMPRESSION_COL_FSST,
   {Shape::Plain, ByteCodec::Fsst},
   FsstFunction},
  {CompressionType::COMPRESSION_COL_DICT_FSST_PLUS,
   {Shape::Dedup, ByteCodec::FsstPlus},
   DictFsstPlusFunction},
  {CompressionType::COMPRESSION_COL_FSST_PLUS,
   {Shape::Plain, ByteCodec::FsstPlus},
   FsstPlusFunction},
  {CompressionType::COMPRESSION_COL_DICT_ONPAIR,
   {Shape::Dedup, ByteCodec::OnPair},
   DictOnPairFunction},
  {CompressionType::COMPRESSION_COL_ONPAIR,
   {Shape::Plain, ByteCodec::OnPair},
   OnPairFunction},
  {CompressionType::COMPRESSION_COL_DICT_FSST12,
   {Shape::Dedup, ByteCodec::Fsst12},
   DictFsst12Function},
  {CompressionType::COMPRESSION_COL_FSST12,
   {Shape::Plain, ByteCodec::Fsst12},
   Fsst12Function},
  {CompressionType::COMPRESSION_ZSTD, {Shape::Plain, ByteCodec::Zstd}, nullptr},
  {CompressionType::COMPRESSION_DICT_FSST,
   {Shape::Dedup, ByteCodec::Fsst},
   nullptr},
  {CompressionType::COMPRESSION_FSST, {Shape::Plain, ByteCodec::Fsst}, nullptr},
};

const TextCodec* TextCodecOf(CompressionType type) noexcept {
  const auto* it = absl::c_find_if(
    kTextCodecs, [&](const TextCodec& codec) { return codec.type == type; });
  return it == std::end(kTextCodecs) ? nullptr : it;
}

constexpr PhysicalType kPhysicalTypes[] = {
  PhysicalType::BOOL,    PhysicalType::UINT8,   PhysicalType::INT8,
  PhysicalType::UINT16,  PhysicalType::INT16,   PhysicalType::UINT32,
  PhysicalType::INT32,   PhysicalType::UINT64,  PhysicalType::INT64,
  PhysicalType::FLOAT,   PhysicalType::DOUBLE,  PhysicalType::INTERVAL,
  PhysicalType::LIST,    PhysicalType::STRUCT,  PhysicalType::ARRAY,
  PhysicalType::VARCHAR, PhysicalType::UINT128, PhysicalType::INT128,
  PhysicalType::BIT,
};

constexpr CompressionType kCandidates[] = {
  CompressionType::COMPRESSION_UNCOMPRESSED,
  CompressionType::COMPRESSION_RLE,
  CompressionType::COMPRESSION_BITPACKING,
  CompressionType::COMPRESSION_ALP,
  CompressionType::COMPRESSION_ALPRD,
  CompressionType::COMPRESSION_ROARING,
};

using CandidateTable = std::array<std::vector<const CompressionFunction*>,
                                  std::size(kPhysicalTypes)>;

CandidateTable MakeCandidates() {
  CandidateTable table;
  for (size_t p = 0; p < std::size(kPhysicalTypes); ++p) {
    for (const auto type : kCandidates) {
      const auto* function = GetCodec(type, kPhysicalTypes[p]);
      if (function && function->init_analyze) {
        table[p].push_back(function);
      }
    }
  }
  return table;
}

}  // namespace

const CompressionFunction* GetCodec(CompressionType type,
                                    PhysicalType physical) {
  switch (type) {
    case CompressionType::COMPRESSION_COL_NUMERIC:
      return NumericFunction(physical);
    case CompressionType::COMPRESSION_CONSTANT:
      return ConstantFunction(physical);
    case CompressionType::COMPRESSION_UNCOMPRESSED:
      return UncompressedFunction(physical);
    case CompressionType::COMPRESSION_RLE:
      return RleFunction(physical);
    case CompressionType::COMPRESSION_BITPACKING:
      return BitpackingFunction(physical);
    case CompressionType::COMPRESSION_ALP:
      return AlpFunction(physical);
    case CompressionType::COMPRESSION_ALPRD:
      return AlpRdFunction(physical);
    case CompressionType::COMPRESSION_EMPTY:
      return physical == PhysicalType::BIT ? &EmptyValidityFunction() : nullptr;
    default:
      break;
  }
  if (const auto* text = TextCodecOf(type);
      text && text->function && physical == PhysicalType::VARCHAR) {
    return &text->function();
  }
  return DuckDBFunction(type, physical);
}

std::span<const CompressionFunction* const> GetCompressionFunctions(
  PhysicalType physical) {
  static const auto kTable = MakeCandidates();
  const auto* it = absl::c_find(kPhysicalTypes, physical);
  if (it == std::end(kPhysicalTypes)) {
    return {};
  }
  return kTable[static_cast<size_t>(it - std::begin(kPhysicalTypes))];
}

const StringChoice* ChoiceOf(CompressionType type) {
  const auto* text = TextCodecOf(type);
  return text ? &text->choice : nullptr;
}

CompressionType TypeOf(StringChoice choice) noexcept {
  const auto* it = absl::c_find_if(kTextCodecs, [&](const TextCodec& codec) {
    return codec.function && codec.choice == choice;
  });
  return it == std::end(kTextCodecs) ? CompressionType::COMPRESSION_AUTO
                                     : it->type;
}

uint8_t MaxLevel(CompressionType type) {
  const auto* choice = ChoiceOf(type);
  if (!choice) {
    return 0;
  }
  switch (choice->leaf) {
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
