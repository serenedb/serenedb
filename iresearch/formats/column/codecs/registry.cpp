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

#include <cstddef>
#include <span>

#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/zstd.hpp"
#include "iresearch/formats/column/codecs/zxc.hpp"

namespace irs::codecs {
namespace {

using duckdb::CompressionType;
using duckdb::PhysicalType;

template<const CompressionFunction& kFunction>
const CompressionFunction* Text(PhysicalType physical) {
  return physical == PhysicalType::VARCHAR ? &kFunction : nullptr;
}

const CompressionFunction* EmptyFunction(PhysicalType physical) {
  return physical == PhysicalType::BIT ? &EmptyValidityFunction() : nullptr;
}

constexpr Codec kCodecs[] = {
  {CompressionType::COMPRESSION_UNCOMPRESSED, UncompressedFunction},
  {CompressionType::COMPRESSION_RLE, RleFunction},
  {CompressionType::COMPRESSION_BITPACKING, BitpackingFunction},
  {CompressionType::COMPRESSION_ALP, AlpFunction},
  {CompressionType::COMPRESSION_ALPRD, AlpRdFunction},
  {CompressionType::COMPRESSION_ROARING, RoaringFunction},
  {CompressionType::COMPRESSION_CONSTANT, ConstantFunction},
  {CompressionType::COMPRESSION_EMPTY, EmptyFunction},
  {CompressionType::COMPRESSION_COL_NUMERIC, NumericFunction},
  {CompressionType::COMPRESSION_DICT_LZ4,
   Text<kDictLz4Function>,
   {Shape::Dedup, ByteCodec::Lz4}},
  {CompressionType::COMPRESSION_LZ4,
   Text<kLz4Function>,
   {Shape::Plain, ByteCodec::Lz4}},
  {CompressionType::COMPRESSION_DICT_ZSTD,
   Text<kDictZstdFunction>,
   {Shape::Dedup, ByteCodec::Zstd}},
  {CompressionType::COMPRESSION_COL_ZSTD,
   Text<kZstdFunction>,
   {Shape::Plain, ByteCodec::Zstd}},
  {CompressionType::COMPRESSION_DICT_ZXC,
   Text<kDictZxcFunction>,
   {Shape::Dedup, ByteCodec::Zxc}},
  {CompressionType::COMPRESSION_ZXC,
   Text<kZxcFunction>,
   {Shape::Plain, ByteCodec::Zxc}},
  {CompressionType::COMPRESSION_COL_DICT_FSST,
   Text<kDictFsstFunction>,
   {Shape::Dedup, ByteCodec::Fsst}},
  {CompressionType::COMPRESSION_COL_FSST,
   Text<kFsstFunction>,
   {Shape::Plain, ByteCodec::Fsst}},
  {CompressionType::COMPRESSION_COL_DICT_FSST_PLUS,
   Text<kDictFsstPlusFunction>,
   {Shape::Dedup, ByteCodec::FsstPlus}},
  {CompressionType::COMPRESSION_COL_FSST_PLUS,
   Text<kFsstPlusFunction>,
   {Shape::Plain, ByteCodec::FsstPlus}},
  {CompressionType::COMPRESSION_COL_DICT_ONPAIR,
   Text<kDictOnPairFunction>,
   {Shape::Dedup, ByteCodec::OnPair}},
  {CompressionType::COMPRESSION_COL_ONPAIR,
   Text<kOnPairFunction>,
   {Shape::Plain, ByteCodec::OnPair}},
  {CompressionType::COMPRESSION_COL_DICT_FSST12,
   Text<kDictFsst12Function>,
   {Shape::Dedup, ByteCodec::Fsst12}},
  {CompressionType::COMPRESSION_COL_FSST12,
   Text<kFsst12Function>,
   {Shape::Plain, ByteCodec::Fsst12}},
};

constexpr size_t kInternalBegin = 6;
constexpr size_t kTextBegin = 9;
static_assert(kCodecs[kInternalBegin].type ==
              CompressionType::COMPRESSION_CONSTANT);
static_assert(kCodecs[kTextBegin].type ==
              CompressionType::COMPRESSION_DICT_LZ4);

constexpr std::span<const Codec> kAnalyzed =
  std::span{kCodecs}.first(kInternalBegin);
constexpr std::span<const Codec> kText = std::span{kCodecs}.subspan(kTextBegin);

constexpr CompressionType Canonical(CompressionType type) noexcept {
  switch (type) {
    case CompressionType::COMPRESSION_ZSTD:
      return CompressionType::COMPRESSION_COL_ZSTD;
    case CompressionType::COMPRESSION_DICT_FSST:
      return CompressionType::COMPRESSION_COL_DICT_FSST;
    case CompressionType::COMPRESSION_FSST:
      return CompressionType::COMPRESSION_COL_FSST;
    default:
      return type;
  }
}

const Codec* Find(std::span<const Codec> codecs,
                  CompressionType type) noexcept {
  const auto it = absl::c_find_if(
    codecs, [&](const Codec& codec) { return codec.type == type; });
  return it == codecs.end() ? nullptr : &*it;
}

}  // namespace

std::span<const Codec> AnalyzedCodecs() noexcept { return kAnalyzed; }

const CompressionFunction* GetCodec(CompressionType type,
                                    PhysicalType physical) {
  const auto* codec = Find(kCodecs, type);
  return codec ? codec->function(physical) : nullptr;
}

const CompressionFunction* GetCompressionFunction(CompressionType type,
                                                  PhysicalType physical) {
  const auto* function = GetCodec(type, physical);
  return function && function->analyze ? function : nullptr;
}

bool Compresses(CompressionType type, PhysicalType physical) {
  const auto* function = GetCodec(type, physical);
  return function && function->compress;
}

const StringChoice* ChoiceOf(CompressionType type) {
  const auto* codec = Find(kText, Canonical(type));
  return codec ? &codec->choice : nullptr;
}

CompressionType TypeOf(StringChoice choice) noexcept {
  const auto it = absl::c_find_if(
    kText, [&](const Codec& codec) { return codec.choice == choice; });
  return it == kText.end() ? CompressionType::COMPRESSION_AUTO : it->type;
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
