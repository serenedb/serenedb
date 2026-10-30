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

#include <duckdb/main/config.hpp>
#include <iterator>

#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/zstd.hpp"
#include "iresearch/formats/column/codecs/zxc.hpp"

namespace irs::codecs {
namespace {

using duckdb::CompressionType;

struct TextCodec {
  CompressionType type;
  StringChoice choice;
  const duckdb::CompressionFunction& (*function)();
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

}  // namespace

const duckdb::CompressionFunction* GetCodec(duckdb::DatabaseInstance& db,
                                            CompressionType type,
                                            duckdb::PhysicalType physical) {
  if (type == CompressionType::COMPRESSION_COL_SEQUENCE) {
    return SequenceFunction(physical);
  }
  if (type == CompressionType::COMPRESSION_COL_NUMERIC) {
    return NumericFunction(physical);
  }
  if (const auto* text = TextCodecOf(type);
      text && text->function && physical == duckdb::PhysicalType::VARCHAR) {
    return &text->function();
  }
  return duckdb::DBConfig::GetConfig(db)
    .TryGetCompressionFunction(type, physical)
    .get();
}

std::optional<StringChoice> ChoiceOf(CompressionType type) {
  if (const auto* text = TextCodecOf(type)) {
    return text->choice;
  }
  return std::nullopt;
}

CompressionType TypeOf(StringChoice choice) noexcept {
  const auto* it = absl::c_find_if(kTextCodecs, [&](const TextCodec& codec) {
    return codec.function && codec.choice == choice;
  });
  return it == std::end(kTextCodecs) ? CompressionType::COMPRESSION_AUTO
                                     : it->type;
}

uint8_t MaxLevel(CompressionType type) {
  const auto choice = ChoiceOf(type);
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
      return 0;
  }
  return 0;
}

}  // namespace irs::codecs
