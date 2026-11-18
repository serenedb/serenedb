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

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types.hpp>
#include <span>
#include <string_view>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"

namespace irs::codecs {

extern const Codec kDictLz4Codec;
extern const Codec kLz4Codec;
extern const Codec kDictZstdCodec;
extern const Codec kZstdCodec;
extern const Codec kDictZxcCodec;
extern const Codec kZxcCodec;
extern const Codec kDictFsstFrontCodec;
extern const Codec kFsstFrontCodec;
extern const Codec kDictFsstPlusCodec;
extern const Codec kFsstPlusCodec;
extern const Codec kDictOnPairCodec;
extern const Codec kOnPairCodec;
extern const Codec kDictFsst12FrontCodec;
extern const Codec kFsst12FrontCodec;
const Codec* NumericCodecOf(Shape shape, ByteCodec leaf,
                            duckdb::PhysicalType physical);
std::span<const Contender> NumericContenders(Shape shape, ByteCodec leaf,
                                             duckdb::PhysicalType physical);
std::span<const Contender> Lz4TextContenders(Shape shape);
std::span<const Contender> FsstTextContenders(Shape shape);
const Codec* ConstantCodec(duckdb::PhysicalType physical);
bool CompressConstant(ColumnDataCheckpointData& data);
const Codec* EmptyCodec(duckdb::PhysicalType physical);
const Codec* UncompressedCodec(duckdb::PhysicalType physical);
const Codec* RleCodec(duckdb::PhysicalType physical);
const Codec* BitpackingCodec(duckdb::PhysicalType physical);
const Codec* AlpCodec(duckdb::PhysicalType physical);
const Codec* AlpRdCodec(duckdb::PhysicalType physical);
const Codec* RoaringCodec(duckdb::PhysicalType physical);
const Codec* AutoCodec(duckdb::PhysicalType physical);

const Codec* GetCodec(CodecType type, duckdb::PhysicalType physical);

std::span<const Contender> Contenders(CodecType type,
                                      duckdb::PhysicalType physical);

bool Writes(CodecType type, duckdb::PhysicalType physical,
            const CodecSet& disabled) noexcept;

inline constexpr auto kNoCodec =
  static_cast<CodecType>(magic_enum::enum_count<CodecType>());

CodecType FindCodec(std::string_view name);
CodecType FindCodec(duckdb::CompressionType type);

uint8_t MaxLevel(CodecType type) noexcept;

}  // namespace irs::codecs
