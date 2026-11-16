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

struct CodecInfo {
  CodecType type;
  std::string_view name;
  const Codec* (&function)(duckdb::PhysicalType physical);
};

extern const Codec kDictLz4Function;
extern const Codec kLz4Function;
extern const Codec kDictZstdFunction;
extern const Codec kZstdFunction;
extern const Codec kDictZxcFunction;
extern const Codec kZxcFunction;
extern const Codec kDictFsstFunction;
extern const Codec kFsstFunction;
extern const Codec kDictFsstPlusFunction;
extern const Codec kFsstPlusFunction;
extern const Codec kDictOnPairFunction;
extern const Codec kOnPairFunction;
extern const Codec kDictFsst12Function;
extern const Codec kFsst12Function;
const Codec* NumericCodecOf(CodecType type, duckdb::PhysicalType physical);
const Codec* ConstantFunction(duckdb::PhysicalType physical);
bool CompressConstant(ColumnDataCheckpointData& data);
const Codec& EmptyValidityFunction();
const Codec* UncompressedFunction(duckdb::PhysicalType physical);
const Codec* RleFunction(duckdb::PhysicalType physical);
const Codec* BitpackingFunction(duckdb::PhysicalType physical);
const Codec* AlpFunction(duckdb::PhysicalType physical);
const Codec* AlpRdFunction(duckdb::PhysicalType physical);
const Codec* RoaringFunction(duckdb::PhysicalType physical);

const Codec* GetCodec(CodecType type, duckdb::PhysicalType physical);

bool Writes(CodecType type, duckdb::PhysicalType physical,
            const CodecSet& disabled) noexcept;

std::string_view NameOf(CodecType type) noexcept;
const CodecInfo* FindCodec(std::string_view name) noexcept;
const CodecInfo* FindCodec(duckdb::CompressionType type) noexcept;

uint8_t MaxLevel(CodecType type) noexcept;

}  // namespace irs::codecs
