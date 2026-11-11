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

#include <duckdb/common/types.hpp>
#include <span>
#include <string_view>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"

namespace irs::codecs {

struct Codec {
  CodecType type;
  std::string_view name;
  const CompressionFunction* (&function)(duckdb::PhysicalType physical);
  StringChoice choice{};
};

std::span<const Codec> AnalyzedCodecs() noexcept;

extern const CompressionFunction kDictLz4Function;
extern const CompressionFunction kLz4Function;
extern const CompressionFunction kDictZstdFunction;
extern const CompressionFunction kZstdFunction;
extern const CompressionFunction kDictZxcFunction;
extern const CompressionFunction kZxcFunction;
extern const CompressionFunction kDictFsstFunction;
extern const CompressionFunction kFsstFunction;
extern const CompressionFunction kDictFsstPlusFunction;
extern const CompressionFunction kFsstPlusFunction;
extern const CompressionFunction kDictOnPairFunction;
extern const CompressionFunction kOnPairFunction;
extern const CompressionFunction kDictFsst12Function;
extern const CompressionFunction kFsst12Function;
const CompressionFunction* NumericFunction(duckdb::PhysicalType physical);
const CompressionFunction* ConstantFunction(duckdb::PhysicalType physical);
const CompressionFunction& EmptyValidityFunction();
const CompressionFunction* UncompressedFunction(duckdb::PhysicalType physical);
const CompressionFunction* RleFunction(duckdb::PhysicalType physical);
const CompressionFunction* BitpackingFunction(duckdb::PhysicalType physical);
const CompressionFunction* AlpFunction(duckdb::PhysicalType physical);
const CompressionFunction* AlpRdFunction(duckdb::PhysicalType physical);
const CompressionFunction* RoaringFunction(duckdb::PhysicalType physical);

bool CompressRoaringValidity(ColumnDataCheckpointData& checkpoint_data,
                             std::span<const WriteChunk> chunks,
                             duckdb::idx_t limit);

const CompressionFunction* GetCodec(CodecType type,
                                    duckdb::PhysicalType physical);

const Codec* FindCodec(std::string_view name) noexcept;

std::string_view NameOf(CodecType type) noexcept;

const StringChoice* ChoiceOf(CodecType type) noexcept;

CodecType TypeOf(StringChoice choice) noexcept;

uint8_t MaxLevel(CodecType type) noexcept;

}  // namespace irs::codecs
