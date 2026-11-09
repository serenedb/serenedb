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

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"

namespace irs::codecs {

const CompressionFunction& DictLz4Function();
const CompressionFunction& Lz4Function();
const CompressionFunction& DictZstdFunction();
const CompressionFunction& ZstdFunction();
const CompressionFunction& DictZxcFunction();
const CompressionFunction& ZxcFunction();
const CompressionFunction& DictFsstFunction();
const CompressionFunction& FsstFunction();
const CompressionFunction& DictFsstPlusFunction();
const CompressionFunction& FsstPlusFunction();
const CompressionFunction& DictOnPairFunction();
const CompressionFunction& OnPairFunction();
const CompressionFunction& DictFsst12Function();
const CompressionFunction& Fsst12Function();
const CompressionFunction* NumericFunction(duckdb::PhysicalType physical);
const CompressionFunction* ConstantFunction(duckdb::PhysicalType physical);
const CompressionFunction& EmptyValidityFunction();
const CompressionFunction* UncompressedFunction(duckdb::PhysicalType physical);
const CompressionFunction* RleFunction(duckdb::PhysicalType physical);
const CompressionFunction* BitpackingFunction(duckdb::PhysicalType physical);
const CompressionFunction* AlpFunction(duckdb::PhysicalType physical);
const CompressionFunction* AlpRdFunction(duckdb::PhysicalType physical);
const CompressionFunction* RoaringFunction(duckdb::PhysicalType physical);

const CompressionFunction* GetCodec(duckdb::CompressionType type,
                                    duckdb::PhysicalType physical);

std::span<const CompressionFunction* const> GetCompressionFunctions(
  duckdb::PhysicalType physical);

const StringChoice* ChoiceOf(duckdb::CompressionType type);

duckdb::CompressionType TypeOf(StringChoice choice) noexcept;

uint8_t MaxLevel(duckdb::CompressionType type);

}  // namespace irs::codecs
