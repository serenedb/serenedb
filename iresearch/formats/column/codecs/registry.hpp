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
#include <duckdb/function/compression_function.hpp>
#include <optional>

#include "iresearch/formats/column/codecs/byte_codec.hpp"

namespace irs::codecs {

const duckdb::CompressionFunction& DictLz4Function();
const duckdb::CompressionFunction& Lz4Function();
const duckdb::CompressionFunction& DictZstdFunction();
const duckdb::CompressionFunction& ZstdFunction();
const duckdb::CompressionFunction& DictZxcFunction();
const duckdb::CompressionFunction& ZxcFunction();
const duckdb::CompressionFunction& DictFsstFunction();
const duckdb::CompressionFunction& FsstFunction();
const duckdb::CompressionFunction* NumericFunction(
  duckdb::PhysicalType physical);
const duckdb::CompressionFunction* SequenceFunction(
  duckdb::PhysicalType physical);

const duckdb::CompressionFunction* GetCodec(duckdb::DatabaseInstance& db,
                                            duckdb::CompressionType type,
                                            duckdb::PhysicalType physical);

std::optional<StringChoice> ChoiceOf(duckdb::CompressionType type);

duckdb::CompressionType TypeOf(StringChoice choice) noexcept;

uint8_t MaxLevel(duckdb::CompressionType type);

}  // namespace irs::codecs
