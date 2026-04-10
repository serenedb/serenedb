////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/functional/function_ref.h>

#include <duckdb.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>

#include "basics/message_buffer.h"
#include "query/config.h"

namespace sdb::pg {

enum class VarFormat : int16_t {
  Text = 0,
  Binary = 1,
};

struct SerializationContext {
  message::Buffer* buffer;
  int8_t extra_float_digits = 0;
  ByteaOutput bytea_output;
  std::shared_ptr<const catalog::Snapshot> snapshot;
};

// TODO: consider optimizing with type-switch + UnifiedVectorFormat per column
// instead of RecursiveUnifiedVectorFormat (avoids recursive child traversal
// for scalar types, and can lazily create child format for arrays).
using DuckDBSerializationFunction = void (*)(
  SerializationContext context,
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row);

void FillContext(const Config& config, SerializationContext& context);

DuckDBSerializationFunction GetDuckDBSerialization(
  const duckdb::LogicalType& type, VarFormat format);

template<bool NeedArrayEscaping>
void ByteaOutHex(char* buf, std::string_view value);

template<bool NeedArrayEscaping>
void ByteaOutEscape(char* buf, std::string_view value);

template<bool InArray>
size_t ByteaOutEscapeLength(std::string_view value);

}  // namespace sdb::pg
