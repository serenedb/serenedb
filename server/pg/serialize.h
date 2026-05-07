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

#include <duckdb/common/typedefs.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <memory>

#include "basics/containers/node_hash_map.h"
#include "basics/message_buffer.h"
#include "query/config.h"

namespace sdb::pg {

enum class VarFormat : int16_t {
  Text = 0,
  Binary = 1,
};

// TODO: consider optimizing with type-switch + UnifiedVectorFormat per column
// instead of RecursiveUnifiedVectorFormat (avoids recursive child traversal
// for scalar types, and can lazily create child format for arrays).
using SerializationFunction = void (*)(
  struct SerializationContext& context,
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row);

struct RecordSerializers {
  std::vector<SerializationFunction> fns;
  std::vector<int32_t> oids;  // populated for binary, empty for text
};

struct TypesSerializationCache {
  containers::NodeHashMap<const duckdb::LogicalType*, RecordSerializers>
    type2serializers;
};

struct SerializationContext {
  message::Buffer* buffer;
  int8_t extra_float_digits = 0;
  ByteaOutput bytea_output;
  const catalog::Snapshot* snapshot = nullptr;
  std::string_view quote_seq = "\"";  // can be mixed with backslashes
  uint32_t backslash_count = 1;
  bool in_record = false;
  std::unique_ptr<TypesSerializationCache> types_cache;
};

void FillContext(const Config& config, SerializationContext& context);

SerializationFunction GetSerialization(const duckdb::LogicalType& type,
                                       VarFormat format,
                                       SerializationContext& context);

template<bool NeedArrayEscaping>
void ByteaOutHex(char* buf, std::string_view value);

template<bool NeedArrayEscaping>
void ByteaOutEscape(char* buf, std::string_view value);

template<bool InArray>
size_t ByteaOutEscapeLength(std::string_view value);

}  // namespace sdb::pg
