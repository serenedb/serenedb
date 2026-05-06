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

#include "basics/containers/node_hash_map.h"
#include "basics/message_buffer.h"
#include "query/config.h"

namespace sdb::pg {

enum class VarFormat : int16_t {
  Text = 0,
  Binary = 1,
};

using SerializationFunction = void (*)(
  struct SerializationContext context,
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row);

struct RecordSerializers {
  std::vector<SerializationFunction> text_in_record;  // for text path
  std::vector<SerializationFunction> binary_fns;      // for binary path
  std::vector<int32_t> binary_oids;                   // binary header OIDs
  bool text_built = false;
  bool binary_built = false;
};

struct TypesSerializationCache {
  containers::NodeHashMap<const void*, RecordSerializers> by_type;
};

// Escape sequences for emitting one literal '"' or '\\' byte at the current
// position in the wrap stack. Default (top-level): a single byte each. Each
// containing wrap rewrites them via EnterArrayWrap / EnterRecordWrap so
// that nested wraps produce PG-conformant escape sequences without a scratch
// buffer.
struct SerializationContext {
  message::Buffer* buffer;
  int8_t extra_float_digits = 0;
  ByteaOutput bytea_output;
  const catalog::Snapshot* snapshot = nullptr;
  std::string_view quote_seq{"\"", 1};
  std::string_view backslash_seq{"\\", 1};
  // Optional per-portal cache of struct-field dispatch plans. Owned by the
  // portal; nullptr disables caching (each call recomputes).
  TypesSerializationCache* types_cache = nullptr;
};

void FillContext(const Config& config, SerializationContext& context);

// `in_record`: if true, return the in-record-field variant directly (no
// SerializeNullable length prefix). Used by SerializeRecord's text path to
// emit each field's bytes inline at the current escape depth.
SerializationFunction GetSerialization(const duckdb::LogicalType& type,
                                       VarFormat format,
                                       SerializationContext& context,
                                       bool in_record = false);

template<bool NeedArrayEscaping>
void ByteaOutHex(char* buf, std::string_view value);

template<bool NeedArrayEscaping>
void ByteaOutEscape(char* buf, std::string_view value);

template<bool InArray>
size_t ByteaOutEscapeLength(std::string_view value);

}  // namespace sdb::pg
