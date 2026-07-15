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
#include <unicode/timezone.h>

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
  // PG COPY text field renderer: reuses the Text value renderers but, driven by
  // SerializationContext::copy_text, backslash-escapes the field body inline
  // (no int32 length prefix; NULL is emitted as the two bytes \N by the
  // caller's wrapper). Field/row framing (tab between columns, newline per row)
  // is done by WriteCopyChunk<CopyText>, not the serializer.
  CopyText = 2,
};

// TODO: consider optimizing with type-switch + UnifiedVectorFormat per column
// instead of RecursiveUnifiedVectorFormat (avoids recursive child traversal
// for scalar types, and can lazily create child format for arrays).
using SerializationFunction = bool (*)(
  struct SerializationContext& context,
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row);

struct RecordSerializers {
  std::vector<SerializationFunction> functions;
  std::vector<int32_t> oids;  // populated for binary, empty for text
};

using TypesSerializationCache =
  containers::NodeHashMap<const duckdb::LogicalType*, RecordSerializers>;

struct SerializationContext {
  message::Writer* writer = nullptr;
  int8_t extra_float_digits = 0;
  ByteaOutput bytea_output;
  const catalog::Snapshot* snapshot = nullptr;
  std::string_view quote_seq = "\"";  // can be mixed with backslashes
  uint32_t backslash_count = 1;
  bool in_record = false;
  // PG COPY text mode: EmitEscaped additionally backslash-escapes the COPY
  // control bytes (\b \f \n \r \t \v). Set together with backslash_count = 2
  // (the COPY base, so a literal backslash becomes \\ and array/record depth
  // doubling composes on top), so the existing Text renderers produce COPY-text
  // output without a second pass.
  bool copy_text = false;
  char copy_delim = '\t';
  std::string copy_null = "\\N";
  // Null means UTC (the default) and keeps the cheap ToString + "+00" path.
  std::unique_ptr<icu::TimeZone> time_zone;
  std::unique_ptr<TypesSerializationCache> types_cache;
  std::vector<duckdb::RecursiveUnifiedVectorFormat> decoded;
};

// UTC fast-path spellings; other zero-offset aliases just take the ICU path.
inline bool IsUtcTimeZoneName(std::string_view name) noexcept {
  return name == "UTC" || name == "GMT" || name == "Etc/UTC";
}

void FillContext(const Config& config, SerializationContext& context);

SerializationFunction GetSerialization(const duckdb::LogicalType& type,
                                       VarFormat format,
                                       SerializationContext& context);

void ByteaOutHex(char* buf, std::string_view value);

void ByteaOutEscape(char* buf, std::string_view value);

size_t ByteaOutEscapeLength(std::string_view value);

}  // namespace sdb::pg
