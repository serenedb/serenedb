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

#include <array>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/formats/csv_encoder.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <tz_calendar.hpp>
#include <vector>

#include "pg/catalog/utils/session.h"
#include "query/config.h"
#include "server/utils/message_buffer.h"

namespace duckdb {

class ZoneLUT;

}  // namespace duckdb
namespace sdb::pg {

enum class VarFormat : int16_t {
  Text = 0,
  Binary = 1,
  // PG COPY text field renderer: reuses the Text value renderers but, driven by
  // SerializationContext::copy_text, backslash-escapes the field body inline
  // (no int32 length prefix; NULL is emitted as the two bytes \N by the
  // caller's wrapper). Field/row framing (tab between columns, newline per row)
  // is done by ChunkSerializer, not the serializer.
  CopyText = 2,
  CopyCsv = 3,
  CopyCsvQuoted = 4,
};

// TODO: consider optimizing with type-switch + UnifiedVectorFormat per column
// instead of RecursiveUnifiedVectorFormat (avoids recursive child traversal
// for scalar types, and can lazily create child format for arrays).
using SerializationFunction = bool (*)(
  struct SerializationContext& context,
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row);

enum class ArrayWrap : uint8_t {
  ByValue,
  Never,
  Always,
};

struct RecordSerializers {
  std::vector<SerializationFunction> functions;
  std::vector<uint64_t> oids;  // populated for binary, empty for text
  ArrayWrap array_wrap = ArrayWrap::ByValue;
};

using TypesSerializationCache =
  irs::containers::NodeHashMap<const duckdb::LogicalType*, RecordSerializers>;

struct SerializationCaches {
  TypesSerializationCache types;
  std::unique_ptr<const Session> session;
};

std::array<duckdb::CsvSpecials, 4> TextEscapes(bool copy_text, char delim);

struct SerializationSettings {
  int8_t extra_float_digits = 0;
  ByteaOutput bytea_output;
  // The session a regclass / regnamespace rendering resolves through.
  duckdb::ClientContext* client = nullptr;
  std::string_view quote_seq = "\"";  // can be mixed with backslashes
  uint32_t backslash_count = 1;
  // PG COPY text mode: EmitEscaped additionally backslash-escapes the COPY
  // control bytes (\b \f \n \r \t \v). Set together with backslash_count = 2
  // (the COPY base, so a literal backslash becomes \\ and array/record depth
  // doubling composes on top), so the existing Text renderers produce COPY-text
  // output without a second pass.
  bool copy_text = false;
  char copy_delim = '\t';
  std::string copy_null = "\\N";
  std::string copy_newline = "\n";
  char csv_quote = '"';
  char csv_escape = '"';
  bool csv_single_column = false;
  duckdb::CsvSpecials csv_specials;
  std::array<duckdb::CsvSpecials, 4> text_escapes = TextEscapes(false, '\t');
  // Null means UTC (the default) and keeps the cheap ToString + "+00" path.
  duckdb::shared_ptr<const duckdb::TimeZone> time_zone;
  duckdb::shared_ptr<const duckdb::ZoneLUT> zone_lut;
};

struct SerializationContext : SerializationSettings {
  message::Writer* writer = nullptr;
  bool in_record = false;
  std::string csv_scratch;
  std::unique_ptr<SerializationCaches> caches;
  std::vector<duckdb::RecursiveUnifiedVectorFormat> decoded;
};

inline constexpr size_t kMaxMessageSize = std::numeric_limits<int32_t>::max();

inline constexpr size_t kIntStrMaxLen = 22;

[[noreturn]] void ThrowMessageTooLarge();

IRS_FORCE_INLINE inline void CheckMessageSize(size_t size) {
  if (size > kMaxMessageSize) [[unlikely]] {
    ThrowMessageTooLarge();
  }
}

inline bool IsUtcTimeZoneName(std::string_view name) noexcept {
  return name == "UTC" || name == "GMT" || name == "Etc/UTC";
}

void FillContext(duckdb::ClientContext& client, SerializationContext& context);

SerializationFunction GetSerialization(const duckdb::LogicalType& type,
                                       VarFormat format,
                                       SerializationContext& context);

struct RowShape {
  char tag = '\0';
  bool field_count = false;
  bool delimited = false;
  std::string_view first;
  std::string_view before;
  std::string_view after;
};

struct ColumnSerializer {
  using Write = void (*)(SerializationContext& context,
                         const duckdb::RecursiveUnifiedVectorFormat& vdata,
                         duckdb::idx_t begin, duckdb::idx_t end,
                         uint8_t** cursors);
  using Bound = void (*)(const SerializationContext& context,
                         const duckdb::RecursiveUnifiedVectorFormat& vdata,
                         duckdb::idx_t begin, duckdb::idx_t end,
                         size_t* bounds);

  Write write = nullptr;
  Bound bound = nullptr;
  size_t fixed = 0;
  SerializationFunction field = nullptr;
};

class ChunkSerializer {
 public:
  ChunkSerializer(std::span<const duckdb::LogicalType> types,
                  std::span<const VarFormat> formats,
                  SerializationContext& context);

  void Write(message::Buffer& out, SerializationContext& context,
             const duckdb::DataChunk& chunk, duckdb::idx_t begin,
             duckdb::idx_t end, const RowShape& shape);

 private:
  struct ScratchField {
    message::Chunk* chunk;
    size_t offset;
    size_t size;
  };

  size_t WriteRows(SerializationContext& context, const RowShape& shape,
                   uint8_t* base, duckdb::idx_t begin, size_t first,
                   size_t last, size_t count);

  std::vector<ColumnSerializer> _columns;
  size_t _fixed = 0;
  std::vector<size_t> _bounds;
  std::vector<uint8_t*> _cursors;
  std::vector<ScratchField> _fields;
  message::Buffer _scratch{1 << 10, 1 << 20};
};

void SetCopyText(SerializationContext& context, char delim,
                 std::string null_str);

void SerializeCopyTextHeader(SerializationContext& context,
                             std::span<const duckdb::Identifier> names);

void SetCopyCsv(SerializationContext& context, char delim, char quote,
                char escape, std::string null_str, std::string new_line,
                bool single_column);

void SerializeCopyCsvHeader(SerializationContext& context,
                            std::span<const duckdb::Identifier> names);

void ByteaOutHex(char* buf, std::string_view value);

void ByteaOutEscape(char* buf, std::string_view value);

size_t ByteaOutEscapeLength(std::string_view value);

}  // namespace sdb::pg
