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

#include "network/pg/wire_frames.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <cstdint>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/types/vector.hpp>
#include <utility>
#include <vector>

#include "basics/dtoa.h"
#include "pg/command_tag.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/protocol.h"
#include "pg/sql_exception.h"
#include "pg/sql_utils.h"
#include "query/utils.h"

namespace sdb::network::pg {
namespace {

constexpr std::string_view kNull{"\0", 1};

// Backend frame layout: [type:1][length:int32][body]. The int32 length counts
// itself plus the body but never the type tag, so a length backpatch is
// uncommitted-size - frame-start - kFrameTag.
constexpr size_t kFrameTag = 1;  // leading message-type byte
constexpr size_t kInt16 = 2;     // a big-endian int16 wire field
constexpr size_t kInt32 = 4;     // a big-endian int32 wire field
constexpr size_t kFrameHeader = kFrameTag + kInt32;  // type byte + length

int DuckExceptionToErrcode(duckdb::ExceptionType type) {
  switch (type) {
    case duckdb::ExceptionType::SYNTAX:
    case duckdb::ExceptionType::PARSER:
    case duckdb::ExceptionType::EXPRESSION:
      return ERRCODE_SYNTAX_ERROR;
    case duckdb::ExceptionType::CATALOG:
      return ERRCODE_UNDEFINED_TABLE;
    case duckdb::ExceptionType::BINDER:
      return ERRCODE_UNDEFINED_COLUMN;
    case duckdb::ExceptionType::CONVERSION:
      return ERRCODE_INVALID_TEXT_REPRESENTATION;
    case duckdb::ExceptionType::MISMATCH_TYPE:
    case duckdb::ExceptionType::INVALID_TYPE:
    case duckdb::ExceptionType::DECIMAL:
      return ERRCODE_DATA_EXCEPTION;
    case duckdb::ExceptionType::OUT_OF_RANGE:
      return ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
    // Bad input value/argument, setting value, or configuration are user
    // errors, not internal_error (XX000) -- e.g. SET with an uncastable value,
    // or an out-of-range option. PG report invalid_parameter_value here.
    case duckdb::ExceptionType::INVALID_INPUT:
    case duckdb::ExceptionType::SETTINGS:
    case duckdb::ExceptionType::INVALID_CONFIGURATION:
      return ERRCODE_INVALID_PARAMETER_VALUE;
    case duckdb::ExceptionType::DIVIDE_BY_ZERO:
      return ERRCODE_DIVISION_BY_ZERO;
    case duckdb::ExceptionType::CONSTRAINT:
      return ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION;
    case duckdb::ExceptionType::PERMISSION:
      return ERRCODE_INSUFFICIENT_PRIVILEGE;
    case duckdb::ExceptionType::TRANSACTION:
      return ERRCODE_T_R_SERIALIZATION_FAILURE;
    case duckdb::ExceptionType::OUT_OF_MEMORY:
      return ERRCODE_OUT_OF_MEMORY;
    case duckdb::ExceptionType::NOT_IMPLEMENTED:
    case duckdb::ExceptionType::MISSING_EXTENSION:
    case duckdb::ExceptionType::AUTOLOAD:
      return ERRCODE_FEATURE_NOT_SUPPORTED;
    case duckdb::ExceptionType::PARAMETER_NOT_RESOLVED:
    case duckdb::ExceptionType::PARAMETER_NOT_ALLOWED:
      return ERRCODE_INDETERMINATE_DATATYPE;
    case duckdb::ExceptionType::CONNECTION:
    case duckdb::ExceptionType::IO:
    case duckdb::ExceptionType::NETWORK:
    case duckdb::ExceptionType::HTTP:
      return ERRCODE_CONNECTION_EXCEPTION;
    case duckdb::ExceptionType::INTERRUPT:
      return ERRCODE_QUERY_CANCELED;
    default:
      return ERRCODE_INTERNAL_ERROR;
  }
}

// Refine the SQLSTATE from the structured extra_info DuckDB attaches to
// catalog/binder errors: the bare ExceptionType collapses undefined_table /
// undefined_column / undefined_function / undefined_schema (and the duplicate_*
// counterparts) into one code each, which PG distinguishes.
int DuckExceptionToErrcode(const duckdb::ErrorData& error) {
  const auto& extra = error.ExtraInfo();
  const auto subtype_it = extra.find("error_subtype");
  const std::string_view subtype =
    subtype_it != extra.end() ? subtype_it->second : std::string_view{};
  if (error.Type() == duckdb::ExceptionType::CATALOG) {
    const auto kind_it = extra.find("type");
    const std::string_view kind =
      kind_it != extra.end() ? kind_it->second : std::string_view{};
    const bool dup = subtype == "ENTRY_ALREADY_EXISTS";
    if (kind == "Schema") {
      return dup ? ERRCODE_DUPLICATE_SCHEMA : ERRCODE_UNDEFINED_SCHEMA;
    }
    if (kind == "Database") {
      return dup ? ERRCODE_DUPLICATE_DATABASE : ERRCODE_UNDEFINED_DATABASE;
    }
    if (kind == "Column") {
      return dup ? ERRCODE_DUPLICATE_COLUMN : ERRCODE_UNDEFINED_COLUMN;
    }
    // "Scalar Function", "Aggregate Function", "Table Function", ...
    if (kind.find("Function") != std::string_view::npos) {
      return dup ? ERRCODE_DUPLICATE_FUNCTION : ERRCODE_UNDEFINED_FUNCTION;
    }
    if (kind == "Type") {
      return dup ? ERRCODE_DUPLICATE_OBJECT : ERRCODE_UNDEFINED_OBJECT;
    }
    if (kind == "configuration parameter") {
      // Unknown GUC -- DuckDB throws CatalogException::MissingEntry(
      // "configuration parameter", ...) for SET / RESET / current_setting. PG
      // reports undefined_object (42704), not undefined_table.
      return ERRCODE_UNDEFINED_OBJECT;
    }
    // Table / View / Sequence / Index and the untyped fallback: a relation.
    return dup ? ERRCODE_DUPLICATE_TABLE : ERRCODE_UNDEFINED_TABLE;
  }
  if (error.Type() == duckdb::ExceptionType::BINDER) {
    if (subtype == "NO_MATCHING_FUNCTION") {
      return ERRCODE_UNDEFINED_FUNCTION;
    }
    if (subtype == "UNSUPPORTED") {
      return ERRCODE_FEATURE_NOT_SUPPORTED;
    }
    const std::string_view binder_msg = error.RawMessage();
    if (absl::StrContainsIgnoreCase(binder_msg, "ambiguous")) {
      return ERRCODE_AMBIGUOUS_COLUMN;
    }
    if (absl::StrContainsIgnoreCase(binder_msg,
                                    "must appear in the GROUP BY")) {
      return ERRCODE_GROUPING_ERROR;
    }
    return ERRCODE_UNDEFINED_COLUMN;
  }
  if (error.Type() == duckdb::ExceptionType::CONSTRAINT) {
    const std::string_view msg = error.RawMessage();
    if (absl::StrContainsIgnoreCase(msg, "not null")) {
      return ERRCODE_NOT_NULL_VIOLATION;
    }
    if (absl::StrContainsIgnoreCase(msg, "foreign key")) {
      return ERRCODE_FOREIGN_KEY_VIOLATION;
    }
    if (absl::StrContainsIgnoreCase(msg, "check constraint")) {
      return ERRCODE_CHECK_VIOLATION;
    }
    if (absl::StrContainsIgnoreCase(msg, "unique") ||
        absl::StrContainsIgnoreCase(msg, "primary key") ||
        absl::StrContainsIgnoreCase(msg, "duplicate")) {
      return ERRCODE_UNIQUE_VIOLATION;
    }
    return ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION;
  }
  return DuckExceptionToErrcode(error.Type());
}

void WriteNoticeField(message::Writer& w, char tag, std::string_view value) {
  w.Write({&tag, 1});
  w.Write(value);
  w.Write(kNull);
}

}  // namespace

void WriteDataChunk(message::Buffer& out, const duckdb::DataChunk& chunk,
                    std::span<const sdb::pg::SerializationFunction> serializers,
                    sdb::pg::SerializationContext& context, duckdb::idx_t begin,
                    duckdb::idx_t end) {
  const auto columns = static_cast<uint16_t>(chunk.ColumnCount());
  auto& decoded = context.decoded;
  decoded.clear();
  decoded.resize(columns);
  for (uint16_t column = 0; column < columns; ++column) {
    duckdb::Vector::RecursiveToUnifiedFormat(chunk.data[column], chunk.size(),
                                             decoded[column]);
  }
  // One 'D' message per row, each carrying its own length + field count. The
  // Writer rewinds the row if a serializer throws partway (no truncated bytes
  // reach the client).
  for (duckdb::idx_t row = begin; row < end; ++row) {
    message::Writer writer{out};
    context.writer = &writer;
    auto* prefix = writer.Alloc(kFrameHeader + kInt16);
    for (uint16_t column = 0; column < columns; ++column) {
      serializers[column](context, decoded[column], row);
    }
    prefix[0] = PQ_MSG_DATA_ROW;
    absl::big_endian::Store32(
      prefix + kFrameTag, static_cast<int32_t>(writer.Written() - kFrameTag));
    absl::big_endian::Store16(prefix + kFrameHeader, columns);
    writer.Commit(false);
  }
}

template<RowEncoding Encoding>
void WriteCopyChunk(message::Buffer& out, const duckdb::DataChunk& chunk,
                    std::span<const sdb::pg::SerializationFunction> serializers,
                    sdb::pg::SerializationContext& context) {
  const auto rows = chunk.size();
  if (rows == 0) {
    return;
  }
  const auto columns = static_cast<uint16_t>(chunk.ColumnCount());
  auto& decoded = context.decoded;
  decoded.clear();
  decoded.resize(columns);
  for (uint16_t column = 0; column < columns; ++column) {
    duckdb::Vector::RecursiveToUnifiedFormat(chunk.data[column], rows,
                                             decoded[column]);
  }
  // The whole chunk's rows go in one CopyData ('d') frame whose length is
  // backpatched after the rows. The frame is atomic: if a row throws partway,
  // the Writer rewinds the entire partial frame.
  message::Writer writer{out};
  context.writer = &writer;
  auto* prefix = writer.Alloc(kFrameHeader);
  for (duckdb::idx_t row = 0; row < rows; ++row) {
    if constexpr (Encoding == RowEncoding::CopyBinary) {
      // PGCOPY row: int16 field count, then the binary fields.
      absl::big_endian::Store16(writer.Alloc(kInt16), columns);
      for (uint16_t column = 0; column < columns; ++column) {
        serializers[column](context, decoded[column], row);
      }
    } else {
      static_assert(Encoding == RowEncoding::CopyText,
                    "WriteCopyChunk is for the COPY encodings only");
      // PG-text row: TAB-separated fields, terminated by \n.
      for (uint16_t column = 0; column < columns; ++column) {
        if (column > 0) {
          writer.Write({&context.copy_delim, 1});
        }
        serializers[column](context, decoded[column], row);
      }
      writer.Write("\n");
    }
  }
  prefix[0] = PQ_MSG_COPY_DATA;
  absl::big_endian::Store32(prefix + kFrameTag,
                            static_cast<int32_t>(writer.Written() - kFrameTag));
  writer.Commit(false);
}

template void WriteCopyChunk<RowEncoding::CopyBinary>(
  message::Buffer&, const duckdb::DataChunk&,
  std::span<const sdb::pg::SerializationFunction>,
  sdb::pg::SerializationContext&);
template void WriteCopyChunk<RowEncoding::CopyText>(
  message::Buffer&, const duckdb::DataChunk&,
  std::span<const sdb::pg::SerializationFunction>,
  sdb::pg::SerializationContext&);

namespace {

// PGCOPY header: 11-byte signature, int32 flags (0), int32 header-extension
// length (0).
constexpr std::array<uint8_t, 19> kPgCopyHeader{
  'P',  'G', 'C', 'O', 'P', 'Y', '\n', 0xFF, '\r', '\n',
  0x00, 0,   0,   0,   0,   0,   0,    0,    0};

}  // namespace

void WriteCopyOutResponse(message::Buffer& out, bool binary, int16_t columns) {
  message::Writer w{out};
  const auto code = static_cast<int16_t>(binary ? 1 : 0);
  const auto formats_bytes =
    columns > 0 ? static_cast<size_t>(columns) * kInt16 : size_t{0};
  auto* data = w.Alloc(kFrameHeader + 1 + kInt16 + formats_bytes);
  data[0] = PQ_MSG_COPY_OUT_RESPONSE;
  absl::big_endian::Store32(
    data + kFrameTag,
    static_cast<int32_t>(kInt32 + 1 + kInt16 + formats_bytes));
  data[kFrameHeader] = binary ? 1 : 0;  // overall format
  absl::big_endian::Store16(data + kFrameHeader + 1, columns);  // column count
  for (int16_t i = 0; i < columns; ++i) {
    absl::big_endian::Store16(
      data + kFrameHeader + 1 + kInt16 + static_cast<size_t>(i) * kInt16, code);
  }
  w.Commit(false);
}

void WriteCopyBinaryHeader(message::Buffer& out) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  w.Write({reinterpret_cast<const char*>(kPgCopyHeader.data()),
           kPgCopyHeader.size()});
  prefix[0] = PQ_MSG_COPY_DATA;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteCopyTextHeader(message::Buffer& out,
                         std::span<const duckdb::Identifier> names,
                         char delim) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  bool first = true;
  for (const auto& name : names) {
    if (!first) {
      *w.Alloc(1) = delim;
    }
    first = false;
    for (const char c : name.GetIdentifierName()) {
      switch (c) {
        case '\\':
          w.Write("\\\\");
          break;
        case '\n':
          w.Write("\\n");
          break;
        case '\r':
          w.Write("\\r");
          break;
        case '\t':
          w.Write("\\t");
          break;
        default:
          if (c == delim) {
            *w.Alloc(1) = '\\';
          }
          *w.Alloc(1) = c;
      }
    }
  }
  *w.Alloc(1) = '\n';
  prefix[0] = PQ_MSG_COPY_DATA;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteCopyBinaryFooter(message::Buffer& out) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  absl::big_endian::Store16(w.Alloc(kInt16), static_cast<int16_t>(-1));
  prefix[0] = PQ_MSG_COPY_DATA;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteCopyDone(message::Buffer& out) {
  message::Writer w{out};
  auto* data = w.Alloc(kFrameHeader);
  data[0] = PQ_MSG_COPY_DONE;
  absl::big_endian::Store32(data + kFrameTag, kInt32);
  w.Commit(false);
}

void WriteParameterStatus(message::Buffer& out, std::string_view name,
                          std::string_view value) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  w.Write(name);
  w.Write(kNull);
  w.Write(value);
  w.Write(kNull);
  prefix[0] = PQ_MSG_PARAMETER_STATUS;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteRowDescription(message::Buffer& out,
                         std::span<const duckdb::LogicalType> types,
                         std::span<const duckdb::Identifier> names,
                         std::span<const sdb::pg::VarFormat> formats) {
  message::Writer w{out};
  const auto num_fields = static_cast<uint16_t>(types.size());
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader + kInt16);
  const auto default_format =
    formats.empty() ? sdb::pg::VarFormat::Text : formats[0];
  for (uint16_t i = 0; i < num_fields; ++i) {
    // ToAlias is a pure view (substr of the name); write it straight out,
    // no per-query vector<string> of owned copies.
    w.Write(query::ToAlias(names[i].GetIdentifierName()));
    w.Write(kNull);
    absl::big_endian::Store32(w.Alloc(kInt32), 0);
    absl::big_endian::Store16(w.Alloc(kInt16), 0);
    const auto type_info = sdb::pg::Logical2Pg(types[i]);
    absl::big_endian::Store32(w.Alloc(kInt32), type_info.oid);
    absl::big_endian::Store16(w.Alloc(kInt16), type_info.typlen);
    absl::big_endian::Store32(w.Alloc(kInt32), type_info.typmod);
    const auto format = i < formats.size() ? formats[i] : default_format;
    absl::big_endian::Store16(w.Alloc(kInt16), std::to_underlying(format));
  }
  prefix[0] = PQ_MSG_ROW_DESCRIPTION;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  absl::big_endian::Store16(prefix + kFrameHeader, num_fields);
  w.Commit(false);
}

void WriteCommandComplete(message::Buffer& out,
                          const duckdb::PreparedStatement& prepared,
                          uint64_t rows) {
  WriteCommandComplete(out, sdb::pg::BuildCommandTag(prepared), rows);
}

void WriteCommandComplete(message::Buffer& out, const sdb::pg::CommandTag& tag,
                          uint64_t rows) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  w.Write(tag.tag);
  if (tag.rowcount) {
    // PG appends the affected/returned count straight into the frame: " 0 <n>"
    // for INSERT (the legacy oid field, always 0), " <n>" for the rest.
    w.Write(basics::kIntStrMaxLen + 3, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      char* ptr = buf;
      *ptr++ = ' ';
      if (tag.effective_type == duckdb::StatementType::INSERT_STATEMENT) {
        *ptr++ = '0';
        *ptr++ = ' ';
      }
      ptr = absl::numbers_internal::FastIntToBuffer(rows, ptr);
      return static_cast<size_t>(ptr - buf);
    });
  }
  w.Write(kNull);
  prefix[0] = PQ_MSG_COMMAND_COMPLETE;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteDiagnostic(message::Buffer& out, char type, std::string_view severity,
                     const sdb::pg::SqlErrorData& data) {
  message::Writer w{out};
  char sql_state[sdb::pg::kSqlStateSize];
  sdb::pg::UnpackSqlState(sql_state, data.errcode);
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader);
  WriteNoticeField(w, 'S', severity);
  WriteNoticeField(w, 'V', severity);
  WriteNoticeField(w, 'C', {sql_state, sdb::pg::kSqlStateSize});
  WriteNoticeField(w, 'M', data.errmsg);
  if (!data.errdetail.empty()) {
    WriteNoticeField(w, 'D', data.errdetail);
  }
  if (!data.errhint.empty()) {
    WriteNoticeField(w, 'H', data.errhint);
  }
  if (!data.context.empty()) {
    WriteNoticeField(w, 'W', data.context);
  }
  if (data.cursorpos > 0) {
    WriteNoticeField(w, 'P', absl::StrCat(data.cursorpos));
  }
  w.Write(kNull);
  prefix[0] = static_cast<uint8_t>(type);
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

void WriteErrorResponse(message::Buffer& out,
                        const sdb::pg::SqlErrorData& error) {
  WriteDiagnostic(out, PQ_MSG_ERROR_RESPONSE, "ERROR", error);
}

void WriteFatalResponse(message::Buffer& out,
                        const sdb::pg::SqlErrorData& error) {
  WriteDiagnostic(out, PQ_MSG_ERROR_RESPONSE, "FATAL", error);
}

void WriteNoticeResponse(message::Buffer& out,
                         const sdb::pg::SqlErrorData& notice) {
  WriteDiagnostic(out, PQ_MSG_NOTICE_RESPONSE, "WARNING", notice);
}

sdb::pg::SqlErrorData DuckErrorToSqlData(const duckdb::ErrorData& error) {
  // DuckDB reports running a statement in an aborted explicit transaction as a
  // generic TransactionException (40001 by type); postgres flags it 25P02. The
  // message is the stable ErrorManager::INVALIDATED_TRANSACTION text.
  if (error.Type() == duckdb::ExceptionType::TRANSACTION &&
      error.RawMessage().starts_with("current transaction is aborted")) {
    return sdb::pg::SqlErrorData{
      .errcode = ERRCODE_IN_FAILED_SQL_TRANSACTION,
      .errmsg = error.RawMessage(),
    };
  }
  // An interrupted query is DuckDB "Interrupted!"; report postgres's wording.
  const bool interrupted = error.Type() == duckdb::ExceptionType::INTERRUPT;
  sdb::pg::SqlErrorData data{
    .errcode = DuckExceptionToErrcode(error),
    .errmsg = interrupted ? "canceling statement due to user request"
                          : error.RawMessage(),
  };
  // PG's error Position is a 1-based offset into the query; DuckDB records a
  // 0-based one in extra_info["position"]. This is a byte offset -- exact for
  // ASCII; PG counts characters, so a multibyte query would need the query
  // text to convert it precisely (left as a followup).
  if (auto it = error.ExtraInfo().find("position");
      it != error.ExtraInfo().end()) {
    int pos = 0;
    if (absl::SimpleAtoi(it->second, &pos)) {
      data.cursorpos = pos + 1;
    }
  }
  return data;
}

sdb::pg::SqlErrorData ToSqlError(const std::exception& exception) {
  if (const auto* sql = dynamic_cast<const sdb::SqlException*>(&exception)) {
    return sql->error();
  }
  if (const auto* duck = dynamic_cast<const duckdb::Exception*>(&exception)) {
    return DuckErrorToSqlData(duckdb::ErrorData{*duck});
  }
  return sdb::pg::SqlErrorData{.errcode = ERRCODE_INTERNAL_ERROR,
                               .errmsg = exception.what()};
}

void WriteEmptyFrame(message::Buffer& out, char type) {
  message::Writer w{out};
  auto* data = w.Alloc(kFrameHeader);
  data[0] = static_cast<uint8_t>(type);
  absl::big_endian::Store32(data + kFrameTag, kInt32);
  w.Commit(false);
}

void WriteParameterDescription(message::Buffer& out,
                               std::span<const int32_t> oids) {
  message::Writer w{out};
  const auto count = static_cast<uint16_t>(oids.size());
  auto* prefix = w.Alloc(kFrameHeader + kInt16);
  prefix[0] = PQ_MSG_PARAMETER_DESCRIPTION;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(kInt32 + kInt16 + kInt32 * count));
  absl::big_endian::Store16(prefix + kFrameHeader, count);
  for (const auto oid : oids) {
    absl::big_endian::Store32(w.Alloc(kInt32), oid);
  }
  w.Commit(false);
}

void WriteAuthRequest(message::Buffer& out, int32_t code,
                      std::string_view payload) {
  message::Writer w{out};
  auto* prefix = w.Alloc(kFrameHeader + kInt32);
  prefix[0] = PQ_MSG_AUTHENTICATION_REQUEST;
  // length excludes the type byte: 4 (length) + 4 (code) + payload.
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(kInt32 + kInt32 + payload.size()));
  absl::big_endian::Store32(prefix + kFrameHeader, code);
  w.Write(payload);
  w.Commit(false);
}

void WriteCopyInResponse(message::Buffer& out, bool binary, int16_t columns) {
  message::Writer w{out};
  const auto code = static_cast<int16_t>(binary ? 1 : 0);
  const auto formats_bytes =
    columns > 0 ? static_cast<size_t>(columns) * kInt16 : size_t{0};
  auto* data = w.Alloc(kFrameHeader + 1 + kInt16 + formats_bytes);
  data[0] = PQ_MSG_COPY_IN_RESPONSE;
  absl::big_endian::Store32(
    data + kFrameTag,
    static_cast<int32_t>(kInt32 + 1 + kInt16 + formats_bytes));
  data[kFrameHeader] = binary ? 1 : 0;  // overall format
  absl::big_endian::Store16(data + kFrameHeader + 1, columns);  // column count
  for (int16_t i = 0; i < columns; ++i) {
    absl::big_endian::Store16(
      data + kFrameHeader + 1 + kInt16 + static_cast<size_t>(i) * kInt16, code);
  }
  w.Commit(false);
}

void WriteReadyForQuery(message::Buffer& out, char txn_status) {
  message::Writer w{out};
  auto* data = w.Alloc(kFrameHeader + 1);
  data[0] = PQ_MSG_READY_FOR_QUERY;
  absl::big_endian::Store32(data + kFrameTag, kInt32 + 1);
  data[kFrameHeader] = static_cast<uint8_t>(txn_status);
  w.Commit(false);
}

void WriteNegotiateProtocolVersion(
  message::Buffer& out, int32_t protocol_version,
  std::span<const std::string> unrecognized_options) {
  message::Writer w{out};
  const auto start = w.Written();
  auto* prefix = w.Alloc(kFrameHeader + kInt32);
  absl::big_endian::Store32(prefix + kFrameHeader, protocol_version);
  absl::big_endian::Store32(w.Alloc(kInt32),
                            static_cast<int32_t>(unrecognized_options.size()));
  for (const auto& option : unrecognized_options) {
    w.Write(option);
    w.Write(kNull);
  }
  prefix[0] = PQ_MSG_NEGOTIATE_PROTOCOL_VERSION;
  absl::big_endian::Store32(
    prefix + kFrameTag, static_cast<int32_t>(w.Written() - start - kFrameTag));
  w.Commit(false);
}

}  // namespace sdb::network::pg
