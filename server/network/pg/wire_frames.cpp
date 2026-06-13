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
#include <absl/strings/str_cat.h>

#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <utility>
#include <vector>

#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/protocol.h"
#include "pg/sql_utils.h"
#include "query/utils.h"

namespace sdb::network::pg {
namespace {

constexpr std::string_view kNul{"\0", 1};

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
    case duckdb::ExceptionType::MISMATCH_TYPE:
    case duckdb::ExceptionType::INVALID_TYPE:
    case duckdb::ExceptionType::DECIMAL:
      return ERRCODE_DATA_EXCEPTION;
    case duckdb::ExceptionType::OUT_OF_RANGE:
      return ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
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

void WriteNoticeField(message::Buffer& out, char tag, std::string_view value) {
  out.WriteUncommitted({&tag, 1});
  out.WriteUncommitted(value);
  out.WriteUncommitted(kNul);
}

}  // namespace

void WriteDataChunkRange(
  message::Buffer& out, const duckdb::DataChunk& chunk,
  std::span<const sdb::pg::SerializationFunction> serializers,
  sdb::pg::SerializationContext& context, duckdb::idx_t begin,
  duckdb::idx_t end) {
  const auto rows = chunk.size();
  const auto columns = static_cast<uint16_t>(chunk.ColumnCount());
  std::vector<duckdb::RecursiveUnifiedVectorFormat> decoded(columns);
  for (uint16_t column = 0; column < columns; ++column) {
    duckdb::Vector::RecursiveToUnifiedFormat(chunk.data[column], rows,
                                             decoded[column]);
  }
  for (duckdb::idx_t row = begin; row < end; ++row) {
    const auto start = out.GetUncommittedSize();
    auto* prefix = out.GetContiguousData(7);
    for (uint16_t column = 0; column < columns; ++column) {
      serializers[column](context, decoded[column], row);
    }
    prefix[0] = PQ_MSG_DATA_ROW;
    absl::big_endian::Store32(
      prefix + 1, static_cast<int32_t>(out.GetUncommittedSize() - start - 1));
    absl::big_endian::Store16(prefix + 5, columns);
    out.Commit(false);
  }
}

void WriteDataChunk(message::Buffer& out, const duckdb::DataChunk& chunk,
                    std::span<const sdb::pg::SerializationFunction> serializers,
                    sdb::pg::SerializationContext& context) {
  WriteDataChunkRange(out, chunk, serializers, context, 0, chunk.size());
}

void WriteParameterStatus(message::Buffer& out, std::string_view name,
                          std::string_view value) {
  const auto start = out.GetUncommittedSize();
  auto* prefix = out.GetContiguousData(5);
  out.WriteUncommitted(name);
  out.WriteUncommitted(kNul);
  out.WriteUncommitted(value);
  out.WriteUncommitted(kNul);
  prefix[0] = PQ_MSG_PARAMETER_STATUS;
  absl::big_endian::Store32(
    prefix + 1, static_cast<int32_t>(out.GetUncommittedSize() - start - 1));
  out.Commit(false);
}

void WriteRowDescription(message::Buffer& out,
                         std::span<const duckdb::LogicalType> types,
                         std::span<const std::string> names,
                         std::span<const sdb::pg::VarFormat> formats) {
  const auto num_fields = static_cast<uint16_t>(types.size());
  const auto start = out.GetUncommittedSize();
  auto* prefix = out.GetContiguousData(7);
  const auto default_format =
    formats.empty() ? sdb::pg::VarFormat::Text : formats[0];
  for (uint16_t i = 0; i < num_fields; ++i) {
    // ToAlias is a pure view (substr of the name); write it straight out,
    // no per-query vector<string> of owned copies.
    out.WriteUncommitted(query::ToAlias(names[i]));
    out.WriteUncommitted(kNul);
    absl::big_endian::Store32(out.GetContiguousData(4), 0);
    absl::big_endian::Store16(out.GetContiguousData(2), 0);
    absl::big_endian::Store32(out.GetContiguousData(4),
                              sdb::pg::Type2Oid(types[i]));
    absl::big_endian::Store16(out.GetContiguousData(2),
                              static_cast<int16_t>(-1));
    absl::big_endian::Store32(out.GetContiguousData(4),
                              static_cast<int32_t>(-1));
    const auto format = i < formats.size() ? formats[i] : default_format;
    absl::big_endian::Store16(out.GetContiguousData(2),
                              std::to_underlying(format));
  }
  prefix[0] = PQ_MSG_ROW_DESCRIPTION;
  absl::big_endian::Store32(
    prefix + 1, static_cast<int32_t>(out.GetUncommittedSize() - start - 1));
  absl::big_endian::Store16(prefix + 5, num_fields);
  out.Commit(false);
}

void WriteCommandComplete(message::Buffer& out, std::string_view tag) {
  const auto start = out.GetUncommittedSize();
  auto* prefix = out.GetContiguousData(5);
  out.WriteUncommitted(tag);
  out.WriteUncommitted(kNul);
  prefix[0] = PQ_MSG_COMMAND_COMPLETE;
  absl::big_endian::Store32(
    prefix + 1, static_cast<int32_t>(out.GetUncommittedSize() - start - 1));
  out.Commit(false);
}

void WriteErrorResponse(message::Buffer& out, std::string_view message,
                        std::string_view sqlstate) {
  const auto start = out.GetUncommittedSize();
  auto* prefix = out.GetContiguousData(5);
  out.WriteUncommitted("SERROR");
  out.WriteUncommitted(kNul);
  out.WriteUncommitted("VERROR");
  out.WriteUncommitted(kNul);
  out.WriteUncommitted("C");
  out.WriteUncommitted(sqlstate);
  out.WriteUncommitted(kNul);
  out.WriteUncommitted("M");
  out.WriteUncommitted(message);
  out.WriteUncommitted(kNul);
  out.WriteUncommitted(kNul);
  prefix[0] = PQ_MSG_ERROR_RESPONSE;
  absl::big_endian::Store32(
    prefix + 1, static_cast<int32_t>(out.GetUncommittedSize() - start - 1));
  out.Commit(false);
}

void WriteDiagnostic(message::Buffer& out, char type, std::string_view severity,
                     const sdb::pg::SqlErrorData& data) {
  char sql_state[sdb::pg::kSqlStateSize];
  sdb::pg::UnpackSqlState(sql_state, data.errcode);
  const auto start = out.GetUncommittedSize();
  auto* prefix = out.GetContiguousData(5);
  WriteNoticeField(out, 'S', severity);
  WriteNoticeField(out, 'V', severity);
  WriteNoticeField(out, 'C', {sql_state, sdb::pg::kSqlStateSize});
  WriteNoticeField(out, 'M', data.errmsg);
  if (!data.errdetail.empty()) {
    WriteNoticeField(out, 'D', data.errdetail);
  }
  if (!data.errhint.empty()) {
    WriteNoticeField(out, 'H', data.errhint);
  }
  if (!data.context.empty()) {
    WriteNoticeField(out, 'W', data.context);
  }
  if (data.cursorpos > 0) {
    WriteNoticeField(out, 'P', absl::StrCat(data.cursorpos));
  }
  out.WriteUncommitted(kNul);
  prefix[0] = static_cast<uint8_t>(type);
  absl::big_endian::Store32(
    prefix + 1, static_cast<int32_t>(out.GetUncommittedSize() - start - 1));
  out.Commit(false);
}

void WriteErrorResponse(message::Buffer& out,
                        const sdb::pg::SqlErrorData& error) {
  WriteDiagnostic(out, PQ_MSG_ERROR_RESPONSE, "ERROR", error);
}

void WriteNoticeResponse(message::Buffer& out,
                         const sdb::pg::SqlErrorData& notice) {
  WriteDiagnostic(out, PQ_MSG_NOTICE_RESPONSE, "WARNING", notice);
}

sdb::pg::SqlErrorData DuckErrorToSqlData(const duckdb::ErrorData& error) {
  // An interrupted query is DuckDB "Interrupted!"; report postgres's wording.
  const bool interrupted = error.Type() == duckdb::ExceptionType::INTERRUPT;
  return sdb::pg::SqlErrorData{
    .errcode = DuckExceptionToErrcode(error.Type()),
    .errmsg = interrupted ? "canceling statement due to user request"
                          : error.RawMessage(),
  };
}

void WriteEmptyFrame(message::Buffer& out, char type) {
  auto* data = out.GetContiguousData(5);
  data[0] = static_cast<uint8_t>(type);
  absl::big_endian::Store32(data + 1, 4);
  out.Commit(false);
}

void WriteParameterDescription(message::Buffer& out,
                               std::span<const int32_t> oids) {
  const auto count = static_cast<uint16_t>(oids.size());
  auto* prefix = out.GetContiguousData(7);
  prefix[0] = PQ_MSG_PARAMETER_DESCRIPTION;
  absl::big_endian::Store32(prefix + 1,
                            static_cast<int32_t>(4 + 2 + 4 * count));
  absl::big_endian::Store16(prefix + 5, count);
  for (const auto oid : oids) {
    absl::big_endian::Store32(out.GetContiguousData(4), oid);
  }
  out.Commit(false);
}

void WriteAuthRequest(message::Buffer& out, int32_t code,
                      std::string_view payload) {
  auto* prefix = out.GetContiguousData(9);
  prefix[0] = PQ_MSG_AUTHENTICATION_REQUEST;
  // length excludes the type byte: 4 (length) + 4 (code) + payload.
  absl::big_endian::Store32(prefix + 1,
                            static_cast<int32_t>(8 + payload.size()));
  absl::big_endian::Store32(prefix + 5, code);
  out.WriteUncommitted(payload);
  out.Commit(false);
}

void WriteCopyInResponse(message::Buffer& out, bool binary) {
  auto* data = out.GetContiguousData(8);
  data[0] = PQ_MSG_COPY_IN_RESPONSE;
  absl::big_endian::Store32(data + 1, 7);  // length: format(1)+cols(2)+self(4)
  data[5] = binary ? 1 : 0;                // overall format
  absl::big_endian::Store16(data + 6, 0);  // column count
  out.Commit(false);
}

void WriteReadyForQuery(message::Buffer& out, char txn_status) {
  auto* data = out.GetContiguousData(6);
  data[0] = PQ_MSG_READY_FOR_QUERY;
  absl::big_endian::Store32(data + 1, 5);
  data[5] = static_cast<uint8_t>(txn_status);
  out.Commit(false);
}

void WriteNegotiateProtocolVersion(
  message::Buffer& out, int32_t newest_minor,
  std::span<const std::string_view> unrecognized_options) {
  const auto start = out.GetUncommittedSize();
  auto* prefix = out.GetContiguousData(9);
  absl::big_endian::Store32(prefix + 5, newest_minor);
  absl::big_endian::Store32(out.GetContiguousData(4),
                            static_cast<int32_t>(unrecognized_options.size()));
  for (const auto option : unrecognized_options) {
    out.WriteUncommitted(option);
    out.WriteUncommitted(kNul);
  }
  prefix[0] = PQ_MSG_NEGOTIATE_PROTOCOL_VERSION;
  absl::big_endian::Store32(
    prefix + 1, static_cast<int32_t>(out.GetUncommittedSize() - start - 1));
  out.Commit(false);
}

}  // namespace sdb::network::pg
