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

#include <cstdint>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <span>
#include <string>
#include <string_view>

#include "basics/assert.h"
#include "basics/message_buffer.h"
#include "pg/command_tag.h"
#include "pg/serialize.h"
#include "pg/sql_error.h"

namespace duckdb {

class PreparedStatement;
}

namespace sdb::network::pg {

inline sdb::pg::VarFormat FormatFor(std::span<const sdb::pg::VarFormat> formats,
                                    size_t column) {
  if (formats.empty()) {
    return sdb::pg::VarFormat::Text;
  }
  if (column < formats.size()) {
    return formats[column];
  }
  SDB_ASSERT(formats.size() == 1);
  return formats.front();
}

void WriteParameterStatus(message::Buffer& out, std::string_view name,
                          std::string_view value);

// How a chunk's rows are encoded onto the wire (see WriteDataChunk /
// WriteCopyChunk):
//   DataRow    -- one pg 'D' DataRow message per row (normal SELECT result)
//   CopyBinary -- PGCOPY rows (int16 field-count + binary fields), the whole
//                 row run wrapped in one CopyData ('d') frame
//   CopyText   -- PG-text rows (TAB-separated fields, \N null, \n per row), the
//                 whole row run wrapped in one CopyData ('d') frame
enum class RowEncoding : uint8_t {
  DataRow,
  CopyBinary,
  CopyText,
};

// Normal SELECT result: one pg 'D' DataRow message per row. Serializes rows
// [begin, end) -- the sub-range serves Execute max_rows paging; a full chunk is
// [0, chunk.size()).
void WriteDataChunk(message::Buffer& out, const duckdb::DataChunk& chunk,
                    std::span<const sdb::pg::SerializationFunction> serializers,
                    sdb::pg::SerializationContext& context, duckdb::idx_t begin,
                    duckdb::idx_t end);

// COPY ... TO STDOUT: the whole chunk's rows in one CopyData ('d') frame --
// PGCOPY rows (CopyBinary) or PG-text rows (CopyText). An empty chunk emits no
// frame, so a chain of frames is always a valid PGCOPY/COPY row sequence.
template<RowEncoding Encoding>
void WriteCopyChunk(message::Buffer& out, const duckdb::DataChunk& chunk,
                    std::span<const sdb::pg::SerializationFunction> serializers,
                    sdb::pg::SerializationContext& context);

// CopyOutResponse for a COPY TO STDOUT: overall format 0 (text) or 1 (binary),
// the column count, and one per-column format code (each equal to the overall
// format, as PG does). Sent by the session before the first CopyData.
void WriteCopyOutResponse(message::Buffer& out, bool binary, int16_t columns);

// A CopyData frame carrying the COPY TO STDOUT (text) header line: the column
// names, delimiter-separated and newline-terminated, with PG text escaping.
// Sent after CopyOutResponse and before the row frames when HEADER is set.
void WriteCopyTextHeader(message::Buffer& out,
                         std::span<const std::string> names, char delim);

// The 19-byte PGCOPY header as its own CopyData frame (start of a binary COPY
// TO STDOUT stream).
void WriteCopyBinaryHeader(message::Buffer& out);

// The int16 -1 PGCOPY trailer as its own CopyData frame (end of the rows).
void WriteCopyBinaryFooter(message::Buffer& out);

// CopyDone ('c'): ends the CopyData stream after the trailer.
void WriteCopyDone(message::Buffer& out);

void WriteRowDescription(message::Buffer& out,
                         std::span<const duckdb::LogicalType> types,
                         std::span<const std::string> names,
                         std::span<const sdb::pg::VarFormat> formats);

// CommandComplete built straight from the prepared statement: writes the PG
// verb (BuildCommandTag) and, for row-returning/affecting tags, the count --
// "INSERT 0 N" / "UPDATE N" / "SELECT N" / "COPY N" / ... -- directly into
// `out` with no intermediate tag string. `rows` is the affected/returned count
// (ignored for tags without a row count, e.g. DDL / BEGIN / SET).
void WriteCommandComplete(message::Buffer& out,
                          const duckdb::PreparedStatement& prepared,
                          uint64_t rows);

void WriteCommandComplete(message::Buffer& out, const sdb::pg::CommandTag& tag,
                          uint64_t rows);

// A body-less frame (5 bytes: type + length 4), e.g. ParseComplete '1',
// BindComplete '2', CloseComplete '3', NoData 'n', EmptyQueryResponse 'I',
// PortalSuspended 's'.
void WriteEmptyFrame(message::Buffer& out, char type);

void WriteParameterDescription(message::Buffer& out,
                               std::span<const int32_t> oids);

void WriteFatalResponse(message::Buffer& out,
                        const sdb::pg::SqlErrorData& error);

void WriteErrorResponse(message::Buffer& out,
                        const sdb::pg::SqlErrorData& error);

void WriteNoticeResponse(message::Buffer& out,
                         const sdb::pg::SqlErrorData& notice);

// Maps a DuckDB error to a pg SqlErrorData with a real SQLSTATE (the DuckDB
// ExceptionType -> errcode mapping), instead of flattening every error to
// XX000.
sdb::pg::SqlErrorData DuckErrorToSqlData(const duckdb::ErrorData& error);

// Funnels any exception caught at the command-loop boundary into pg
// SqlErrorData: a serenedb SqlException keeps its sqlstate/detail/hint, a
// DuckDB exception maps via DuckErrorToSqlData, anything else is XX000
// internal.
sdb::pg::SqlErrorData ToSqlError(const std::exception& exception);

void WriteReadyForQuery(message::Buffer& out, char txn_status);

// NegotiateProtocolVersion ('v'): tells a client that requested a newer minor
// version (or sent unrecognized _pq_ protocol options) the full protocol
// version we support (the whole int, e.g. 196608 for 3.0 -- libpq rejects a
// bare minor) and which of its options we ignored, instead of dropping it.
void WriteNegotiateProtocolVersion(
  message::Buffer& out, int32_t protocol_version,
  std::span<const std::string> unrecognized_options);

// CopyInResponse for a COPY FROM STDIN: overall format 0 (text) or 1 (binary),
// the column count, and one per-column format code (each equal to the overall
// format, as PG does). Sent by the session before it reads CopyData.
void WriteCopyInResponse(message::Buffer& out, bool binary, int16_t columns);

// An AuthenticationRequest ('R' + Int32 code + payload), e.g. code 3
// (CleartextPassword), 10 (SASL), 11 (SASLContinue), 12 (SASLFinal).
void WriteAuthRequest(message::Buffer& out, int32_t code,
                      std::string_view payload);

}  // namespace sdb::network::pg
