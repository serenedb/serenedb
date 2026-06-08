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

#include "basics/message_buffer.h"
#include "pg/serialize.h"
#include "pg/sql_error.h"

namespace sdb::network::pg {

void WriteParameterStatus(message::Buffer& out, std::string_view name,
                          std::string_view value);

void WriteDataChunk(message::Buffer& out, const duckdb::DataChunk& chunk,
                    std::span<const sdb::pg::SerializationFunction> serializers,
                    sdb::pg::SerializationContext& context);

// Like WriteDataChunk but serializes only rows [begin, end) of the chunk, used
// by Execute max_rows paging when a fetched chunk holds more rows than the
// client's limit. Fetch() returns flat chunks, so an arbitrary row sub-range
// indexes correctly.
void WriteDataChunkRange(
  message::Buffer& out, const duckdb::DataChunk& chunk,
  std::span<const sdb::pg::SerializationFunction> serializers,
  sdb::pg::SerializationContext& context, duckdb::idx_t begin,
  duckdb::idx_t end);

void WriteRowDescription(message::Buffer& out,
                         std::span<const duckdb::LogicalType> types,
                         std::span<const std::string> names,
                         std::span<const sdb::pg::VarFormat> formats);

void WriteCommandComplete(message::Buffer& out, std::string_view tag);

// A body-less frame (5 bytes: type + length 4), e.g. ParseComplete '1',
// BindComplete '2', CloseComplete '3', NoData 'n', EmptyQueryResponse 'I',
// PortalSuspended 's'.
void WriteEmptyFrame(message::Buffer& out, char type);

void WriteParameterDescription(message::Buffer& out,
                               std::span<const int32_t> oids);

void WriteErrorResponse(message::Buffer& out, std::string_view message,
                        std::string_view sqlstate);

void WriteErrorResponse(message::Buffer& out,
                        const sdb::pg::SqlErrorData& error);

void WriteNoticeResponse(message::Buffer& out,
                         const sdb::pg::SqlErrorData& notice);

// Maps a DuckDB error to a pg SqlErrorData with a real SQLSTATE (the DuckDB
// ExceptionType -> errcode mapping), instead of flattening every error to
// XX000.
sdb::pg::SqlErrorData DuckErrorToSqlData(const duckdb::ErrorData& error);

void WriteReadyForQuery(message::Buffer& out, char txn_status);

// NegotiateProtocolVersion ('v'): tells a client that requested a newer minor
// version (or sent unrecognized _pq_ protocol options) the newest minor we
// support and which of its options we ignored, instead of dropping it.
void WriteNegotiateProtocolVersion(
  message::Buffer& out, int32_t newest_minor,
  std::span<const std::string_view> unrecognized_options);

// CopyInResponse for a COPY FROM STDIN: overall format 0 (text) or 1 (binary),
// with 0 per-column formats. Sent by the session before it reads CopyData.
void WriteCopyInResponse(message::Buffer& out, bool binary);

// An AuthenticationRequest ('R' + Int32 code + payload), e.g. code 3
// (CleartextPassword), 10 (SASL), 11 (SASLContinue), 12 (SASLFinal).
void WriteAuthRequest(message::Buffer& out, int32_t code,
                      std::string_view payload);

}  // namespace sdb::network::pg
