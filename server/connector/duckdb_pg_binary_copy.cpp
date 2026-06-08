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

#include "connector/duckdb_pg_binary_copy.h"

#include <absl/base/internal/endian.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include "basics/message_buffer.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/protocol.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

// PGCOPY header: 11-byte signature, int32 flags (0), int32 header-extension (0).
constexpr std::array<uint8_t, 19> kPgCopyHeader{
  'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xFF, '\r', '\n', 0x00,
  0,   0,   0,   0,   0,   0,   0,    0};

// Frame `payload_size` bytes already appended after a 5-byte prefix as a
// CopyData ('d') message: backpatch type + length (length excludes the type
// byte, includes the 4 length bytes -- matches CopyOutFileHandle::DoWrite).
void FinishCopyData(message::Buffer& send, uint8_t* prefix, size_t before) {
  prefix[0] = PQ_MSG_COPY_DATA;
  absl::big_endian::Store32(
    prefix + 1, static_cast<int32_t>(send.GetUncommittedSize() - before - 1));
  send.Commit(false);
}

struct PgBinaryCopyBindData final : public duckdb::FunctionData {
  explicit PgBinaryCopyBindData(duckdb::vector<duckdb::LogicalType> types)
    : sql_types{std::move(types)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
    return duckdb::make_uniq<PgBinaryCopyBindData>(sql_types);
  }
  bool Equals(const duckdb::FunctionData& other) const override {
    return sql_types == other.Cast<PgBinaryCopyBindData>().sql_types;
  }

  duckdb::vector<duckdb::LogicalType> sql_types;
};

struct PgBinaryCopyGlobalState final : public duckdb::GlobalFunctionData {
  message::Buffer* send = nullptr;
  sdb::pg::SerializationContext ctx;
  std::vector<sdb::pg::SerializationFunction> serializers;
};

duckdb::unique_ptr<duckdb::FunctionData> BindCopyTo(
  duckdb::ClientContext&, duckdb::CopyFunctionBindInput&,
  const duckdb::vector<std::string>&,
  const duckdb::vector<duckdb::LogicalType>& sql_types) {
  return duckdb::make_uniq<PgBinaryCopyBindData>(sql_types);
}

duckdb::unique_ptr<duckdb::LocalFunctionData> InitLocal(
  duckdb::ExecutionContext&, duckdb::FunctionData&) {
  return duckdb::make_uniq<duckdb::LocalFunctionData>();
}

duckdb::unique_ptr<duckdb::GlobalFunctionData> InitGlobal(
  duckdb::ClientContext& context, duckdb::FunctionData& bind_data,
  const std::string&) {
  auto& bdata = bind_data.Cast<PgBinaryCopyBindData>();
  auto* state = context.registered_state
                  ->Get<SereneDBClientState>(kSereneDBClientStateKey)
                  .get();
  if (!state) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ... TO STDOUT (FORMAT binary) is only valid "
                            "on a server-side PostgreSQL wire connection"));
  }
  auto& conn = state->GetConnectionContext();
  auto* send = conn.GetSendBuffer();
  if (!send) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ... TO STDOUT (FORMAT binary) requires a PG "
                            "wire connection (transport not attached)"));
  }

  auto result = duckdb::make_uniq<PgBinaryCopyGlobalState>();
  result->send = send;
  result->ctx.buffer = send;
  sdb::pg::FillContext(conn, result->ctx);
  result->serializers.reserve(bdata.sql_types.size());
  for (const auto& type : bdata.sql_types) {
    result->serializers.push_back(
      sdb::pg::GetSerialization(type, sdb::pg::VarFormat::Binary, result->ctx));
  }

  // CopyOutResponse, overall format = binary (1), 0 columns.
  auto* response = send->GetContiguousData(8);
  response[0] = PQ_MSG_COPY_OUT_RESPONSE;
  absl::big_endian::Store32(response + 1, 7);
  response[5] = 1;
  absl::big_endian::Store16(response + 6, 0);
  send->Commit(false);

  // The PGCOPY header as its own CopyData frame.
  const auto before = send->GetUncommittedSize();
  auto* prefix = send->GetContiguousData(5);
  send->WriteUncommitted({reinterpret_cast<const char*>(kPgCopyHeader.data()),
                          kPgCopyHeader.size()});
  FinishCopyData(*send, prefix, before);
  return result;
}

void Sink(duckdb::ExecutionContext&, duckdb::FunctionData&,
          duckdb::GlobalFunctionData& gstate, duckdb::LocalFunctionData&,
          duckdb::DataChunk& input) {
  auto& g = gstate.Cast<PgBinaryCopyGlobalState>();
  const auto rows = input.size();
  if (rows == 0) {
    return;
  }
  const auto columns = static_cast<uint16_t>(input.ColumnCount());
  std::vector<duckdb::RecursiveUnifiedVectorFormat> decoded(columns);
  for (uint16_t column = 0; column < columns; ++column) {
    duckdb::Vector::RecursiveToUnifiedFormat(input.data[column], rows,
                                             decoded[column]);
  }

  // One CopyData frame per chunk; each row is int16 field-count followed by the
  // per-field int32-length-prefixed binary values the serializers emit (the
  // same encoding as a binary DataRow).
  const auto before = g.send->GetUncommittedSize();
  auto* prefix = g.send->GetContiguousData(5);
  for (duckdb::idx_t row = 0; row < rows; ++row) {
    absl::big_endian::Store16(g.send->GetContiguousData(2), columns);
    for (uint16_t column = 0; column < columns; ++column) {
      g.serializers[column](g.ctx, decoded[column], row);
    }
  }
  FinishCopyData(*g.send, prefix, before);
}

void Combine(duckdb::ExecutionContext&, duckdb::FunctionData&,
             duckdb::GlobalFunctionData&, duckdb::LocalFunctionData&) {}

void Finalize(duckdb::ClientContext&, duckdb::FunctionData&,
              duckdb::GlobalFunctionData& gstate) {
  auto& g = gstate.Cast<PgBinaryCopyGlobalState>();
  // Trailer: int16 -1, then CopyDone.
  const auto before = g.send->GetUncommittedSize();
  auto* prefix = g.send->GetContiguousData(5);
  absl::big_endian::Store16(g.send->GetContiguousData(2),
                            static_cast<int16_t>(-1));
  FinishCopyData(*g.send, prefix, before);
  static constexpr uint8_t kCopyDone[5] = {PQ_MSG_COPY_DONE, 0, 0, 0, 4};
  g.send->Write({reinterpret_cast<const char*>(kCopyDone), 5}, false);
}

// Parallel/batch copy reorders rows, which would corrupt the single ordered
// PGCOPY stream on the socket -- force the regular single-threaded path.
duckdb::CopyFunctionExecutionMode ExecutionMode(bool, bool) {
  return duckdb::CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

struct PgBinaryCopyFromBindData final : public duckdb::TableFunctionData {
  explicit PgBinaryCopyFromBindData(duckdb::vector<duckdb::LogicalType> types)
    : sql_types{std::move(types)} {}
  duckdb::vector<duckdb::LogicalType> sql_types;
};

struct PgBinaryCopyFromGlobalState final
  : public duckdb::GlobalTableFunctionState {
  duckdb::unique_ptr<duckdb::FileHandle> handle;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  bool header_done = false;
  bool finished = false;
};

duckdb::unique_ptr<duckdb::FunctionData> BindFrom(
  duckdb::ClientContext&, duckdb::CopyFromFunctionBindInput&,
  duckdb::vector<std::string>&,
  duckdb::vector<duckdb::LogicalType>& expected_types) {
  return duckdb::make_uniq<PgBinaryCopyFromBindData>(expected_types);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobalFrom(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput&) {
  auto* state = context.registered_state
                  ->Get<SereneDBClientState>(kSereneDBClientStateKey)
                  .get();
  if (!state) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ... FROM STDIN (FORMAT binary) is only valid "
                            "on a server-side PostgreSQL wire connection"));
  }
  auto result = duckdb::make_uniq<PgBinaryCopyFromGlobalState>();
  result->snapshot = state->GetConnectionContext().EnsureCatalogSnapshot();
  result->handle = duckdb::FileSystem::GetFileSystem(context).OpenFile(
    "/dev/stdin", duckdb::FileFlags::FILE_FLAGS_READ);
  return result;
}

// The COPY bridge blocks until n bytes are available, so this returns n except
// at end of stream (CopyDone), where it returns the remaining count.
int64_t ReadFull(duckdb::FileHandle& handle, char* dst, int64_t n) {
  int64_t total = 0;
  while (total < n) {
    const auto read =
      handle.Read(dst + total, static_cast<duckdb::idx_t>(n - total));
    if (read <= 0) {
      break;
    }
    total += read;
  }
  return total;
}

void ScanFrom(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
              duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<PgBinaryCopyFromGlobalState>();
  const auto& bind = input.bind_data->Cast<PgBinaryCopyFromBindData>();
  if (g.finished) {
    output.SetCardinality(0);
    return;
  }
  if (!g.header_done) {
    char header[19];
    const auto got = ReadFull(*g.handle, header, sizeof(header));
    if (got == 0) {  // empty stream
      g.finished = true;
      output.SetCardinality(0);
      return;
    }
    if (got < static_cast<int64_t>(sizeof(header)) ||
        std::memcmp(header, kPgCopyHeader.data(), 11) != 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                      ERR_MSG("COPY FROM STDIN: invalid PGCOPY signature"));
    }
    const auto ext = static_cast<int32_t>(absl::big_endian::Load32(header + 15));
    for (int32_t left = ext; left > 0;) {
      char scratch[256];
      const auto take = std::min<int32_t>(left, sizeof(scratch));
      if (ReadFull(*g.handle, scratch, take) < take) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                        ERR_MSG("COPY FROM STDIN: truncated PGCOPY header"));
      }
      left -= take;
    }
    g.header_done = true;
  }

  const auto columns = bind.sql_types.size();
  std::string field;
  duckdb::idx_t row = 0;
  for (; row < STANDARD_VECTOR_SIZE; ++row) {
    char count_bytes[2];
    const auto got = ReadFull(*g.handle, count_bytes, 2);
    if (got == 0) {  // end of stream without an explicit -1 trailer
      g.finished = true;
      break;
    }
    if (got < 2) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                      ERR_MSG("COPY FROM STDIN: truncated row header"));
    }
    const auto fields =
      static_cast<int16_t>(absl::big_endian::Load16(count_bytes));
    if (fields == -1) {  // PGCOPY trailer
      // Drain to EOF (the feeder's CopyDone) before finishing. The COPY bridge
      // is strict lock-step: this final blocking Read parks the worker until the
      // feeder reaches Finish, so the worker does not race ahead of the feeder
      // (which would corrupt the shared want-more event).
      char drain[64];
      while (ReadFull(*g.handle, drain, sizeof(drain)) > 0) {
      }
      g.finished = true;
      break;
    }
    if (static_cast<size_t>(fields) != columns) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                      ERR_MSG("COPY FROM STDIN: row has ", fields,
                              " fields, expected ", columns));
    }
    for (duckdb::idx_t column = 0; column < columns; ++column) {
      char len_bytes[4];
      if (ReadFull(*g.handle, len_bytes, 4) < 4) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                        ERR_MSG("COPY FROM STDIN: truncated field length"));
      }
      const auto len = static_cast<int32_t>(absl::big_endian::Load32(len_bytes));
      if (len == -1) {
        output.SetValue(column, row, duckdb::Value{bind.sql_types[column]});
        continue;
      }
      if (len < 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                        ERR_MSG("COPY FROM STDIN: negative field length"));
      }
      field.resize(static_cast<size_t>(len));
      if (ReadFull(*g.handle, field.data(), len) < len) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                        ERR_MSG("COPY FROM STDIN: truncated field value"));
      }
      auto value = sdb::pg::DeserializeParameter(
        bind.sql_types[column], sdb::pg::VarFormat::Binary,
        {field.data(), static_cast<size_t>(len)}, *g.snapshot);
      if (!value) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
          ERR_MSG("COPY FROM STDIN: invalid binary value in column ",
                  column + 1));
      }
      output.SetValue(column, row, std::move(*value));
    }
  }
  output.SetCardinality(row);
}

}  // namespace

void RegisterPgBinaryCopyFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  duckdb::CopyFunction func("binary");
  func.extension = "binary";
  func.copy_to_bind = BindCopyTo;
  func.copy_to_initialize_local = InitLocal;
  func.copy_to_initialize_global = InitGlobal;
  func.copy_to_sink = Sink;
  func.copy_to_combine = Combine;
  func.copy_to_finalize = Finalize;
  func.execution_mode = ExecutionMode;
  func.copy_from_bind = BindFrom;
  func.copy_from_function =
    duckdb::TableFunction("pg_binary_copy_from", {}, ScanFrom,
                          /*bind=*/nullptr, InitGlobalFrom);
  loader.RegisterFunction(std::move(func));
}

}  // namespace sdb::connector
