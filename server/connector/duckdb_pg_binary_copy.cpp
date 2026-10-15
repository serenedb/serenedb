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
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <string>
#include <vector>

#include "connector/copy_from_batches.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_copy_filesystem.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "pg/serialize.h"
#include "server/utils/message_buffer.h"

namespace sdb::connector {
namespace {

// PGCOPY header: 11-byte signature, int32 flags (0), int32 header-extension
// (0).
constexpr std::array<uint8_t, 19> kPgCopyHeader{
  'P',  'G', 'C', 'O', 'P', 'Y', '\n', 0xFF, '\r', '\n',
  0x00, 0,   0,   0,   0,   0,   0,    0,    0};

struct PgBinaryCopyBindData final : public duckdb::FunctionData {
  explicit PgBinaryCopyBindData(duckdb::vector<duckdb::LogicalType> types)
    : sql_types{std::move(types)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<PgBinaryCopyBindData>(sql_types);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    return sql_types == other.Cast<PgBinaryCopyBindData>().sql_types;
  }

  duckdb::vector<duckdb::LogicalType> sql_types;
};

// TO target is a file (or real stdout): `handle` is a plain DuckDB FileHandle
// and `file_buffer` is a private staging buffer the serializers write into;
// after each chunk its committed bytes are drained to the handle as the raw,
// unframed PGCOPY stream. (pg-wire STDOUT no longer routes through this
// CopyFunction -- the session runs it through the wire collector.)
struct PgBinaryCopyGlobalState final : public duckdb::GlobalFunctionData {
  duckdb::unique_ptr<duckdb::FileHandle> handle;
  duckdb::unique_ptr<message::Buffer> file_buffer;
  sdb::pg::SerializationContext ctx;
  std::vector<sdb::pg::SerializationFunction> serializers;
  // Committed-but-undrained bytes; drained to the handle once past a block.
  size_t pending = 0;
};

duckdb::unique_ptr<duckdb::FunctionData> BindCopyTo(
  duckdb::ClientContext&, duckdb::CopyFunctionBindInput&,
  const duckdb::vector<duckdb::Identifier>&,
  const duckdb::vector<duckdb::LogicalType>& sql_types) {
  return duckdb::make_uniq<PgBinaryCopyBindData>(sql_types);
}

duckdb::unique_ptr<duckdb::LocalFunctionData> InitLocal(
  duckdb::ExecutionContext&, duckdb::FunctionData&) {
  return duckdb::make_uniq<duckdb::LocalFunctionData>();
}

duckdb::unique_ptr<duckdb::GlobalFunctionData> InitGlobal(
  duckdb::ClientContext& context, duckdb::FunctionData& bind_data,
  const std::string& file_path) {
  auto& bdata = bind_data.Cast<PgBinaryCopyBindData>();
  auto result = duckdb::make_uniq<PgBinaryCopyGlobalState>();

  // pg-wire STDOUT is owned by the session's wire collector (parallel + PGCOPY
  // CopyData framing), so it never reaches this CopyFunction. If it somehow
  // does, the session failed to route it -- fail clearly rather than emit an
  // unframed PGCOPY stream into a send buffer that expects CopyData.
  if (file_path == "/dev/stdout") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("COPY ... TO STDOUT (FORMAT binary) must be routed through the "
              "pg-wire collector, not the binary CopyFunction"));
  }

  // File (or real stdout): write the raw PGCOPY stream through a plain DuckDB
  // FileHandle. No CopyData framing, no CopyOutResponse/CopyDone -- those are
  // wire-only. Works without a wire connection. A private staging buffer feeds
  // the same serializers; FillContext takes what it needs from the
  // wire connection when present, otherwise from the client context's catalog.
  result->handle = duckdb::FileSystem::GetFileSystem(context).OpenFile(
    file_path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                 duckdb::FileFlags::FILE_FLAGS_FILE_CREATE);
  result->file_buffer =
    duckdb::make_uniq<message::Buffer>(64u * 1024, 1u << 20);
  if (auto* connection = GetSereneDBContextPtr(context)) {
    sdb::pg::FillContext(*connection, result->ctx);
  }
  result->serializers.reserve(bdata.sql_types.size());
  for (const auto& type : bdata.sql_types) {
    result->serializers.push_back(
      sdb::pg::GetSerialization(type, sdb::pg::VarFormat::Binary, result->ctx));
  }

  {
    message::Writer writer{*result->file_buffer};
    writer.Write({reinterpret_cast<const char*>(kPgCopyHeader.data()),
                  kPgCopyHeader.size()});
    writer.Commit(false);
  }
  DrainToHandle(*result->file_buffer, *result->handle);
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

  // Each row is int16 field-count followed by the per-field
  // int32-length-prefixed binary values the serializers emit (the same encoding
  // as a binary DataRow). The bytes go to the handle as the raw, unframed
  // PGCOPY stream. Drain at a fixed block so peak staging memory stays bounded
  // to ~one block regardless of chunk/value size, rather than holding the whole
  // chunk (the sync-sink equivalent of the wire send buffer's flush threshold).
  constexpr size_t kFileBlock = 256u * 1024;
  for (duckdb::idx_t row = 0; row < rows; ++row) {
    message::Writer writer{*g.file_buffer};
    g.ctx.writer = &writer;
    absl::big_endian::Store16(writer.Alloc(2), columns);
    for (uint16_t column = 0; column < columns; ++column) {
      g.serializers[column](g.ctx, decoded[column], row);
    }
    g.pending += writer.Commit(false);
    if (g.pending >= kFileBlock) {
      DrainToHandle(*g.file_buffer, *g.handle);
      g.pending = 0;
    }
  }
}

void Combine(duckdb::ExecutionContext&, duckdb::FunctionData&,
             duckdb::GlobalFunctionData&, duckdb::LocalFunctionData&) {}

void Finalize(duckdb::ClientContext&, duckdb::FunctionData&,
              duckdb::GlobalFunctionData& gstate) {
  auto& g = gstate.Cast<PgBinaryCopyGlobalState>();
  // File: raw int16 -1 trailer, then close the handle.
  {
    message::Writer writer{*g.file_buffer};
    absl::big_endian::Store16(writer.Alloc(2), static_cast<int16_t>(-1));
    writer.Commit(false);
  }
  DrainToHandle(*g.file_buffer, *g.handle);
  g.handle->Close();
}

// Parallel/batch copy reorders rows, which would corrupt the single ordered
// PGCOPY stream on the socket -- force the regular single-threaded path.
duckdb::CopyFunctionExecutionMode ExecutionMode(bool, bool) {
  return duckdb::CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

struct PgBinaryCopyFromBindData final : public duckdb::TableFunctionData {
  PgBinaryCopyFromBindData(duckdb::vector<duckdb::LogicalType> types,
                           std::string path)
    : sql_types{std::move(types)}, file_path{std::move(path)} {}
  duckdb::vector<duckdb::LogicalType> sql_types;
  std::string file_path;
  bool ordered = false;
};

duckdb::unique_ptr<duckdb::FunctionData> BindFrom(
  duckdb::ClientContext& context, duckdb::CopyFromFunctionBindInput& input,
  duckdb::vector<duckdb::Identifier>&,
  duckdb::vector<duckdb::LogicalType>& expected_types) {
  auto result = duckdb::make_uniq<PgBinaryCopyFromBindData>(
    expected_types, input.info.file_path);
  result->ordered = BindCopyFrom(context, input, true);
  return result;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobalFrom(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind = input.bind_data->Cast<PgBinaryCopyFromBindData>();
  return InitCopyFromGlobal(
    context, bind.file_path,
    pg::CopyInTarget{
      .binary = true,
      .types = {bind.sql_types.begin(), bind.sql_types.end()},
    },
    bind.ordered);
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
    duckdb::TableFunction("pg_binary_copy_from", {}, ScanCopyFrom,
                          /*bind=*/nullptr, InitGlobalFrom, InitCopyFromLocal);
  loader.RegisterFunction(std::move(func));
}

}  // namespace sdb::connector
