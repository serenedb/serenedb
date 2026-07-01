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
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <memory>
#include <string>
#include <vector>

#include "basics/message_buffer.h"
#include "connector/copy_byte_source.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
#include "pg/deserialize.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"

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

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
    return duckdb::make_uniq<PgBinaryCopyBindData>(sql_types);
  }
  bool Equals(const duckdb::FunctionData& other) const override {
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

// Drain everything committed to `buffer` into `handle` as raw bytes, leaving
// the buffer empty for the next chunk.
void DrainToHandle(message::Buffer& buffer, duckdb::FileHandle& handle) {
  auto chain = buffer.ReleaseChain();
  for (auto* chunk = chain.head; chunk != nullptr; chunk = chunk->Next()) {
    const auto data = chunk->Data(chunk->GetEnd());
    if (!data.empty()) {
      handle.Write(const_cast<uint8_t*>(data.data()),
                   static_cast<duckdb::idx_t>(data.size()));
    }
  }
}

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
  // the same serializers; FillContext needs a catalog snapshot, taken from the
  // wire connection when present, otherwise from the client context's catalog.
  result->handle = duckdb::FileSystem::GetFileSystem(context).OpenFile(
    file_path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                 duckdb::FileFlags::FILE_FLAGS_FILE_CREATE);
  result->file_buffer =
    duckdb::make_uniq<message::Buffer>(64u * 1024, 1u << 20);
  if (auto* state = context.registered_state
                      ->Get<SereneDBClientState>(kSereneDBClientStateKey)
                      .get()) {
    sdb::pg::FillContext(state->GetConnectionContext(), result->ctx);
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
};

struct PgBinaryCopyFromGlobalState final
  : public duckdb::GlobalTableFunctionState {
  std::unique_ptr<ByteSource> source;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  std::vector<sdb::pg::DeserializationFunction<sdb::pg::VectorSink>>
    deserializers;
  bool header_done = false;
  bool finished = false;
};

duckdb::unique_ptr<duckdb::FunctionData> BindFrom(
  duckdb::ClientContext&, duckdb::CopyFromFunctionBindInput& input,
  duckdb::vector<std::string>&,
  duckdb::vector<duckdb::LogicalType>& expected_types) {
  return duckdb::make_uniq<PgBinaryCopyFromBindData>(expected_types,
                                                     input.info.file_path);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobalFrom(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind = input.bind_data->Cast<PgBinaryCopyFromBindData>();
  auto result = duckdb::make_uniq<PgBinaryCopyFromGlobalState>();

  // pg-wire STDIN routes through SereneDBCopyFileSystem -> CopyInBridge, which
  // needs the server-side wire connection (and its catalog snapshot for binary
  // value deserialization). A real file falls through to the OS filesystem and
  // works without any connection.
  auto* state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
      .get();
  if (bind.file_path == "/dev/stdin") {
    if (!state) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("COPY ... FROM STDIN (FORMAT binary) is only valid "
                "on a server-side PostgreSQL wire connection"));
    }
    auto& conn = state->GetConnectionContext();
    result->snapshot = conn.EnsureCatalogSnapshot();
    // pg-stdin: borrow the recv-buffer view the bridge already holds; skip the
    // FileHandle (and its per-field memcpy) entirely. Binary COPY opens stdin
    // once (single-pass), so nothing else reads the handle, and the session has
    // already sent CopyInResponse.
    auto* bridge = conn.GetCopyInBridge();
    if (!bridge) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("COPY ... FROM STDIN (FORMAT binary) requires a PG wire "
                "connection (transport not attached)"));
    }
    result->source = std::make_unique<BridgeByteSource>(*bridge);
    return result;
  }
  if (state) {
    result->snapshot = state->GetConnectionContext().EnsureCatalogSnapshot();
  }
  // file / real-stdin: block-buffered reader over the OS FileHandle.
  result->source = std::make_unique<HandleByteSource>(
    duckdb::FileSystem::GetFileSystem(context).OpenFile(
      bind.file_path, duckdb::FileFlags::FILE_FLAGS_READ));
  return result;
}

void ScanFrom(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
              duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<PgBinaryCopyFromGlobalState>();
  const auto& bind = input.bind_data->Cast<PgBinaryCopyFromBindData>();
  auto& source = *g.source;
  if (g.finished) {
    output.SetCardinality(0);
    return;
  }

  if (!g.header_done) {
    // PGCOPY header: 11-byte signature, int32 flags, int32 header-extension
    // length, then `ext` extension bytes. Decode straight from the source view,
    // spanning boundaries via Fill where needed.
    char header[19];
    const auto got = source.Fill(header, sizeof(header));
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
    // Flags word (bytes 11..14): the WITH-OIDS bit is unsupported and any other
    // high-16-bit (critical) flag is from a newer format we cannot read; PG
    // rejects both rather than misparsing the body.
    const auto flags = absl::big_endian::Load32(header + 11);
    if ((flags & (1U << 16)) != 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
        ERR_MSG("COPY FROM STDIN: invalid COPY file header (WITH OIDS)"));
    }
    if (((flags & ~(1U << 16)) >> 16) != 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                      ERR_MSG("COPY FROM STDIN: unrecognized critical flags in "
                              "COPY file header"));
    }
    auto ext = static_cast<int32_t>(absl::big_endian::Load32(header + 15));
    if (ext < 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
        ERR_MSG("COPY FROM STDIN: invalid COPY file header (missing length)"));
    }
    while (ext > 0) {
      auto v = source.View();
      if (v.empty()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                        ERR_MSG("COPY FROM STDIN: truncated PGCOPY header"));
      }
      const auto take = std::min<int32_t>(ext, static_cast<int32_t>(v.size()));
      source.Advance(static_cast<size_t>(take));
      ext -= take;
    }
    g.header_done = true;
  }

  const auto columns = bind.sql_types.size();
  if (g.deserializers.size() != columns) {
    g.deserializers.reserve(columns);
    for (const auto& type : bind.sql_types) {
      g.deserializers.push_back(
        sdb::pg::GetDeserialization<sdb::pg::VectorSink>(
          type, sdb::pg::VarFormat::Binary));
    }
  }
  sdb::pg::DeserializeContext dctx{g.snapshot.get()};
  std::string
    field;  // a field that straddles a frame boundary, or the fallback
  duckdb::idx_t row = 0;
  for (; row < STANDARD_VECTOR_SIZE; ++row) {
    // Fast path: the whole row (int16 count + every int32-len-prefixed field)
    // lies in the current window. Walk the field lengths once (no decode) to
    // confirm it fits and find the row end, then decode each field straight
    // from the window -- no per-field Fill/View/Advance virtual call, one
    // Advance per row. Any anomaly (too little in window, trailer, count
    // mismatch, bad or straddling length, EOF) drops to the slow path below,
    // which owns the cross-frame stitching and all error/trailer handling.
    if (const auto win = source.View(); win.size() >= 2) {
      const char* const base = win.data();
      const char* const limit = base + win.size();
      const char* p = base + 2;
      bool in_window = static_cast<int16_t>(absl::big_endian::Load16(base)) ==
                       static_cast<int16_t>(columns);
      for (size_t c = 0; in_window && c < columns; ++c) {
        if (limit - p < 4) {
          in_window = false;
          break;
        }
        const auto len = static_cast<int32_t>(absl::big_endian::Load32(p));
        p += 4;
        if (len == -1) {
          continue;
        }
        if (len < 0 || limit - p < len) {
          in_window = false;
          break;
        }
        p += len;
      }
      if (in_window) {
        const char* q = base + 2;
        for (duckdb::idx_t column = 0; column < columns; ++column) {
          const auto len = static_cast<int32_t>(absl::big_endian::Load32(q));
          q += 4;
          auto& vec = output.data[column];
          if (len == -1) {
            duckdb::FlatVector::SetNull(vec, row, true);
            continue;
          }
          sdb::pg::VectorSink sink{vec, row};
          if (!g.deserializers[column](dctx, {q, static_cast<size_t>(len)},
                                       sink)) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
              ERR_MSG("COPY FROM STDIN: invalid binary value in column ",
                      column + 1));
          }
          q += len;
        }
        source.Advance(static_cast<size_t>(p - base));
        continue;
      }
    }

    // Slow path: field-by-field with cross-frame stitching, plus the trailer /
    // EOF-marker / error handling the fast path defers here.
    // Row header: int16 field count.
    char count_bytes[2];
    const auto got = source.Fill(count_bytes, sizeof(count_bytes));
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
      // PG requires the protocol EOF (CopyDone) to follow the trailer
      // immediately: any bytes between it and CopyDone are an error. Reading
      // one more byte also parks the worker until the feeder reaches Finish
      // (the COPY bridge is strict lock-step), so it cannot race ahead.
      char after_marker;
      if (source.Fill(&after_marker, 1) != 0) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
          ERR_MSG("COPY FROM STDIN: received copy data after EOF marker"));
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
      if (source.Fill(len_bytes, sizeof(len_bytes)) < 4) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                        ERR_MSG("COPY FROM STDIN: truncated field length"));
      }
      const auto len =
        static_cast<int32_t>(absl::big_endian::Load32(len_bytes));
      auto& vec = output.data[column];
      if (len == -1) {  // SQL NULL
        duckdb::FlatVector::SetNull(vec, row, true);
        continue;
      }
      if (len < 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                        ERR_MSG("COPY FROM STDIN: negative field length"));
      }
      const auto ulen = static_cast<size_t>(len);

      // One contiguous span of the field: the recv-buffer view when it holds
      // the whole field (zero-copy), else stitched across the frame boundary.
      const auto fn = g.deserializers[column];
      auto v = source.View();
      const char* span;
      bool advance_after = false;
      if (v.size() >= ulen) {
        span = v.data();
        advance_after = true;
      } else {
        field.resize(ulen);
        if (source.Fill(field.data(), ulen) < ulen) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                          ERR_MSG("COPY FROM STDIN: truncated field value"));
        }
        span = field.data();
      }
      sdb::pg::VectorSink sink{vec, row};
      const bool ok = fn(dctx, {span, ulen}, sink);
      if (advance_after) {
        source.Advance(ulen);
      }
      if (!ok) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
          ERR_MSG("COPY FROM STDIN: invalid binary value in column ",
                  column + 1));
      }
    }
  }
  // SetChildCardinality (not SetCardinality): fork vectors carry their own
  // v_size, and a downstream size-deriving op (e.g. a LIKE/prefix filter on the
  // scanned chunk) reads it. SetCardinality sets only chunk.count, leaving the
  // column vectors at v_size 0 -> "Mismatch in input vector sizes".
  output.SetChildCardinality(row);
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
