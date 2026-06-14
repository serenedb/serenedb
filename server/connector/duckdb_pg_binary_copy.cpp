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
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
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
  result->ctx.buffer = result->file_buffer.get();
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

  result->file_buffer->WriteUncommitted(
    {reinterpret_cast<const char*>(kPgCopyHeader.data()),
     kPgCopyHeader.size()});
  result->file_buffer->Commit(false);
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
  // PGCOPY stream.
  for (duckdb::idx_t row = 0; row < rows; ++row) {
    absl::big_endian::Store16(g.file_buffer->GetContiguousData(2), columns);
    for (uint16_t column = 0; column < columns; ++column) {
      g.serializers[column](g.ctx, decoded[column], row);
    }
  }
  g.file_buffer->Commit(false);
  DrainToHandle(*g.file_buffer, *g.handle);
}

void Combine(duckdb::ExecutionContext&, duckdb::FunctionData&,
             duckdb::GlobalFunctionData&, duckdb::LocalFunctionData&) {}

void Finalize(duckdb::ClientContext&, duckdb::FunctionData&,
              duckdb::GlobalFunctionData& gstate) {
  auto& g = gstate.Cast<PgBinaryCopyGlobalState>();
  // File: raw int16 -1 trailer, then close the handle.
  absl::big_endian::Store16(g.file_buffer->GetContiguousData(2),
                            static_cast<int16_t>(-1));
  g.file_buffer->Commit(false);
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

// Zero-copy byte source: the parser decodes the PGCOPY stream STRAIGHT into the
// output Vector out of the source's current view. `View()` returns the bytes on
// hand (blocking/refilling when empty; empty == clean EOF). `Advance(n)`
// consumes within the current view. `Fill(dst, len)` copies a value that
// straddles a view boundary into `dst` (a small stack temp for fixed-width, or
// the string heap slot for varchar) -- the only copies are the unavoidable
// across-boundary stitch and the varchar materialization into its final heap.
struct ByteSource {
  virtual ~ByteSource() = default;
  // Current view; blocks/refills if empty. Empty == clean EOF.
  virtual std::string_view View() = 0;
  // Consume `n` bytes of the current view (n <= View().size()).
  virtual void Advance(size_t n) = 0;
  // Copy exactly `len` bytes into `dst`, spanning views as needed. Returns the
  // number actually copied (< len only at premature EOF).
  virtual size_t Fill(char* dst, size_t len) = 0;
  // Block until EOF, releasing remaining bytes. Used after the PGCOPY trailer
  // to keep the pg-stdin bridge in lock-step with the feeder until CopyDone.
  virtual void DrainToEof() = 0;
};

struct PgBinaryCopyFromGlobalState final
  : public duckdb::GlobalTableFunctionState {
  std::unique_ptr<ByteSource> source;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  bool header_done = false;
  bool finished = false;
};

// pg-stdin source: borrows the recv-buffer view the CopyInBridge holds. The
// common case decodes straight out of the live bridge window (zero-copy) and
// Advance drives the bridge in lock-step -- one want-more per drained CopyData
// frame. A value that straddles a frame boundary is decoded ACROSS it: the
// fixed/var Fill loop pulls each frame's remainder and Consumes it, so
// want-more still fires once per frame. There is NO field-level scratch and NO
// re-expose-remainder in the source.
class BridgeByteSource final : public ByteSource {
 public:
  explicit BridgeByteSource(sdb::pg::CopyInBridge& bridge) : _bridge{bridge} {}

  std::string_view View() final {
    if (_view.empty()) {
      const auto next = _bridge.Window();
      _view = {next.data(), next.size()};
    }
    return _view;
  }

  void Advance(size_t n) final {
    if (n == 0) {
      return;  // never Consume(0): it could fire a spurious want-more
    }
    _view.remove_prefix(n);
    _bridge.Consume(n);
  }

  size_t Fill(char* dst, size_t len) final {
    size_t done = 0;
    while (done < len) {
      auto v = View();
      if (v.empty()) {
        break;  // premature EOF
      }
      const auto take = std::min(len - done, v.size());
      std::memcpy(dst + done, v.data(), take);
      done += take;
      Advance(take);
    }
    return done;
  }

  void DrainToEof() final {
    for (;;) {
      auto v = View();
      if (v.empty()) {
        return;  // feeder reached CopyDone
      }
      Advance(v.size());
    }
  }

 private:
  sdb::pg::CopyInBridge& _bridge;
  std::string_view _view;
};

// file / real-stdin source: one block-buffered read per `kBlock`, hands views
// straight into the block (zero-copy) and refills when exhausted. A value that
// straddles two blocks is decoded across them via Fill (no field-level
// scratch).
class HandleByteSource final : public ByteSource {
 public:
  explicit HandleByteSource(duckdb::unique_ptr<duckdb::FileHandle> handle)
    : _handle{std::move(handle)}, _block(kBlock) {}

  std::string_view View() final {
    if (_view.empty()) {
      const auto read = FillBlock();
      _view = {_block.data(), static_cast<size_t>(read)};
    }
    return _view;
  }

  void Advance(size_t n) final { _view.remove_prefix(n); }

  size_t Fill(char* dst, size_t len) final {
    size_t done = 0;
    while (done < len) {
      auto v = View();
      if (v.empty()) {
        break;
      }
      const auto take = std::min(len - done, v.size());
      std::memcpy(dst + done, v.data(), take);
      done += take;
      Advance(take);
    }
    return done;
  }

  void DrainToEof() final {
    _view = {};
    while (FillBlock() != 0) {
    }
  }

 private:
  static constexpr size_t kBlock = 64u * 1024;

  // Read one full block into `_block`; returns bytes read (0 = EOF).
  int64_t FillBlock() {
    int64_t total = 0;
    while (total < static_cast<int64_t>(kBlock)) {
      const auto read = _handle->Read(
        _block.data() + total, static_cast<duckdb::idx_t>(kBlock - total));
      if (read <= 0) {
        break;
      }
      total += read;
    }
    return total;
  }

  duckdb::unique_ptr<duckdb::FileHandle> _handle;
  std::vector<char> _block;
  std::string_view _view;
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
    // once with copy_stdin_no_replay set, so nothing else reads the handle, and
    // the session has already sent CopyInResponse.
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

// Read a fixed-width big-endian field straight from `field` (a span that holds
// the whole value) into a flat vector slot, byteswapping into native order.
template<typename Wire, typename Store>
void FlatStoreInt(duckdb::Vector& vec, duckdb::idx_t row, const char* field) {
  duckdb::FlatVector::GetDataMutable<Store>(vec)[row] =
    static_cast<Store>(absl::big_endian::Load<Wire>(field));
}

// Decode one non-null fixed-width binary field directly into a flat vector from
// a contiguous `field` span (len bytes). Returns true if handled; false means
// the type is not fast-pathed, or the field length does not match the wire
// width, so the caller must fall back to the Value path. Fast set: bool,
// int2/int4/int8, float4/float8. (OID-family BIGINT travels as 4 bytes -> not
// fast-pathed.)
bool FlatDecodeFixed(duckdb::Vector& vec, const duckdb::LogicalType& type,
                     duckdb::idx_t row, const char* field, int32_t len) {
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case BOOLEAN:
      if (len != 1) {
        return false;
      }
      duckdb::FlatVector::GetDataMutable<bool>(vec)[row] = field[0] != 0;
      return true;
    case SMALLINT:
      if (len != static_cast<int32_t>(sizeof(int16_t))) {
        return false;
      }
      FlatStoreInt<int16_t, int16_t>(vec, row, field);
      return true;
    case INTEGER:
      if (len != static_cast<int32_t>(sizeof(int32_t))) {
        return false;
      }
      FlatStoreInt<int32_t, int32_t>(vec, row, field);
      return true;
    case BIGINT:
      if (sdb::pg::IsOidLike(type) ||
          len != static_cast<int32_t>(sizeof(int64_t))) {
        return false;  // 4-byte OID on the wire -> defer to the Value path
      }
      FlatStoreInt<int64_t, int64_t>(vec, row, field);
      return true;
    case FLOAT:
      if (len != static_cast<int32_t>(sizeof(float))) {
        return false;
      }
      FlatStoreInt<float, float>(vec, row, field);
      return true;
    case DOUBLE:
      if (len != static_cast<int32_t>(sizeof(double))) {
        return false;
      }
      FlatStoreInt<double, double>(vec, row, field);
      return true;
    default:
      return false;
  }
}

bool IsFastVar(const duckdb::LogicalType& type) {
  return type.id() == duckdb::LogicalTypeId::VARCHAR ||
         type.id() == duckdb::LogicalTypeId::BLOB;
}

bool IsFastFixed(const duckdb::LogicalType& type) {
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case BOOLEAN:
    case SMALLINT:
    case INTEGER:
    case FLOAT:
    case DOUBLE:
      return true;
    case BIGINT:
      return !sdb::pg::IsOidLike(type);
    default:
      return false;
  }
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
    auto ext = static_cast<int32_t>(absl::big_endian::Load32(header + 15));
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
  std::string field;  // reused only by the slow fallback path
  duckdb::idx_t row = 0;
  for (; row < STANDARD_VECTOR_SIZE; ++row) {
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
      // Drain to EOF (the feeder's CopyDone) before finishing. The COPY bridge
      // is strict lock-step: this final blocking wait parks the worker until
      // the feeder reaches Finish, so the worker does not race ahead of the
      // feeder (which would corrupt the shared want-more event).
      source.DrainToEof();
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
      const auto& type = bind.sql_types[column];
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

      // Fast path: varchar/blob -- allocate the string's FINAL heap slot and
      // fill it straight from the input (one memcpy when the view holds it,
      // piece-by-piece across boundaries). The fill IS the materialization.
      if (IsFastVar(type)) {
        auto str = duckdb::StringVector::EmptyString(vec, ulen);
        auto* dst = str.GetDataWriteable();
        auto v = source.View();
        if (v.size() >= ulen) {
          std::memcpy(dst, v.data(), ulen);  // whole field in view
          source.Advance(ulen);
        } else if (source.Fill(dst, ulen) < ulen) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                          ERR_MSG("COPY FROM STDIN: truncated field value"));
        }
        str.Finalize();
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] = str;
        continue;
      }

      // Fast path: fixed-width -- when the whole field is in view, decode +
      // byteswap straight from it (no copy); otherwise stitch the <=8 bytes
      // into a stack temp across the boundary, then decode from that.
      if (IsFastFixed(type) && len <= 8) {
        auto v = source.View();
        if (v.size() >= ulen) {
          if (FlatDecodeFixed(vec, type, row, v.data(), len)) {
            source.Advance(ulen);
            continue;
          }
        } else {
          char tmp[8];
          if (source.Fill(tmp, ulen) < ulen) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                            ERR_MSG("COPY FROM STDIN: truncated field value"));
          }
          if (FlatDecodeFixed(vec, type, row, tmp, len)) {
            continue;
          }
          // Length mismatch: fall through to the Value path with the bytes we
          // already pulled into `tmp`.
          field.assign(tmp, ulen);
          auto value =
            sdb::pg::DeserializeParameter(type, sdb::pg::VarFormat::Binary,
                                          {field.data(), ulen}, *g.snapshot);
          if (!value) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
              ERR_MSG("COPY FROM STDIN: invalid binary value in column ",
                      column + 1));
          }
          output.SetValue(column, row, std::move(*value));
          continue;
        }
      }

      // Slow fallback: types not fast-pathed (decimal, timestamp, arrays,
      // structs, ...) need a contiguous span for DeserializeParameter, which
      // copies the bytes into the resulting Value. One memcpy when the view
      // holds it (deserialize then Advance), across boundaries otherwise.
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
      auto value = sdb::pg::DeserializeParameter(
        type, sdb::pg::VarFormat::Binary, {span, ulen}, *g.snapshot);
      if (advance_after) {
        source.Advance(ulen);
      }
      if (!value) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
          ERR_MSG("COPY FROM STDIN: invalid binary value in column ",
                  column + 1));
      }
      output.SetValue(column, row, std::move(*value));
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
