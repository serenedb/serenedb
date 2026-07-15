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

#include "connector/duckdb_pg_text_copy.h"

#include <fast_float/fast_float.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "basics/message_buffer.h"
#include "basics/string_utils.h"
#include "connector/copy_byte_source.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
#include "pg/deserialize.h"
#include "pg/errcodes.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

constexpr char kRowSep = '\n';

bool ParseCopyBool(std::string_view value) {
  // A bare option (no value) means true, e.g. COPY ... (HEADER).
  if (value.empty()) {
    return true;
  }
  if (const auto parsed = basics::ParseBool(value)) {
    return *parsed;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("invalid value for boolean COPY option: \"", value, "\""));
}

void ApplyTextCopyOption(TextCopyOptions& out, std::string_view key,
                         std::string_view value) {
  if (key == "delimiter" || key == "delim" || key == "sep" ||
      key == "separator") {
    if (value.size() != 1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("COPY delimiter must be a single character"));
    }
    out.delim = value.front();
  } else if (key == "null" || key == "nullstr") {
    out.null_str = std::string{value};
  } else if (key == "header") {
    out.header = ParseCopyBool(value);
  } else if (key == "quote" || key == "escape" || key == "force_quote" ||
             key == "force_not_null" || key == "force_null") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("COPY option \"", key, "\" is only available in CSV mode"));
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("COPY option \"", key, "\" not recognized"));
  }
}

struct PgTextCopyBindData final : public duckdb::FunctionData {
  PgTextCopyBindData(duckdb::vector<duckdb::LogicalType> types, char delim,
                     std::string null_str)
    : sql_types{std::move(types)},
      delim{delim},
      null_str{std::move(null_str)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
    return duckdb::make_uniq<PgTextCopyBindData>(sql_types, delim, null_str);
  }
  bool Equals(const duckdb::FunctionData& other) const override {
    const auto& o = other.Cast<PgTextCopyBindData>();
    return sql_types == o.sql_types && delim == o.delim &&
           null_str == o.null_str;
  }

  duckdb::vector<duckdb::LogicalType> sql_types;
  char delim;
  std::string null_str;
};

// TO target is a file (or real stdout): `handle` is a plain DuckDB FileHandle
// and `file_buffer` is a private staging buffer the serializers write into;
// after each chunk its committed bytes are drained to the handle as the raw,
// tab-separated PG TEXT stream (no header, no length prefixes). (pg-wire STDOUT
// no longer routes through this CopyFunction -- the session runs it through the
// wire collector.)
struct PgTextCopyGlobalState final : public duckdb::GlobalFunctionData {
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
  duckdb::ClientContext&, duckdb::CopyFunctionBindInput& input,
  const duckdb::vector<duckdb::Identifier>&,
  const duckdb::vector<duckdb::LogicalType>& sql_types) {
  auto opts = ResolveTextCopyOptions(input.info.options);
  return duckdb::make_uniq<PgTextCopyBindData>(sql_types, opts.delim,
                                               std::move(opts.null_str));
}

duckdb::unique_ptr<duckdb::LocalFunctionData> InitLocal(
  duckdb::ExecutionContext&, duckdb::FunctionData&) {
  return duckdb::make_uniq<duckdb::LocalFunctionData>();
}

duckdb::unique_ptr<duckdb::GlobalFunctionData> InitGlobal(
  duckdb::ClientContext& context, duckdb::FunctionData& bind_data,
  const std::string& file_path) {
  auto& bdata = bind_data.Cast<PgTextCopyBindData>();
  auto result = duckdb::make_uniq<PgTextCopyGlobalState>();

  // pg-wire STDOUT is owned by the session's wire collector (parallel + text
  // CopyData framing), so it never reaches this CopyFunction. If it somehow
  // does, the session failed to route it -- fail clearly rather than emit an
  // unframed text stream into a send buffer that expects CopyData.
  if (file_path == "/dev/stdout") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("COPY ... TO STDOUT (FORMAT text) must be routed through the "
              "pg-wire collector, not the text CopyFunction"));
  }

  // File (or real stdout): write the raw PG TEXT stream through a plain DuckDB
  // FileHandle. No CopyData framing -- that is wire-only. Works without a wire
  // connection. A private staging buffer feeds the same serializers;
  // FillContext needs a catalog snapshot, taken from the wire connection when
  // present, otherwise from the client context's catalog.
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
  // CopyText reuses the Text renderers; the inline COPY escaping is keyed off
  // copy_text + the doubled backslash base, so it is a no-op without these two.
  result->ctx.copy_text = true;
  result->ctx.backslash_count = 2;
  result->ctx.copy_delim = bdata.delim;
  result->ctx.copy_null = bdata.null_str;
  result->serializers.reserve(bdata.sql_types.size());
  for (const auto& type : bdata.sql_types) {
    result->serializers.push_back(sdb::pg::GetSerialization(
      type, sdb::pg::VarFormat::CopyText, result->ctx));
  }
  return result;
}

void Sink(duckdb::ExecutionContext&, duckdb::FunctionData&,
          duckdb::GlobalFunctionData& gstate, duckdb::LocalFunctionData&,
          duckdb::DataChunk& input) {
  auto& g = gstate.Cast<PgTextCopyGlobalState>();
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

  // Each row is the per-column escaped field bodies (or \N for NULL) separated
  // by a TAB and terminated by a newline; no header line, no length prefixes.
  // The bytes go to the handle as the raw text stream. Drain at a fixed block
  // so peak staging memory stays bounded to ~one block regardless of
  // chunk/value size, rather than holding the whole chunk (the sync-sink
  // equivalent of the wire send buffer's flush threshold).
  constexpr size_t kFileBlock = 256u * 1024;
  for (duckdb::idx_t row = 0; row < rows; ++row) {
    message::Writer writer{*g.file_buffer};
    g.ctx.writer = &writer;
    for (uint16_t column = 0; column < columns; ++column) {
      if (column > 0) {
        writer.Write({&g.ctx.copy_delim, 1});
      }
      g.serializers[column](g.ctx, decoded[column], row);
    }
    writer.Write("\n");
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
  auto& g = gstate.Cast<PgTextCopyGlobalState>();
  // No trailer in PG TEXT format: drain anything left and close the handle.
  DrainToHandle(*g.file_buffer, *g.handle);
  g.handle->Close();
}

// Parallel/batch copy reorders rows, which would corrupt the single ordered
// text stream on the socket -- force the regular single-threaded path.
duckdb::CopyFunctionExecutionMode ExecutionMode(bool, bool) {
  return duckdb::CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

struct PgTextCopyFromBindData final : public duckdb::TableFunctionData {
  PgTextCopyFromBindData(duckdb::vector<duckdb::LogicalType> types,
                         std::string path, char delim, std::string null_str,
                         bool header)
    : sql_types{std::move(types)},
      file_path{std::move(path)},
      delim{delim},
      null_str{std::move(null_str)},
      header{header} {}
  duckdb::vector<duckdb::LogicalType> sql_types;
  std::string file_path;
  char delim;
  std::string null_str;
  bool header;
};

struct PgTextCopyFromGlobalState final
  : public duckdb::GlobalTableFunctionState {
  std::unique_ptr<ByteSource> source;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  std::vector<sdb::pg::DeserializationFunction<sdb::pg::VectorSink>>
    deserializers;
  std::string partial;  // bytes pulled but not yet ending in a full row
  bool finished = false;
  bool header_pending = false;  // drop the first line (COPY ... HEADER)
};

duckdb::unique_ptr<duckdb::FunctionData> BindFrom(
  duckdb::ClientContext&, duckdb::CopyFromFunctionBindInput& input,
  duckdb::vector<std::string>&,
  duckdb::vector<duckdb::LogicalType>& expected_types) {
  // HEADER lands in parsed_options (still populated here -- the binder folds it
  // into options only after this bind); delimiter/null arrive via options.
  auto opts = ResolveTextCopyOptions(input.info.parsed_options);
  ResolveTextCopyOptions(input.info.options, opts);
  return duckdb::make_uniq<PgTextCopyFromBindData>(
    expected_types, input.info.file_path, opts.delim, std::move(opts.null_str),
    opts.header);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobalFrom(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind = input.bind_data->Cast<PgTextCopyFromBindData>();
  auto result = duckdb::make_uniq<PgTextCopyFromGlobalState>();
  result->header_pending = bind.header;

  // pg-wire STDIN routes through the CopyInBridge, which needs the server-side
  // wire connection. A real file falls through to the OS filesystem and works
  // without any connection.
  auto* state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
      .get();
  if (bind.file_path == "/dev/stdin") {
    if (!state) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("COPY ... FROM STDIN (FORMAT text) is only valid "
                              "on a server-side PostgreSQL wire connection"));
    }
    auto& conn = state->GetConnectionContext();
    result->snapshot = conn.CatalogSnapshot();
    // pg-stdin: borrow the recv-buffer view the bridge already holds; skip the
    // FileHandle entirely. Text COPY opens stdin once (single-pass), so nothing
    // else reads the handle, and the session has already sent CopyInResponse.
    auto* bridge = conn.GetCopyInBridge();
    if (!bridge) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("COPY ... FROM STDIN (FORMAT text) requires a PG wire "
                "connection (transport not attached)"));
    }
    result->source = std::make_unique<BridgeByteSource>(*bridge);
    return result;
  }
  if (state) {
    result->snapshot = state->GetConnectionContext().CatalogSnapshot();
  }
  // file / real-stdin: block-buffered reader over the OS FileHandle.
  result->source = std::make_unique<HandleByteSource>(
    duckdb::FileSystem::GetFileSystem(context).OpenFile(
      bind.file_path, duckdb::FileFlags::FILE_FLAGS_READ));
  return result;
}

// Decode one PG TEXT field body that contains a backslash escape, unescaping
// into `out` and returning a view over it. The no-escape common case never
// calls this: ProcessRow hands the raw view straight to the decoder
// (zero-copy). The exact two-byte `\N` SQL NULL sentinel is checked by the
// caller before this runs.
std::string_view DecodeFieldView(std::string_view field, std::string& out) {
  out.clear();
  out.reserve(field.size());
  for (size_t i = 0; i < field.size();) {
    const char c = field[i++];
    if (c != '\\') {
      out.push_back(c);
      continue;
    }
    if (i == field.size()) {
      out.push_back('\\');  // trailing backslash is literal
      break;
    }
    const char e = field[i++];
    switch (e) {
      case 'b':
        out.push_back('\b');
        break;
      case 'f':
        out.push_back('\f');
        break;
      case 'n':
        out.push_back('\n');
        break;
      case 'r':
        out.push_back('\r');
        break;
      case 't':
        out.push_back('\t');
        break;
      case 'v':
        out.push_back('\v');
        break;
      case '\\':
        out.push_back('\\');
        break;
      case 'x': {
        const char* begin = field.data() + i;
        const char* end = field.data() + std::min(i + 2, field.size());
        unsigned value = 0;
        const auto res = fast_float::from_chars(begin, end, value, 16);
        if (res.ptr == begin) {
          out.push_back('x');  // `\x` with no hex digit -> literal x
        } else {
          out.push_back(static_cast<char>(value));
          i += static_cast<size_t>(res.ptr - begin);
        }
        break;
      }
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7': {
        // The switch already consumed the first octal digit, so parse the run
        // of up to three starting one byte back. A byte value wraps (& 0xFF).
        const char* begin = field.data() + (i - 1);
        const char* end = field.data() + std::min(i + 2, field.size());
        unsigned value = 0;
        const auto res = fast_float::from_chars(begin, end, value, 8);
        i = static_cast<size_t>(res.ptr - field.data());
        out.push_back(static_cast<char>(value & 0xFF));
        break;
      }
      default:
        out.push_back(e);  // `\<anyotherchar>` -> drop the backslash
        break;
    }
  }
  return out;
}

// Scan from `pos` to the field's end -- the next unescaped delimiter, or the
// row end if none. A backslash escapes the following byte, so an escaped
// delimiter
// (\<delim>, the form EmitEscaped produces) stays in the body. Sets
// `had_escape` when any backslash is seen, so a field with none skips
// DecodeFieldView and reaches the decoder as a zero-copy view straight into
// `line`.
size_t ScanField(std::string_view line, char delim, size_t pos,
                 bool& had_escape) {
  had_escape = false;
  while (pos < line.size()) {
    const char c = line[pos];
    if (c == '\\') {
      had_escape = true;
      pos += 2;
      continue;
    }
    if (c == delim) {
      return pos;
    }
    ++pos;
  }
  return line.size();
}

// Process one complete row (the bytes between row separators, delimiters still
// raw) into `output` at `row`. One pass per field finds the unescaped delimiter
// and notes whether the field needs unescaping: a field with no backslash
// reaches the decoder as a zero-copy view straight into `line`; only an escaped
// field is materialized through `field_buf`. The field count is validated
// against the column count.
void ProcessRow(
  std::string_view line, const PgTextCopyFromBindData& bind,
  const std::vector<sdb::pg::DeserializationFunction<sdb::pg::VectorSink>>&
    deserializers,
  sdb::pg::DeserializeContext& dctx, duckdb::DataChunk& output,
  duckdb::idx_t row, std::string& field_buf) {
  const auto columns = bind.sql_types.size();
  duckdb::idx_t column = 0;
  size_t pos = 0;
  for (;;) {
    bool had_escape = false;
    const auto end = ScanField(line, bind.delim, pos, had_escape);
    if (column >= columns) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
        ERR_MSG("COPY FROM: row has more than ", columns, " fields"));
    }
    const auto raw = line.substr(pos, end - pos);
    auto& vec = output.data[column];
    if (raw == bind.null_str) {
      duckdb::FlatVector::SetNull(vec, row, true);
    } else {
      const auto field = had_escape ? DecodeFieldView(raw, field_buf) : raw;
      sdb::pg::VectorSink sink{vec, row};
      if (!deserializers[column](dctx, field, sink)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
          ERR_MSG("COPY FROM: invalid input for column ", column + 1));
      }
    }
    ++column;
    if (end == line.size()) {
      break;
    }
    pos = end + 1;
  }
  if (column != columns) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
      ERR_MSG("COPY FROM: row has ", column, " fields, expected ", columns));
  }
}

void ScanFrom(duckdb::ClientContext& context, duckdb::TableFunctionInput& input,
              duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<PgTextCopyFromGlobalState>();
  const auto& bind = input.bind_data->Cast<PgTextCopyFromBindData>();
  auto& source = *g.source;
  if (g.finished) {
    output.SetCardinality(0);
    return;
  }

  if (g.deserializers.size() != bind.sql_types.size()) {
    g.deserializers.reserve(bind.sql_types.size());
    for (const auto& type : bind.sql_types) {
      g.deserializers.push_back(
        sdb::pg::GetDeserialization<sdb::pg::VectorSink>(
          type, sdb::pg::VarFormat::Text));
    }
  }
  sdb::pg::DeserializeContext dctx{g.snapshot.get()};
  sdb::pg::FillDeserializeContext(context, dctx);
  std::string field_buf;  // reused per field across this chunk
  duckdb::idx_t row = 0;
  bool eof = false;
  while (row < STANDARD_VECTOR_SIZE) {
    auto view = source.View();
    if (view.empty()) {
      eof = true;
      break;
    }
    // Scan the current window for as many complete rows as fit. A row that
    // straddles the window boundary is stitched through `partial`.
    while (row < STANDARD_VECTOR_SIZE && !view.empty()) {
      const auto nl = view.find(kRowSep);
      if (nl == std::string_view::npos) {
        g.partial.append(view.data(), view.size());
        source.Advance(view.size());
        view = {};
        break;
      }
      const auto chunk = view.substr(0, nl);
      std::string_view line;
      if (g.partial.empty()) {
        line = chunk;  // whole row in view, no stitch
      } else {
        g.partial.append(chunk.data(), chunk.size());
        line = g.partial;
      }
      if (g.header_pending) {
        g.header_pending = false;  // COPY ... HEADER: drop the first line
      } else {
        ProcessRow(line, bind, g.deserializers, dctx, output, row, field_buf);
        ++row;
      }
      g.partial.clear();
      source.Advance(nl + 1);
      view.remove_prefix(nl + 1);
    }
  }

  if (eof) {
    // PG accepts a missing trailing newline: a non-empty leftover partial is
    // the final row. Then keep the bridge in lock-step until the feeder's
    // CopyDone.
    if (!g.partial.empty() && row < STANDARD_VECTOR_SIZE) {
      if (g.header_pending) {
        g.header_pending = false;  // header-only input with no trailing newline
      } else {
        ProcessRow(g.partial, bind, g.deserializers, dctx, output, row,
                   field_buf);
        ++row;
      }
      g.partial.clear();
    }
    if (g.partial.empty()) {
      source.DrainToEof();
      g.finished = true;
    }
  }

  // SetChildCardinality (not SetCardinality): fork vectors carry their own
  // v_size, and a downstream size-deriving op reads it; SetCardinality leaves
  // the column vectors at v_size 0 -> "Mismatch in input vector sizes".
  output.SetChildCardinality(row);
}

}  // namespace

void ResolveTextCopyOptions(
  const duckdb::case_insensitive_map_t<duckdb::vector<duckdb::Value>>& options,
  TextCopyOptions& out) {
  for (const auto& [key, values] : options) {
    if (values.empty()) {
      ApplyTextCopyOption(out, key, {});
    } else {
      ApplyTextCopyOption(out, key, values[0].GetValue<std::string>());
    }
  }
}

TextCopyOptions ResolveTextCopyOptions(
  const duckdb::case_insensitive_map_t<duckdb::vector<duckdb::Value>>&
    options) {
  TextCopyOptions result;
  ResolveTextCopyOptions(options, result);
  return result;
}

TextCopyOptions ResolveTextCopyOptions(
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& parsed_options) {
  TextCopyOptions result;
  for (const auto& [key, expr] : parsed_options) {
    if (!expr) {
      // A bare boolean flag (e.g. HEADER with no value) means true.
      ApplyTextCopyOption(result, key, {});
      continue;
    }
    if (expr->GetExpressionClass() != duckdb::ExpressionClass::CONSTANT) {
      continue;
    }
    const auto& value = expr->Cast<duckdb::ConstantExpression>().GetValue();
    ApplyTextCopyOption(result, key, value.GetValue<std::string>());
  }
  return result;
}

void RegisterPgTextCopyFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  duckdb::CopyFunction func("text");
  func.extension = "text";
  func.copy_to_bind = BindCopyTo;
  func.copy_to_initialize_local = InitLocal;
  func.copy_to_initialize_global = InitGlobal;
  func.copy_to_sink = Sink;
  func.copy_to_combine = Combine;
  func.copy_to_finalize = Finalize;
  func.execution_mode = ExecutionMode;
  func.copy_from_bind = BindFrom;
  func.copy_from_function =
    duckdb::TableFunction("pg_text_copy_from", {}, ScanFrom,
                          /*bind=*/nullptr, InitGlobalFrom);
  loader.RegisterFunction(std::move(func));
}

}  // namespace sdb::connector
