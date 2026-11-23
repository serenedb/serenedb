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
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string_utils.hpp>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "connector/copy_from_batches.h"
#include "connector/duckdb_copy_filesystem.h"
#include "pg/connection_context.h"
#include "pg/serialize.h"
#include "server/utils/message_buffer.h"

namespace sdb::connector {
namespace {

bool ParseCopyBool(std::string_view value) {
  // A bare option (no value) means true, e.g. COPY ... (HEADER).
  if (value.empty()) {
    return true;
  }
  if (const auto parsed = irs::utils::ParseBool(value)) {
    return *parsed;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("invalid value for boolean COPY option: \"", value, "\""));
}

uint64_t ParseRejectLimit(std::string_view value) {
  if (value.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("reject_limit requires a numeric value"));
  }
  int64_t limit = 0;
  const auto* end = value.data() + value.size();
  const auto [ptr, ec] = fast_float::from_chars(value.data(), end, limit);
  if (ec != std::errc{} || ptr != end) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type bigint: \"", value, "\""));
  }
  if (limit <= 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("REJECT_LIMIT (", limit, ") must be greater than zero"));
  }
  return static_cast<uint64_t>(limit);
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
    out.null_str.assign(value);
  } else if (key == "header") {
    out.header = ParseCopyBool(value);
  } else if (key == "ignore_errors") {
    out.ignore_errors = ParseCopyBool(value);
  } else if (key == "rejects_limit") {
    out.reject_limit = ParseRejectLimit(value);
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
                     std::string null_str, bool header,
                     duckdb::vector<duckdb::Identifier> names)
    : sql_types{std::move(types)},
      delim{delim},
      null_str{std::move(null_str)},
      header{header},
      names{std::move(names)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<PgTextCopyBindData>(sql_types, delim, null_str,
                                                 header, names);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    const auto& o = other.Cast<PgTextCopyBindData>();
    return sql_types == o.sql_types && delim == o.delim &&
           null_str == o.null_str && header == o.header && names == o.names;
  }

  duckdb::vector<duckdb::LogicalType> sql_types;
  char delim;
  std::string null_str;
  bool header;
  duckdb::vector<duckdb::Identifier> names;
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

duckdb::unique_ptr<duckdb::FunctionData> BindCopyTo(
  duckdb::ClientContext&, duckdb::CopyFunctionBindInput& input,
  const duckdb::vector<duckdb::Identifier>&,
  const duckdb::vector<duckdb::LogicalType>& sql_types) {
  auto opts = ResolveTextCopyOptions(input.info.options);
  CheckTextCopyOptions(opts, false);
  return duckdb::make_uniq<PgTextCopyBindData>(sql_types, opts.delim,
                                               std::move(opts.null_str),
                                               opts.header, input.query_names);
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
  if (file_path == "/dev/stdout" &&
      ConnectionContext::Get(context).GetSendBuffer()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("COPY ... TO STDOUT (FORMAT text) must be routed through the "
              "pg-wire collector, not the text CopyFunction"));
  }

  // File (or real stdout): write the raw PG TEXT stream through a plain DuckDB
  // FileHandle. No CopyData framing -- that is wire-only. Works without a wire
  // connection. A private staging buffer feeds the same serializers;
  // FillContext takes what it needs from the wire connection when
  // present, otherwise from the client context's catalog.
  result->handle = duckdb::FileSystem::GetFileSystem(context).OpenFile(
    file_path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                 duckdb::FileFlags::FILE_FLAGS_FILE_CREATE |
                 duckdb::FileCompressionType::AUTO_DETECT);
  result->file_buffer =
    duckdb::make_uniq<message::Buffer>(64u * 1024, 1u << 20);
  sdb::pg::FillContext(ConnectionContext::Get(context), result->ctx);
  sdb::pg::SetCopyText(result->ctx, bdata.delim, bdata.null_str);
  result->serializers.reserve(bdata.sql_types.size());
  for (const auto& type : bdata.sql_types) {
    result->serializers.push_back(sdb::pg::GetSerialization(
      type, sdb::pg::VarFormat::CopyText, result->ctx));
  }
  if (bdata.header) {
    message::Writer writer{*result->file_buffer};
    result->ctx.writer = &writer;
    sdb::pg::SerializeCopyTextHeader(result->ctx, bdata.names);
    result->pending += writer.Commit(false);
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
                         std::string path, TextCopyOptions options)
    : sql_types{std::move(types)},
      file_path{std::move(path)},
      options{std::move(options)} {}
  duckdb::vector<duckdb::LogicalType> sql_types;
  std::string file_path;
  TextCopyOptions options;
  bool ordered = false;
};

duckdb::unique_ptr<duckdb::FunctionData> BindFrom(
  duckdb::ClientContext& context, duckdb::CopyFromFunctionBindInput& input,
  duckdb::vector<duckdb::Identifier>&,
  duckdb::vector<duckdb::LogicalType>& expected_types) {
  auto opts = ResolveTextCopyOptions(input.info.options);
  CheckTextCopyOptions(opts, true);
  auto result = duckdb::make_uniq<PgTextCopyFromBindData>(
    expected_types, input.info.file_path, std::move(opts));
  result->ordered = BindCopyFrom(context, input);
  return result;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobalFrom(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind = input.bind_data->Cast<PgTextCopyFromBindData>();
  return InitCopyFromGlobal(
    context, bind.file_path,
    pg::CopyInTarget{
      .binary = false,
      .delim = bind.options.delim,
      .null_str = bind.options.null_str,
      .header = bind.options.header,
      .ignore_errors = bind.options.ignore_errors,
      .reject_limit = bind.options.reject_limit,
      .types = {bind.sql_types.begin(), bind.sql_types.end()},
    },
    bind.ordered);
}

}  // namespace

void CheckTextCopyOptions(const TextCopyOptions& options, bool from) {
  if (options.delim == '\r' || options.delim == '\n') {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("COPY delimiter cannot be newline or carriage return"));
  }
  if (options.null_str.find_first_of("\r\n") != std::string::npos) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(
        "COPY null representation cannot use newline or carriage return"));
  }
  if (std::string_view{"\\.abcdefghijklmnopqrstuvwxyz0123456789"}.contains(
        options.delim)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("COPY delimiter cannot be \"",
                            std::string_view{&options.delim, 1}, "\""));
  }
  if (options.null_str.contains(options.delim)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("COPY delimiter character must not appear in the "
                            "NULL specification"));
  }
  if (!from && options.ignore_errors) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("COPY ON_ERROR cannot be used with COPY TO"));
  }
  if (options.reject_limit != 0 && !options.ignore_errors) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE"));
  }
}

void ResolveTextCopyOptions(
  const duckdb::identifier_map_t<duckdb::vector<duckdb::Value>>& options,
  TextCopyOptions& out) {
  for (const auto& [key, values] : options) {
    if (values.empty()) {
      ApplyTextCopyOption(out, key.GetIdentifierName(), {});
    } else {
      ApplyTextCopyOption(out, key.GetIdentifierName(),
                          values[0].GetValue<std::string>());
    }
  }
}

TextCopyOptions ResolveTextCopyOptions(
  const duckdb::identifier_map_t<duckdb::vector<duckdb::Value>>& options) {
  TextCopyOptions result;
  ResolveTextCopyOptions(options, result);
  return result;
}

TextCopyOptions ResolveTextCopyOptions(
  const duckdb::identifier_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>&
    parsed_options) {
  TextCopyOptions result;
  for (const auto& [key, expr] : parsed_options) {
    if (!expr) {
      // A bare boolean flag (e.g. HEADER with no value) means true.
      ApplyTextCopyOption(result, key.GetIdentifierName(), {});
      continue;
    }
    if (expr->GetExpressionClass() != duckdb::ExpressionClass::CONSTANT) {
      continue;
    }
    ApplyTextCopyOption(result, key.GetIdentifierName(),
                        expr->Cast<duckdb::ConstantExpression>()
                          .GetLiteral()
                          .ToValue()
                          .GetValue<std::string>());
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
    duckdb::TableFunction("pg_text_copy_from", {}, ScanCopyFrom,
                          /*bind=*/nullptr, InitGlobalFrom, InitCopyFromLocal);
  loader.RegisterFunction(std::move(func));
}

}  // namespace sdb::connector
