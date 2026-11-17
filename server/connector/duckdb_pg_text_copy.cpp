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
#include <duckdb/common/operator/cast_operators.hpp>
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
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "connector/copy_from_batches.h"
#include "connector/copy_to_file.h"
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
  if (bool parsed = false;
      duckdb::TryCastStringBool(value.data(), value.size(), parsed, false)) {
    return parsed;
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

class PgTextCopyEncoder final : public CopyChunkEncoder {
 public:
  PgTextCopyEncoder(duckdb::ClientContext& context,
                    std::span<const duckdb::LogicalType> types, char delim,
                    const std::string& null_str) {
    sdb::pg::FillContext(ConnectionContext::Get(context), _ctx);
    sdb::pg::SetCopyText(_ctx, delim, null_str);
    _serializers.reserve(types.size());
    for (const auto& type : types) {
      _serializers.push_back(
        sdb::pg::GetSerialization(type, sdb::pg::VarFormat::CopyText, _ctx));
    }
  }

  void Encode(const duckdb::DataChunk& chunk, message::Buffer& out) final {
    const auto rows = chunk.size();
    if (rows == 0) {
      return;
    }
    const auto columns = chunk.ColumnCount();
    auto& decoded = _ctx.decoded;
    decoded.clear();
    decoded.resize(columns);
    for (duckdb::idx_t column = 0; column < columns; ++column) {
      duckdb::Vector::RecursiveToUnifiedFormat(chunk.data[column], rows,
                                               decoded[column]);
    }
    for (duckdb::idx_t row = 0; row < rows; ++row) {
      message::Writer writer{out};
      _ctx.writer = &writer;
      for (duckdb::idx_t column = 0; column < columns; ++column) {
        if (column > 0) {
          writer.Write({&_ctx.copy_delim, 1});
        }
        _serializers[column](_ctx, decoded[column], row);
      }
      writer.Write("\n");
      writer.Commit(false);
    }
  }

 private:
  sdb::pg::SerializationContext _ctx;
  std::vector<sdb::pg::SerializationFunction> _serializers;
};

class PgTextCopyBindData final : public CopyToFileBindData {
 public:
  PgTextCopyBindData(duckdb::vector<duckdb::LogicalType> types, char delim,
                     std::string null_str, bool header,
                     duckdb::vector<duckdb::Identifier> names)
    : _sql_types{std::move(types)},
      _delim{delim},
      _null_str{std::move(null_str)},
      _header{header},
      _names{std::move(names)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<PgTextCopyBindData>(_sql_types, _delim, _null_str,
                                                 _header, _names);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    const auto& o = other.Cast<PgTextCopyBindData>();
    return _sql_types == o._sql_types && _delim == o._delim &&
           _null_str == o._null_str && _header == o._header &&
           _names == o._names;
  }

  std::unique_ptr<CopyChunkEncoder> MakeEncoder(
    duckdb::ClientContext& context) const final {
    return std::make_unique<PgTextCopyEncoder>(context, _sql_types, _delim,
                                               _null_str);
  }

  void WriteHeader(duckdb::ClientContext& context,
                   message::Buffer& out) const final {
    if (!_header) {
      return;
    }
    sdb::pg::SerializationContext ctx;
    sdb::pg::FillContext(ConnectionContext::Get(context), ctx);
    sdb::pg::SetCopyText(ctx, _delim, _null_str);
    message::Writer writer{out};
    ctx.writer = &writer;
    sdb::pg::SerializeCopyTextHeader(ctx, _names);
    writer.Commit(false);
  }

  void WriteTrailer(message::Buffer&) const final {}

 private:
  duckdb::vector<duckdb::LogicalType> _sql_types;
  char _delim;
  std::string _null_str;
  bool _header;
  duckdb::vector<duckdb::Identifier> _names;
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
  SetCopyToFileCallbacks(func);
  func.copy_from_bind = BindFrom;
  func.copy_from_function =
    duckdb::TableFunction("pg_text_copy_from", {}, ScanCopyFrom,
                          /*bind=*/nullptr, InitGlobalFrom, InitCopyFromLocal);
  loader.RegisterFunction(std::move(func));
}

}  // namespace sdb::connector
