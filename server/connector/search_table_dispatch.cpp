////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "connector/search_table_dispatch.h"

#include <duckdb.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <optional>
#include <string>

#include "basics/assert.h"
#include "catalog/table.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

// Extracts a string-valued WITH option. Returns std::nullopt if absent.
// Throws on non-string shapes.
std::optional<std::string> ExtractString(std::string_view option_key,
                                         const duckdb::ParsedExpression& expr) {
  if (expr.GetExpressionType() != duckdb::ExpressionType::VALUE_CONSTANT) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("WITH option \"", option_key, "\" expects a string literal"));
  }
  auto& cexpr = expr.Cast<duckdb::ConstantExpression>();
  try {
    return cexpr.GetValue()
      .DefaultCastAs(duckdb::LogicalType::VARCHAR)
      .GetValue<std::string>();
  } catch (...) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("WITH option \"", option_key, "\" expects a string literal"));
  }
}

}  // namespace

void RejectIfSearchTable(const catalog::Table& table,
                         std::string_view operation) {
  if (table.GetEngine() == catalog::TableEngine::Fast) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(operation, " on a search-backed table is not yet supported"));
  }
}

void ApplyStorageKind(
  catalog::CreateTableOptions& options,
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& with_options) {
  static constexpr std::string_view kStorageKey = "storage";
  auto it = with_options.find(std::string{kStorageKey});
  if (it == with_options.end() || !it->second) {
    return;  // default TableEngine::Transactional
  }
  auto value = ExtractString(kStorageKey, *it->second);
  SDB_ASSERT(value);
  auto lower = duckdb::StringUtil::Lower(*value);
  if (lower == "transactional") {
    options.engine = catalog::TableEngine::Transactional;
  } else if (lower == "search") {
    options.engine = catalog::TableEngine::Fast;
  } else {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("WITH option \"", kStorageKey,
              "\" must be 'transactional' or 'search', got \"", *value, "\""));
  }
}

}  // namespace sdb::connector
