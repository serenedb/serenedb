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
#include <iresearch/index/directory_reader.hpp>
#include <optional>
#include <string>

#include "basics/assert.h"
#include "catalog/table.h"
#include "connector/inverted_index_options_util.h"
#include "connector/with_option_resolver.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "query/config_variable_names.h"
#include "search/search_table.h"

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

// Evaluates an integer-valued WITH option.
// Throws on a non-integer literal.
duckdb::Value ExtractUint(std::string_view option_key,
                          const duckdb::ParsedExpression& expr) {
  if (expr.GetExpressionType() != duckdb::ExpressionType::VALUE_CONSTANT) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("WITH option \"", option_key, "\" expects an integer literal"));
  }
  auto& cexpr = expr.Cast<duckdb::ConstantExpression>();
  try {
    return cexpr.GetValue().DefaultCastAs(duckdb::LogicalType::UINTEGER);
  } catch (...) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("WITH option \"", option_key, "\" expects an integer literal"));
  }
}

// Extract + validate a byte-sized WITH option (e.g. segment_memory_max) through
// the same validator the inverted-index CREATE path uses, so a search table
// rejects the same values (0, non-integer, out of range) with identical errors.
uint64_t ExtractValidatedUbigint(std::string_view option_key,
                                 const duckdb::ParsedExpression& expr) {
  if (expr.GetExpressionType() != duckdb::ExpressionType::VALUE_CONSTANT) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("WITH option \"", option_key, "\" expects an integer literal"));
  }
  return ValidateInvertedIndexOptionValue(
    option_key, expr.Cast<duckdb::ConstantExpression>().GetValue());
}

constexpr std::string_view kStorageKey = "storage";

}  // namespace

catalog::TableEngine ReadStorageEngine(
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& with_options) {
  auto it = with_options.find(std::string{kStorageKey});
  if (it == with_options.end() || !it->second) {
    return catalog::TableEngine::Transactional;
  }
  auto value = ExtractString(kStorageKey, *it->second);
  SDB_ASSERT(value);
  auto lower = duckdb::StringUtil::Lower(*value);
  if (lower == "transactional") {
    return catalog::TableEngine::Transactional;
  }
  if (lower == "search") {
    return catalog::TableEngine::Search;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("WITH option \"", kStorageKey,
            "\" must be 'transactional' or 'search', got \"", *value, "\""));
}

void RejectIfSearchTable(const catalog::Table& table,
                         std::string_view operation) {
  if (table.GetEngine() == catalog::TableEngine::Search) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(operation, " on a search-backed table is not yet supported"));
  }
}

void ValidateSearchTableCreateIndex(const catalog::Table& table,
                                    std::string_view index_type) {
  if (table.GetEngine() != catalog::TableEngine::Search) {
    return;
  }
  if (duckdb::StringUtil::Lower(std::string{index_type}) != "inverted") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("only inverted indexes are supported on a search-backed table"));
  }
  const auto& shard = table.GetData();
  SDB_ASSERT(shard);
  shard->VacuumRefresh();  // publish committed WAL rows so live_docs is exact
  if (shard->GetDirectoryReader().live_docs_count() != 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("CREATE INDEX on a non-empty search-backed table is not yet "
              "supported (indexing existing rows)"));
  }
}

void ApplyStorageKind(
  duckdb::ClientContext& context, catalog::CreateTableOptions& options,
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>&
    with_options) {
  options.engine = ReadStorageEngine(with_options);
  with_options.erase(std::string{kStorageKey});
  if (options.engine != catalog::TableEngine::Search) {
    // Interval WITH options are search-only; leave any other keys in place so
    // the caller's unrecognized-parameter check still rejects them.
    return;
  }
  auto resolve = [&](std::string_view key) -> uint32_t {
    auto it = with_options.find(std::string{key});
    if (it != with_options.end() && it->second) {
      auto value = ExtractUint(key, *it->second);
      with_options.erase(std::string{key});
      return ResolveUintWithOption(context, key, &value);
    }
    return ResolveUintWithOption(context, key, /*with_value=*/nullptr);
  };
  auto resolve_ubigint = [&](std::string_view key) -> uint64_t {
    auto it = with_options.find(std::string{key});
    if (it != with_options.end() && it->second) {
      const uint64_t value = ExtractValidatedUbigint(key, *it->second);
      with_options.erase(std::string{key});
      return value;
    }
    return ResolveUbigintWithOption(context, key, /*with_value=*/nullptr);
  };
  options.search_options.refresh_interval_ms = resolve(kRefreshIntervalSetting);
  options.search_options.compaction_interval_ms =
    resolve(kCompactionIntervalSetting);
  options.search_options.cleanup_interval_step =
    resolve(kCleanupIntervalStepSetting);
  options.search_options.segment_memory_max =
    resolve_ubigint(kSegmentMemoryMaxSetting);
}

}  // namespace sdb::connector
