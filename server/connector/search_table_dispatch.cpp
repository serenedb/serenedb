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

}  // namespace

catalog::TableEngine ReadStorageEngine(
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& with_options) {
  auto it = with_options.find(std::string{catalog::kStorageOption});
  if (it == with_options.end() || !it->second) {
    return catalog::TableEngine::Transactional;
  }
  auto value = ExtractString(catalog::kStorageOption, *it->second);
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
    ERR_MSG("WITH option \"", catalog::kStorageOption,
            "\" must be 'transactional' or 'search', got \"", *value, "\""));
}

void RejectIfSearchTable(catalog::TableEngine engine,
                         std::string_view operation) {
  if (engine == catalog::TableEngine::Search) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(operation, " on a search-backed table is not yet supported"));
  }
}

void ValidateSearchTableCreateIndex(const catalog::SereneDBTableEntry& entry,
                                    std::string_view index_type) {
  if (!entry.IsSearchTable()) {
    return;
  }
  if (duckdb::StringUtil::Lower(std::string{index_type}) != "inverted") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("only inverted indexes are supported on a search-backed table"));
  }
  const auto& shard = entry.GetSearchData();
  SDB_ASSERT(shard);
  shard->VacuumRefresh();  // publish committed WAL rows so live_docs is exact
  if (shard->GetDirectoryReader().live_docs_count() != 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("CREATE INDEX on a non-empty search-backed table is not yet "
              "supported (indexing existing rows)"));
  }
}

SearchWriteTarget ResolveSearchWriteTarget(
  duckdb::ClientContext& context, const catalog::SereneDBTableEntry& entry) {
  SearchWriteTarget target;
  target.table_id = catalog::IdOf(entry);
  target.data = entry.GetSearchData();
  const auto& columns = entry.GetColumns();
  target.column_ids.reserve(columns.LogicalColumnCount());
  target.chunk_types.reserve(columns.LogicalColumnCount());
  for (const auto& column : columns.Logical()) {
    target.column_ids.emplace_back(column.CatalogOid());
    target.chunk_types.push_back(column.Type());
  }
  const auto pk_indexes = entry.GetPKColumnIndexes();
  target.pk_columns.reserve(pk_indexes.size());
  for (const auto index : pk_indexes) {
    target.pk_columns.push_back(
      {.input_col_idx = index.index, .type = columns.GetColumn(index).Type()});
  }
  if (pk_indexes.empty()) {
    target.generated_pk_seq = entry.GetGeneratedPkSequence(context);
    SDB_ASSERT(target.generated_pk_seq);
  }
  return target;
}

std::vector<catalog::duckdb_primary_key::PKColumn> RowIdentityPKColumns(
  const SearchWriteTarget& target,
  std::span<const duckdb::idx_t> chunk_positions) {
  std::vector<catalog::duckdb_primary_key::PKColumn> out;
  out.reserve(chunk_positions.size());
  for (size_t i = 0; i != chunk_positions.size(); ++i) {
    out.push_back({.input_col_idx = chunk_positions[i],
                   .type = i < target.pk_columns.size()
                             ? target.pk_columns[i].type
                             : duckdb::LogicalType::BIGINT});
  }
  return out;
}

void BuildReturnedRow(duckdb::DataChunk& out, duckdb::DataChunk& chunk,
                      std::span<const duckdb::idx_t> column_map) {
  const auto rows = chunk.size();
  for (duckdb::idx_t i = 0; i < out.ColumnCount(); ++i) {
    const auto from =
      i < column_map.size() ? column_map[i] : duckdb::DConstants::INVALID_INDEX;
    if (from == duckdb::DConstants::INVALID_INDEX) {
      out.data[i].Reference(duckdb::Value(out.data[i].GetType()),
                            duckdb::count_t(rows));
    } else {
      out.data[i].Reference(chunk.data[from]);
    }
  }
  out.SetCardinality(rows);
}

void ApplyStorageKind(
  duckdb::ClientContext& context, duckdb::CreateTableInfo& info,
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>&
    with_options) {
  const auto engine = ReadStorageEngine(with_options);
  with_options.erase(std::string{catalog::kStorageOption});
  catalog::persistence::SearchTableOptions search_options;
  if (engine == catalog::TableEngine::Search) {
    const auto resolve = [&](std::string_view key) -> uint32_t {
      auto it = with_options.find(std::string{key});
      if (it != with_options.end() && it->second) {
        auto value = ExtractUint(key, *it->second);
        with_options.erase(std::string{key});
        return ResolveUintWithOption(context, key, &value);
      }
      return ResolveUintWithOption(context, key, /*with_value=*/nullptr);
    };
    search_options.refresh_interval_ms = resolve(kRefreshIntervalSetting);
    search_options.compaction_interval_ms = resolve(kCompactionIntervalSetting);
    search_options.cleanup_interval_step = resolve(kCleanupIntervalStepSetting);
    const auto it = with_options.find(std::string{kSegmentMemoryMaxSetting});
    if (it != with_options.end() && it->second) {
      search_options.segment_memory_max =
        ExtractValidatedUbigint(kSegmentMemoryMaxSetting, *it->second);
      with_options.erase(std::string{kSegmentMemoryMaxSetting});
    } else {
      search_options.segment_memory_max = ResolveUbigintWithOption(
        context, kSegmentMemoryMaxSetting, /*with_value=*/nullptr);
    }
  }
  // The sequence feeding the synthetic primary key is not known until the
  // create runs under the catalog mutex; the tags are rewritten there.
  catalog::SetTableTags(info, engine, search_options, ObjectId{});
}

}  // namespace sdb::connector
