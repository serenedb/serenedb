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
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/parser/parsed_data/create_sequence_info.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <optional>
#include <string>

#include "basics/assert.h"
#include "catalog1/catalog.h"
#include "catalog1/entry/search_table.h"
#include "catalog1/lookup.h"
#include "connector/primary_key.h"
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

duckdb::Identifier GeneratedPkSequenceName(const duckdb::Identifier& table) {
  return duckdb::Identifier{table.GetIdentifierName() + "__sdb_pk_seq"};
}

void EnsureGeneratedPkSequence(duckdb::CatalogTransaction transaction,
                               duckdb::DuckSchemaEntry& schema,
                               const catalog::SearchTableEntry& entry) {
  if (!primary_key::KeyColumns(entry).empty()) {
    return;
  }
  duckdb::CreateSequenceInfo info;
  info.SetSequenceName(GeneratedPkSequenceName(entry.name));
  info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  info.internal = true;
  schema.CreateSequence(transaction, info);
}

duckdb::optional_ptr<duckdb::SequenceCatalogEntry> FindGeneratedPkSequence(
  duckdb::ClientContext& context, const catalog::SearchTableEntry& entry) {
  auto& schema = const_cast<duckdb::SchemaCatalogEntry&>(entry.ParentSchema());
  auto found = schema.GetEntry(entry.catalog.GetCatalogTransaction(context),
                               duckdb::CatalogType::SEQUENCE_ENTRY,
                               GeneratedPkSequenceName(entry.name));
  return found ? &found->Cast<duckdb::SequenceCatalogEntry>() : nullptr;
}

SearchWriteTarget ResolveSearchWriteTarget(
  duckdb::ClientContext& context, const catalog::SearchTableEntry& entry) {
  SearchWriteTarget target;
  target.table_id = entry.oid;
  target.data = entry.EnsureStorage();
  const auto& columns = entry.GetColumns();
  target.column_ids.reserve(columns.LogicalColumnCount());
  target.chunk_types.reserve(columns.LogicalColumnCount());
  for (const auto& column : columns.Logical()) {
    target.column_ids.emplace_back(column.Oid());
    target.chunk_types.push_back(column.Type());
  }
  const auto pk_indexes = primary_key::KeyColumns(entry);
  target.pk_columns.reserve(pk_indexes.size());
  for (const auto index : pk_indexes) {
    target.pk_columns.push_back(
      {.input_col_idx = index.index, .type = columns.GetColumn(index).Type()});
  }
  if (pk_indexes.empty()) {
    target.generated_pk_seq = FindGeneratedPkSequence(context, entry);
    SDB_ASSERT(target.generated_pk_seq);
  }
  return target;
}

std::vector<primary_key::PKColumn> RowIdentityPKColumns(
  const SearchWriteTarget& target,
  std::span<const duckdb::idx_t> chunk_positions) {
  std::vector<primary_key::PKColumn> out;
  out.reserve(chunk_positions.size());
  for (size_t i = 0; i != chunk_positions.size(); ++i) {
    out.push_back({.input_col_idx = chunk_positions[i],
                   .type = i < target.pk_columns.size()
                             ? target.pk_columns[i].type
                             : duckdb::LogicalType::BIGINT});
  }
  return out;
}

void ApplyStorageKind(duckdb::optional_ptr<duckdb::ClientContext> context,
                      duckdb::CreateTableInfo& info) {
  auto& with_options = info.options;
  if (ReadStorageEngine(with_options) != catalog::TableEngine::Search) {
    with_options.erase(std::string{catalog::kStorageOption});
    return;
  }
  catalog::SearchTableOptions search_options =
    catalog::ReadSearchTableOptions(with_options);
  if (context) {
    const auto resolve = [&](std::string_view key, uint32_t& out) {
      if (with_options.find(std::string{key}) == with_options.end()) {
        out = ResolveUintWithOption(*context, key, nullptr);
      }
    };
    resolve(kRefreshIntervalSetting, search_options.refresh_interval_ms);
    resolve(kCompactionIntervalSetting, search_options.compaction_interval_ms);
    resolve(kCleanupIntervalStepSetting, search_options.cleanup_interval_step);
  }
  catalog::WriteSearchTableOptions(search_options, with_options);
}

}  // namespace sdb::connector
namespace sdb::catalog {}  // namespace sdb::catalog
