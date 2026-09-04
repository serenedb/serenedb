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

#include "catalog1/entry/search_table.h"

#include <absl/strings/numbers.h>

#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/storage/table_storage_info.hpp>
#include <filesystem>
#include <utility>

#include "catalog1/catalog.h"
#include <duckdb/planner/operator/logical_update.hpp>

#include "connector/column_id.h"
#include "connector/duckdb_table_function.h"
#include "connector/primary_key.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/config_variable_names.h"
#include "search/search_table.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kEngineSearch = "search";

uint32_t ReadUint(const TableOptions& options, std::string_view key,
                  uint32_t fallback) {
  const auto it = options.find(std::string{key});
  if (it == options.end() || !it->second ||
      it->second->GetExpressionType() !=
        duckdb::ExpressionType::VALUE_CONSTANT) {
    return fallback;
  }
  return it->second->Cast<duckdb::ConstantExpression>()
    .GetValue()
    .DefaultCastAs(duckdb::LogicalType::UINTEGER)
    .GetValue<uint32_t>();
}

void WriteUint(TableOptions& options, std::string_view key, uint32_t value) {
  options[std::string{key}] = duckdb::make_uniq<duckdb::ConstantExpression>(
    duckdb::Value::UINTEGER(value));
}

}  // namespace

SearchTableOptions ReadSearchTableOptions(const TableOptions& options) {
  SearchTableOptions result;
  result.refresh_interval_ms =
    ReadUint(options, kRefreshIntervalSetting, result.refresh_interval_ms);
  result.compaction_interval_ms = ReadUint(options, kCompactionIntervalSetting,
                                           result.compaction_interval_ms);
  result.cleanup_interval_step = ReadUint(options, kCleanupIntervalStepSetting,
                                          result.cleanup_interval_step);
  return result;
}

void WriteSearchTableOptions(const SearchTableOptions& search_options,
                             TableOptions& options) {
  options[std::string{kStorageOption}] =
    duckdb::make_uniq<duckdb::ConstantExpression>(
      duckdb::Value{std::string{kEngineSearch}});
  WriteUint(options, kRefreshIntervalSetting,
            search_options.refresh_interval_ms);
  WriteUint(options, kCompactionIntervalSetting,
            search_options.compaction_interval_ms);
  WriteUint(options, kCleanupIntervalStepSetting,
            search_options.cleanup_interval_step);
}

SearchTableEntry::SearchTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::BoundCreateTableInfo& info,
  std::shared_ptr<search::SearchTable> inherited_storage)
  : duckdb::TableCatalogEntry{catalog, schema, info.Base()},
    _storage{std::move(inherited_storage)} {
  auto& base = info.Base();
  comment = base.comment;
  tags = base.tags;
  dependencies = info.dependencies;
  _options = ReadSearchTableOptions(base.options);
}

duckdb::unique_ptr<duckdb::BaseStatistics> SearchTableEntry::GetStatistics(
  duckdb::ClientContext&, duckdb::column_t) {
  // iresearch keeps no duckdb-shaped per-column statistics.
  return nullptr;
}

duckdb::TableStorageInfo SearchTableEntry::GetStorageInfo(
  duckdb::ClientContext&) {
  // Cardinality is unknown without opening the store; duckdb treats an unset
  // cardinality as "no estimate" rather than as zero.
  return {};
}

duckdb::virtual_column_map_t SearchTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  const auto keys = connector::primary_key::KeyColumns(*this);
  if (keys.empty()) {
    result.insert({connector::kColumnIdentifierGeneratedPk,
                   duckdb::TableColumn{duckdb::Identifier{"__sdb_pk"},
                                       duckdb::LogicalType::BIGINT}});
    return result;
  }
  // Keyed by the real column's id so the binder reuses its projection; named
  // apart from it so the two do not collide in the bind context.
  for (size_t i = 0; i != keys.size(); ++i) {
    const auto& column = GetColumns().GetColumn(keys[i]);
    result.insert(
      {connector::kColumnIdentifierPrimaryKeyBase + i,
       duckdb::TableColumn{duckdb::Identifier{"__sdb_pk" + std::to_string(i)},
                           column.Type()}});
  }
  return result;
}

duckdb::vector<duckdb::column_t> SearchTableEntry::GetRowIdColumns() const {
  duckdb::vector<duckdb::column_t> result;
  const auto keys = connector::primary_key::KeyColumns(*this);
  if (keys.empty()) {
    result.push_back(connector::kColumnIdentifierGeneratedPk);
    return result;
  }
  for (size_t i = 0; i != keys.size(); ++i) {
    result.push_back(connector::kColumnIdentifierPrimaryKeyBase + i);
  }
  return result;
}

void SearchTableEntry::BindUpdateConstraints(duckdb::Binder& binder,
                                             duckdb::LogicalGet& get,
                                             duckdb::LogicalProjection& proj,
                                             duckdb::LogicalUpdate& update,
                                             duckdb::ClientContext& context) {
  duckdb::TableCatalogEntry::BindUpdateConstraints(binder, get, proj, update,
                                                   context);
  if (update.update_is_del_and_insert) {
    return;
  }
  // iresearch cannot edit a document in place, so every update rewrites the
  // whole row. The base only projects the whole row when it decides that
  // itself, so forcing the flag after it has run means repeating its tail.
  update.update_is_del_and_insert = true;
  update.update_column_count = 0;
  duckdb::physical_index_set_t all_columns;
  for (const auto& column : GetColumns().Physical()) {
    all_columns.insert(column.Physical());
  }
  duckdb::LogicalUpdate::BindExtraColumns(*this, get, proj, update,
                                          all_columns);
}

duckdb::unique_ptr<duckdb::CreateInfo> SearchTableEntry::GetInfo() const {
  auto info = duckdb::TableCatalogEntry::GetInfo();
  WriteSearchTableOptions(_options, info->Cast<duckdb::CreateTableInfo>().options);
  return info;
}

const std::shared_ptr<search::SearchTable>& SearchTableEntry::EnsureStorage()
  const {
  if (_storage) {
    return _storage;
  }
  const auto db_id = catalog.Cast<SereneDBCatalog>().GetOid();
  const auto is_new = !std::filesystem::exists(
    search::SearchTable::GetPath(db_id, schema.oid, oid));
  _storage =
    search::SearchTable::Create(db_id, schema.oid, oid, is_new, _options);
  _storage->StartTasks();
  return _storage;
}

duckdb::TableFunction SearchTableEntry::GetScanFunction(
  duckdb::ClientContext&, duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  return connector::BindSearchTableScan(*this, bind_data);
}

duckdb::unique_ptr<duckdb::CatalogEntry> SearchTableEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  auto binder = duckdb::Binder::CreateBinder(context);
  auto bound = binder->BindCreateTableInfo(std::move(info));
  return duckdb::make_uniq<SearchTableEntry>(catalog, schema, *bound, _storage);
}

}  // namespace sdb::catalog
