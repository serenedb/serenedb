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

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/storage/table_storage_info.hpp>
#include <filesystem>
#include <utility>

#include "basics/serializer.h"
#include "catalog1/catalog.h"
#include "connector/column_id.h"
#include "connector/duckdb_table_function.h"
#include "connector/primary_key.h"
#include "connector/with_option_resolver.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/config_variable_names.h"
#include "search/search_table.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kEngineSearch = "search";
constexpr std::string_view kPayloadOption = "sdb_payload";

using WithOptions =
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>;

duckdb::optional_ptr<const duckdb::ConstantExpression> FindConstant(
  const WithOptions& options, std::string_view key) {
  const auto it = options.find(std::string{key});
  if (it == options.end() || !it->second ||
      it->second->GetExpressionType() !=
        duckdb::ExpressionType::VALUE_CONSTANT) {
    return nullptr;
  }
  return &it->second->Cast<duckdb::ConstantExpression>();
}

std::optional<SearchTableOptions> Unpack(const WithOptions& options) {
  const auto payload = FindConstant(options, kPayloadOption);
  if (!payload) {
    return std::nullopt;
  }
  const auto& bytes = duckdb::StringValue::Get(payload->GetValue());
  duckdb::MemoryStream stream{
    const_cast<duckdb::data_ptr_t>(
      reinterpret_cast<duckdb::const_data_ptr_t>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  SearchTableOptions result;
  basics::ReadTuple(deserializer, result);
  return result;
}

duckdb::unique_ptr<duckdb::ParsedExpression> Pack(
  const SearchTableOptions& options) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  basics::WriteTuple(serializer, options);
  return duckdb::make_uniq<duckdb::ConstantExpression>(
    duckdb::Value::BLOB(stream.GetData(), stream.GetPosition()));
}

SearchTableOptions ResolveOptions(
  duckdb::optional_ptr<duckdb::ClientContext> context,
  const WithOptions& options) {
  SearchTableOptions result;
  const auto resolve = [&](std::string_view key, uint32_t& out) {
    if (const auto value = FindConstant(options, key)) {
      out = value->GetValue()
              .DefaultCastAs(duckdb::LogicalType::UINTEGER)
              .GetValue<uint32_t>();
    } else if (context) {
      out = connector::ResolveUintWithOption(*context, key, nullptr);
    }
  };
  resolve(kRefreshIntervalSetting, result.refresh_interval_ms);
  resolve(kCompactionIntervalSetting, result.compaction_interval_ms);
  resolve(kCleanupIntervalStepSetting, result.cleanup_interval_step);
  return result;
}

}  // namespace

SearchTableEntry::SearchTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::BoundCreateTableInfo& info, duckdb::CatalogTransaction transaction,
  std::shared_ptr<search::SearchTable> inherited_storage)
  : duckdb::TableCatalogEntry{catalog, schema, info.Base()},
    _storage{std::move(inherited_storage)} {
  auto& base = info.Base();
  comment = base.comment;
  tags = base.tags;
  dependencies = info.dependencies;
  if (auto persisted = Unpack(base.options)) {
    _options = std::move(*persisted);
  } else {
    _options = ResolveOptions(transaction.context, base.options);
  }
}

duckdb::unique_ptr<duckdb::BaseStatistics> SearchTableEntry::GetStatistics(
  duckdb::ClientContext&, duckdb::column_t) {
  return nullptr;
}

duckdb::TableStorageInfo SearchTableEntry::GetStorageInfo(
  duckdb::ClientContext&) {
  return {};
}

duckdb::virtual_column_map_t SearchTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  const auto keys = connector::primary_key::KeyColumns(*this);
  result.reserve(std::max<size_t>(keys.size(), 1));
  if (keys.empty()) {
    result.insert({connector::kColumnIdentifierGeneratedPk,
                   duckdb::TableColumn{duckdb::Identifier{"rowid"},
                                       duckdb::LogicalType::ROW_TYPE}});
    return result;
  }
  for (size_t i = 0; i != keys.size(); ++i) {
    const auto& column = GetColumns().GetColumn(keys[i]);
    result.insert({connector::kColumnIdentifierPrimaryKeyBase + i,
                   duckdb::TableColumn{column.Name(), column.Type()}});
  }
  return result;
}

duckdb::vector<duckdb::column_t> SearchTableEntry::GetRowIdColumns() const {
  duckdb::vector<duckdb::column_t> result;
  const auto keys = connector::primary_key::KeyColumns(*this);
  result.reserve(std::max<size_t>(keys.size(), 1));
  if (keys.empty()) {
    result.push_back(connector::kColumnIdentifierGeneratedPk);
    return result;
  }
  for (size_t i = 0; i != keys.size(); ++i) {
    result.push_back(connector::kColumnIdentifierPrimaryKeyBase + i);
  }
  return result;
}

void SearchTableEntry::BindUpdateConstraints(duckdb::Binder&,
                                             duckdb::LogicalGet& get,
                                             duckdb::LogicalProjection& proj,
                                             duckdb::LogicalUpdate& update,
                                             duckdb::ClientContext&) {
  // iresearch cannot edit a document in place, so every update rewrites the
  // whole row.
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
  auto& options = info->Cast<duckdb::CreateTableInfo>().options;
  options[std::string{kStorageOption}] =
    duckdb::make_uniq<duckdb::ConstantExpression>(
      duckdb::Value{std::string{kEngineSearch}});
  options[std::string{kPayloadOption}] = Pack(_options);
  return info;
}

std::string SearchTableEntry::ToSQL() const {
  auto info = GetInfo();
  info->Cast<duckdb::CreateTableInfo>().options.erase(
    std::string{kPayloadOption});
  return info->ToString();
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
  return duckdb::make_uniq<SearchTableEntry>(
    catalog, schema, *bound, catalog.GetCatalogTransaction(context), _storage);
}

}  // namespace sdb::catalog
