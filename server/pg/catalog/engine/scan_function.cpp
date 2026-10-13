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

#include "pg/catalog/engine/scan_function.h"

#include <algorithm>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/node_statistics.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>

#include "auth/role_closure.h"
#include "catalog/entry/system_table.h"
#include "connector/duckdb_client_state.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

struct SystemTableBindData final : duckdb::FunctionData {
  const pg::SystemTable* table = nullptr;
  duckdb::DuckCatalog* catalog = nullptr;
  catalog::SystemTableEntry* entry = nullptr;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<SystemTableBindData>(*this);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    const auto& data = other.Cast<SystemTableBindData>();
    return table == data.table && catalog == data.catalog &&
           entry == data.entry;
  }
};

struct SystemFunctionInfo final : duckdb::TableFunctionInfo {
  explicit SystemFunctionInfo(const pg::SystemTable& table) : table{table} {}

  const pg::SystemTable& table;
};

duckdb::BindInfo SystemTableGetBindInfo(
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  return duckdb::BindInfo{*bind_data->Cast<SystemTableBindData>().entry};
}

void SystemTableNoScan(duckdb::ClientContext&, duckdb::TableFunctionInput&,
                       duckdb::DataChunk&) {}

duckdb::unique_ptr<duckdb::NodeStatistics> SystemTableCardinality(
  duckdb::ClientContext&, const duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<duckdb::NodeStatistics>(
    bind_data->Cast<SystemTableBindData>().table->Rows());
}

duckdb::unique_ptr<duckdb::BaseStatistics> SystemTableStatistics(
  duckdb::ClientContext&, duckdb::TableFunctionGetStatisticsInput& input) {
  const auto& table = *input.bind_data->Cast<SystemTableBindData>().table;
  const auto column = input.column_index.GetPrimaryIndex();
  const auto& sql = table.Sql();
  if (column >= sql.columns.size()) {
    return nullptr;
  }
  const auto* fact = table.FactOf(static_cast<uint32_t>(column));
  duckdb::idx_t distinct = 0;
  switch (table.KeyOf(static_cast<uint32_t>(column))) {
    case pg::SystemKey::None:
      if (!fact) {
        return nullptr;
      }
      break;
    case pg::SystemKey::Unique:
      distinct = table.Rows();
      break;
    case pg::SystemKey::Indexed:
      distinct = std::max<duckdb::idx_t>(table.Rows() / 10, 1);
      break;
  }
  const auto& type =
    table.Columns().GetColumn(duckdb::LogicalIndex{column}).Type();
  auto stats = duckdb::BaseStatistics::CreateUnknown(type);
  if (sql.columns[column].not_null) {
    stats.Set(duckdb::StatsInfo::CANNOT_HAVE_NULL_VALUES);
  }
  if (fact) {
    duckdb::NumericStats::SetMin(
      stats, duckdb::Value::BIGINT(fact->min).DefaultCastAs(type));
    duckdb::NumericStats::SetMax(
      stats, duckdb::Value::BIGINT(fact->max).DefaultCastAs(type));
  }
  if (distinct != 0) {
    stats.SetDistinctCount(distinct);
  }
  return stats.ToUnique();
}

bool SystemTablePushdownExpression(duckdb::ClientContext&,
                                   const duckdb::LogicalGet& get,
                                   duckdb::Expression& expr) {
  std::optional<duckdb::column_t> column;
  duckdb::ExpressionIterator::VisitExpression<duckdb::BoundColumnRefExpression>(
    expr, [&](const duckdb::BoundColumnRefExpression& ref) {
      column = get.GetColumnIndex(ref.Binding()).GetPrimaryIndex();
    });
  return column && pg::SystemPushdown(
                     *get.bind_data->Cast<SystemTableBindData>().table, *column,
                     expr) != duckdb::TableFilterPushdown::Reject;
}

duckdb::TableFilterPushdown SystemTableSupportsPushdownFilter(
  duckdb::FunctionData& bind_data, duckdb::idx_t column,
  duckdb::TableFilter& filter) {
  return pg::SystemPushdown(
    *bind_data.Cast<SystemTableBindData>().table, column,
    *duckdb::ExpressionFilter::GetExpressionFilter(filter, "SystemScan").expr);
}

duckdb::virtual_column_map_t SystemTableVirtualColumns(
  duckdb::ClientContext&,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  return bind_data->Cast<SystemTableBindData>().entry->GetVirtualColumns();
}

void SystemTableScanSerialize(
  duckdb::Serializer& serializer,
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data,
  const duckdb::BoundTableFunction&) {
  const auto& entry = *bind_data->Cast<SystemTableBindData>().entry;
  serializer.WriteProperty(100, "catalog", entry.catalog.GetName());
  serializer.WriteProperty(101, "schema", entry.ParentSchemaName());
  serializer.WriteProperty(102, "table", entry.name);
}

duckdb::unique_ptr<duckdb::FunctionData> SystemTableScanDeserialize(
  duckdb::Deserializer& deserializer, duckdb::BoundTableFunction& function) {
  const auto catalog =
    deserializer.ReadProperty<duckdb::Identifier>(100, "catalog");
  const auto schema =
    deserializer.ReadProperty<duckdb::Identifier>(101, "schema");
  const auto table =
    deserializer.ReadProperty<duckdb::Identifier>(102, "table");
  auto& entry = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    deserializer.Get<duckdb::ClientContext&>(),
    duckdb::QualifiedName(catalog, schema, table));
  auto data = duckdb::make_uniq<SystemTableBindData>();
  data->entry = &entry.Cast<catalog::SystemTableEntry>();
  data->table = &data->entry->Table();
  data->catalog = &entry.catalog.Cast<duckdb::DuckCatalog>();
  const auto& functions = data->table->Functions();
  function.init_global = functions.init;
  function.function = functions.scan;
  return data;
}

duckdb::TableFunction CreateSystemTableScanFunction(
  const pg::SystemScanFunctions& functions) {
  duckdb::TableFunction func{
    "system_table_scan", {}, functions.scan, nullptr, functions.init};
  func.projection_pushdown = true;
  func.filter_pushdown = true;
  func.filter_prune = true;
  func.pushdown_expression = SystemTablePushdownExpression;
  func.supports_pushdown_filter = SystemTableSupportsPushdownFilter;
  func.get_virtual_columns = SystemTableVirtualColumns;
  func.cardinality = SystemTableCardinality;
  func.statistics_extended = SystemTableStatistics;
  func.get_bind_info = SystemTableGetBindInfo;
  func.serialize = SystemTableScanSerialize;
  func.deserialize = SystemTableScanDeserialize;
  return func;
}

duckdb::unique_ptr<duckdb::FunctionData> SystemFunctionBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<duckdb::Identifier>& names) {
  const auto& table = input.info->Cast<SystemFunctionInfo>().table;
  if (table.SuperuserOnly()) {
    const auto state = context.registered_state->Get<SereneDBClientState>(
      kSereneDBClientStateKey);
    if (state &&
        !auth::ClosureFor(&context, state->GetConnectionContext().GetRoleId())
           ->is_superuser) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for function ", table.Sql().name));
    }
  }
  for (const auto& column : table.Columns().Logical()) {
    types.push_back(column.Type());
    names.push_back(column.Name());
  }
  auto data = duckdb::make_uniq<SystemTableBindData>();
  data->table = &table;
  data->catalog =
    &duckdb::Catalog::GetCatalog(
       context, duckdb::DatabaseManager::GetDefaultDatabase(context))
       .Cast<duckdb::DuckCatalog>();
  return data;
}

}  // namespace

duckdb::TableFunction BindSystemTableScan(
  catalog::SystemTableEntry& entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto data = duckdb::make_uniq<SystemTableBindData>();
  data->table = &entry.Table();
  data->catalog = &entry.catalog.Cast<duckdb::DuckCatalog>();
  data->entry = &entry;
  bind_data = std::move(data);
  return CreateSystemTableScanFunction(entry.Table().Functions());
}

void RegisterSystemTableScanFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  loader.RegisterFunction(
    CreateSystemTableScanFunction({nullptr, &SystemTableNoScan}));
  for (const auto* table : pg::SystemTableFunctions()) {
    auto function = CreateSystemTableScanFunction(table->Functions());
    function.name = duckdb::Identifier{table->Sql().name};
    function.bind = SystemFunctionBind;
    function.function_info =
      duckdb::make_shared_ptr<SystemFunctionInfo>(*table);
    function.get_virtual_columns = nullptr;
    function.get_bind_info = nullptr;
    function.serialize = nullptr;
    function.deserialize = nullptr;
    duckdb::CreateTableFunctionInfo info{std::move(function)};
    info.SetSchema(duckdb::Identifier{table->Sql().schema});
    info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }
}

duckdb::DuckCatalog& BoundSystemCatalog(
  const duckdb::TableFunctionInitInput& input) {
  return *input.bind_data->Cast<SystemTableBindData>().catalog;
}

const pg::SystemTable& BoundSystemTable(
  const duckdb::TableFunctionInitInput& input) {
  return *input.bind_data->Cast<SystemTableBindData>().table;
}

}  // namespace sdb::connector
