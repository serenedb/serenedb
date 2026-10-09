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
#include <duckdb/function/partition_stats.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/node_statistics.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>

#include "auth/role_closure.h"
#include "catalog/entry/system_table.h"
#include "pg/catalog/engine/system_table.h"

namespace sdb::connector {
namespace {

struct SystemTableBindData final : duckdb::FunctionData {
  catalog::SystemTableEntry* entry = nullptr;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    auto copy = duckdb::make_uniq<SystemTableBindData>();
    copy->entry = entry;
    return copy;
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return entry == other.Cast<SystemTableBindData>().entry;
  }
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
    bind_data->Cast<SystemTableBindData>().entry->Table().Sql().rows);
}

duckdb::unique_ptr<duckdb::BaseStatistics> SystemTableStatistics(
  duckdb::ClientContext&, duckdb::TableFunctionGetStatisticsInput& input) {
  const auto& table =
    input.bind_data->Cast<SystemTableBindData>().entry->Table();
  const auto column = input.column_index.GetPrimaryIndex();
  const auto& sql = table.Sql();
  if (column >= sql.columns.size()) {
    return nullptr;
  }
  const auto* fact = table.FactOf(static_cast<uint32_t>(column));
  duckdb::idx_t distinct = 0;
  switch (sql.columns[column].key) {
    case pg::SystemKey::None:
      if (!fact) {
        return nullptr;
      }
      break;
    case pg::SystemKey::Unique:
      distinct = sql.rows;
      break;
    case pg::SystemKey::Indexed:
      distinct = std::max<duckdb::idx_t>(sql.rows / 10, 1);
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
                                   const duckdb::LogicalGet&,
                                   duckdb::Expression& expr) {
  return pg::RangeOf(expr).has_value() || pg::BooleanOf(expr).has_value();
}

duckdb::vector<duckdb::PartitionStatistics> SystemTablePartitionStats(
  duckdb::ClientContext&, duckdb::GetPartitionStatsInput& input) {
  const auto& table =
    input.bind_data->Cast<SystemTableBindData>().entry->Table();
  if (table.Cells().empty()) {
    return {};
  }
  duckdb::PartitionStatistics stats;
  stats.count = table.Cells().size() / table.Sql().columns.size();
  stats.count_type = duckdb::CountType::COUNT_EXACT;
  return {stats};
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
  const auto& functions = data->entry->Table().Functions();
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
  func.get_partition_stats = SystemTablePartitionStats;
  func.cardinality = SystemTableCardinality;
  func.statistics_extended = SystemTableStatistics;
  func.get_bind_info = SystemTableGetBindInfo;
  func.serialize = SystemTableScanSerialize;
  func.deserialize = SystemTableScanDeserialize;
  return func;
}

}  // namespace

duckdb::TableFunction BindSystemTableScan(
  catalog::SystemTableEntry& entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto data = duckdb::make_uniq<SystemTableBindData>();
  data->entry = &entry;
  bind_data = std::move(data);
  return CreateSystemTableScanFunction(entry.Table().Functions());
}

void RegisterSystemTableScanFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  loader.RegisterFunction(
    CreateSystemTableScanFunction({nullptr, &SystemTableNoScan}));
}

catalog::SystemTableEntry& BoundSystemTable(
  const duckdb::TableFunctionInitInput& input) {
  return *input.bind_data->Cast<SystemTableBindData>().entry;
}

}  // namespace sdb::connector
