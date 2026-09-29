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

#include "connector/system_table_scan.h"

#include <algorithm>
#include <duckdb/main/extension/extension_loader.hpp>

#include "catalog/entry/system_table.h"
#include "connector/column_id.h"
#include "pg/virtual_table.h"

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

struct SystemTableState final : duckdb::GlobalTableFunctionState {
  pg::MaterializedData data;
  duckdb::vector<duckdb::column_t> column_ids;
  duckdb::idx_t offset = 0;
};

duckdb::BindInfo SystemTableGetBindInfo(
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  return duckdb::BindInfo{*bind_data->Cast<SystemTableBindData>().entry};
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SystemTableInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& entry = *input.bind_data->Cast<SystemTableBindData>().entry;
  auto state = duckdb::make_uniq<SystemTableState>();
  state->data = entry.Table().Materialize(entry.catalog, context);
  state->column_ids = input.column_ids;
  return state;
}

void SystemTableScanSerialize(
  duckdb::Serializer& serializer,
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data,
  const duckdb::TableFunction&) {
  const auto& entry = *bind_data->Cast<SystemTableBindData>().entry;
  serializer.WriteProperty(100, "catalog", entry.catalog.GetName());
  serializer.WriteProperty(101, "schema", entry.ParentSchemaName());
  serializer.WriteProperty(102, "table", entry.name);
}

duckdb::unique_ptr<duckdb::FunctionData> SystemTableScanDeserialize(
  duckdb::Deserializer& deserializer, duckdb::TableFunction&) {
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
  return data;
}

void SystemTableScan(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                     duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<SystemTableState>();
  if (state.offset >= state.data.size) {
    return;
  }
  const auto count = std::min<duckdb::idx_t>(state.data.size - state.offset,
                                             STANDARD_VECTOR_SIZE);
  const auto& table =
    input.bind_data->Cast<SystemTableBindData>().entry->Table();
  for (duckdb::idx_t column = 0; column < output.ColumnCount(); ++column) {
    const auto column_id = state.column_ids[column];
    if (column_id == kColumnIdentifierTableOid) {
      output.data[column].Reference(duckdb::Value::BIGINT(table.Id()),
                                    duckdb::count_t(count));
    } else {
      output.data[column].Slice(state.data.columns[column_id], state.offset,
                                state.offset + count);
    }
  }
  output.SetCardinality(count);
  state.offset += count;
}

duckdb::TableFunction CreateSystemTableScanFunction() {
  duckdb::TableFunction func{
    "system_table_scan", {}, SystemTableScan, nullptr, SystemTableInit};
  func.projection_pushdown = true;
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
  return CreateSystemTableScanFunction();
}

void RegisterSystemTableScanFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  loader.RegisterFunction(CreateSystemTableScanFunction());
}

}  // namespace sdb::connector
