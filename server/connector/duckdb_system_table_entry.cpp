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

#include "connector/duckdb_system_table_entry.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/storage/table_storage_info.hpp>

#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

struct SystemTableBindData : public duckdb::FunctionData {
  SystemTableEntry* entry = nullptr;
  std::vector<std::string> column_names;
  int64_t tableoid = 0;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    auto copy = duckdb::make_uniq<SystemTableBindData>();
    copy->entry = entry;
    copy->column_names = column_names;
    copy->tableoid = tableoid;
    return copy;
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return entry == other.Cast<SystemTableBindData>().entry;
  }
};

struct SystemTableState : public duckdb::GlobalTableFunctionState {
  // Keeps snapshot alive so MaterializedData (owned by snapshot) stays valid.
  std::shared_ptr<catalog::VirtualTableSnapshot> snapshot;
  const catalog::MaterializedData* data = nullptr;
  duckdb::idx_t offset = 0;
  // Maps output column index -> data column index (INVALID_INDEX for tableoid).
  std::vector<duckdb::idx_t> column_mapping;
  int64_t tableoid = 0;
};

duckdb::BindInfo SystemTableGetBindInfo(
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  auto& data = bind_data->Cast<SystemTableBindData>();
  return duckdb::BindInfo(*data.entry);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SystemTableInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SystemTableBindData>();
  auto state = duckdb::make_uniq_base<duckdb::GlobalTableFunctionState,
                                      SystemTableState>();
  auto& s = state->Cast<SystemTableState>();
  s.snapshot = bind_data.entry->CreateSnapshot(context);
  s.data = &s.snapshot->GetData(bind_data.column_names);
  s.tableoid = bind_data.tableoid;

  // Build column mapping: output col -> data col (or INVALID_INDEX for virtual)
  for (auto col_id : input.column_ids) {
    if (col_id == kColumnIdentifierTableOid) {
      s.column_mapping.push_back(duckdb::DConstants::INVALID_INDEX);
    } else {
      s.column_mapping.push_back(col_id);
    }
  }

  return state;
}

void SystemTableScan(duckdb::ClientContext& context,
                     duckdb::TableFunctionInput& input,
                     duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<SystemTableState>();

  if (state.offset >= state.data->row_count) {
    return;
  }

  auto remaining = state.data->row_count - state.offset;
  duckdb::idx_t count = duckdb::MinValue<duckdb::idx_t>(
    remaining, static_cast<duckdb::idx_t>(STANDARD_VECTOR_SIZE));

  for (duckdb::idx_t col = 0; col < output.ColumnCount(); col++) {
    auto data_col = state.column_mapping[col];
    if (data_col == duckdb::DConstants::INVALID_INDEX) {
      // tableoid -- constant vector with the table's OID
      output.data[col].Reference(duckdb::Value::BIGINT(state.tableoid),
                                 duckdb::count_t(count));
    } else {
      output.data[col].Slice(state.data->columns[data_col], state.offset,
                             state.offset + count);
    }
  }
  output.SetChildCardinality(count);
  state.offset += count;
}

}  // namespace

SystemTableEntry::SystemTableEntry(duckdb::Catalog& catalog,
                                   duckdb::SchemaCatalogEntry& schema,
                                   duckdb::CreateTableInfo& info,
                                   const catalog::VirtualTable& virtual_table)
  : duckdb::TableCatalogEntry(catalog, schema, info),
    _virtual_table(virtual_table),
    _system_object{virtual_table.Id(), virtual_table.GetName(),
                   catalog::Acl{virtual_table.GetAcl().begin(),
                                virtual_table.GetAcl().end()}} {}

duckdb::unique_ptr<duckdb::BaseStatistics> SystemTableEntry::GetStatistics(
  duckdb::ClientContext&, duckdb::column_t) {
  return nullptr;
}

// TODO(Dronplane): consider caching if that will start to slowdown queries.
// Beware that storing cache in catalog snapshot will require syncing write
// access between connections
std::shared_ptr<catalog::VirtualTableSnapshot> SystemTableEntry::CreateSnapshot(
  duckdb::ClientContext& context) {
  auto& conn_ctx = GetSereneDBContext(context);
  return _virtual_table.CreateSnapshot(conn_ctx.GetDatabaseId(), conn_ctx);
}

duckdb::TableFunction SystemTableEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto data = duckdb::make_uniq<SystemTableBindData>();
  data->entry = this;
  data->tableoid = static_cast<int64_t>(_virtual_table.Id().id());

  auto row_type = _virtual_table.RowType();
  for (auto& [name, type] : duckdb::StructType::GetChildTypes(row_type)) {
    data->column_names.emplace_back(name.GetIdentifierName());
  }

  bind_data = std::move(data);

  duckdb::TableFunction func("system_table_scan", {}, SystemTableScan,
                             nullptr /* bind */, SystemTableInit);
  func.projection_pushdown = true;
  func.get_bind_info = SystemTableGetBindInfo;
  return func;
}

duckdb::virtual_column_map_t SystemTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  result.insert({kColumnIdentifierTableOid,
                 duckdb::TableColumn("tableoid", duckdb::LogicalType::BIGINT)});
  return result;
}

duckdb::TableStorageInfo SystemTableEntry::GetStorageInfo(
  duckdb::ClientContext&) {
  return {};
}

}  // namespace sdb::connector
