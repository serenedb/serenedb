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

#include "connector/duckdb_index_scan_entry.h"

#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/storage/table_storage_info.hpp>

#include "basics/assert.h"
#include "catalog/store/store.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"

namespace sdb::connector {

SereneDBIndexScanEntry::SereneDBIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::vector<size_t> indexed_col_indices)
  : duckdb::TableCatalogEntry(catalog, schema, info),
    _indexed_col_indices(std::move(indexed_col_indices)) {}

duckdb::unique_ptr<duckdb::BaseStatistics>
SereneDBIndexScanEntry::GetStatistics(duckdb::ClientContext& /*context*/,
                                      duckdb::column_t /*column_id*/) {
  return nullptr;
}

InvertedIndexScanEntry::InvertedIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::vector<size_t> indexed_col_indices,
  std::shared_ptr<const catalog::InvertedIndex> inverted_index)
  : SereneDBIndexScanEntry(catalog, schema, info,
                           std::move(indexed_col_indices)),
    _inverted_index(std::move(inverted_index)) {
  SDB_ASSERT(_inverted_index);
}

TableInvertedIndexScanEntry::TableInvertedIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
  std::vector<size_t> indexed_col_indices,
  std::shared_ptr<const catalog::InvertedIndex> inverted_index)
  : InvertedIndexScanEntry(catalog, schema, info,
                           std::move(indexed_col_indices),
                           std::move(inverted_index)),
    _sdb_table(std::move(sdb_table)) {
  SDB_ASSERT(_sdb_table);
  _relation = _sdb_table.get();
}

duckdb::TableFunction TableInvertedIndexScanEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(_inverted_index->GetId());
  auto data = duckdb::make_uniq<TableScanBindData>();
  data->table = _sdb_table;
  for (const auto& col : _sdb_table->Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    data->column_ids.push_back(col.GetId());
    data->column_types.push_back(col.type);
  }
  data->table_entry = this;
  data->entry_kind = ScanEntryKind::InvertedIndex;
  data->inverted_index = _inverted_index;
  data->lookup_label = "table";
  data->snapshot = std::move(snapshot);
  bind_data = std::move(data);
  return CreateIResearchScanFunction();
}

duckdb::TableStorageInfo TableInvertedIndexScanEntry::GetStorageInfo(
  duckdb::ClientContext& /*context*/) {
  return SereneDBTableEntry::BuildStorageInfo(*_sdb_table);
}

duckdb::vector<duckdb::column_t> TableInvertedIndexScanEntry::GetRowIdColumns()
  const {
  return SereneDBTableEntry::BuildRowIdColumns(*_sdb_table,
                                               _indexed_col_indices);
}

duckdb::virtual_column_map_t TableInvertedIndexScanEntry::GetVirtualColumns()
  const {
  return SereneDBTableEntry::BuildVirtualColumns(*_sdb_table,
                                                 _indexed_col_indices);
}

ViewInvertedIndexScanEntry::ViewInvertedIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info,
  std::shared_ptr<const catalog::PgSqlView> sdb_view,
  std::vector<size_t> indexed_col_indices,
  std::shared_ptr<const catalog::InvertedIndex> inverted_index)
  : InvertedIndexScanEntry(catalog, schema, info,
                           std::move(indexed_col_indices),
                           std::move(inverted_index)),
    _sdb_view(std::move(sdb_view)) {
  SDB_ASSERT(_sdb_view);
  _relation = _sdb_view.get();
}

duckdb::TableFunction ViewInvertedIndexScanEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(_inverted_index->GetId());
  // The index only captures post-WHERE/ORDER/LIMIT rows; we must not
  // stream the reader directly.
  auto data = duckdb::make_uniq<ViewScanBindData>();
  data->view = _sdb_view;
  const auto& vinfo = _sdb_view->GetInfo();
  for (size_t i = 0; i < vinfo.names.size(); ++i) {
    data->column_ids.push_back(static_cast<catalog::Column::Id>(i));
    data->column_types.push_back(vinfo.types[i]);
  }
  data->table_entry = this;
  data->entry_kind = ScanEntryKind::InvertedIndex;
  data->inverted_index = _inverted_index;
  if (auto fp = ResolveViewFastPath(context, *_sdb_view)) {
    data->lookup_label = FormatLookupLabel(*fp);
    data->lookup_supports_filters = fp->supports_filters;
  } else {
    data->lookup_label = "view";
  }
  data->snapshot = std::move(snapshot);
  bind_data = std::move(data);
  return CreateIResearchScanFunction();
}

duckdb::TableStorageInfo ViewInvertedIndexScanEntry::GetStorageInfo(
  duckdb::ClientContext& /*context*/) {
  return duckdb::TableStorageInfo{};
}

duckdb::vector<duckdb::column_t> ViewInvertedIndexScanEntry::GetRowIdColumns()
  const {
  return {kColumnIdentifierGeneratedPk};
}

duckdb::virtual_column_map_t ViewInvertedIndexScanEntry::GetVirtualColumns()
  const {
  duckdb::virtual_column_map_t result;
  result.reserve(3);
  result.emplace(kColumnIdentifierTableOid,
                 duckdb::TableColumn{"tableoid", duckdb::LogicalType::BIGINT});
  result.emplace(
    kColumnIdentifierGeneratedPk,
    duckdb::TableColumn{"generated_pk", duckdb::LogicalType::ROW_TYPE});
  result.emplace(duckdb::COLUMN_IDENTIFIER_EMPTY,
                 duckdb::TableColumn{"", duckdb::LogicalType::BOOLEAN});
  return result;
}

SecondaryIndexScanEntry::SecondaryIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::vector<size_t> indexed_col_indices,
  ObjectId secondary_index_id, bool sk_unique)
  : SereneDBIndexScanEntry(catalog, schema, info,
                           std::move(indexed_col_indices)),
    _secondary_index_id(secondary_index_id),
    _sk_unique(sk_unique) {
  SDB_ASSERT(_secondary_index_id != ObjectId{});
}

TableSecondaryIndexScanEntry::TableSecondaryIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
  std::vector<size_t> indexed_col_indices, ObjectId secondary_index_id,
  bool sk_unique)
  : SecondaryIndexScanEntry(catalog, schema, info,
                            std::move(indexed_col_indices), secondary_index_id,
                            sk_unique),
    _sdb_table(std::move(sdb_table)) {
  SDB_ASSERT(_sdb_table);
  _relation = _sdb_table.get();
}

duckdb::TableFunction TableSecondaryIndexScanEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  // Scanning a secondary index by name reads the table: the index itself
  // is a native ART on the store table.
  auto store_name = catalog::StoreTableName(
    ParentCatalog().GetName().GetIdentifierName(),
    ParentSchema().name.GetIdentifierName(), _sdb_table->GetName());
  auto& store_entry =
    duckdb::Catalog::GetEntry(
      context, duckdb::CatalogType::TABLE_ENTRY,
      duckdb::QualifiedName(duckdb::Identifier{catalog::kStoreDatabaseName},
                            duckdb::Identifier{"main"},
                            duckdb::Identifier{store_name}))
      .Cast<duckdb::TableCatalogEntry>();
  auto function = store_entry.GetScanFunction(context, bind_data);
  if (bind_data) {
    if (auto* table_bind =
          dynamic_cast<duckdb::TableScanBindData*>(bind_data.get())) {
      table_bind->display_name = name.GetIdentifierName();
    }
  }
  return function;
}

duckdb::TableStorageInfo TableSecondaryIndexScanEntry::GetStorageInfo(
  duckdb::ClientContext& /*context*/) {
  return SereneDBTableEntry::BuildStorageInfo(*_sdb_table);
}

}  // namespace sdb::connector
