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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/storage/table/row_group_collection.hpp>
#include <duckdb/storage/table_storage_info.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "catalog/store/store.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {

SereneDBIndexScanEntry::SereneDBIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, const catalog::CreateIndexInfoBase& index,
  std::vector<size_t> indexed_col_indices)
  : duckdb::TableCatalogEntry(catalog, schema, info),
    _indexed_col_indices(std::move(indexed_col_indices)) {
  // Durability and reclamation are the serenedb catalog log's, not duckdb's.
  // An index carries no owner and no ACL of its own; what governs access is
  // the relation it stands in front of, which SetIndexedRelation brings in.
  catalog::AdoptEntryIdentity(*this, index.GetId());
}

void SereneDBIndexScanEntry::SetIndexedRelation(IndexedRelation relation) {
  _relation_perm = std::move(relation.perm);
  _relation_type = relation.type;
  _relation_id = relation.id;
  _relation_name = std::move(relation.name);
  _relation_column_acls = std::move(relation.column_acls);
}

duckdb::unique_ptr<duckdb::BaseStatistics>
SereneDBIndexScanEntry::GetStatistics(duckdb::ClientContext& /*context*/,
                                      duckdb::column_t /*column_id*/) {
  return nullptr;
}

const catalog::CreateIndexInfoBase& InvertedIndexScanEntry::IndexOf(
  const catalog::IndexInfoRef& index) {
  SDB_ASSERT(index);
  return *index;
}

InvertedIndexScanEntry::InvertedIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::vector<size_t> indexed_col_indices,
  catalog::IndexInfoRef inverted_index)
  : SereneDBIndexScanEntry(catalog, schema, info, IndexOf(inverted_index),
                           std::move(indexed_col_indices)),
    _inverted_index(std::move(inverted_index)) {
  SDB_ASSERT(_inverted_index);
}

TableInvertedIndexScanEntry::TableInvertedIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, IndexedRelation relation,
  std::vector<size_t> indexed_col_indices, catalog::IndexInfoRef inverted_index)
  : InvertedIndexScanEntry(catalog, schema, info,
                           std::move(indexed_col_indices),
                           std::move(inverted_index)) {
  SetIndexedRelation(std::move(relation));
}

duckdb::TableFunction TableInvertedIndexScanEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(*_inverted_index);
  auto data = duckdb::make_uniq<TableScanBindData>();
  for (const auto& col : GetColumns().Logical()) {
    data->column_ids.emplace_back(col.HostId());
    data->column_types.push_back(col.Type());
  }
  data->table_entry = this;
  data->entry_kind = ScanEntryKind::InvertedIndex;
  data->inverted_index = catalog::InvertedInfoRef(_inverted_index);
  data->inverted_storage = _inverted_index->GetData();
  data->lookup_label = "table";
  data->snapshot = std::move(snapshot);
  bind_data = std::move(data);
  return CreateIResearchScanFunction();
}

duckdb::TableStorageInfo TableInvertedIndexScanEntry::GetStorageInfo(
  duckdb::ClientContext& /*context*/) {
  return BuildStorageInfo(*this);
}

std::vector<IResearchColumnBinding>
InvertedIndexScanEntry::IndexSegmentInfoBindings() const {
  auto bindings = SegmentInfoBindings();
  bindings.push_back({RowIdentityColumnId(), catalog::kGeneratedPKId.id()});
  return bindings;
}

duckdb::vector<duckdb::ColumnSegmentInfo>
InvertedIndexScanEntry::GetColumnSegmentInfo(
  const duckdb::QueryContext& context,
  const duckdb::ColumnSegmentInfoScanOptions& /*options*/) {
  auto client = context.GetClientContext();
  if (!client) {
    return {};
  }
  auto snapshot =
    GetSereneDBContext(*client).EnsureSearchSnapshot(*_inverted_index);
  duckdb::vector<duckdb::ColumnSegmentInfo> result;
  BuildIResearchColumnSegmentInfo(snapshot->reader, IndexSegmentInfoBindings(),
                                  GetVirtualColumns(), result);
  return result;
}

bool InvertedIndexScanEntry::ScanColumnSegmentInfo(
  const duckdb::QueryContext& context,
  duckdb::ColumnSegmentInfoScanState& state,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result) {
  auto client = context.GetClientContext();
  if (!client) {
    return false;
  }
  auto snapshot =
    GetSereneDBContext(*client).EnsureSearchSnapshot(*_inverted_index);
  return ScanIResearchColumnSegmentInfo(snapshot->reader,
                                        IndexSegmentInfoBindings(),
                                        GetVirtualColumns(), state, result);
}

std::vector<IResearchColumnBinding>
TableInvertedIndexScanEntry::SegmentInfoBindings() const {
  std::vector<IResearchColumnBinding> bindings;
  for (const auto& col : GetColumns().Physical()) {
    bindings.push_back({col.Physical().index, col.HostId()});
  }
  return bindings;
}

duckdb::column_t TableInvertedIndexScanEntry::RowIdentityColumnId() const {
  return connector::RowIdentityColumnId(*this);
}

duckdb::vector<duckdb::column_t> TableInvertedIndexScanEntry::GetRowIdColumns()
  const {
  return connector::BuildRowIdColumns(*this, _indexed_col_indices);
}

duckdb::virtual_column_map_t TableInvertedIndexScanEntry::GetVirtualColumns()
  const {
  return connector::BuildVirtualColumns(*this, _indexed_col_indices);
}

ViewInvertedIndexScanEntry::ViewInvertedIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, const duckdb::ViewCatalogEntry& view,
  IndexedRelation relation, std::vector<size_t> indexed_col_indices,
  catalog::IndexInfoRef inverted_index)
  : InvertedIndexScanEntry(catalog, schema, info,
                           std::move(indexed_col_indices),
                           std::move(inverted_index)),
    _sdb_view{
      duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
        view.GetInfo())
        .release()} {
  SDB_ASSERT(_sdb_view);
  SetIndexedRelation(std::move(relation));
}

duckdb::TableFunction ViewInvertedIndexScanEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(*_inverted_index);
  // The index only captures post-WHERE/ORDER/LIMIT rows; we must not
  // stream the reader directly.
  auto data = duckdb::make_uniq<ViewScanBindData>();
  data->view = _sdb_view;
  const auto& vinfo = *_sdb_view;
  for (size_t i = 0; i < vinfo.names.size(); ++i) {
    data->column_ids.push_back(static_cast<catalog::ColumnId>(i));
    data->column_types.push_back(vinfo.types[i]);
  }
  data->table_entry = this;
  data->entry_kind = ScanEntryKind::InvertedIndex;
  data->inverted_index = catalog::InvertedInfoRef(_inverted_index);
  data->inverted_storage = _inverted_index->GetData();
  std::span<const std::string> key_cols =
    data->inverted_index->GetOptions().key_columns;
  data->fast_path = ResolveViewFastPath(context, *_sdb_view, key_cols);
  if (data->fast_path) {
    data->lookup_label = FormatLookupLabel(*data->fast_path);
    data->lookup_supports_filters = data->fast_path->supports_filters;
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

std::vector<IResearchColumnBinding>
ViewInvertedIndexScanEntry::SegmentInfoBindings() const {
  std::vector<IResearchColumnBinding> bindings;
  for (const auto& col : GetColumns().Physical()) {
    const auto physical = col.Physical().index;
    bindings.push_back({physical, physical});
  }
  return bindings;
}

duckdb::column_t ViewInvertedIndexScanEntry::RowIdentityColumnId() const {
  return kColumnIdentifierGeneratedPk;
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
  duckdb::CreateTableInfo& info, const catalog::CreateIndexInfoBase& index,
  std::vector<size_t> indexed_col_indices, bool sk_unique)
  : SereneDBIndexScanEntry(catalog, schema, info, index,
                           std::move(indexed_col_indices)),
    _secondary_index_id(index.GetId()),
    _sk_unique(sk_unique) {
  SDB_ASSERT(_secondary_index_id != ObjectId{});
}

TableSecondaryIndexScanEntry::TableSecondaryIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, IndexedRelation relation,
  const catalog::CreateIndexInfoBase& index,
  std::vector<size_t> indexed_col_indices, bool sk_unique)
  : SecondaryIndexScanEntry(catalog, schema, info, index,
                            std::move(indexed_col_indices), sk_unique) {
  SetIndexedRelation(std::move(relation));
}

duckdb::TableCatalogEntry& TableSecondaryIndexScanEntry::ResolveRelationEntry(
  duckdb::ClientContext& context) const {
  return *catalog::GetStoreTableEntry(
    context, const_cast<duckdb::Catalog&>(ParentCatalog()), _relation_id,
    duckdb::OnEntryNotFound::THROW_EXCEPTION);
}

duckdb::TableFunction TableSecondaryIndexScanEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  // Scanning a secondary index by name reads the relation: the index itself is
  // a native ART over its rows. The plan still names what the user asked for.
  auto function =
    ResolveRelationEntry(context).GetScanFunction(context, bind_data);
  if (bind_data) {
    if (auto* scan =
          dynamic_cast<duckdb::TableScanBindData*>(bind_data.get())) {
      scan->display_name = name.GetIdentifierName();
    }
  }
  return function;
}

duckdb::TableStorageInfo TableSecondaryIndexScanEntry::GetStorageInfo(
  duckdb::ClientContext& /*context*/) {
  return BuildStorageInfo(*this);
}

}  // namespace sdb::connector
