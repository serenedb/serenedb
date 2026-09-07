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

#include "catalog/entry/duckdb_index_scan_entry.h"

#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/storage/table_storage_info.hpp>
#include <iresearch/index/directory_reader.hpp>

#include "basics/assert.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/log/store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::catalog {

SereneDBIndexScanEntry::SereneDBIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, ObjectId index_id)
  : duckdb::TableCatalogEntry(catalog, schema, info) {
  // Durability and reclamation are the serenedb catalog log's, not duckdb's.
  catalog::AdoptEntryIdentity(*this, index_id);
}

duckdb::unique_ptr<duckdb::BaseStatistics>
SereneDBIndexScanEntry::GetStatistics(duckdb::ClientContext& /*context*/,
                                      duckdb::column_t /*column_id*/) {
  return nullptr;
}

InvertedIndexScanEntry::InvertedIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, const catalog::Index& inverted_index)
  : SereneDBIndexScanEntry(catalog, schema, info, inverted_index.GetId()),
    _index_id(inverted_index.GetId()) {
  SDB_ASSERT(_index_id.isSet());
}

TableInvertedIndexScanEntry::TableInvertedIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, ObjectId relation_id,
  const catalog::Index& inverted_index)
  : InvertedIndexScanEntry(catalog, schema, info, inverted_index) {
  _relation_id = relation_id;
}

duckdb::TableFunction TableInvertedIndexScanEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto& conn_ctx = connector::GetSereneDBContext(context);
  auto data = duckdb::make_uniq<connector::TableScanBindData>();
  for (const auto& col : GetColumns().Logical()) {
    data->column_ids.emplace_back(col.CatalogOid());
    data->column_types.push_back(col.Type());
  }
  data->table_entry = this;
  // Carried in both roads, so pushdown targets this index's own term fields.
  data->indexes = {
    ::sdb::catalog::InvertedDefinitionIn(&context, this->catalog, _index_id)};

  const auto* relation =
    catalog::FindSessionTableEntry(context, GetIndexedRelationId());
  if (relation != nullptr && relation->IsSearchTable()) {
    // Storage-less index: it shares the table's own iresearch store, so serve
    // the scan exactly like a plain search-table one -- there is no separate
    // index reader to open.
    auto reader = conn_ctx.SearchTxn().EnsureSearchTableReader(
      GetIndexedRelationId(),
      [&] { return relation->GetSearchData()->GetDirectoryReader(); });
    data->entry_kind = connector::ScanEntryKind::SearchTableIndex;
    data->topk_scorer = relation->SearchOptions().topk_scorer;
    data->lookup_label = "search";
    data->snapshot = std::make_shared<search::InvertedIndexSnapshot>(
      irs::DirectoryReader{*reader}, nullptr);
  } else {
    data->entry_kind = connector::ScanEntryKind::InvertedIndex;
    data->lookup_label = "table";
    data->topk_scorer = data->ScannedIndex().GetTopKScorer();
    data->snapshot = conn_ctx.EnsureSearchSnapshot(
      _index_id, ::sdb::catalog::InvertedStorageIn(this->catalog, _index_id));
  }
  bind_data = std::move(data);
  return connector::CreateIResearchScanFunction();
}

duckdb::TableStorageInfo SereneDBIndexScanEntry::GetStorageInfo(
  duckdb::ClientContext& /*context*/) {
  return BuildStorageInfo(*this);
}

std::vector<IResearchColumnBinding>
InvertedIndexScanEntry::IndexSegmentInfoBindings() const {
  auto bindings = SegmentInfoBindings();
  bindings.push_back({RowIdentityColumnId(), catalog::kGeneratedPKId.id()});
  return bindings;
}

bool InvertedIndexScanEntry::ScanColumnSegmentInfo(
  const duckdb::QueryContext& context,
  duckdb::ColumnSegmentInfoScanState& state,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result) {
  auto client = context.GetClientContext();
  if (!client) {
    return false;
  }
  auto snapshot = connector::GetSereneDBContext(*client).EnsureSearchSnapshot(
    _index_id, ::sdb::catalog::InvertedStorageIn(this->catalog, _index_id));
  return ScanIResearchColumnSegmentInfo(snapshot->reader,
                                        IndexSegmentInfoBindings(),
                                        GetVirtualColumns(), state, result);
}

std::vector<IResearchColumnBinding>
TableInvertedIndexScanEntry::SegmentInfoBindings() const {
  std::vector<IResearchColumnBinding> bindings;
  for (const auto& col : GetColumns().Physical()) {
    bindings.push_back({col.Physical().index, col.CatalogOid()});
  }
  return bindings;
}

duckdb::column_t TableInvertedIndexScanEntry::RowIdentityColumnId() const {
  return catalog::RowIdentityColumnId(*this);
}

duckdb::vector<duckdb::column_t> TableInvertedIndexScanEntry::GetRowIdColumns()
  const {
  return catalog::BuildRowIdColumns(
    *this, catalog.Cast<SereneDBCatalog>().IndexedColumns(_relation_id));
}

duckdb::virtual_column_map_t TableInvertedIndexScanEntry::GetVirtualColumns()
  const {
  return catalog::BuildVirtualColumns(
    *this, catalog.Cast<SereneDBCatalog>().IndexedColumns(_relation_id));
}

ViewInvertedIndexScanEntry::ViewInvertedIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, const duckdb::ViewCatalogEntry& view,
  ObjectId relation_id, const catalog::Index& inverted_index)
  : InvertedIndexScanEntry(catalog, schema, info, inverted_index),
    _sdb_view{
      duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
        view.GetInfo())},
    _pk_column{catalog::InvertedInfo(inverted_index).GetOptions().pk_column} {
  SDB_ASSERT(_sdb_view);
  _relation_id = relation_id;
}

duckdb::TableFunction ViewInvertedIndexScanEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto snapshot = connector::GetSereneDBContext(context).EnsureSearchSnapshot(
    _index_id, ::sdb::catalog::InvertedStorageIn(this->catalog, _index_id));
  // The index only captures post-WHERE/ORDER/LIMIT rows; we must not
  // stream the reader directly.
  auto data = duckdb::make_uniq<connector::ViewScanBindData>();
  data->view_id = catalog::IdOf(*_sdb_view);
  const auto& vinfo = *_sdb_view;
  // The name as the view is called now -- a rename does not rewrite this
  // wrapper -- while the definition scanned is the one the index was built on.
  const auto* live =
    FindIn<SereneDBViewEntry>(&context, this->catalog, data->view_id);
  data->view_name = live ? live->name.GetIdentifierName()
                         : vinfo.GetViewName().GetIdentifierName();
  for (size_t i = 0; i < vinfo.names.size(); ++i) {
    data->column_ids.push_back(static_cast<catalog::ColumnId>(i));
    data->column_types.push_back(vinfo.types[i]);
    data->column_names.emplace_back(vinfo.names[i].GetIdentifierName());
  }
  data->table_entry = this;
  data->entry_kind = connector::ScanEntryKind::InvertedIndex;
  data->indexes = {
    ::sdb::catalog::InvertedDefinitionIn(&context, this->catalog, _index_id)};
  data->topk_scorer = data->ScannedIndex().GetTopKScorer();
  std::span<const std::string> key_cols =
    data->ScannedIndex().GetOptions().key_columns;
  data->fast_path =
    connector::ResolveViewFastPath(context, *_sdb_view, key_cols);
  if (data->fast_path) {
    data->lookup_label = FormatLookupLabel(*data->fast_path);
    data->lookup_supports_filters = data->fast_path->supports_filters;
  } else {
    data->lookup_label = "view";
  }
  data->snapshot = std::move(snapshot);
  bind_data = std::move(data);
  return connector::CreateIResearchScanFunction();
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
  return {duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX,
          kColumnIdentifierPkRowNumber};
}

duckdb::virtual_column_map_t ViewInvertedIndexScanEntry::GetVirtualColumns()
  const {
  duckdb::virtual_column_map_t result;
  result.reserve(4);
  result.emplace(kColumnIdentifierTableOid,
                 duckdb::TableColumn{"tableoid", duckdb::LogicalType::BIGINT});
  result.emplace(duckdb::COLUMN_IDENTIFIER_EMPTY,
                 duckdb::TableColumn{"", duckdb::LogicalType::BOOLEAN});
  if (_pk_column == catalog::PkColumnKind::Has) {
    // The pk halves as flat, FIXED-type columns: what the internal roads bind
    // (nothing to classify, no context needed). The scan itself rejects them
    // on an index that does not key rows by (file, row).
    result.emplace(
      duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX,
      duckdb::TableColumn{"file_index", duckdb::LogicalType::UBIGINT});
    result.emplace(
      kColumnIdentifierPkRowNumber,
      duckdb::TableColumn{"row_number", duckdb::LogicalType::BIGINT});
  }
  return result;
}

TableSecondaryIndexScanEntry::TableSecondaryIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, ObjectId relation_id,
  const catalog::CreateIndexInfo& index)
  : SereneDBIndexScanEntry(catalog, schema, info, index.GetId()) {
  SDB_ASSERT(index.GetId().isSet());
  _relation_id = relation_id;
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

duckdb::unique_ptr<duckdb::CatalogEntry> MakeIndexScanEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  std::string_view entry_name, const catalog::CreateIndexInfo& record,
  duckdb::ClientContext* context) {
  // In the catalog the entry is being built for rather than the session's: an
  // attach reads these before the attachment is in the database manager.
  if (const auto* view =
        FindIn<SereneDBViewEntry>(context, catalog, record.GetRelationId())) {
    const auto view_columns = view->GetColumnInfo();
    if (!view_columns) {
      return nullptr;
    }
    const auto& vinfo = *view_columns;
    auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
    info->columns = duckdb::ColumnList(/*allow_duplicate_names=*/false,
                                       /*case_sensitive=*/true);
    info->SetTableName(duckdb::Identifier{entry_name});
    info->SetSchema(schema.name);
    for (size_t i = 0; i < vinfo.names.size(); ++i) {
      info->columns.AddColumn(
        duckdb::ColumnDefinition(vinfo.names[i], vinfo.types[i]));
    }
    if (record.IsInverted()) {
      return duckdb::make_uniq<ViewInvertedIndexScanEntry>(
        catalog, schema, *info, *view, ObjectId{view->oid}, *record.GetIndex());
    }
    // CREATE INDEX rejects a plain (secondary) index on a view at bind time.
    return nullptr;
  }

  const auto* table_entry =
    FindIn<SereneDBTableEntry>(context, catalog, record.GetRelationId());
  if (table_entry == nullptr) {
    return nullptr;
  }
  const auto table = table_entry->Definition();
  auto info =
    SereneDBTableEntry::BuildInfo(entry_name, schema, catalog, *table, context);
  const auto relation_id = catalog::IdOf(*table);

  if (record.IsInverted()) {
    return duckdb::make_uniq<TableInvertedIndexScanEntry>(
      catalog, schema, *info, relation_id, *record.GetIndex());
  }

  return duckdb::make_uniq<TableSecondaryIndexScanEntry>(catalog, schema, *info,
                                                         relation_id, record);
}

}  // namespace sdb::catalog
