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

#include "connector/index_source_view_table.h"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <duckdb/transaction/duck_transaction.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

duckdb::TableCatalogEntry& ResolveTableEntry(duckdb::ClientContext& context,
                                             const ViewFastPath& fast_path) {
  SDB_ASSERT(fast_path.catalog_ref);
  auto& entry =
    duckdb::Catalog::GetEntry(
      context, duckdb::CatalogType::TABLE_ENTRY,
      duckdb::QualifiedName(duckdb::Identifier{fast_path.catalog_ref->catalog},
                            duckdb::Identifier{fast_path.catalog_ref->schema},
                            duckdb::Identifier{fast_path.catalog_ref->table}))
      .Cast<duckdb::TableCatalogEntry>();
  if (!entry.IsDuckTable()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot materialise rows from \"", entry.name.GetIdentifierName(),
              "\" -- the attached source table has no native storage"));
  }
  return entry;
}

duckdb::TableCatalogEntry& ResolveStoreTableEntry(
  duckdb::ClientContext& context, const duckdb::TableCatalogEntry& scan_entry,
  const catalog::Table& table) {
  // The scan entry is the facade table or one of its index entries; either
  // way it shares the table's database and schema.
  auto store_name = catalog::StoreTableName(
    scan_entry.ParentCatalog().GetName().GetIdentifierName(),
    scan_entry.ParentSchema().name.GetIdentifierName(), table.GetName());
  return duckdb::Catalog::GetEntry(
           context, duckdb::CatalogType::TABLE_ENTRY,
           duckdb::QualifiedName(
             duckdb::Identifier{catalog::kStoreDatabaseName},
             duckdb::Identifier{"main"}, duckdb::Identifier{store_name}))
    .Cast<duckdb::TableCatalogEntry>();
}

}  // namespace

duckdb::LogicalType RowIdFetchIndexSource::AddFetchColumn(
  const duckdb::ColumnDefinition& col) {
  if (col.Generated()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot materialise generated column \"",
              col.Name().GetIdentifierName(), "\" of \"",
              _table->name.GetIdentifierName(), "\" through an index lookup"));
  }
  const auto storage_idx =
    _table->GetStorageIndex(duckdb::ColumnIndex(col.Logical().index));
  duckdb::idx_t fetch_idx = duckdb::DConstants::INVALID_INDEX;
  for (duckdb::idx_t i = 0; i < _fetch_columns.size(); ++i) {
    if (_fetch_columns[i].GetPrimaryIndex() == storage_idx.GetPrimaryIndex()) {
      fetch_idx = i;
      break;
    }
  }
  if (fetch_idx == duckdb::DConstants::INVALID_INDEX) {
    fetch_idx = _fetch_columns.size();
    _fetch_columns.push_back(storage_idx);
    _fetch_types.push_back(col.Type());
  }
  _col_to_fetch.push_back(fetch_idx);
  return col.Type();
}

void RowIdFetchIndexSource::FinishInit(duckdb::ClientContext& context) {
  _fetch_chunk.Initialize(context, _fetch_types);
}

void RowIdFetchIndexSource::BuildPushedFilters(
  const duckdb::TableFilterSet* input_filters) {
  if (!input_filters || !input_filters->HasFilters()) {
    return;
  }
  // Each source column added by InitProjection sits at output slot
  // _real_proj_slots[c] and fetch-column index _col_to_fetch[c]. The scan's
  // pushed filters are keyed by output slot; re-key those hitting a fetched
  // column to the fetch-column index the native lookup scan understands.
  auto set = duckdb::make_uniq<duckdb::TableFilterSet>();
  for (duckdb::idx_t c = 0; c < _real_proj_slots.size(); ++c) {
    auto filter = input_filters->TryGetFilterByColumnIndex(
      duckdb::ProjectionIndex(_real_proj_slots[c]));
    if (!filter) {
      continue;
    }
    const auto& expr_filter = duckdb::ExpressionFilter::GetExpressionFilter(
      *filter, "BuildPushedFilters");
    set->PushFilter(duckdb::ProjectionIndex(_col_to_fetch[c]),
                    expr_filter.Copy());
  }
  if (set->HasFilters()) {
    _pushed_filters = std::move(set);
  }
}

ViewTableIndexSource::ViewTableIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  duckdb::TableFilterSet* pushed_filters)
  : RowIdFetchIndexSource{std::move(fast_path)} {
  auto& table = ResolveTableEntry(context, _fast_path);
  SetTable(table);
  // Registers the attached database with the meta transaction, keeping it
  // alive for the query even if it is detached concurrently.
  duckdb::DuckTransaction::Get(context, table.ParentCatalog());
  const auto& columns = table.GetColumns();
  containers::FlatHashMap<std::string_view, duckdb::idx_t> name_to_col;
  if (!_fast_path.projection_columns.empty()) {
    name_to_col.reserve(columns.LogicalColumnCount());
    duckdb::idx_t logical = 0;
    for (const auto& col : columns.Logical()) {
      name_to_col.emplace(col.Name().GetIdentifierName(), logical++);
    }
  }
  InitProjection(
    context, projected_columns, projected_types, bind_column_ids,
    [&](std::string_view name) {
      auto it = name_to_col.find(name);
      SDB_ASSERT(it != name_to_col.end());
      return it->second;
    },
    [&](duckdb::idx_t table_col_idx) {
      SDB_ASSERT(table_col_idx < columns.LogicalColumnCount());
      return AddFetchColumn(
        columns.GetColumn(duckdb::LogicalIndex(table_col_idx)));
    });
  FinishInit(context);
  BuildPushedFilters(pushed_filters);
}

TableRowIdIndexSource::TableRowIdIndexSource(
  duckdb::ClientContext& context, const duckdb::TableCatalogEntry& scan_entry,
  const catalog::Table& sdb_table,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  duckdb::TableFilterSet* pushed_filters)
  : RowIdFetchIndexSource{ViewFastPath{}} {
  auto& table = ResolveStoreTableEntry(context, scan_entry, sdb_table);
  SetTable(table);
  duckdb::DuckTransaction::Get(context, table.ParentCatalog());
  const auto& columns = table.GetColumns();
  // Store physical positions follow the facade column order minus the
  // generated PK; map catalog column ids through that order.
  containers::FlatHashMap<duckdb::idx_t, duckdb::idx_t> id_to_pos;
  id_to_pos.reserve(sdb_table.Columns().size());
  duckdb::idx_t pos = 0;
  for (const auto& col : sdb_table.Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    id_to_pos.emplace(static_cast<duckdb::idx_t>(col.GetId()), pos++);
  }
  InitProjection(
    context, projected_columns, projected_types, bind_column_ids,
    [&](std::string_view) -> duckdb::idx_t {
      SDB_ASSERT(false, "table index sources resolve columns by id");
      return 0;
    },
    [&](duckdb::idx_t col_id) {
      auto it = id_to_pos.find(col_id);
      SDB_ENSURE(it != id_to_pos.end(), ERROR_INTERNAL,
                 "column id is not on the store table");
      SDB_ASSERT(it->second < columns.LogicalColumnCount());
      return AddFetchColumn(
        columns.GetColumn(duckdb::LogicalIndex(it->second)));
    });
  FinishInit(context);
  BuildPushedFilters(pushed_filters);
}

duckdb::idx_t RowIdFetchIndexSource::Materialize(duckdb::ClientContext& context,
                                                 PrimaryKeyBatch& batch,
                                                 duckdb::idx_t start,
                                                 duckdb::idx_t count,
                                                 duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  auto& pk = batch;
  SDB_ASSERT(start + count <= pk.rows.size());

  SortRows(pk, start, count);
  AliasOutput(output);

  auto& storage = _table->Cast<duckdb::DuckTableEntry>().GetStorage();
  auto& transaction =
    duckdb::DuckTransaction::Get(context, _table->ParentCatalog());

  // Single path: the scan writes survivors compactly and records each output row's requested-pk index in
  // _survivor_idx (drives the doc-id-keyed gather). It evaluates any pushed lookup-column filters natively
  // (FilterSelection + late materialization); with none, a pk the source no longer holds is kept as a NULL
  // row (eventually-consistent null-on-miss), otherwise the filter drops it.
  _survivor_idx.resize(count);
  const auto rows = storage.LookupScan(
    transaction, context, _fetch_columns, _pushed_filters.get(), _sorted_rows.data(),
    _sorted_rows.data() + count, _survivor_idx.data(), _col_to_fetch.data(), _fetch_chunk, _tf_target,
    _lookup_scan_state);
  _tf_target.SetCardinality(rows);

  RunCastPass(output, rows);
  GatherNonLookupColumns(output, rows, _survivor_idx.data());
  return rows;
}

}  // namespace sdb::connector
