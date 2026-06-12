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

#include "connector/index_source_factory.h"

#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/pk_spec.h"
#include "catalog/table.h"
#include "catalog/view.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source_rocksdb.h"
#include "connector/index_source_view_file.h"
#include "connector/index_source_view_rocksdb.h"
#include "connector/index_source_view_table.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_shard.h"

namespace sdb::connector {

std::unique_ptr<IndexSource> MakeIndexSource(
  duckdb::ClientContext& context, const SereneDBScanBindData& bind_data,
  const rocksdb::Snapshot* snapshot, rocksdb::Transaction* txn,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids) {
  if (bind_data.IsViewBacked()) {
    const auto& vbd = bind_data.As<ViewScanBindData>();
    auto fp = ResolveViewFastPath(context, *vbd.view);
    if (!fp) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("materialising real columns from this view-backed inverted "
                "index is not yet supported -- view body must be a simple "
                "`SELECT * FROM <reader>(literal_args)` over a recognised "
                "fast-path source (read_parquet/csv/json/...)"));
    }
    // Re-bind must target the same manifest as CREATE INDEX did.
    if (vbd.inverted_index) {
      auto cat_snapshot = GetSereneDBContext(context).EnsureCatalogSnapshot();
      if (auto shard =
            cat_snapshot->GetIndexShard(vbd.inverted_index->GetId())) {
        if (shard->GetType() == catalog::ObjectType::InvertedIndex) {
          fp->pinned_iceberg_snapshot_id =
            basics::downCast<const search::InvertedIndexShard>(*shard)
              .GetIcebergSnapshotId();
        }
      }
    }
    if (catalog::IsRocksPK(fp->pk_spec)) {
      return std::make_unique<ViewRocksDBIndexSource>(
        context, std::move(*fp), projected_columns, projected_types,
        bind_column_ids, snapshot, txn);
    }
    if (fp->catalog_ref && fp->pk_spec == catalog::PkSpec::DuckDBRowId) {
      return std::make_unique<ViewTableIndexSource>(
        context, std::move(*fp), projected_columns, projected_types,
        bind_column_ids);
    }
    if (catalog::IsGlobPK(fp->pk_spec)) {
      return std::make_unique<ViewFileGlobIndexSource>(
        context, std::move(*fp), projected_columns, projected_types,
        bind_column_ids);
    }
    return std::make_unique<ViewFileSingleFileIndexSource>(
      context, std::move(*fp), projected_columns, projected_types,
      bind_column_ids);
  }
  const auto& tbd = bind_data.As<TableScanBindData>();
  SDB_ASSERT(tbd.table);
  SDB_ASSERT(tbd.table_entry);
  return std::make_unique<TableRowIdIndexSource>(
    context, *tbd.table_entry, *tbd.table, projected_columns, projected_types,
    bind_column_ids);
}

}  // namespace sdb::connector
