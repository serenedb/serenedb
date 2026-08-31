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

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>

#include "catalog1/catalog.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source_external_lookup.h"
#include "connector/index_source_view_file.h"
#include "connector/index_source_view_table.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {

std::unique_ptr<IndexSource> MakeIndexSource(
  duckdb::ClientContext& context, const SereneDBScanBindData& bind_data,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const ColumnId> bind_column_ids,
  duckdb::TableFilterSet* pushed_filters) {
  if (bind_data.IsViewBacked()) {
    const auto& vbd = bind_data.As<ViewScanBindData>();
    if (!vbd.fast_path) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("materialising real columns from this view-backed inverted "
                "index is not yet supported -- view body must be a simple "
                "`SELECT * FROM <reader>(literal_args)` over a recognised "
                "fast-path source (read_parquet/csv/json/...)"));
    }
    // Copy: the bind data outlives this execution, and the snapshot pin below
    // is per-execution state.
    auto fp = *vbd.fast_path;
    // Re-bind must target the same source version these docs were built
    // from: the pin travels with the pinned snapshot's manifest, so a
    // refresh mid-query cannot skew this read. No manifest = an external-pk
    // view index, which has no pin to carry.
    SDB_ASSERT(bind_data.snapshot);
    if (bind_data.snapshot->file_manifest) {
      fp.pinned_iceberg_snapshot_id =
        bind_data.snapshot->file_manifest->version;
    }
    if (fp.catalog_ref && fp.pk_spec == PkSpec::DuckDBRowId) {
      return std::make_unique<ViewTableIndexSource>(
        context, std::move(fp), projected_columns, projected_types,
        bind_column_ids, pushed_filters);
    }
    if (fp.catalog_ref && IsExternalPK(fp.pk_spec)) {
      return std::make_unique<ExternalLookupIndexSource>(
        context, std::move(fp), projected_columns, projected_types,
        bind_column_ids);
    }
    if (IsGlobPK(fp.pk_spec)) {
      return std::make_unique<ViewFileGlobIndexSource>(
        context, std::move(fp), projected_columns, projected_types,
        bind_column_ids, pushed_filters, bind_data.snapshot->file_manifest);
    }
    return std::make_unique<ViewFileSingleFileIndexSource>(
      context, std::move(fp), projected_columns, projected_types,
      bind_column_ids, pushed_filters);
  }
  SDB_ASSERT(bind_data.table_entry);
  // The bind data is const, but the entry it points at is the live catalog
  // object the fetch reads storage from -- non-owning, so the copy of the
  // pointer is what carries that.
  auto table = bind_data.table_entry;
  return std::make_unique<TableRowIdIndexSource>(
    context, *table, projected_columns, projected_types, bind_column_ids,
    pushed_filters);
}

}  // namespace sdb::connector
