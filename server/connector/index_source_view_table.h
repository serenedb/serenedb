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

#pragma once

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <duckdb/storage/storage_index.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <span>

#include "connector/index_source_view.h"

namespace sdb::connector {

// Shared rowid-keyed materializer over a table with native storage: rows
// are fetched by rowid from the live catalog entry through the caller's
// transaction, so reads are consistent with any in-transaction changes to
// the source. Subclasses resolve the table entry and the projection.
class RowIdFetchIndexSource : public ViewIndexSourceBase {
 public:
  PrimaryKeyBatch::Kind PkKind() const final {
    return PrimaryKeyBatch::Kind::I64;
  }
  duckdb::idx_t Materialize(duckdb::ClientContext& context,
                            PrimaryKeyBatch& batch, duckdb::idx_t start,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 protected:
  explicit RowIdFetchIndexSource(ViewFastPath fast_path)
    : ViewIndexSourceBase{std::move(fast_path)} {}

  void SetTable(duckdb::TableCatalogEntry& table) { _table = &table; }

  // Registers a fetch column for the table's storage index, deduplicating
  // repeats, and records the output slot mapping. Returns the column type.
  duckdb::LogicalType AddFetchColumn(const duckdb::ColumnDefinition& col);
  // Sizes the fetch chunk; call after InitProjection.
  void FinishInit(duckdb::ClientContext& context);
  // Builds `_pushed_filters` (keyed by fetch-column index) from the scan's
  // pushed filters that target fetched columns; call after InitProjection.
  void BuildPushedFilters(const duckdb::TableFilterSet* input_filters);

 private:
  duckdb::TableCatalogEntry* _table = nullptr;
  duckdb::vector<duckdb::StorageIndex> _fetch_columns;
  duckdb::vector<duckdb::LogicalType> _fetch_types;
  std::vector<duckdb::idx_t> _col_to_fetch;
  duckdb::DataChunk _fetch_chunk;
  // Lookup-column filters keyed by fetch-column index (built from the scan's
  // pushed filters); passed to the native lookup scan. Null when none apply.
  duckdb::unique_ptr<duckdb::TableFilterSet> _pushed_filters;
  // Persistent lookup cursor, built once and reused across Materialize batches
  // so the scan/decode state (pinned blocks, FSST dicts) stays warm between
  // calls.
  duckdb::unique_ptr<duckdb::TableScanState> _lookup_scan_state;
};

// Views over a table living in an attached database with native storage.
class ViewTableIndexSource final : public RowIdFetchIndexSource {
 public:
  ViewTableIndexSource(duckdb::ClientContext& context, ViewFastPath fast_path,
                       std::span<const duckdb::idx_t> projected_columns,
                       std::span<const duckdb::LogicalType> projected_types,
                       std::span<const catalog::Column::Id> bind_column_ids,
                       duckdb::TableFilterSet* pushed_filters = nullptr);
};

// SereneDB tables: postings carry the store-table rowid; rows are fetched
// from the hidden store table backing the facade entry.
class TableRowIdIndexSource final : public RowIdFetchIndexSource {
 public:
  TableRowIdIndexSource(duckdb::ClientContext& context,
                        const duckdb::TableCatalogEntry& scan_entry,
                        const catalog::Table& sdb_table,
                        std::span<const duckdb::idx_t> projected_columns,
                        std::span<const duckdb::LogicalType> projected_types,
                        std::span<const catalog::Column::Id> bind_column_ids,
                        duckdb::TableFilterSet* pushed_filters = nullptr);
};

}  // namespace sdb::connector
