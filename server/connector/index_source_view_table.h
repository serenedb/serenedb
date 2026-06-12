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
#include <duckdb/storage/storage_index.hpp>
#include <span>

#include "connector/index_source_view.h"

namespace sdb::connector {

// Shared rowid-keyed materializer over a table with native storage: rows
// are fetched by rowid from the live catalog entry through the caller's
// transaction, so reads are consistent with any in-transaction changes to
// the source. Subclasses resolve the table entry and the projection.
class RowIdFetchIndexSource : public ViewIndexSourceBase {
 public:
  PrimaryKeyBatch CreatePkBatch() const final {
    return PrimaryKeyBatch{std::in_place_type<PrimaryKeyI64>};
  }
  void Materialize(duckdb::ClientContext& context, PrimaryKeyBatch& batch,
                   duckdb::idx_t start, duckdb::idx_t count,
                   duckdb::DataChunk& output) final;

 protected:
  explicit RowIdFetchIndexSource(ViewFastPath fast_path)
    : ViewIndexSourceBase{std::move(fast_path)} {}

  void SetTable(duckdb::TableCatalogEntry& table) { _table = &table; }

  // Registers a fetch column for the table's storage index, deduplicating
  // repeats, and records the output slot mapping. Returns the column type.
  duckdb::LogicalType AddFetchColumn(const duckdb::ColumnDefinition& col);
  // Appends the rowid fetch column and sizes the fetch chunk; call after
  // InitProjection.
  void FinishInit(duckdb::ClientContext& context);

 private:
  duckdb::TableCatalogEntry* _table = nullptr;
  duckdb::vector<duckdb::StorageIndex> _fetch_columns;
  duckdb::vector<duckdb::LogicalType> _fetch_types;
  std::vector<duckdb::idx_t> _col_to_fetch;
  duckdb::idx_t _rowid_fetch_idx = 0;
  duckdb::DataChunk _fetch_chunk;
};

// Views over a table living in an attached database with native storage.
class ViewTableIndexSource final : public RowIdFetchIndexSource {
 public:
  ViewTableIndexSource(duckdb::ClientContext& context, ViewFastPath fast_path,
                       std::span<const duckdb::idx_t> projected_columns,
                       std::span<const duckdb::LogicalType> projected_types,
                       std::span<const catalog::Column::Id> bind_column_ids);
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
                        std::span<const catalog::Column::Id> bind_column_ids);
};

}  // namespace sdb::connector
