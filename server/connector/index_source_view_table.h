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

// Materializer for views over a table living in an attached database with
// native storage: rows are fetched by rowid from the live catalog entry
// through the caller's transaction, so reads are consistent with any
// in-transaction changes to the source.
class ViewTableIndexSource final : public ViewIndexSourceBase {
 public:
  ViewTableIndexSource(duckdb::ClientContext& context, ViewFastPath fast_path,
                       std::span<const duckdb::idx_t> projected_columns,
                       std::span<const duckdb::LogicalType> projected_types,
                       std::span<const catalog::Column::Id> bind_column_ids);

  PrimaryKeyBatch CreatePkBatch() const final {
    return PrimaryKeyBatch{std::in_place_type<PrimaryKeyI64>};
  }
  void Materialize(duckdb::ClientContext& context, PrimaryKeyBatch& batch,
                   duckdb::idx_t start, duckdb::idx_t count,
                   duckdb::DataChunk& output) final;

 private:
  duckdb::TableCatalogEntry& _table;
  duckdb::vector<duckdb::StorageIndex> _fetch_columns;
  duckdb::vector<duckdb::LogicalType> _fetch_types;
  std::vector<duckdb::idx_t> _col_to_fetch;
  duckdb::idx_t _rowid_fetch_idx = 0;
  duckdb::DataChunk _fetch_chunk;
};

}  // namespace sdb::connector
