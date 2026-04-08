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

#include <duckdb.hpp>
#include <duckdb/function/table_function.hpp>

#include <iresearch/search/filter.hpp>

#include "catalog/table.h"

namespace irs {
class IndexReader;
}

#include "search/inverted_index_shard.h"

namespace sdb::connector {

struct SereneDBScanBindData : public duckdb::FunctionData {
  std::shared_ptr<catalog::Table> table;
  std::vector<catalog::Column::Id> column_ids;
  std::vector<duckdb::LogicalType> column_types;
  bool has_rowid = false;
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;

  // Search state (set by pushdown_complex_filter)
  irs::Filter::Query::ptr search_query;
  search::InvertedIndexSnapshotPtr search_snapshot;
  const irs::IndexReader* search_reader = nullptr;

  bool IsSearchScan() const { return search_query != nullptr; }

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
  bool Equals(const duckdb::FunctionData& other) const override;
};

// Creates a DuckDB TableFunction that performs a full scan of a SereneDB
// RocksDB table. Reuses the same key layout and column iteration as the
// existing Velox RocksDBFullScanDataSource.
duckdb::TableFunction CreateSereneDBScanFunction();

}  // namespace sdb::connector
