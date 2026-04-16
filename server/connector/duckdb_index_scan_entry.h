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
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>

#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"

namespace sdb::connector {

// Catalog entry for the "index name as table" pattern:
//   SELECT * FROM idx_name WHERE phrase(col, 'text')
//   SELECT * FROM sk_index_name WHERE col BETWEEN 1 AND 9
//
// Distinct from SereneDBTableEntry (which represents the user-facing
// table). An index entry references the underlying user table for column
// metadata + materialisation, but its default scan function is one of the
// index-aware full scans (CreateFullSkScanFunction /
// CreateFullIresearchScanFunction). Specialised optimizer rules
// (rocksdb_plan, iresearch_plan) further swap the function on a match.
class SereneDBIndexScanEntry final : public duckdb::TableCatalogEntry {
 public:
  // Construct for an inverted-index entry: `inverted_index` set,
  // sk_shard_id default-constructed.
  static duckdb::unique_ptr<SereneDBIndexScanEntry> ForInvertedIndex(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
    std::vector<size_t> indexed_col_indices,
    std::shared_ptr<const catalog::InvertedIndex> inverted_index);

  // Construct for a secondary (rocksdb-backed) index entry: sk_shard_id +
  // is_unique set, inverted_index null.
  static duckdb::unique_ptr<SereneDBIndexScanEntry> ForSecondaryIndex(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
    std::vector<size_t> indexed_col_indices, ObjectId sk_shard_id,
    bool sk_unique);

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) override;

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) override;

  duckdb::TableStorageInfo GetStorageInfo(
    duckdb::ClientContext& context) override;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const override;
  duckdb::virtual_column_map_t GetVirtualColumns() const override;

  const std::shared_ptr<catalog::Table>& GetSereneDBTable() const {
    return _sdb_table;
  }

  const std::vector<size_t>& GetIndexedColumnIndices() const {
    return _indexed_col_indices;
  }

  const std::shared_ptr<const catalog::InvertedIndex>& GetInvertedIndex()
    const {
    return _inverted_index;
  }

  bool IsInvertedIndex() const { return static_cast<bool>(_inverted_index); }
  bool IsSecondaryIndex() const { return _sk_shard_id != ObjectId{}; }

  ObjectId GetSecondaryIndexShardId() const { return _sk_shard_id; }
  bool IsSecondaryIndexUnique() const { return _sk_unique; }

 private:
  SereneDBIndexScanEntry(duckdb::Catalog& catalog,
                         duckdb::SchemaCatalogEntry& schema,
                         duckdb::CreateTableInfo& info,
                         std::shared_ptr<catalog::Table> sdb_table,
                         std::vector<size_t> indexed_col_indices,
                         std::shared_ptr<const catalog::InvertedIndex>
                           inverted_index,
                         ObjectId sk_shard_id, bool sk_unique);

  std::shared_ptr<catalog::Table> _sdb_table;
  std::vector<size_t> _indexed_col_indices;
  // One of these two is set, the other is empty.
  std::shared_ptr<const catalog::InvertedIndex> _inverted_index;
  ObjectId _sk_shard_id;
  bool _sk_unique = false;
};

}  // namespace sdb::connector
