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

#include "catalog/inverted_index.h"
#include "catalog/table.h"

namespace sdb::connector {

// Convert a SereneDB Velox type to DuckDB LogicalType.
duckdb::LogicalType VeloxTypeToDuckDB(const velox::TypePtr& type);

// Convert a DuckDB LogicalType to Velox TypePtr.
// Temporary -- will be removed when catalog stops using Velox types.
velox::TypePtr DuckDBTypeToVelox(const duckdb::LogicalType& type);

class SereneDBTableEntry final : public duckdb::TableCatalogEntry {
 public:
  // indexed_col_indices: table column indices that are part of any index
  // inverted_index: set when entry was created from an index name (FROM idx)
  SereneDBTableEntry(duckdb::Catalog& catalog,
                     duckdb::SchemaCatalogEntry& schema,
                     duckdb::CreateTableInfo& info,
                     std::shared_ptr<catalog::Table> sdb_table,
                     std::vector<size_t> indexed_col_indices = {},
                     std::shared_ptr<const catalog::InvertedIndex>
                       inverted_index = nullptr);

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) override;

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) override;

  duckdb::TableStorageInfo GetStorageInfo(
    duckdb::ClientContext& context) override;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const override;
  duckdb::virtual_column_map_t GetVirtualColumns() const override;

  void BindUpdateConstraints(duckdb::Binder& binder, duckdb::LogicalGet& get,
                             duckdb::LogicalProjection& proj,
                             duckdb::LogicalUpdate& update,
                             duckdb::ClientContext& context) override;

  // Convert a virtual column ID (VIRTUAL_COLUMN_START + i) back to a real
  // column index. Returns DConstants::INVALID_INDEX if not a PK virtual col.
  static duckdb::column_t VirtualToPKColumnIndex(duckdb::column_t virtual_id);

  const std::shared_ptr<catalog::Table>& GetSereneDBTable() const {
    return _sdb_table;
  }

  const std::vector<size_t>& GetIndexedColumnIndices() const {
    return _indexed_col_indices;
  }

  const std::shared_ptr<const catalog::InvertedIndex>& GetInvertedIndex() const {
    return _inverted_index;
  }

  ObjectId GetSecondaryIndexShardId() const { return _sk_shard_id; }
  bool IsSecondaryIndexUnique() const { return _sk_unique; }
  bool HasSecondaryIndex() const { return _sk_shard_id != ObjectId{}; }

  void SetSecondaryIndex(ObjectId shard_id, bool is_unique) {
    _sk_shard_id = shard_id;
    _sk_unique = is_unique;
  }

 private:
  std::shared_ptr<catalog::Table> _sdb_table;
  std::vector<size_t> _indexed_col_indices;
  // Set when entry was created from an index name (FROM idx_name).
  std::shared_ptr<const catalog::InvertedIndex> _inverted_index;
  // Set when entry was created from a secondary index name.
  ObjectId _sk_shard_id;
  bool _sk_unique = false;
};

}  // namespace sdb::connector
