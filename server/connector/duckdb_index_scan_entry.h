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
#include "catalog/object.h"
#include "catalog/table.h"
#include "catalog/view.h"

namespace sdb::connector {

// Catalog entry for `SELECT * FROM idx_name WHERE ...`.
class SereneDBIndexScanEntry : public duckdb::TableCatalogEntry {
 public:
  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) final;

  const std::vector<size_t>& GetIndexedColumnIndices() const {
    return _indexed_col_indices;
  }

 protected:
  SereneDBIndexScanEntry(duckdb::Catalog& catalog,
                         duckdb::SchemaCatalogEntry& schema,
                         duckdb::CreateTableInfo& info,
                         std::vector<size_t> indexed_col_indices);

  std::vector<size_t> _indexed_col_indices;
};

class InvertedIndexScanEntry : public SereneDBIndexScanEntry {
 public:
  const std::shared_ptr<const catalog::InvertedIndex>& GetInvertedIndex()
    const {
    return _inverted_index;
  }

 protected:
  InvertedIndexScanEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    duckdb::CreateTableInfo& info, std::vector<size_t> indexed_col_indices,
    std::shared_ptr<const catalog::InvertedIndex> inverted_index);

  std::shared_ptr<const catalog::InvertedIndex> _inverted_index;
};

class TableInvertedIndexScanEntry final : public InvertedIndexScanEntry {
 public:
  TableInvertedIndexScanEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
    std::vector<size_t> indexed_col_indices,
    std::shared_ptr<const catalog::InvertedIndex> inverted_index);

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const final;
  duckdb::virtual_column_map_t GetVirtualColumns() const final;

  const std::shared_ptr<catalog::Table>& GetSereneDBTable() const {
    return _sdb_table;
  }

 private:
  std::shared_ptr<catalog::Table> _sdb_table;
};

class ViewInvertedIndexScanEntry final : public InvertedIndexScanEntry {
 public:
  ViewInvertedIndexScanEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    duckdb::CreateTableInfo& info,
    std::shared_ptr<const catalog::PgSqlView> sdb_view,
    std::vector<size_t> indexed_col_indices,
    std::shared_ptr<const catalog::InvertedIndex> inverted_index);

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const final;
  duckdb::virtual_column_map_t GetVirtualColumns() const final;

  const std::shared_ptr<const catalog::PgSqlView>& GetSereneDBView() const {
    return _sdb_view;
  }

 private:
  std::shared_ptr<const catalog::PgSqlView> _sdb_view;
};

class SecondaryIndexScanEntry : public SereneDBIndexScanEntry {
 public:
  ObjectId GetSecondaryIndexShardId() const { return _sk_shard_id; }
  bool IsUnique() const { return _sk_unique; }

 protected:
  SecondaryIndexScanEntry(duckdb::Catalog& catalog,
                          duckdb::SchemaCatalogEntry& schema,
                          duckdb::CreateTableInfo& info,
                          std::vector<size_t> indexed_col_indices,
                          ObjectId sk_shard_id, bool sk_unique);

  ObjectId _sk_shard_id;
  bool _sk_unique;
};

class TableSecondaryIndexScanEntry final : public SecondaryIndexScanEntry {
 public:
  TableSecondaryIndexScanEntry(duckdb::Catalog& catalog,
                               duckdb::SchemaCatalogEntry& schema,
                               duckdb::CreateTableInfo& info,
                               std::shared_ptr<catalog::Table> sdb_table,
                               std::vector<size_t> indexed_col_indices,
                               ObjectId sk_shard_id, bool sk_unique);

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

  const std::shared_ptr<catalog::Table>& GetSereneDBTable() const {
    return _sdb_table;
  }

 private:
  std::shared_ptr<catalog::Table> _sdb_table;
};

class ViewSecondaryIndexScanEntry final : public SecondaryIndexScanEntry {
 public:
  ViewSecondaryIndexScanEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    duckdb::CreateTableInfo& info,
    std::shared_ptr<const catalog::PgSqlView> sdb_view,
    std::vector<size_t> indexed_col_indices, ObjectId sk_shard_id,
    bool sk_unique);

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const final;
  duckdb::virtual_column_map_t GetVirtualColumns() const final;

  const std::shared_ptr<const catalog::PgSqlView>& GetSereneDBView() const {
    return _sdb_view;
  }

 private:
  std::shared_ptr<const catalog::PgSqlView> _sdb_view;
};

}  // namespace sdb::connector
