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

#include "connector/duckdb_index_scan_entry.h"

#include <duckdb/storage/table_storage_info.hpp>

#include "basics/assert.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"

namespace sdb::connector {

duckdb::unique_ptr<SereneDBIndexScanEntry>
SereneDBIndexScanEntry::ForInvertedIndex(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
  std::vector<size_t> indexed_col_indices,
  std::shared_ptr<const catalog::InvertedIndex> inverted_index) {
  SDB_ASSERT(inverted_index != nullptr);
  return duckdb::unique_ptr<SereneDBIndexScanEntry>(new SereneDBIndexScanEntry(
    catalog, schema, info, std::move(sdb_table), std::move(indexed_col_indices),
    std::move(inverted_index), ObjectId{}, false));
}

duckdb::unique_ptr<SereneDBIndexScanEntry>
SereneDBIndexScanEntry::ForSecondaryIndex(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
  std::vector<size_t> indexed_col_indices, ObjectId sk_shard_id,
  bool sk_unique) {
  SDB_ASSERT(sk_shard_id != ObjectId{});
  return duckdb::unique_ptr<SereneDBIndexScanEntry>(new SereneDBIndexScanEntry(
    catalog, schema, info, std::move(sdb_table), std::move(indexed_col_indices),
    nullptr, sk_shard_id, sk_unique));
}

SereneDBIndexScanEntry::SereneDBIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
  std::vector<size_t> indexed_col_indices,
  std::shared_ptr<const catalog::InvertedIndex> inverted_index,
  ObjectId sk_shard_id, bool sk_unique)
  : duckdb::TableCatalogEntry(catalog, schema, info),
    _sdb_table(std::move(sdb_table)),
    _indexed_col_indices(std::move(indexed_col_indices)),
    _inverted_index(std::move(inverted_index)),
    _sk_shard_id(sk_shard_id),
    _sk_unique(sk_unique) {}

duckdb::unique_ptr<duckdb::BaseStatistics>
SereneDBIndexScanEntry::GetStatistics(duckdb::ClientContext& /*context*/,
                                      duckdb::column_t /*column_id*/) {
  return nullptr;
}

duckdb::TableFunction SereneDBIndexScanEntry::GetScanFunction(
  duckdb::ClientContext& /*context*/,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto data = duckdb::make_uniq<SereneDBScanBindData>();
  data->table = _sdb_table;
  for (const auto& col : _sdb_table->Columns()) {
    if (col.id == catalog::Column::kGeneratedPKId) {
      continue;  // Skip generated PK -- not stored as a value
    }
    data->column_ids.push_back(col.id);
    data->column_types.push_back(col.type);
  }
  data->has_rowid = true;
  data->table_entry = this;

  if (IsSecondaryIndex()) {
    auto sk = std::make_unique<SecondaryIndexScan>();
    sk->shard_id = _sk_shard_id;
    sk->is_unique = _sk_unique;
    data->scan_source = std::move(sk);
    bind_data = std::move(data);
    return CreateFullSkScanFunction();
  }
  // Inverted-index entry: leave scan_source as default FullTableScan; the
  // iresearch_plan rule (Phase 5) swaps the function on a search/ANN/range
  // match. The full-iresearch stub iterates all docs in the meantime.
  bind_data = std::move(data);
  return CreateFullIresearchScanFunction();
}

duckdb::TableStorageInfo SereneDBIndexScanEntry::GetStorageInfo(
  duckdb::ClientContext& /*context*/) {
  return SereneDBTableEntry::BuildStorageInfo(*_sdb_table);
}

duckdb::vector<duckdb::column_t> SereneDBIndexScanEntry::GetRowIdColumns()
  const {
  return SereneDBTableEntry::BuildRowIdColumns(*_sdb_table,
                                               _indexed_col_indices);
}

duckdb::virtual_column_map_t SereneDBIndexScanEntry::GetVirtualColumns() const {
  return SereneDBTableEntry::BuildVirtualColumns(*_sdb_table,
                                                 _indexed_col_indices);
}

}  // namespace sdb::connector
