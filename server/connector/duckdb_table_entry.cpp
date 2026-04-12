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

#include "connector/duckdb_table_entry.h"

#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/storage/table_storage_info.hpp>

#include "connector/duckdb_table_function.h"

namespace sdb::connector {

SereneDBTableEntry::SereneDBTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
  std::vector<size_t> indexed_col_indices,
  std::shared_ptr<const catalog::InvertedIndex> inverted_index)
  : duckdb::TableCatalogEntry(catalog, schema, info),
    _sdb_table(std::move(sdb_table)),
    _indexed_col_indices(std::move(indexed_col_indices)),
    _inverted_index(std::move(inverted_index)) {}

duckdb::unique_ptr<duckdb::BaseStatistics> SereneDBTableEntry::GetStatistics(
  duckdb::ClientContext& context, duckdb::column_t column_id) {
  return nullptr;
}

duckdb::TableFunction SereneDBTableEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto data = duckdb::make_uniq<SereneDBScanBindData>();
  data->table = _sdb_table;
  for (const auto& col : _sdb_table->Columns()) {
    if (col.id == catalog::Column::kGeneratedPKId || col.IsGenerated()) {
      continue;  // Skip generated PK and virtual generated columns
    }
    data->column_ids.push_back(col.id);
    data->column_types.push_back(col.type);
  }
  // Always include rowid (PK bytes) as the last column for DELETE/UPDATE
  data->has_rowid = true;
  data->table_entry = this;

  // If this entry was created from a secondary index name, use SK scan
  if (HasSecondaryIndex()) {
    data->scan_source = SecondaryIndexScan{
      .shard_id = _sk_shard_id,
      .is_unique = _sk_unique,
    };
  }

  bind_data = std::move(data);
  return CreateSereneDBScanFunction();
}

void SereneDBTableEntry::BindUpdateConstraints(duckdb::Binder& binder,
                                               duckdb::LogicalGet& get,
                                               duckdb::LogicalProjection& proj,
                                               duckdb::LogicalUpdate& update,
                                               duckdb::ClientContext& context) {
  // PK columns are now added via GetRowIdColumns/BindRowIdColumns.
  // Just call default logic for CHECK constraints etc.
  duckdb::TableCatalogEntry::BindUpdateConstraints(binder, get, proj, update,
                                                   context);
}

duckdb::vector<duckdb::column_t> SereneDBTableEntry::GetRowIdColumns() const {
  duckdb::vector<duckdb::column_t> result;
  const auto& pk_col_ids = _sdb_table->PKColumns();
  const auto& columns = _sdb_table->Columns();

  // Collect unique column indices: PK columns + indexed columns
  containers::FlatHashSet<size_t> needed;
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        needed.insert(i);
        break;
      }
    }
  }
  for (auto idx : _indexed_col_indices) {
    needed.insert(idx);
  }

  // Register as virtual columns in stable order (PK first, then indexed)
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        result.push_back(duckdb::VIRTUAL_COLUMN_START + i);
        break;
      }
    }
  }
  for (auto idx : _indexed_col_indices) {
    if (!needed.contains(idx)) {
      continue;  // already added as PK
    }
    // Only add if not already in the PK set
    bool is_pk = false;
    for (auto pk_id : pk_col_ids) {
      for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].id == pk_id && i == idx) {
          is_pk = true;
          break;
        }
      }
      if (is_pk) {
        break;
      }
    }
    if (!is_pk) {
      result.push_back(duckdb::VIRTUAL_COLUMN_START + idx);
    }
  }

  result.push_back(duckdb::COLUMN_IDENTIFIER_ROW_ID);
  return result;
}

duckdb::virtual_column_map_t SereneDBTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  const auto& pk_col_ids = _sdb_table->PKColumns();
  const auto& columns = _sdb_table->Columns();

  // PK columns
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        result.insert({duckdb::VIRTUAL_COLUMN_START + i,
                       duckdb::TableColumn(columns[i].name, columns[i].type)});
        break;
      }
    }
  }

  // Indexed columns (skip if already added as PK)
  for (auto idx : _indexed_col_indices) {
    auto virt_id = duckdb::VIRTUAL_COLUMN_START + idx;
    if (!result.contains(virt_id)) {
      result.insert(
        {virt_id, duckdb::TableColumn(columns[idx].name, columns[idx].type)});
    }
  }

  // Standard rowid
  result.insert({duckdb::COLUMN_IDENTIFIER_ROW_ID,
                 duckdb::TableColumn("rowid", duckdb::LogicalType::ROW_TYPE)});
  return result;
}

duckdb::column_t SereneDBTableEntry::VirtualToPKColumnIndex(
  duckdb::column_t virtual_id) {
  if (virtual_id >= duckdb::VIRTUAL_COLUMN_START &&
      virtual_id < duckdb::COLUMN_IDENTIFIER_ROW_ID) {
    return virtual_id - duckdb::VIRTUAL_COLUMN_START;
  }
  return duckdb::DConstants::INVALID_INDEX;
}

duckdb::TableStorageInfo SereneDBTableEntry::GetStorageInfo(
  duckdb::ClientContext& context) {
  duckdb::TableStorageInfo info;

  // Report PK as a unique index so DuckDB binder can use it for ON CONFLICT
  const auto& pk_col_ids = _sdb_table->PKColumns();
  if (!pk_col_ids.empty()) {
    duckdb::IndexInfo idx_info;
    idx_info.is_unique = true;
    idx_info.is_primary = true;
    idx_info.is_foreign = false;
    // Map PK column IDs to column indices in the table
    const auto& columns = _sdb_table->Columns();
    for (auto pk_id : pk_col_ids) {
      for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].id == pk_id) {
          idx_info.column_set.insert(i);
          break;
        }
      }
    }
    info.index_info.push_back(std::move(idx_info));
  }

  return info;
}

}  // namespace sdb::connector
