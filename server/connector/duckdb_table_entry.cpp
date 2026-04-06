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
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/storage/table_storage_info.hpp>
#include <velox/type/Type.h>

#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_update.hpp>

#include "connector/duckdb_table_function.h"

namespace sdb::connector {

duckdb::LogicalType VeloxTypeToDuckDB(const velox::TypePtr& type) {
  switch (type->kind()) {
    case velox::TypeKind::BOOLEAN:
      return duckdb::LogicalType::BOOLEAN;
    case velox::TypeKind::TINYINT:
      return duckdb::LogicalType::TINYINT;
    case velox::TypeKind::SMALLINT:
      return duckdb::LogicalType::SMALLINT;
    case velox::TypeKind::INTEGER:
      return duckdb::LogicalType::INTEGER;
    case velox::TypeKind::BIGINT:
      return duckdb::LogicalType::BIGINT;
    case velox::TypeKind::REAL:
      return duckdb::LogicalType::FLOAT;
    case velox::TypeKind::DOUBLE:
      return duckdb::LogicalType::DOUBLE;
    case velox::TypeKind::VARCHAR:
      return duckdb::LogicalType::VARCHAR;
    case velox::TypeKind::VARBINARY:
      return duckdb::LogicalType::BLOB;
    case velox::TypeKind::TIMESTAMP:
      return duckdb::LogicalType::TIMESTAMP;
    case velox::TypeKind::HUGEINT:
      return duckdb::LogicalType::HUGEINT;
    case velox::TypeKind::ARRAY: {
      auto child = VeloxTypeToDuckDB(type->asArray().elementType());
      return duckdb::LogicalType::LIST(std::move(child));
    }
    case velox::TypeKind::MAP: {
      auto key = VeloxTypeToDuckDB(type->asMap().keyType());
      auto val = VeloxTypeToDuckDB(type->asMap().valueType());
      return duckdb::LogicalType::MAP(std::move(key), std::move(val));
    }
    case velox::TypeKind::ROW: {
      auto& row = type->asRow();
      duckdb::child_list_t<duckdb::LogicalType> children;
      for (size_t i = 0; i < row.size(); ++i) {
        children.emplace_back(row.nameOf(i),
                              VeloxTypeToDuckDB(row.childAt(i)));
      }
      return duckdb::LogicalType::STRUCT(std::move(children));
    }
    default:
      return duckdb::LogicalType::VARCHAR;
  }
}

velox::TypePtr DuckDBTypeToVelox(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return velox::BOOLEAN();
    case duckdb::LogicalTypeId::TINYINT:
      return velox::TINYINT();
    case duckdb::LogicalTypeId::SMALLINT:
      return velox::SMALLINT();
    case duckdb::LogicalTypeId::INTEGER:
      return velox::INTEGER();
    case duckdb::LogicalTypeId::BIGINT:
      return velox::BIGINT();
    case duckdb::LogicalTypeId::FLOAT:
      return velox::REAL();
    case duckdb::LogicalTypeId::DOUBLE:
      return velox::DOUBLE();
    case duckdb::LogicalTypeId::VARCHAR:
      return velox::VARCHAR();
    case duckdb::LogicalTypeId::BLOB:
      return velox::VARBINARY();
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return velox::TIMESTAMP();
    case duckdb::LogicalTypeId::DATE:
      return velox::DATE();
    case duckdb::LogicalTypeId::HUGEINT:
      return velox::HUGEINT();
    default:
      return velox::VARCHAR();
  }
}

SereneDBTableEntry::SereneDBTableEntry(duckdb::Catalog& catalog,
                                       duckdb::SchemaCatalogEntry& schema,
                                       duckdb::CreateTableInfo& info,
                                       std::shared_ptr<catalog::Table> sdb_table)
  : duckdb::TableCatalogEntry(catalog, schema, info),
    _sdb_table(std::move(sdb_table)) {}

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
    if (col.id == catalog::Column::kGeneratedPKId) {
      continue;  // Skip generated PK — not a real column
    }
    data->column_ids.push_back(col.id);
    data->column_types.push_back(VeloxTypeToDuckDB(col.type));
  }
  // Always include rowid (PK bytes) as the last column for DELETE/UPDATE
  data->has_rowid = true;
  data->table_entry = this;
  bind_data = std::move(data);
  return CreateSereneDBScanFunction();
}

void SereneDBTableEntry::BindUpdateConstraints(
  duckdb::Binder& binder, duckdb::LogicalGet& get,
  duckdb::LogicalProjection& proj, duckdb::LogicalUpdate& update,
  duckdb::ClientContext& context) {
  // PK columns are now added via GetRowIdColumns/BindRowIdColumns.
  // Just call default logic for CHECK constraints etc.
  duckdb::TableCatalogEntry::BindUpdateConstraints(binder, get, proj, update,
                                                   context);
}

duckdb::vector<duckdb::column_t> SereneDBTableEntry::GetRowIdColumns() const {
  duckdb::vector<duckdb::column_t> result;
  // Register each PK column as a virtual column so BindRowIdColumns
  // adds them to the scan for DELETE/UPDATE
  const auto& pk_col_ids = _sdb_table->PKColumns();
  const auto& columns = _sdb_table->Columns();
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        result.push_back(duckdb::VIRTUAL_COLUMN_START + i);
        break;
      }
    }
  }
  // Also keep standard rowid for DuckDB internals
  result.push_back(duckdb::COLUMN_IDENTIFIER_ROW_ID);
  return result;
}

duckdb::virtual_column_map_t SereneDBTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  // PK columns as virtual columns with IDs >= VIRTUAL_COLUMN_START
  const auto& pk_col_ids = _sdb_table->PKColumns();
  const auto& columns = _sdb_table->Columns();
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        result.insert({duckdb::VIRTUAL_COLUMN_START + i,
                       duckdb::TableColumn(columns[i].name,
                                           VeloxTypeToDuckDB(columns[i].type))});
        break;
      }
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
