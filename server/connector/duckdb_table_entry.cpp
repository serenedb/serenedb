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

#include <duckdb/planner/expression/bound_columnref_expression.hpp>
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
  std::cerr << "SereneDB BindUpdateConstraints called, update.columns before: "
            << update.columns.size() << std::endl;
  // Add PK columns to the update projection so our PhysicalUpdate can
  // read them to build RocksDB keys. Uses DuckDB's BindExtraColumns
  // which adds "identity" updates (col=col) for columns not in SET.
  const auto& pk_col_ids = _sdb_table->PKColumns();
  if (!pk_col_ids.empty()) {
    duckdb::physical_index_set_t pk_set;
    const auto& columns = _sdb_table->Columns();
    for (auto pk_id : pk_col_ids) {
      for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].id == pk_id) {
          pk_set.insert(duckdb::PhysicalIndex(i));
          break;
        }
      }
    }
    // BindExtraColumns skips sets of size <= 1, so we add PK columns
    // manually by mimicking its logic: project col from scan, add identity
    // update expression (col=col).
    for (auto& physical_id : pk_set) {
      bool already_present = false;
      for (auto& existing : update.columns) {
        if (existing == physical_id) {
          already_present = true;
          break;
        }
      }
      if (already_present) {
        continue;
      }
      auto& column = GetColumns().GetColumn(physical_id);
      auto proj_ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        column.Type(),
        duckdb::ColumnBinding(
          get.table_index,
          duckdb::ProjectionIndex(get.GetColumnIds().size())));
      auto proj_index = duckdb::ColumnBinding::PushExpression(
        proj.expressions, std::move(proj_ref));
      update.expressions.push_back(
        duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
          column.Type(),
          duckdb::ColumnBinding(proj.table_index, proj_index)));
      get.AddColumnId(column.Logical().index);
      update.columns.push_back(physical_id);
    }
  }

  // Also call default logic for CHECK constraints etc.
  duckdb::TableCatalogEntry::BindUpdateConstraints(binder, get, proj, update,
                                                   context);
}

duckdb::vector<duckdb::column_t> SereneDBTableEntry::GetRowIdColumns() const {
  duckdb::vector<duckdb::column_t> result;
  result.push_back(duckdb::COLUMN_IDENTIFIER_ROW_ID);
  return result;
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
