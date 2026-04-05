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
    data->column_ids.push_back(col.id);
    data->column_types.push_back(VeloxTypeToDuckDB(col.type));
  }
  bind_data = std::move(data);
  return CreateSereneDBScanFunction();
}

duckdb::TableStorageInfo SereneDBTableEntry::GetStorageInfo(
  duckdb::ClientContext& context) {
  duckdb::TableStorageInfo info;
  // TODO: Fill with actual storage info
  return info;
}

}  // namespace sdb::connector
