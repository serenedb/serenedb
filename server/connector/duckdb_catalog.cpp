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

#include "connector/duckdb_catalog.h"

#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/storage/database_size.hpp>

#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>

#include "connector/duckdb_physical_delete.h"
#include "connector/duckdb_physical_insert.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_table_entry.h"

namespace sdb::connector {

SereneDBCatalog::SereneDBCatalog(duckdb::AttachedDatabase& db)
  : duckdb::Catalog(db) {}

void SereneDBCatalog::Initialize(bool load_builtin) {
  auto info = duckdb::make_uniq<duckdb::CreateSchemaInfo>();
  info->schema = "main";  // "main"
  _default_schema = duckdb::make_uniq<SereneDBSchemaEntry>(*this, *info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateSchema(
  duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo& info) {
  throw duckdb::NotImplementedException("CREATE SCHEMA through DuckDB");
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SereneDBCatalog::LookupSchema(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& schema_lookup,
  duckdb::OnEntryNotFound if_not_found) {
  auto schema_name = schema_lookup.GetEntryName();
  // Map "main", "public", or default to our single schema
  if (schema_name == "main" || schema_name == "public" ||
      schema_name.empty()) {
    return _default_schema.get();
  }
  // Return null for unknown schemas — DuckDB will fall through to system catalog
  return nullptr;
}

void SereneDBCatalog::ScanSchemas(
  duckdb::ClientContext& context,
  std::function<void(duckdb::SchemaCatalogEntry&)> callback) {
  callback(*_default_schema);
}

void SereneDBCatalog::DropSchema(duckdb::ClientContext& context,
                                 duckdb::DropInfo& info) {
  throw duckdb::NotImplementedException("DROP SCHEMA through DuckDB");
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanCreateTableAs(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) {
  throw duckdb::NotImplementedException("CREATE TABLE AS through DuckDB");
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanInsert(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalInsert& op,
  duckdb::optional_ptr<duckdb::PhysicalOperator> plan) {
  auto& table_entry = op.table.Cast<SereneDBTableEntry>();
  auto sdb_table = table_entry.GetSereneDBTable();

  // Handle default column projection if not all columns specified
  if (!op.column_index_map.empty()) {
    plan = &planner.ResolveDefaultsProjection(op, *plan);
  }

  auto& insert = planner.Make<SereneDBPhysicalInsert>(
    std::move(sdb_table), std::move(op.types), op.estimated_cardinality,
    op.on_conflict_info.action_type);
  if (plan) {
    insert.children.push_back(*plan);
  }
  return insert;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanDelete(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalDelete& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = op.table.Cast<SereneDBTableEntry>();
  auto sdb_table = table_entry.GetSereneDBTable();

  // Find PK column indices in the scan output.
  // The scan outputs columns in table order. PK columns are at their
  // natural positions (e.g., if PK is column 0, it's output index 0).
  const auto& columns = sdb_table->Columns();
  const auto& pk_col_ids = sdb_table->PKColumns();
  std::vector<duckdb::idx_t> pk_indices;
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        pk_indices.push_back(i);
        break;
      }
    }
  }

  auto& del = planner.Make<SereneDBPhysicalDelete>(
    std::move(sdb_table), std::move(pk_indices), op.estimated_cardinality);
  del.children.push_back(plan);
  return del;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  throw duckdb::NotImplementedException("UPDATE through DuckDB catalog");
}

duckdb::DatabaseSize SereneDBCatalog::GetDatabaseSize(
  duckdb::ClientContext& context) {
  return {};
}

}  // namespace sdb::connector
