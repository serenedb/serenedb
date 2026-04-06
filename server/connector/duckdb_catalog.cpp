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
#include <duckdb/planner/operator/logical_update.hpp>

#include "connector/duckdb_physical_delete.h"
#include "connector/duckdb_physical_insert.h"
#include "connector/duckdb_physical_update.h"
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

  // PK columns are added to the scan by BindRowIdColumns via GetRowIdColumns.
  // They appear at the end of the child output, before the dummy rowid.
  // Layout: [..., pk_col_0, pk_col_1, ..., rowid]
  const auto& pk_col_ids = sdb_table->PKColumns();
  auto num_pk = pk_col_ids.size();
  auto child_cols = plan.types.size();
  // PK columns are at positions [child_cols - 1 - num_pk .. child_cols - 2]
  // (last position is rowid)
  std::vector<duckdb::idx_t> pk_indices;
  for (size_t i = 0; i < num_pk; ++i) {
    pk_indices.push_back(child_cols - 1 - num_pk + i);
  }

  auto& del = planner.Make<SereneDBPhysicalDelete>(
    std::move(sdb_table), std::move(pk_indices), op.estimated_cardinality);
  del.children.push_back(plan);
  return del;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = op.table.Cast<SereneDBTableEntry>();
  auto sdb_table = table_entry.GetSereneDBTable();

  // Input chunk layout: [SET_val_0, ..., SET_val_N, pk_virtual_0, ..., rowid]
  // PK columns are added by BindRowIdColumns at the end, before rowid.

  const auto& pk_col_ids = sdb_table->PKColumns();
  auto num_pk = pk_col_ids.size();
  auto child_cols = plan.types.size();

  // SET values are at 0..op.columns.size()-1
  std::vector<duckdb::idx_t> update_input_indices;
  for (size_t i = 0; i < op.columns.size(); ++i) {
    update_input_indices.push_back(i);
  }

  // PK columns are at [child_cols - 1 - num_pk .. child_cols - 2]
  std::vector<duckdb::idx_t> pk_indices;
  for (size_t i = 0; i < num_pk; ++i) {
    pk_indices.push_back(child_cols - 1 - num_pk + i);
  }

  auto& upd = planner.Make<SereneDBPhysicalUpdate>(
    std::move(sdb_table), std::move(pk_indices), std::move(op.columns),
    std::move(update_input_indices), op.estimated_cardinality);
  upd.children.push_back(plan);
  return upd;
}

duckdb::DatabaseSize SereneDBCatalog::GetDatabaseSize(
  duckdb::ClientContext& context) {
  return {};
}

}  // namespace sdb::connector
