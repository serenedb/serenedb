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
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/storage/database_size.hpp>

#include "connector/duckdb_physical_delete.h"
#include "connector/duckdb_physical_insert.h"
#include "connector/duckdb_physical_sst_insert.h"
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
  if (schema_name == "main" || schema_name == "public" || schema_name.empty()) {
    return _default_schema.get();
  }
  // Return null for unknown schemas -- DuckDB will fall through to system
  // catalog
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

  // Use SST bulk insert when:
  //   1. Has child plan (COPY FROM / INSERT...SELECT, not INSERT VALUES)
  //   2. Generated PK (monotonic -> naturally sorted, no conflict possible)
  //   3. No ON CONFLICT DO NOTHING/REPLACE (SST bypasses transaction)
  bool use_sst =
    plan != nullptr && sdb_table->PKColumns().empty() &&
    op.on_conflict_info.action_type == duckdb::OnConflictAction::THROW;

  if (use_sst) {
    auto& insert = planner.Make<SereneDBPhysicalSSTInsert>(
      std::move(sdb_table), std::move(op.types), op.estimated_cardinality);
    insert.children.push_back(*plan);
    return insert;
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

  // Child output layout (from GetRowIdColumns):
  //   [..., pk_0, pk_1, ..., idx_col_0, idx_col_1, ..., rowid]
  // rowid is last, before it are indexed columns, before those are PK columns.
  const auto& pk_col_ids = sdb_table->PKColumns();
  const auto& idx_col_indices = table_entry.GetIndexedColumnIndices();
  auto num_pk = pk_col_ids.size();

  // Count non-PK indexed columns
  const auto& columns = sdb_table->Columns();
  containers::FlatHashSet<size_t> pk_set;
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        pk_set.insert(i);
        break;
      }
    }
  }
  std::vector<size_t> non_pk_idx;
  for (auto idx : idx_col_indices) {
    if (!pk_set.contains(idx)) {
      non_pk_idx.push_back(idx);
    }
  }
  auto num_idx = non_pk_idx.size();
  auto num_virtual = num_pk + num_idx + 1;  // +1 for rowid
  auto child_cols = plan.types.size();

  // PK columns at [child_cols - num_virtual .. child_cols - num_virtual +
  // num_pk - 1]
  std::vector<duckdb::idx_t> pk_indices;
  for (size_t i = 0; i < num_pk; ++i) {
    pk_indices.push_back(child_cols - num_virtual + i);
  }

  // Indexed columns at [child_cols - num_virtual + num_pk .. child_cols - 2]
  std::vector<duckdb::idx_t> indexed_indices;
  for (size_t i = 0; i < num_idx; ++i) {
    indexed_indices.push_back(child_cols - num_virtual + num_pk + i);
  }

  auto& del = planner.Make<SereneDBPhysicalDelete>(
    std::move(sdb_table), std::move(pk_indices), std::move(indexed_indices),
    op.estimated_cardinality);
  del.children.push_back(plan);
  return del;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = op.table.Cast<SereneDBTableEntry>();
  auto sdb_table = table_entry.GetSereneDBTable();

  // Child output layout:
  //   [SET_val_0, ..., SET_val_N, pk_0, ..., idx_col_0, ..., rowid]
  const auto& pk_col_ids = sdb_table->PKColumns();
  const auto& idx_col_indices = table_entry.GetIndexedColumnIndices();
  auto num_pk = pk_col_ids.size();

  const auto& columns = sdb_table->Columns();
  containers::FlatHashSet<size_t> pk_set;
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        pk_set.insert(i);
        break;
      }
    }
  }
  std::vector<size_t> non_pk_idx;
  for (auto idx : idx_col_indices) {
    if (!pk_set.contains(idx)) {
      non_pk_idx.push_back(idx);
    }
  }
  auto num_idx = non_pk_idx.size();
  auto num_virtual = num_pk + num_idx + 1;  // +1 for rowid
  auto child_cols = plan.types.size();

  // SET values are at 0..op.columns.size()-1
  std::vector<duckdb::idx_t> update_input_indices;
  for (size_t i = 0; i < op.columns.size(); ++i) {
    update_input_indices.push_back(i);
  }

  // PK columns at [child_cols - num_virtual .. child_cols - num_virtual +
  // num_pk - 1]
  std::vector<duckdb::idx_t> pk_indices;
  for (size_t i = 0; i < num_pk; ++i) {
    pk_indices.push_back(child_cols - num_virtual + i);
  }

  // Indexed columns at [child_cols - num_virtual + num_pk .. child_cols - 2]
  std::vector<duckdb::idx_t> indexed_indices;
  for (size_t i = 0; i < num_idx; ++i) {
    indexed_indices.push_back(child_cols - num_virtual + num_pk + i);
  }

  auto& upd = planner.Make<SereneDBPhysicalUpdate>(
    std::move(sdb_table), std::move(pk_indices), std::move(op.columns),
    std::move(update_input_indices), std::move(indexed_indices),
    op.estimated_cardinality);
  upd.children.push_back(plan);
  return upd;
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindCreateIndex(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::TableCatalogEntry& table,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  // Handle CREATE INDEX entirely at bind time -- create the index in SereneDB
  // catalog directly, bypassing DuckDB's PhysicalCreateIndex which requires
  // DuckTableEntry.
  auto create_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateIndexInfo>(
      std::move(stmt.info));
  auto& schema_entry = table.schema;
  auto txn = schema_entry.GetCatalogTransaction(binder.context);
  schema_entry.CreateIndex(txn, *create_info, table);

  // Return the scan plan -- DuckDB will wrap it in a dummy result.
  // The index is already created, nothing more to execute.
  return plan;
}

duckdb::DatabaseSize SereneDBCatalog::GetDatabaseSize(
  duckdb::ClientContext& context) {
  return {};
}

}  // namespace sdb::connector
