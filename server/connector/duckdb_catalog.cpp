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

#include <duckdb/execution/operator/order/physical_order.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/storage/database_size.hpp>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_entry_cache.h"
#include "connector/duckdb_physical_ctas.h"
#include "connector/duckdb_physical_delete.h"
#include "connector/duckdb_physical_insert.h"
#include "connector/duckdb_physical_sst_insert.h"
#include "connector/duckdb_physical_update.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_table_entry.h"
#include "pg/connection_context.h"

namespace sdb::connector {

SereneDBCatalog::SereneDBCatalog(duckdb::AttachedDatabase& db,
                                 std::shared_ptr<catalog::Database> database)
  : duckdb::Catalog{db}, _database{std::move(database)} {}

void SereneDBCatalog::Initialize(bool load_builtin) {}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateSchema(
  duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo& info) {
  auto& catalog_impl = catalog::GetCatalog();
  auto schema = std::make_shared<catalog::Schema>(
    GetDatabaseId(), catalog::SchemaOptions{.name = info.schema});
  auto r = catalog_impl.CreateSchema(GetDatabaseId(), std::move(schema));
  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && if_not_exists) {
    return nullptr;
  }
  if (!r.ok()) {
    throw duckdb::InvalidInputException("Failed to create schema: %s",
                                        std::string{r.errorMessage()});
  }
  // New snapshot will have the schema; next LookupSchema will find it
  return nullptr;
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SereneDBCatalog::LookupSchema(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& schema_lookup,
  duckdb::OnEntryNotFound if_not_found) {
  auto schema_name = std::string{schema_lookup.GetEntryName()};
  // DuckDB uses "main" as default schema; map to "public" for PG compat
  if (schema_name == "main" || schema_name.empty()) {
    schema_name = "public";
  }
  // Get connection's snapshot and delegate to its cache
  auto snapshot =
    GetSereneDBContext(transaction.GetContext()).EnsureCatalogSnapshot();
  return snapshot->GetDuckDBCache(GetDatabaseId())
    .GetOrCreateSchema(*this, GetDatabaseId(), schema_name, *snapshot);
}

void SereneDBCatalog::ScanSchemas(
  duckdb::ClientContext& context,
  std::function<void(duckdb::SchemaCatalogEntry&)> callback) {
  auto snapshot = GetSereneDBContext(context).EnsureCatalogSnapshot();
  snapshot->GetDuckDBCache(GetDatabaseId())
    .ScanSchemas(*this, GetDatabaseId(), callback, *snapshot);
}

void SereneDBCatalog::DropSchema(duckdb::ClientContext& context,
                                 duckdb::DropInfo& info) {
  auto& catalog_impl = catalog::GetCatalog();
  bool cascade = info.cascade;
  auto r = catalog_impl.DropSchema(GetDatabaseId(), info.name, cascade);
  if (!r.ok()) {
    throw duckdb::InvalidInputException("Failed to drop schema: %s",
                                        std::string{r.errorMessage()});
  }
  // No cache to invalidate -- schema entries live on snapshot
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanCreateTableAs(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) {
  // CTAS always gets a generated PK (monotonic), no sort needed.
  auto& ctas = planner.Make<SereneDBPhysicalCTAS>(std::move(op.info), op.schema,
                                                  op.estimated_cardinality);
  ctas.children.push_back(plan);
  return ctas;
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

  // Use SST bulk insert for COPY FROM / INSERT...SELECT (has child plan).
  // SST bypasses transactions -- no conflict detection.
  bool use_sst = plan != nullptr && op.on_conflict_info.action_type ==
                                      duckdb::OnConflictAction::THROW;

  if (use_sst) {
    auto* sorted_plan = plan.get();

    // For explicit PKs, add a Sort by PK columns before SST insert.
    // SST requires keys in ascending order. Generated PKs are monotonic
    // (no sort needed).
    const auto& pk_col_ids = sdb_table->PKColumns();
    if (!pk_col_ids.empty()) {
      const auto& columns = sdb_table->Columns();
      duckdb::vector<duckdb::BoundOrderByNode> orders;
      duckdb::vector<duckdb::idx_t> projections;

      // Sort by PK columns (ascending, nulls first)
      for (auto pk_id : pk_col_ids) {
        for (size_t i = 0; i < columns.size(); ++i) {
          if (columns[i].id == pk_id) {
            auto col_expr =
              duckdb::make_uniq_base<duckdb::Expression,
                                     duckdb::BoundReferenceExpression>(
                columns[i].type, i);
            orders.emplace_back(duckdb::OrderType::ASCENDING,
                                duckdb::OrderByNullType::NULLS_FIRST,
                                std::move(col_expr));
            break;
          }
        }
      }

      // Project all input columns through the sort
      for (duckdb::idx_t i = 0; i < plan->types.size(); ++i) {
        projections.push_back(i);
      }

      auto& sort = planner.Make<duckdb::PhysicalOrder>(
        plan->types, std::move(orders), std::move(projections),
        op.estimated_cardinality, true);
      sort.children.push_back(*plan);
      sorted_plan = &sort;
    }

    auto& insert = planner.Make<SereneDBPhysicalSSTInsert>(
      std::move(sdb_table), std::move(op.types), op.estimated_cardinality);
    insert.children.push_back(*sorted_plan);
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
  // Create LogicalCreateIndex directly without IndexBinder
  // (IndexBinder::BindCreateIndex casts bind_data to TableScanBindData
  // which fails for our SereneDBScanBindData).
  auto create_index_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateIndexInfo>(
      std::move(stmt.info));

  // Normalize index type to our registered types ("secondary" / "inverted").
  // DuckDB defaults to empty or "ART", PG defaults to "btree".
  {
    auto& idx_type = create_index_info->index_type;
    std::string lower;
    lower.resize(idx_type.size());
    std::transform(idx_type.begin(), idx_type.end(), lower.begin(), ::tolower);
    if (lower.empty() || lower == "art" || lower == "btree") {
      create_index_info->index_type = "secondary";
    } else if (lower == "secondary" || lower == "inverted") {
      create_index_info->index_type = lower;
    } else {
      throw duckdb::CatalogException("access method \"%s\" does not exist",
                                     idx_type);
    }
  }

  // Fill in column IDs and scan types from table columns + parsed expressions
  auto& sdb_entry = table.Cast<SereneDBTableEntry>();
  auto sdb_table = sdb_entry.GetSereneDBTable();
  const auto& columns = sdb_table->Columns();

  // Map column names from parsed expressions to column indices
  for (auto& expr : create_index_info->parsed_expressions) {
    if (expr->GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      auto& col_ref = expr->Cast<duckdb::ColumnRefExpression>();
      auto col_name = col_ref.GetColumnName();
      for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].name == col_name) {
          create_index_info->column_ids.push_back(i);
          create_index_info->scan_types.push_back(columns[i].type);
          break;
        }
      }
    }
  }
  create_index_info->scan_types.emplace_back(duckdb::LogicalType::ROW_TYPE);
  auto& get = plan->Cast<duckdb::LogicalGet>();
  // The binder creates an empty LogicalGet. Populate column_ids for all
  // table columns so the scan outputs everything the backfill needs.
  if (get.GetColumnIds().empty()) {
    for (size_t i = 0; i < columns.size(); ++i) {
      get.AddColumnId(static_cast<duckdb::column_t>(i));
    }
    get.types.clear();
    for (size_t i = 0; i < columns.size(); ++i) {
      get.types.push_back(columns[i].type);
    }
  }
  create_index_info->names = get.names;
  create_index_info->schema = table.schema.name;
  create_index_info->catalog = table.catalog.GetName();

  // Build BoundColumnRefExpression for ALL table columns so DuckDB's
  // column pruning keeps them in the scan. The backfill needs all columns
  // (PK bytes + index column values + serialization for the writer).
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions;
  for (size_t i = 0; i < columns.size(); ++i) {
    expressions.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      columns[i].type,
      duckdb::ColumnBinding(get.table_index, duckdb::ProjectionIndex(i))));
  }

  auto result = duckdb::make_uniq<duckdb::LogicalCreateIndex>(
    std::move(create_index_info), std::move(expressions), table, nullptr);
  result->children.push_back(std::move(plan));
  return result;
}

duckdb::DatabaseSize SereneDBCatalog::GetDatabaseSize(
  duckdb::ClientContext& context) {
  return {};
}

}  // namespace sdb::connector
