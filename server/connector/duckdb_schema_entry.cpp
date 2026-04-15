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

#include "connector/duckdb_schema_entry.h"

#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/parsed_data/create_function_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <iostream>

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/function.h"
#include "catalog/index.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/view.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_entry_cache.h"
#include "connector/duckdb_table_entry.h"
#include "pg/connection_context.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/secondary_index_shard.h"

namespace sdb::connector {

ObjectId SereneDBSchemaEntry::GetDatabaseId() const {
  return catalog.Cast<SereneDBCatalog>().GetDatabaseId();
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::LookupEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& lookup_info) {
  auto& conn_ctx = GetSereneDBContext(transaction.GetContext());
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto result = snapshot->GetDuckDBEntryCache().EnsureEntry(
    lookup_info.GetCatalogType(), catalog, *this, GetDatabaseId(), name,
    lookup_info.GetEntryName(), *snapshot);
  fprintf(stderr, "[LookupEntry] schema=%s type=%d name=%.*s -> %s\n",
          name.c_str(), static_cast<int>(lookup_info.GetCatalogType()),
          static_cast<int>(lookup_info.GetEntryName().size()),
          lookup_info.GetEntryName().data(), result ? "FOUND" : "nullptr");
  return result;
}

void SereneDBSchemaEntry::Scan(
  duckdb::ClientContext& context, duckdb::CatalogType type,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  snapshot->GetDuckDBEntryCache().ScanEntries(
    type, catalog, *this, GetDatabaseId(), name, callback, *snapshot);
}

void SereneDBSchemaEntry::Scan(
  duckdb::CatalogType type,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  // Without context -- no snapshot available, skip
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateTable(
  duckdb::CatalogTransaction transaction, duckdb::BoundCreateTableInfo& info) {
  auto& create_info = info.Base();
  auto& table_info = create_info.Cast<duckdb::CreateTableInfo>();

  // Build SereneDB CreateTableRequest from DuckDB types
  catalog::CreateTableRequest request;
  request.name = table_info.table;

  // Convert columns (Logical includes generated columns)
  catalog::Column::Id next_col_id = 0;
  for (auto& col : table_info.columns.Logical()) {
    catalog::Column sdb_col;
    sdb_col.id = next_col_id++;
    sdb_col.name = col.Name();
    sdb_col.type = col.Type();
    if (col.Generated()) {
      sdb_col.generated_type = catalog::Column::GeneratedType::kStored;
      sdb_col.expr =
        std::make_shared<ColumnExpr>(col.GeneratedExpression().Copy());
    } else if (col.HasDefaultValue()) {
      sdb_col.expr = std::make_shared<ColumnExpr>(col.DefaultValue().Copy());
    }
    request.columns.push_back(std::move(sdb_col));
  }

  // --- Constraint helpers (ported from old create_table.cpp) ---

  // PG-style constraint name generator with dedup
  auto choose_constraint_name = [&](std::string_view tbl,
                                    std::string_view column,
                                    std::string_view label) -> std::string {
    std::string base_name;
    if (column.empty()) {
      base_name = absl::StrCat(tbl, "_", label);
    } else {
      base_name = absl::StrCat(tbl, "_", column, "_", label);
    }
    auto name_exists = [&](std::string_view candidate) {
      return std::ranges::any_of(request.checkConstraints, [&](const auto& c) {
        return c.name == candidate;
      });
    };
    if (!name_exists(base_name)) {
      return base_name;
    }
    for (size_t counter = 1;; ++counter) {
      auto candidate = absl::StrCat(base_name, counter);
      if (!name_exists(candidate)) {
        return candidate;
      }
    }
  };

  // Find the column name for constraint naming (PG convention):
  // returns the column name if all column refs point to the same column
  // (column-level CHECK), empty otherwise (table-level CHECK).
  auto find_constraint_column =
    [](const duckdb::ParsedExpression& root) -> std::string {
    std::string result;
    bool multiple = false;
    std::function<void(const duckdb::ParsedExpression&)> visit;
    visit = [&](const duckdb::ParsedExpression& expr) {
      if (multiple) {
        return;
      }
      if (expr.GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
        auto& name = expr.Cast<duckdb::ColumnRefExpression>().GetColumnName();
        if (result.empty()) {
          result = name;
        } else if (result != name) {
          multiple = true;
        }
        return;
      }
      duckdb::ParsedExpressionIterator::EnumerateChildren(
        expr, [&](const duckdb::ParsedExpression& child) { visit(child); });
    };
    visit(root);
    return multiple ? std::string{} : result;
  };

  // Track which columns already have NOT NULL to avoid duplicates
  std::vector<bool> has_not_null(request.columns.size(), false);

  auto append_not_null = [&](duckdb::idx_t col_idx) {
    if (col_idx >= request.columns.size() || has_not_null[col_idx]) {
      return;
    }
    has_not_null[col_idx] = true;
    auto& col_name = request.columns[col_idx].name;
    auto col_ref = duckdb::make_uniq<duckdb::ColumnRefExpression>(col_name);
    auto is_not_null = duckdb::make_uniq<duckdb::OperatorExpression>(
      duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, std::move(col_ref));
    request.checkConstraints.push_back(catalog::CheckConstraint{
      .id = catalog::NextId(),
      .name = choose_constraint_name(table_info.table, col_name, "not_null"),
      .expr = std::make_shared<ColumnExpr>(std::move(is_not_null)),
    });
  };

  auto append_pk = [&](catalog::Column::Id col_id) {
    if (absl::c_linear_search(request.pkColumns, col_id)) {
      throw duckdb::CatalogException(
        "column \"%s\" appears twice in primary key constraint",
        request.columns[col_id].name);
    }
    // PK implies NOT NULL
    append_not_null(col_id);
    request.pkColumns.push_back(col_id);
  };

  // --- Single pass over all constraints ---

  for (auto& constraint : table_info.constraints) {
    switch (constraint->type) {
      case duckdb::ConstraintType::UNIQUE: {
        auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
        if (!unique.IsPrimaryKey()) {
          break;
        }
        if (unique.HasIndex()) {
          auto idx = unique.GetIndex().index;
          SDB_ASSERT(idx < request.columns.size());
          append_pk(request.columns[idx].id);
        } else {
          for (auto& pk_name : unique.GetColumnNames()) {
            auto it = absl::c_find_if(request.columns, [&](const auto& col) {
              return col.name == pk_name;
            });
            if (it == request.columns.end()) {
              throw duckdb::CatalogException(
                "column \"%s\" named in key does not exist", pk_name);
            }
            append_pk(it->id);
          }
        }
        break;
      }
      case duckdb::ConstraintType::NOT_NULL: {
        auto& nn = constraint->Cast<duckdb::NotNullConstraint>();
        append_not_null(nn.index.index);
        break;
      }
      case duckdb::ConstraintType::CHECK: {
        auto& check = constraint->Cast<duckdb::CheckConstraint>();
        std::string name;
        if (!check.constraint_name.empty()) {
          name = check.constraint_name;
        } else {
          auto col = find_constraint_column(*check.expression);
          name = choose_constraint_name(table_info.table, col, "check");
        }
        request.checkConstraints.push_back(catalog::CheckConstraint{
          .id = catalog::NextId(),
          .name = std::move(name),
          .expr = std::make_shared<ColumnExpr>(check.expression->Copy()),
        });
        break;
      }
      default:
        break;
    }
  }

  // Get database info
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto database_id = GetDatabaseId();
  auto database = snapshot->GetDatabase(database_id);
  SDB_ASSERT(database);

  // Create table options
  catalog::CreateTableOptions options;
  auto r = catalog::MakeTableOptions(std::move(request), database_id, options,
                                     database->GetReplicationFactor(),
                                     database->GetWriteConcern(), false);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  bool if_not_exists =
    create_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  catalog::CreateTableOperationOptions op_options;

  r = catalog_impl.CreateTable(database_id, "public", std::move(options),
                               op_options);
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (if_not_exists) {
      return nullptr;
    }
    throw duckdb::CatalogException("relation \"%s\" already exists",
                                   table_info.table);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateIndex(
  duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
  duckdb::TableCatalogEntry& table) {
  auto& sdb_table_entry = table.Cast<SereneDBTableEntry>();
  auto sdb_table = sdb_table_entry.GetSereneDBTable();

  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto database_id = GetDatabaseId();

  // Map DuckDB index type to SereneDB IndexType
  // DuckDB default is empty or "ART"; PG default is "btree"
  catalog::ObjectType index_type;
  auto idx_type_str = info.index_type;
  std::transform(idx_type_str.begin(), idx_type_str.end(), idx_type_str.begin(),
                 ::tolower);
  if (idx_type_str.empty() || idx_type_str == "art" ||
      idx_type_str == "btree" || idx_type_str == "secondary") {
    index_type = catalog::ObjectType::SecondaryIndex;
  } else if (idx_type_str == "inverted") {
    index_type = catalog::ObjectType::InvertedIndex;
  } else {
    throw duckdb::CatalogException("access method \"%s\" does not exist",
                                   info.index_type);
  }

  // Build CreateIndexColumn vector from DuckDB info.
  // At bind time, column_ids may not be populated yet -- use names/expressions.
  const auto& columns = sdb_table->Columns();
  std::vector<catalog::CreateIndexColumn> idx_columns;

  // parsed_expressions has the actual index columns (from CREATE INDEX ON t
  // (col)) info.names has ALL table scan columns -- don't use it for index
  // columns!
  for (auto& expr : info.parsed_expressions) {
    if (expr->GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      auto& col_ref = expr->Cast<duckdb::ColumnRefExpression>();
      auto col_name = col_ref.GetColumnName();
      const catalog::Column* cat_col = nullptr;
      for (const auto& col : columns) {
        if (col.name == col_name) {
          cat_col = &col;
          break;
        }
      }
      if (!cat_col) {
        throw duckdb::CatalogException("column \"%s\" not found in table",
                                       col_name);
      }
      idx_columns.push_back(catalog::CreateIndexColumn{
        .catalog_column = cat_col,
        .name = cat_col->name,
      });
    } else {
      throw duckdb::CatalogException(
        "Expression-based index columns are not supported");
    }
  }

  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  Result create_result;
  if (index_type == catalog::ObjectType::InvertedIndex) {
    search::InvertedIndexShardOptions shard_options;
    auto it = info.options.find("commit_interval");
    if (it != info.options.end()) {
      shard_options.base.commit_interval_ms = it->second.GetValue<int64_t>();
    }
    it = info.options.find("consolidation_interval");
    if (it != info.options.end()) {
      shard_options.base.consolidation_interval_ms =
        it->second.GetValue<int64_t>();
    }
    it = info.options.find("cleanup_interval_step");
    if (it != info.options.end()) {
      shard_options.base.cleanup_interval_step = it->second.GetValue<int64_t>();
    }
    create_result = catalog_impl.CreateInvertedIndex(
      database_id, "public", sdb_table->GetName(), info.index_name,
      std::move(idx_columns), shard_options);
  } else {
    bool unique = (info.constraint_type == duckdb::IndexConstraintType::UNIQUE);
    create_result = catalog_impl.CreateSecondaryIndex(
      database_id, "public", sdb_table->GetName(), info.index_name,
      std::move(idx_columns), unique);
  }

  if (create_result.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (if_not_exists) {
      return nullptr;
    }
    throw duckdb::CatalogException("relation \"%s\" already exists",
                                   info.index_name);
  }
  if (!create_result.ok()) {
    SDB_THROW(std::move(create_result));
  }

  // Start background tasks for inverted indexes
  auto new_snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_index =
    new_snapshot->GetRelation(database_id, "public", info.index_name);
  if (catalog_index) {
    auto shard = new_snapshot->GetIndexShard(catalog_index->GetId());
    if (shard && shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
      auto& inverted_shard =
        basics::downCast<search::InvertedIndexShard>(*shard);
      inverted_shard.StartTasks();
      // No backfill yet -- mark creation as finished so background commits
      // register the flush subscription and run periodically.
      inverted_shard.FinishCreation();
    }
  }
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateFunction(
  duckdb::CatalogTransaction transaction, duckdb::CreateFunctionInfo& info) {
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();
  auto database_id = GetDatabaseId();

  auto macro_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      info.Copy());
  auto function = std::make_shared<catalog::PgSqlFunction>(
    database_id, ObjectId{}, info.name, std::move(macro_info));

  bool replace =
    info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  auto r = catalog_impl.CreateFunction(database_id, name, function, replace);

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    throw duckdb::CatalogException("relation \"%s\" already exists", info.name);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateView(
  duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo& info) {
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();
  auto database_id = GetDatabaseId();

  auto view_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      info.Copy());
  auto view = std::make_shared<catalog::PgSqlView>(
    database_id, ObjectId{}, info.view_name, std::move(view_info));

  bool replace =
    info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  auto r = catalog_impl.CreateView(database_id, name, view, replace);

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    if (replace) {
      throw duckdb::CatalogException("\"%s\" is not a view", info.view_name);
    }
    throw duckdb::CatalogException("relation \"%s\" already exists",
                                   info.view_name);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateSequence(
  duckdb::CatalogTransaction transaction, duckdb::CreateSequenceInfo& info) {
  throw duckdb::NotImplementedException("CREATE SEQUENCE through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreateTableFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateTableFunctionInfo& info) {
  throw duckdb::NotImplementedException("CREATE TABLE FUNCTION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreateCopyFunction(duckdb::CatalogTransaction transaction,
                                        duckdb::CreateCopyFunctionInfo& info) {
  throw duckdb::NotImplementedException("CREATE COPY FUNCTION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreatePragmaFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreatePragmaFunctionInfo& info) {
  throw duckdb::NotImplementedException(
    "CREATE PRAGMA FUNCTION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateCollation(
  duckdb::CatalogTransaction transaction, duckdb::CreateCollationInfo& info) {
  throw duckdb::NotImplementedException("CREATE COLLATION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateType(
  duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo& info) {
  throw duckdb::NotImplementedException("CREATE TYPE through DuckDB");
}

void SereneDBSchemaEntry::DropEntry(duckdb::ClientContext& context,
                                    duckdb::DropInfo& info) {
  DropObject(context, info);
}

void SereneDBSchemaEntry::Alter(duckdb::CatalogTransaction transaction,
                                duckdb::AlterInfo& info) {
  throw duckdb::NotImplementedException("ALTER through DuckDB");
}

}  // namespace sdb::connector
