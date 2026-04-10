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

#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_data/create_function_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
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

SereneDBSchemaEntry::SereneDBSchemaEntry(duckdb::Catalog& catalog,
                                         duckdb::CreateSchemaInfo& info)
  : duckdb::SchemaCatalogEntry(catalog, info) {}

ObjectId SereneDBSchemaEntry::GetDatabaseId() const {
  return catalog.Cast<SereneDBCatalog>().GetDatabaseId();
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::LookupEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& lookup_info) {
  auto& conn_ctx = GetSereneDBContext(transaction.GetContext());
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  return snapshot->GetDuckDBCache().GetOrCreateEntry(
    lookup_info.GetCatalogType(), catalog, *this, GetDatabaseId(), name,
    lookup_info.GetEntryName(), *snapshot);
}

void SereneDBSchemaEntry::Scan(
  duckdb::ClientContext& context, duckdb::CatalogType type,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  snapshot->GetDuckDBCache().ScanEntries(type, catalog, *this, GetDatabaseId(),
                                         name, callback, *snapshot);
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

  // Convert columns
  catalog::Column::Id next_col_id = 0;
  for (auto& col : table_info.columns.Physical()) {
    catalog::Column sdb_col;
    sdb_col.id = next_col_id++;
    sdb_col.name = col.Name();
    sdb_col.type = col.Type();
    request.columns.push_back(std::move(sdb_col));
  }

  // Extract PK columns from constraints
  for (auto& constraint : table_info.constraints) {
    if (constraint->type == duckdb::ConstraintType::UNIQUE) {
      auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
      if (!unique.IsPrimaryKey()) {
        continue;
      }
      if (unique.HasIndex()) {
        // Single-column PK by index
        auto idx = unique.GetIndex().index;
        if (idx < request.columns.size()) {
          request.pkColumns.push_back(request.columns[idx].id);
        }
      } else {
        // Multi-column PK by names
        for (auto& pk_name : unique.GetColumnNames()) {
          for (auto& col : request.columns) {
            if (col.name == pk_name) {
              request.pkColumns.push_back(col.id);
              break;
            }
          }
        }
      }
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

  std::cerr << "SereneDB: Created table " << table_info.table << " via DuckDB"
            << std::endl;
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

  // No cache to invalidate -- entries live on snapshot, new snapshot picks up
  // index

  std::cerr << "SereneDB: Created index " << info.index_name << " on "
            << sdb_table->GetName() << " via DuckDB" << std::endl;
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateFunction(
  duckdb::CatalogTransaction transaction, duckdb::CreateFunctionInfo& info) {
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();
  auto database_id = GetDatabaseId();

  auto sql = info.ToString();
  auto function = std::make_shared<catalog::PgSqlFunction>(
    database_id, ObjectId{}, info.name, std::move(sql));

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

  std::cerr << "SereneDB: Created function " << info.name << " via DuckDB"
            << std::endl;
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateView(
  duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo& info) {
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();
  auto database_id = GetDatabaseId();

  auto sql = info.query->ToString();
  auto view = std::make_shared<catalog::PgSqlView>(
    database_id, ObjectId{}, info.view_name, std::move(sql));

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

  std::cerr << "SereneDB: Created view " << info.view_name << " via DuckDB"
            << std::endl;
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
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();

  Result r;
  if (info.type == duckdb::CatalogType::TABLE_ENTRY) {
    r = catalog_impl.DropTable(GetDatabaseId(), name, info.name);
  } else if (info.type == duckdb::CatalogType::VIEW_ENTRY) {
    r = catalog_impl.DropView(GetDatabaseId(), name, info.name);
  } else {
    throw duckdb::NotImplementedException(
      "DROP for type %s not supported", duckdb::CatalogTypeToString(info.type));
  }
  bool if_exists = info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL;
  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    if (if_exists) {
      return;
    }
    auto type_name = duckdb::CatalogTypeToString(info.type);
    throw duckdb::CatalogException("%s \"%s\" does not exist", type_name,
                                   info.name);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  // No cache to invalidate -- entries live on snapshot, new snapshot won't have
  // this table

  std::cerr << "SereneDB: Dropped table " << info.name << " via DuckDB"
            << std::endl;
}

void SereneDBSchemaEntry::Alter(duckdb::CatalogTransaction transaction,
                                duckdb::AlterInfo& info) {
  throw duckdb::NotImplementedException("ALTER through DuckDB");
}

}  // namespace sdb::connector
