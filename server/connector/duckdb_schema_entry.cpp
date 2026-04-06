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
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>

#include <iostream>

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_table_entry.h"
#include "pg/connection_context.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/secondary_index_shard.h"

namespace sdb::connector {

SereneDBSchemaEntry::SereneDBSchemaEntry(duckdb::Catalog& catalog,
                                         duckdb::CreateSchemaInfo& info)
  : duckdb::SchemaCatalogEntry(catalog, info) {}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::LookupEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& lookup_info) {
  auto type = lookup_info.GetCatalogType();
  auto table_name = lookup_info.GetEntryName();
  if (type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }

  // Return cached entry if we already looked up this table
  auto cached = _table_entries.find(std::string{table_name});
  if (cached != _table_entries.end()) {
    return cached->second.get();
  }

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto databases = snapshot->GetDatabases();
  auto table = snapshot->GetTable(
    databases.front()->GetId(), "public",
    std::string_view{table_name.data(), table_name.size()});
  if (!table) {
    return nullptr;
  }

  // Build a CreateTableInfo from the SereneDB table
  auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
  info->table = table_name;
  info->schema = name;
  for (const auto& col : table->Columns()) {
    info->columns.AddColumn(duckdb::ColumnDefinition(
      col.name, VeloxTypeToDuckDB(col.type)));
  }

  // Add PK constraint if table has explicit PK columns
  const auto& pk_col_ids = table->PKColumns();
  if (!pk_col_ids.empty()) {
    duckdb::vector<duckdb::string> pk_names;
    for (auto pk_id : pk_col_ids) {
      for (const auto& col : table->Columns()) {
        if (col.id == pk_id) {
          pk_names.push_back(col.name);
          break;
        }
      }
    }
    info->constraints.push_back(
      duckdb::make_uniq<duckdb::UniqueConstraint>(std::move(pk_names), true));
  }

  // Discover indexed columns from catalog
  std::vector<size_t> indexed_col_indices;
  {
    auto indexes = snapshot->GetIndexesByTable(table->GetId());
    const auto& cols = table->Columns();
    containers::FlatHashSet<size_t> idx_set;
    for (auto& index : indexes) {
      for (auto col_id : index->GetColumnIds()) {
        for (size_t i = 0; i < cols.size(); ++i) {
          if (cols[i].id == col_id) {
            idx_set.insert(i);
            break;
          }
        }
      }
    }
    indexed_col_indices.assign(idx_set.begin(), idx_set.end());
    std::sort(indexed_col_indices.begin(), indexed_col_indices.end());
  }

  // Create and cache the table entry
  auto entry = duckdb::make_uniq<SereneDBTableEntry>(
    catalog, *this, *info, std::move(table),
    std::move(indexed_col_indices));
  auto* ptr = entry.get();
  _table_entries[table_name] = std::move(entry);
  _table_infos[table_name] = std::move(info);
  return ptr;
}

void SereneDBSchemaEntry::Scan(
  duckdb::ClientContext& context, duckdb::CatalogType type,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  if (type != duckdb::CatalogType::TABLE_ENTRY) {
    return;
  }
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto tables = snapshot->GetTables(
    snapshot->GetDatabases().front()->GetId(), "public");
  for (auto& table : tables) {
    auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
    info->table = table->GetName();
    info->schema = name;
    for (const auto& col : table->Columns()) {
      info->columns.AddColumn(duckdb::ColumnDefinition(
        std::string{col.name}, VeloxTypeToDuckDB(col.type)));
    }
    auto entry = duckdb::make_uniq<SereneDBTableEntry>(
      catalog, *this, *info, std::move(table));
    callback(*entry);
    // Keep alive
    _table_entries[info->table] = std::move(entry);
    _table_infos[info->table] = std::move(info);
  }
}

void SereneDBSchemaEntry::Scan(
  duckdb::CatalogType type,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  // Without context — just scan what we have cached
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateTable(
  duckdb::CatalogTransaction transaction,
  duckdb::BoundCreateTableInfo& info) {
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
    sdb_col.type = DuckDBTypeToVelox(col.Type());
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
  auto databases = snapshot->GetDatabases();
  SDB_ASSERT(!databases.empty());
  auto& database = *databases.front();

  // Create table options
  catalog::CreateTableOptions options;
  auto r = catalog::MakeTableOptions(
    std::move(request), database.GetId(), options,
    database.GetReplicationFactor(), database.GetWriteConcern(), false);
  if (!r.ok()) {
    throw duckdb::InvalidInputException("Failed to create table options: %s",
                                        std::string{r.errorMessage()});
  }

  bool if_not_exists =
    create_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  catalog::CreateTableOperationOptions op_options;

  r = catalog_impl.CreateTable(database.GetId(), "public", std::move(options),
                               op_options);
  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && if_not_exists) {
    return nullptr;
  }
  if (!r.ok()) {
    throw duckdb::InvalidInputException("Failed to create table: %s",
                                        std::string{r.errorMessage()});
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
  auto databases = snapshot->GetDatabases();
  SDB_ASSERT(!databases.empty());
  auto& database = *databases.front();

  // Map DuckDB index type to SereneDB IndexType
  // DuckDB default is empty or "ART"; PG default is "btree"
  IndexType index_type;
  auto idx_type_str = info.index_type;
  std::transform(idx_type_str.begin(), idx_type_str.end(), idx_type_str.begin(),
                 ::tolower);
  if (idx_type_str.empty() || idx_type_str == "art" ||
      idx_type_str == "btree" || idx_type_str == "secondary") {
    index_type = IndexType::Secondary;
  } else if (idx_type_str == "inverted") {
    index_type = IndexType::Inverted;
  } else {
    throw duckdb::CatalogException("access method \"%s\" does not exist",
                                   info.index_type);
  }

  // Build CreateIndexColumn vector from DuckDB info.
  // At bind time, column_ids may not be populated yet — use names/expressions.
  const auto& columns = sdb_table->Columns();
  std::vector<catalog::CreateIndexColumn> idx_columns;

  if (!info.names.empty()) {
    // Use column names (available from parser)
    for (const auto& col_name : info.names) {
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
    }
  } else if (!info.column_ids.empty()) {
    // Fallback: use column IDs if available
    for (auto col_id : info.column_ids) {
      const catalog::Column* cat_col = nullptr;
      for (const auto& col : columns) {
        if (col.id == static_cast<catalog::Column::Id>(col_id)) {
          cat_col = &col;
          break;
        }
      }
      if (!cat_col) {
        throw duckdb::CatalogException("column index %d not found in table",
                                       static_cast<int>(col_id));
      }
      idx_columns.push_back(catalog::CreateIndexColumn{
        .catalog_column = cat_col,
        .name = cat_col->name,
      });
    }
  } else {
    // Try parsed_expressions — extract column names from ColumnRefExpression
    for (auto& expr : info.parsed_expressions) {
      if (expr->GetExpressionType() ==
          duckdb::ExpressionType::COLUMN_REF) {
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
  }

  // Build index options
  catalog::IndexBaseOptions options;
  options.name = info.index_name;
  options.type = index_type;

  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  // Create index based on type
  Result create_result;
  if (index_type == IndexType::Inverted) {
    search::InvertedIndexShardOptions shard_options;
    // Parse WITH options if provided
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
      shard_options.base.cleanup_interval_step =
        it->second.GetValue<int64_t>();
    }
    create_result = catalog_impl.CreateIndex(
      database.GetId(), "public", sdb_table->GetName(),
      std::move(idx_columns), std::move(options), shard_options,
      {.create_with_tombstone = false});
  } else {
    SecondaryIndexShardOptions shard_options;
    shard_options.base.unique =
      (info.constraint_type == duckdb::IndexConstraintType::UNIQUE);
    create_result = catalog_impl.CreateIndex(
      database.GetId(), "public", sdb_table->GetName(),
      std::move(idx_columns), std::move(options), shard_options,
      {.create_with_tombstone = false});
  }

  if (create_result.is(ERROR_SERVER_DUPLICATE_NAME) && if_not_exists) {
    return nullptr;
  }
  if (!create_result.ok()) {
    throw duckdb::CatalogException("Failed to create index: %s",
                                   std::string{create_result.errorMessage()});
  }

  // Start background tasks for inverted indexes
  auto new_snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_index =
    new_snapshot->GetRelation(database.GetId(), "public", info.index_name);
  if (catalog_index) {
    auto shard = new_snapshot->GetIndexShard(catalog_index->GetId());
    if (shard && shard->GetType() == IndexType::Inverted) {
      auto& inverted_shard =
        basics::downCast<search::InvertedIndexShard>(*shard);
      inverted_shard.StartTasks();
    }
  }

  // Invalidate cached table entries so they pick up new indexed columns
  _table_entries.erase(std::string{sdb_table->GetName()});

  std::cerr << "SereneDB: Created index " << info.index_name << " on "
            << sdb_table->GetName() << " via DuckDB" << std::endl;
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateFunctionInfo& info) {
  throw duckdb::NotImplementedException("CREATE FUNCTION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateView(
  duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo& info) {
  throw duckdb::NotImplementedException("CREATE VIEW through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateSequence(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateSequenceInfo& info) {
  throw duckdb::NotImplementedException("CREATE SEQUENCE through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreateTableFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateTableFunctionInfo& info) {
  throw duckdb::NotImplementedException("CREATE TABLE FUNCTION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreateCopyFunction(
  duckdb::CatalogTransaction transaction,
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

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreateCollation(duckdb::CatalogTransaction transaction,
                                     duckdb::CreateCollationInfo& info) {
  throw duckdb::NotImplementedException("CREATE COLLATION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateType(
  duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo& info) {
  throw duckdb::NotImplementedException("CREATE TYPE through DuckDB");
}

void SereneDBSchemaEntry::DropEntry(duckdb::ClientContext& context,
                                    duckdb::DropInfo& info) {
  if (info.type != duckdb::CatalogType::TABLE_ENTRY) {
    throw duckdb::NotImplementedException("DROP for type %s not supported",
                                          duckdb::CatalogTypeToString(info.type));
  }

  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto databases = snapshot->GetDatabases();
  SDB_ASSERT(!databases.empty());

  auto r = catalog_impl.DropTable(databases.front()->GetId(), "public",
                                  info.name);
  bool if_exists =
    info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL;
  if (r.is(ERROR_SERVER_DATABASE_NOT_FOUND) && if_exists) {
    return;
  }
  if (!r.ok()) {
    throw duckdb::InvalidInputException("Failed to drop table: %s",
                                        std::string{r.errorMessage()});
  }

  // Remove from our cache
  _table_entries.erase(info.name);
  _table_infos.erase(info.name);

  std::cerr << "SereneDB: Dropped table " << info.name << " via DuckDB"
            << std::endl;
}

void SereneDBSchemaEntry::Alter(duckdb::CatalogTransaction transaction,
                                duckdb::AlterInfo& info) {
  throw duckdb::NotImplementedException("ALTER through DuckDB");
}

}  // namespace sdb::connector
