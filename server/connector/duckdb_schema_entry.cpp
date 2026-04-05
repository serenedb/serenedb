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
#include <duckdb/parser/parsed_data/create_table_info.hpp>

#include "catalog/catalog.h"
#include "catalog/table.h"
#include "connector/duckdb_table_entry.h"

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

  // Create and cache the table entry
  // TODO: Cache these instead of recreating each time
  auto entry = duckdb::make_uniq<SereneDBTableEntry>(
    catalog, *this, *info, std::move(table));
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
  throw duckdb::NotImplementedException(
    "CREATE TABLE through DuckDB not yet supported — use SereneDB DDL path");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateIndex(
  duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
  duckdb::TableCatalogEntry& table) {
  throw duckdb::NotImplementedException("CREATE INDEX through DuckDB");
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
  throw duckdb::NotImplementedException("DROP through DuckDB");
}

void SereneDBSchemaEntry::Alter(duckdb::CatalogTransaction transaction,
                                duckdb::AlterInfo& info) {
  throw duckdb::NotImplementedException("ALTER through DuckDB");
}

}  // namespace sdb::connector
