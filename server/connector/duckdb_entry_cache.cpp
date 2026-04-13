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

#include "connector/duckdb_entry_cache.h"

#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/create_statement.hpp>

#include "basics/down_cast.h"
#include "catalog/function.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/secondary_index.h"
#include "catalog/view.h"
#include "connector/duckdb_table_entry.h"

namespace sdb::connector {

// --- Schema ---

duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
DuckDBEntryCache::GetOrCreateSchema(duckdb::Catalog& catalog, ObjectId db_id,
                                    std::string_view schema_name,
                                    const catalog::Snapshot& snapshot) {
  // Verify schema exists in snapshot
  auto schema = snapshot.GetSchema(db_id, schema_name);
  if (!schema) {
    return nullptr;
  }

  auto key = std::string{schema_name};
  {
    std::shared_lock lock{_lock};
    auto it = _schemas.find(key);
    if (it != _schemas.end()) {
      return it->second.get();
    }
  }

  std::unique_lock lock{_lock};
  auto it = _schemas.find(key);
  if (it != _schemas.end()) {
    return it->second.get();
  }
  auto info = duckdb::make_uniq<duckdb::CreateSchemaInfo>();
  info->schema = std::string{schema_name};
  auto entry = duckdb::make_uniq<SereneDBSchemaEntry>(catalog, *info);
  auto* ptr = entry.get();
  _schemas[key] = std::move(entry);
  _schema_infos[key] = std::move(info);
  return ptr;
}

void DuckDBEntryCache::ScanSchemas(
  duckdb::Catalog& catalog, ObjectId db_id,
  const std::function<void(duckdb::SchemaCatalogEntry&)>& callback,
  const catalog::Snapshot& snapshot) {
  if (!snapshot.GetDatabase(db_id)) {
    return;
  }
  auto schemas = snapshot.GetSchemas(db_id);
  for (auto& schema : schemas) {
    auto entry = GetOrCreateSchema(catalog, db_id, schema->GetName(), snapshot);
    if (entry) {
      callback(*entry);
    }
  }
}

// --- Table ---

duckdb::optional_ptr<duckdb::CatalogEntry> DuckDBEntryCache::BuildTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
  std::string_view schema_name, std::string_view table_name,
  const catalog::Snapshot& snapshot) {
  auto table = snapshot.GetTable(db_id, schema_name, table_name);
  std::shared_ptr<const catalog::InvertedIndex> inverted_index;
  ObjectId sk_shard_id;
  bool sk_unique = false;

  if (!table) {
    // Not a table -- try resolving as an index name.
    // Enables SELECT * FROM index_name WHERE phrase(col, 'text')
    auto relation = snapshot.GetRelation(db_id, schema_name, table_name);
    if (relation &&
        (relation->GetType() == catalog::ObjectType::SecondaryIndex ||
         relation->GetType() == catalog::ObjectType::InvertedIndex)) {
      auto& index = basics::downCast<const catalog::Index>(*relation);
      table = snapshot.GetObject<catalog::Table>(index.GetRelationId());
      if (relation->GetType() == catalog::ObjectType::InvertedIndex) {
        inverted_index =
          std::dynamic_pointer_cast<const catalog::InvertedIndex>(relation);
      } else {
        // Secondary index -- find shard ID for scanning
        auto& sec_index =
          basics::downCast<const catalog::SecondaryIndex>(index);
        sk_unique = sec_index.IsUnique();
        for (auto& shard : snapshot.GetIndexShardsByTable(table->GetId())) {
          if (shard->GetIndexId() == index.GetId()) {
            sk_shard_id = shard->GetId();
            break;
          }
        }
      }
    }
    if (!table) {
      return nullptr;
    }
  }

  auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
  info->table = table_name;
  info->schema = schema.name;
  for (const auto& col : table->Columns()) {
    auto cd = duckdb::ColumnDefinition(col.name, col.type);
    if (col.IsGenerated() && col.expr && col.expr->HasExpr()) {
      cd.SetGeneratedExpression(col.expr->GetExpr().Copy());
    } else if (col.expr && col.expr->HasExpr()) {
      cd.SetDefaultValue(col.expr->GetExpr().Copy());
    }
    info->columns.AddColumn(std::move(cd));
  }

  // PK constraint
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

  // Indexed columns
  std::vector<size_t> indexed_col_indices;
  {
    auto indexes = snapshot.GetIndexesByTable(table->GetId());
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

  auto key = std::string{table_name};
  auto& cache = _entry_caches[std::string{schema_name}];
  auto entry = duckdb::make_uniq<SereneDBTableEntry>(
    catalog, schema, *info, std::move(table), std::move(indexed_col_indices),
    std::move(inverted_index));
  if (sk_shard_id != ObjectId{}) {
    entry->SetSecondaryIndex(sk_shard_id, sk_unique);
  }
  auto* ptr = entry.get();
  cache.tables[key] = std::move(entry);
  cache.table_infos[key] = std::move(info);
  return ptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> DuckDBEntryCache::GetOrCreateEntry(
  duckdb::CatalogType type, duckdb::Catalog& catalog,
  duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
  std::string_view schema_name, std::string_view name,
  const catalog::Snapshot& snapshot) {
  auto key = std::string{name};

  if (type == duckdb::CatalogType::TABLE_ENTRY) {
    {
      std::shared_lock lock{_lock};
      auto schema_it = _entry_caches.find(schema_name);
      if (schema_it != _entry_caches.end()) {
        auto it = schema_it->second.tables.find(key);
        if (it != schema_it->second.tables.end()) {
          return it->second.get();
        }
      }
    }
    std::unique_lock lock{_lock};
    auto schema_it = _entry_caches.find(schema_name);
    if (schema_it != _entry_caches.end()) {
      auto it = schema_it->second.tables.find(key);
      if (it != schema_it->second.tables.end()) {
        return it->second.get();
      }
    }
    auto result =
      BuildTableEntry(catalog, schema, db_id, schema_name, name, snapshot);
    if (result) {
      return result;
    }
    // Table not found -- try views (DuckDB looks up views as TABLE_ENTRY)
    return BuildViewEntry(catalog, schema, db_id, schema_name, name, snapshot);
  }

  if (type == duckdb::CatalogType::VIEW_ENTRY) {
    {
      std::shared_lock lock{_lock};
      auto schema_it = _entry_caches.find(schema_name);
      if (schema_it != _entry_caches.end()) {
        auto it = schema_it->second.views.find(key);
        if (it != schema_it->second.views.end()) {
          return it->second.get();
        }
      }
    }
    std::unique_lock lock{_lock};
    auto schema_it = _entry_caches.find(schema_name);
    if (schema_it != _entry_caches.end()) {
      auto it = schema_it->second.views.find(key);
      if (it != schema_it->second.views.end()) {
        return it->second.get();
      }
    }
    return BuildViewEntry(catalog, schema, db_id, schema_name, name, snapshot);
  }

  // Function/macro lookup -- match by stored macro type
  if (type == duckdb::CatalogType::TABLE_FUNCTION_ENTRY ||
      type == duckdb::CatalogType::MACRO_ENTRY ||
      type == duckdb::CatalogType::TABLE_MACRO_ENTRY ||
      type == duckdb::CatalogType::SCALAR_FUNCTION_ENTRY) {
    auto result =
      BuildFunctionEntry(catalog, schema, db_id, schema_name, name, snapshot);
    if (!result) {
      return nullptr;
    }
    // TABLE_FUNCTION_ENTRY lookup: only return TABLE_FUNCTION or TABLE_MACRO
    // entries
    if (type == duckdb::CatalogType::TABLE_FUNCTION_ENTRY) {
      if (result->type == duckdb::CatalogType::TABLE_FUNCTION_ENTRY ||
          result->type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
        return result;
      }
      return nullptr;
    }
    // SCALAR_FUNCTION_ENTRY / MACRO_ENTRY lookup: only return scalar entries
    if (type == duckdb::CatalogType::SCALAR_FUNCTION_ENTRY ||
        type == duckdb::CatalogType::MACRO_ENTRY) {
      if (result->type == duckdb::CatalogType::SCALAR_FUNCTION_ENTRY ||
          result->type == duckdb::CatalogType::MACRO_ENTRY) {
        return result;
      }
      return nullptr;
    }
    // TABLE_MACRO_ENTRY lookup: only return table entries
    if (result->type == duckdb::CatalogType::TABLE_FUNCTION_ENTRY ||
        result->type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
      return result;
    }
    return nullptr;
  }

  return nullptr;
}

void DuckDBEntryCache::ScanEntries(
  duckdb::CatalogType type, duckdb::Catalog& catalog,
  duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
  std::string_view schema_name,
  const std::function<void(duckdb::CatalogEntry&)>& callback,
  const catalog::Snapshot& snapshot) {
  if (type != duckdb::CatalogType::TABLE_ENTRY) {
    return;
  }

  auto tables = snapshot.GetTables(db_id, schema_name);
  std::unique_lock lock{_lock};
  auto& cache = _entry_caches[std::string{schema_name}];

  for (auto& sdb_table : tables) {
    auto table_name = std::string{sdb_table->GetName()};
    auto it = cache.tables.find(table_name);
    if (it != cache.tables.end()) {
      callback(*it->second);
      continue;
    }
    // Build and cache
    auto entry = BuildTableEntry(catalog, schema, db_id, schema_name,
                                 table_name, snapshot);
    if (entry) {
      callback(*entry);
    }
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> DuckDBEntryCache::BuildViewEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
  std::string_view schema_name, std::string_view view_name,
  const catalog::Snapshot& snapshot) {
  // Look up the view in the snapshot -- it's a SchemaObject (View subclass)
  auto relation = snapshot.GetRelation(db_id, schema_name, view_name);
  if (!relation || relation->GetType() != catalog::ObjectType::PgSqlView) {
    return nullptr;
  }

  // Get the full CreateViewInfo from the stored view
  auto& sql_view = basics::downCast<const catalog::PgSqlView>(*relation);
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      sql_view.GetInfo().Copy());
  info->schema = std::string{schema_name};
  info->temporary = true;
  info->internal = false;

  auto key = std::string{view_name};
  auto& cache = _entry_caches[std::string{schema_name}];
  auto entry =
    duckdb::make_uniq<duckdb::ViewCatalogEntry>(catalog, schema, *info);
  auto* ptr = entry.get();
  cache.views[key] = std::move(entry);
  cache.view_infos[key] = std::move(info);
  return ptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> DuckDBEntryCache::BuildFunctionEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
  std::string_view schema_name, std::string_view func_name,
  const catalog::Snapshot& snapshot) {
  auto function = snapshot.GetFunction(db_id, schema_name, func_name);
  if (!function) {
    return nullptr;
  }

  // Get the full CreateMacroInfo from the stored function
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      function->GetInfo().Copy());
  info->schema = std::string{schema_name};
  info->name = std::string{func_name};

  // Resolve unbound parameter types (same as DefaultFunctionGenerator)
  for (auto& macro : info->macros) {
    for (auto& type : macro->types) {
      if (type.IsUnbound()) {
        type = duckdb::UnboundType::TryDefaultBind(type);
      }
    }
  }

  auto key = std::string{func_name};
  auto& cache = _entry_caches[std::string{schema_name}];
  duckdb::unique_ptr<duckdb::CatalogEntry> entry;
  if (info->type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
    entry = duckdb::unique_ptr_cast<duckdb::TableMacroCatalogEntry,
                                    duckdb::CatalogEntry>(
      duckdb::make_uniq<duckdb::TableMacroCatalogEntry>(catalog, schema,
                                                        *info));
  } else {
    entry = duckdb::unique_ptr_cast<duckdb::ScalarMacroCatalogEntry,
                                    duckdb::CatalogEntry>(
      duckdb::make_uniq<duckdb::ScalarMacroCatalogEntry>(catalog, schema,
                                                         *info));
  }
  auto* ptr = entry.get();
  cache.functions[key] = std::move(entry);
  cache.function_infos[key] = std::move(info);
  return ptr;
}

}  // namespace sdb::connector
