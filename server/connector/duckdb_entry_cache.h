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

#pragma once

#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <shared_mutex>

#include "basics/containers/node_hash_map.h"
#include "catalog/catalog.h"
#include "connector/duckdb_schema_entry.h"

namespace sdb::connector {

// Hierarchical cache of DuckDB CatalogEntry objects.
// Structure: databases[db_id] -> schemas[name] -> entries[name].
// Lives on SnapshotImpl. Thread-safe via shared_mutex.
class DuckDBEntryCache {
 public:
  // Called by SereneDBCatalog::LookupSchema
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> EnsureSchema(
    duckdb::Catalog& catalog, ObjectId db_id, std::string_view schema_name,
    const catalog::Snapshot& snapshot);

  // Called by SereneDBCatalog::ScanSchemas
  void ScanSchemas(
    duckdb::Catalog& catalog, ObjectId db_id,
    const std::function<void(duckdb::SchemaCatalogEntry&)>& callback,
    const catalog::Snapshot& snapshot);

  // Called by SereneDBSchemaEntry::LookupEntry
  duckdb::optional_ptr<duckdb::CatalogEntry> EnsureEntry(
    duckdb::CatalogType type, duckdb::Catalog& catalog,
    duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
    std::string_view schema_name, std::string_view name,
    const catalog::Snapshot& snapshot);

  // Called by SereneDBSchemaEntry::Scan
  void ScanEntries(duckdb::CatalogType type, duckdb::Catalog& catalog,
                   duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
                   std::string_view schema_name,
                   const std::function<void(duckdb::CatalogEntry&)>& callback,
                   const catalog::Snapshot& snapshot);

 private:
  duckdb::unique_ptr<duckdb::CatalogEntry> BuildEntry(
    duckdb::CatalogType type, duckdb::Catalog& catalog,
    duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
    std::string_view schema_name, std::string_view name,
    const catalog::Snapshot& snapshot);

  duckdb::unique_ptr<duckdb::CatalogEntry> BuildTableEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    ObjectId db_id, std::string_view schema_name, std::string_view table_name,
    const catalog::Snapshot& snapshot);

  struct SchemaCache {
    SchemaCache(duckdb::Catalog& catalog, std::string_view schema_name)
      : info{[&] {
          duckdb::CreateSchemaInfo i;
          i.schema = schema_name;
          return i;
        }()},
        entry{catalog, info} {}

    duckdb::CreateSchemaInfo info;
    SereneDBSchemaEntry entry;
    // Separate maps for relations (tables/views/indexes) and functions
    // because PG allows same name for both (e.g. pg_available_extensions
    // is both a view and a table function).
    containers::NodeHashMap<std::string,
                            duckdb::unique_ptr<duckdb::CatalogEntry>>
      relations;  // TABLE_ENTRY, VIEW_ENTRY, INDEX_ENTRY
    containers::NodeHashMap<std::string,
                            duckdb::unique_ptr<duckdb::CatalogEntry>>
      functions;  // MACRO_ENTRY, TABLE_MACRO_ENTRY, *_FUNCTION_ENTRY
  };

  struct DatabaseCache {
    containers::NodeHashMap<std::string, SchemaCache> schemas;
  };

  mutable std::shared_mutex _lock;
  containers::NodeHashMap<ObjectId, DatabaseCache> _databases;
};

}  // namespace sdb::connector
