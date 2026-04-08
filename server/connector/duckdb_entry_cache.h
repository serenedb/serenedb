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

#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <shared_mutex>

#include "basics/containers/node_hash_map.h"
#include "catalog/catalog.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_table_entry.h"

namespace sdb::connector {

// Lazy cache of DuckDB CatalogEntry objects, stored on a SereneDB Snapshot.
// Thread-safe: shared_mutex for concurrent reads, exclusive for writes.
// Automatically cleaned up when the snapshot is released.
class DuckDBEntryCache {
 public:
  // Schema lookup/creation
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> GetOrCreateSchema(
    duckdb::Catalog& catalog, ObjectId db_id, std::string_view schema_name,
    const catalog::Snapshot& snapshot);

  void ScanSchemas(
    duckdb::Catalog& catalog, ObjectId db_id,
    const std::function<void(duckdb::SchemaCatalogEntry&)>& callback,
    const catalog::Snapshot& snapshot);

  // Table/function/view lookup (within a schema)
  duckdb::optional_ptr<duckdb::CatalogEntry> GetOrCreateEntry(
    duckdb::CatalogType type,
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    ObjectId db_id, std::string_view schema_name, std::string_view name,
    const catalog::Snapshot& snapshot);

  void ScanEntries(
    duckdb::CatalogType type,
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    ObjectId db_id, std::string_view schema_name,
    const std::function<void(duckdb::CatalogEntry&)>& callback,
    const catalog::Snapshot& snapshot);

 private:
  duckdb::optional_ptr<duckdb::CatalogEntry> BuildTableEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    ObjectId db_id, std::string_view schema_name, std::string_view table_name,
    const catalog::Snapshot& snapshot);

  duckdb::optional_ptr<duckdb::CatalogEntry> BuildViewEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    ObjectId db_id, std::string_view schema_name, std::string_view view_name,
    const catalog::Snapshot& snapshot);

  duckdb::optional_ptr<duckdb::CatalogEntry> BuildFunctionEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    ObjectId db_id, std::string_view schema_name, std::string_view func_name,
    const catalog::Snapshot& snapshot);

  mutable std::shared_mutex _lock;

  // Schema entries
  containers::NodeHashMap<std::string,
                          duckdb::unique_ptr<SereneDBSchemaEntry>>
    _schemas;
  containers::NodeHashMap<std::string,
                          duckdb::unique_ptr<duckdb::CreateSchemaInfo>>
    _schema_infos;

  // Per-schema entry caches
  struct SchemaEntryCache {
    duckdb::case_insensitive_map_t<duckdb::unique_ptr<SereneDBTableEntry>>
      tables;
    duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CreateTableInfo>>
      table_infos;
    duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ViewCatalogEntry>>
      views;
    duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CreateViewInfo>>
      view_infos;
    duckdb::case_insensitive_map_t<
      duckdb::unique_ptr<duckdb::ScalarMacroCatalogEntry>>
      functions;
    duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CreateMacroInfo>>
      function_infos;
  };
  containers::NodeHashMap<std::string, SchemaEntryCache> _entry_caches;
};

}  // namespace sdb::connector
