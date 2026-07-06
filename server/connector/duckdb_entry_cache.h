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
#include <memory>

#include "basics/containers/node_hash_map.h"
#include "catalog/fwd.h"
#include "catalog/object.h"
#include "connector/duckdb_schema_entry.h"

namespace sdb::connector {

// Hierarchical cache of DuckDB CatalogEntry objects.
// Structure: databases[db_id] -> schemas[name] -> entries[name].
// Lives on Snapshot. Thread-safe.
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

  // A resolved entry plus the ACL-bearing object behind it (null for builtins),
  // so callers can enforce per-role privileges without a second lookup.
  struct EntryLookup {
    duckdb::optional_ptr<duckdb::CatalogEntry> entry;
    std::shared_ptr<const catalog::Object> object;
  };

  // Called by SereneDBSchemaEntry::LookupEntry.
  EntryLookup EnsureEntry(duckdb::CatalogType type, duckdb::Catalog& catalog,
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
  // A built DuckDB entry plus the ACL-bearing object it wraps (null for
  // builtins); the object is role-independent, so caching it is safe.
  struct CachedEntry {
    duckdb::unique_ptr<duckdb::CatalogEntry> entry;
    std::shared_ptr<const catalog::Object> object;
  };

  CachedEntry BuildEntry(duckdb::CatalogType type, duckdb::Catalog& catalog,
                         duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
                         std::string_view schema_name, std::string_view name,
                         const catalog::Snapshot& snapshot);

  // Builds the DuckDB entry and, when it wraps one of our catalog objects,
  // sets `object` to the ACL-bearing object behind it.
  duckdb::unique_ptr<duckdb::CatalogEntry> BuildEntryObject(
    duckdb::CatalogType type, duckdb::Catalog& catalog,
    duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
    std::string_view schema_name, std::string_view name,
    const catalog::Snapshot& snapshot,
    std::shared_ptr<const catalog::Object>& object);

  duckdb::unique_ptr<duckdb::CatalogEntry> BuildTableEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    ObjectId db_id, std::string_view schema_name, std::string_view table_name,
    const catalog::Snapshot& snapshot);

  // Build a SereneDBIndexScanEntry for the "index name as table" pattern
  // (SELECT * FROM idx_name). The CatalogType requested is TABLE_ENTRY but
  // the resolved relation is an Index -- we wrap it as a scannable entry.
  duckdb::unique_ptr<duckdb::CatalogEntry> BuildIndexScanEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    ObjectId db_id, std::string_view schema_name, std::string_view name,
    const catalog::Index& index, const catalog::Snapshot& snapshot);

  struct SchemaCache {
    SchemaCache(duckdb::Catalog& catalog, std::string_view schema_name)
      : info{[&] {
          duckdb::CreateSchemaInfo i;
          i.SetSchema(duckdb::Identifier{schema_name});
          return i;
        }()},
        entry{catalog, info} {}

    duckdb::CreateSchemaInfo info;
    SereneDBSchemaEntry entry;
    // Separate maps per entry-type group because:
    //  - PG allows the same name for a relation and a function (e.g.
    //    pg_available_extensions is both a view and a table function), and
    //  - an inverted/secondary index legitimately maps to two different
    //    catalog entries under the same name: a SereneDBIndexScanEntry
    //    (TABLE_ENTRY, the "FROM idx_name" scan wrapper) and a
    //    SereneDBIndexEntry (INDEX_ENTRY, for DROP INDEX / duckdb_indexes).
    //
    // Within a group, names are unique across the constituent CatalogTypes
    // (PG semantics: a name can't be both a scalar function and a table
    // function in the same schema). Consequently EnsureEntry uses the
    // requested CatalogType only to pick the bucket -- the returned entry's
    // `type` is the canonical one and may not equal the requested type
    // (e.g. asking for SCALAR_FUNCTION_ENTRY on a procedure returns the
    // TABLE_MACRO_ENTRY). Callers must inspect `entry.type` rather than
    // assuming a match; see ExpressionBinder::BindFunction's switch in
    // duckdb/src/planner/binder/expression/bind_function_expression.cpp.
    using EntryMap = containers::FlatHashMap<std::string, CachedEntry>;
    EntryMap tables;     // TABLE_ENTRY, VIEW_ENTRY
    EntryMap indexes;    // INDEX_ENTRY
    EntryMap sequences;  // SEQUENCE_ENTRY
    EntryMap types;      // TYPE_ENTRY
    EntryMap common;

    auto& MapForType(this auto& self, duckdb::CatalogType t) {
      switch (t) {
        case duckdb::CatalogType::TABLE_ENTRY:
        case duckdb::CatalogType::VIEW_ENTRY:
          return self.tables;
        case duckdb::CatalogType::INDEX_ENTRY:
          return self.indexes;
        case duckdb::CatalogType::SEQUENCE_ENTRY:
          return self.sequences;
        case duckdb::CatalogType::TYPE_ENTRY:
          return self.types;
        default:
          return self.common;
      }
    }
  };

  struct DatabaseCache {
    containers::NodeHashMap<std::string, SchemaCache> schemas;
  };

  mutable absl::Mutex _lock;
  containers::NodeHashMap<ObjectId, DatabaseCache> _databases;
};

}  // namespace sdb::connector
