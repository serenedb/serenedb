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

#include "catalog1/lookup.h"

#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database.hpp>
#include <functional>

namespace sdb::catalog {
namespace {

// duckdb already draws the line the nullable context needs: the ClientContext
// overloads see the caller's uncommitted changes, the bare ones see committed
// state only. Callers with a context should use those directly; these exist
// only so the by-id scans below can serve the sessionless paths too.
void ScanSchemas(duckdb::ClientContext* context, duckdb::Catalog& catalog,
                 const std::function<void(duckdb::SchemaCatalogEntry&)>& fn) {
  if (context != nullptr) {
    catalog.ScanSchemas(*context, fn);
    return;
  }
  catalog.Cast<duckdb::DuckCatalog>().ScanSchemas(fn);
}

void ScanEntries(duckdb::ClientContext* context, duckdb::Catalog& catalog,
                 duckdb::CatalogType type,
                 const std::function<void(duckdb::CatalogEntry&)>& fn) {
  ScanSchemas(context, catalog, [&](duckdb::SchemaCatalogEntry& schema) {
    if (context != nullptr) {
      schema.Scan(*context, type, fn);
      return;
    }
    schema.Scan(type, fn);
  });
}

}  // namespace

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> FindSchemaById(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, duckdb::idx_t id) {
  if (id == 0) {
    return nullptr;
  }
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> result;
  ScanSchemas(context, catalog, [&](duckdb::SchemaCatalogEntry& schema) {
    if (!result && schema.oid == id) {
      result = &schema;
    }
  });
  return result;
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindEntryById(
  duckdb::ClientContext* context, duckdb::Catalog& catalog,
  duckdb::CatalogType type, duckdb::idx_t id) {
  if (id == 0) {
    return nullptr;
  }
  duckdb::optional_ptr<duckdb::CatalogEntry> result;
  ScanEntries(context, catalog, type, [&](duckdb::CatalogEntry& entry) {
    if (!result && entry.oid == id) {
      result = &entry;
    }
  });
  return result;
}

}  // namespace sdb::catalog
