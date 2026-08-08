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

#include "catalog/duckdb_object_index.h"

#include <cstdint>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <string>

#include "catalog/duckdb_catalog.h"
#include "catalog/duckdb_schema_entry.h"
#include "catalog/store/store.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kHexDigits = "0123456789abcdef";
constexpr size_t kIdWidth = 16;

}  // namespace

duckdb::Identifier ObjectIndexName(ObjectId id) noexcept {
  std::string name(kIdWidth, '0');
  auto value = id.id();
  for (size_t i = 0; i < kIdWidth; ++i) {
    name[kIdWidth - 1 - i] = kHexDigits[value & 0xF];
    value >>= 4;
  }
  return duckdb::Identifier{std::move(name)};
}

void SetObjectLocation(duckdb::CatalogTransaction transaction,
                       duckdb::CatalogSet& index, duckdb::Catalog& catalog,
                       ObjectId id, const ObjectLocation* location) {
  auto name = ObjectIndexName(id);
  auto existing = index.GetEntry(transaction, name);
  if (existing != nullptr) {
    const auto& held = existing->Cast<SereneDBObjectIndexEntry>().Location();
    if (location != nullptr && held == *location) {
      return;
    }
    (void)index.DropEntry(transaction, name, /*cascade=*/false);
  }
  if (location == nullptr) {
    return;
  }
  auto entry = duckdb::make_uniq<SereneDBObjectIndexEntry>(
    catalog, duckdb::Identifier{name}, *location);
  (void)index.CreateEntry(transaction, std::move(name), std::move(entry),
                          duckdb::LogicalDependencyList{});
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::CatalogTransaction transaction, SereneDBCatalog& catalog,
  ObjectId id) {
  if (!id.isSet()) {
    return nullptr;
  }
  auto located =
    catalog.GetObjectIndexSet().GetEntry(transaction, ObjectIndexName(id));
  if (located == nullptr) {
    return nullptr;
  }
  const auto& at = located->Cast<SereneDBObjectIndexEntry>().Location();
  duckdb::optional_ptr<duckdb::CatalogSet> set;
  if (at.schema.GetIdentifierName().empty()) {
    set = &catalog.GetForeignServerSet();
  } else if (auto schema = catalog.TryGetSchemaEntry(
               transaction, at.schema.GetIdentifierName())) {
    set = &schema->GetCatalogSet(at.slot);
  }
  if (!set) {
    return nullptr;
  }
  auto entry = set->GetEntry(transaction, at.name);
  // The location is a version of its own: a reader whose view predates a rename
  // finds the old location and the entry still under it, and one whose view
  // predates the create finds neither. An entry under the name that is not this
  // object cannot be answered with.
  if (entry == nullptr || entry->oid != id.id()) {
    return nullptr;
  }
  return entry;
}

namespace {

duckdb::optional_ptr<SereneDBCatalog> DatabaseCatalog(
  duckdb::ClientContext& context, ObjectId database_id) {
  auto attached = catalog::TryStoreDatabase(context, database_id);
  if (!attached) {
    return nullptr;
  }
  auto& duck_catalog = attached->GetCatalog();
  if (duck_catalog.GetCatalogType() != kSereneDBCatalogType) {
    return nullptr;
  }
  return &duck_catalog.Cast<SereneDBCatalog>();
}

}  // namespace

duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::ClientContext& context, ObjectId database_id, ObjectId id) {
  auto catalog = DatabaseCatalog(context, database_id);
  if (!catalog) {
    return nullptr;
  }
  return LookupEntryById(catalog->GetCatalogTransaction(context), *catalog, id);
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::ClientContext& context, ObjectId id) {
  return LookupEntryById(
    context, connector::GetSereneDBContext(context).GetDatabaseId(), id);
}

}  // namespace sdb::catalog
