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

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/optional_ptr.hpp>
#include <utility>

#include "catalog/identifiers/object_id.h"

namespace duckdb {

class CatalogSet;
class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

class SereneDBCatalog;

// Where the entry of one object currently sits: the schema it is under and the
// name it answers to, plus the set that name is keyed in. Empty schema for a
// foreign server, which is a database child rather than a schema child.
struct ObjectLocation {
  duckdb::Identifier schema;
  duckdb::Identifier name;
  duckdb::CatalogType slot{duckdb::CatalogType::INVALID};

  bool operator==(const ObjectLocation& other) const noexcept {
    return slot == other.slot && name == other.name && schema == other.schema;
  }
};

// One object's location, filed under the object's own stable id.
//
// An entry rather than a side map for the reason the dependency edges are one:
// the location changes with a rename, and a rename is a transaction's to see
// before it commits and nobody else's. CatalogSet's MVCC is what makes that
// true here as it is everywhere else, and a rollback drops the version with no
// bookkeeping of ours.
//
// One entry per object, so two DDLs on unrelated objects never meet in this set
// either.
class SereneDBObjectIndexEntry final : public duckdb::InCatalogEntry {
 public:
  SereneDBObjectIndexEntry(duckdb::Catalog& catalog, duckdb::Identifier name,
                           ObjectLocation location)
    : duckdb::InCatalogEntry(duckdb::CatalogType::OBJECT_INDEX_ENTRY, catalog,
                             std::move(name)),
      _location{std::move(location)} {
    // Derived from the definitions at boot, so duckdb must neither write nor
    // checkpoint it.
    duck_managed = false;
  }

  const ObjectLocation& Location() const noexcept { return _location; }

 private:
  ObjectLocation _location;
};

// The name one object's location is filed under: fixed-width lowercase hex of
// the id, so the ordered set is keyed on identity alone.
duckdb::Identifier ObjectIndexName(ObjectId id) noexcept;

// Puts `id` at `location`, or removes it when the object is gone. A no-op when
// the set already says the same thing -- an ALTER that leaves the name alone
// must not chain a version here and turn an unrelated concurrent DDL into a
// conflict.
void SetObjectLocation(duckdb::CatalogTransaction transaction,
                       duckdb::CatalogSet& index, duckdb::Catalog& catalog,
                       ObjectId id, const ObjectLocation* location);

// The entry `id` names in `catalog`, or null when this database holds no such
// object. One lookup in the index set followed by one in the set the location
// points at, both read through `transaction`.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::CatalogTransaction transaction, SereneDBCatalog& catalog,
  ObjectId id);

// The same for a caller that has only a client context: the object is looked up
// in the database the session is connected to.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::ClientContext& context, ObjectId id);

// And for a caller that knows which database to ask.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::ClientContext& context, ObjectId database_id, ObjectId id);

}  // namespace sdb::catalog
