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

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <string>
#include <string_view>

namespace sdb::catalog {

// Roles and the database list belong to no single database, so they get an
// attachment of their own instead of riding whichever database the statement
// happens to run in. Storage-less on purpose: the attach hands duckdb an empty
// path, which gives the attachment an in-memory block manager and no data file
// or data WAL, so its objects stay durable through the one catalog WAL that
// every database's metadata already goes to.
inline constexpr std::string_view kGlobalDatabaseName = "__sdb_global";
inline constexpr std::string_view kGlobalStorageType = "serenedb_global";

class SereneDBGlobalCatalog final : public duckdb::DuckCatalog {
 public:
  explicit SereneDBGlobalCatalog(duckdb::AttachedDatabase& db);

  // Storage-extension key, not DuckCatalog's "duckdb".
  std::string GetCatalogType() final { return std::string{kGlobalStorageType}; }

  // Unquoted identifiers are folded at parse time and then matched exactly, as
  // postgres does -- the same contract SereneDBCatalog states, and what the
  // case-sensitive sets below depend on.
  bool MatchesNamesExactly() const final { return true; }

  // Roles live here and their dependents live in per-database catalogs, so an
  // edge from a table to a role is recorded in that table's manager. Each
  // catalog keeps its own -- a shared one cannot work, because a manager's sets
  // are read through the caller's transaction and a CatalogTransaction is bound
  // to one attachment -- and the readers that must see across them fan out.
  using duckdb::DuckCatalog::GetDependencyManager;

  // As in SereneDBCatalog: edges are addressed by the object's stable id.
  duckdb::CatalogEntryInfo GetDependencyInfo(
    const duckdb::CatalogEntry& entry) const final;
  duckdb::optional_ptr<duckdb::CatalogEntry> GetDependencyEntry(
    duckdb::CatalogTransaction transaction,
    const duckdb::CatalogEntryInfo& info) final;
  bool CascadeDropsThroughDependencies() const final { return false; }

  // Roles and databases are children of the instance, not of a schema, so their
  // version chains hang off the catalog the way a database's foreign servers
  // hang off its own.
  duckdb::CatalogSet& GetRoleSet() { return _roles; }
  duckdb::CatalogSet& GetDatabaseSet() { return _databases; }

  // The set for one of the two cluster-global kinds, or null for anything else.
  duckdb::optional_ptr<duckdb::CatalogSet> TryGetCatalogSet(
    duckdb::CatalogType type);

 private:
  duckdb::CatalogSet _roles;
  duckdb::CatalogSet _databases;
};

// The cluster-global catalog, or null before the attach and after shutdown.
duckdb::optional_ptr<SereneDBGlobalCatalog> TryGlobalCatalog(
  duckdb::ClientContext& context);
duckdb::optional_ptr<SereneDBGlobalCatalog> TryGlobalCatalog();

// Forwards its writes, so the attachment never occupies the transaction's
// single-writable-database slot: `CREATE ROLE r; CREATE TABLE t;` stays one
// transaction once the per-database managers stop forwarding theirs.
class SereneDBGlobalTransactionManager final
  : public duckdb::DuckTransactionManager {
 public:
  using duckdb::DuckTransactionManager::DuckTransactionManager;

  bool ForwardWrites() const final { return true; }
};

void RegisterSereneDBGlobalStorage(duckdb::DBConfig& config);

// Attaches the cluster-global database. Runs before any serenedb database is
// attached, so no cluster-global write can precede its home. Fatal on failure.
void AttachGlobalDatabase();

// Attributes a cluster-global write to that attachment: starts the
// transaction's DuckTransaction there and marks it written, instead of leaving
// the write unattributed or charged to the database the statement runs in.
// No-op before the attach and outside a transaction.
void ModifyGlobalDatabase(duckdb::ClientContext& context,
                          duckdb::DatabaseModificationType modification);

}  // namespace sdb::catalog
