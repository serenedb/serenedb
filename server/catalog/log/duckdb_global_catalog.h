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

#include <absl/functional/function_ref.h>

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/identifiers/object_id.h"

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
  // The cluster-wide kinds go to the catalog log as duckdb records: this
  // catalog has no WAL of its own to write them to, and they are the two the
  // log has to hold before any record can name a database.
  void WriteCatalogChange(duckdb::DuckTransaction& transaction,
                          duckdb::CatalogEntry& entry,
                          duckdb::data_ptr_t extra_data) final;

  // The cluster-wide kinds duckdb keeps no entry class for: a database and a
  // role. A database record attaches its database on the spot -- catalog only,
  // no file -- so every record after it has a set to land in.
  void ReplayCatalogEntry(duckdb::ClientContext& context,
                          duckdb::CreateInfo& info,
                          const duckdb::CatalogPermissions& permissions,
                          bool dropped) final;

  void ReplayCatalogState(duckdb::ClientContext& context,
                          duckdb::const_data_ptr_t data,
                          duckdb::idx_t size) final;

  // A reshape recipe coming back out of the catalog log. It belongs to a table
  // in a database whose file is not open yet, so it is held until that file has
  // been read and its rows are known to be behind.
  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

  explicit SereneDBGlobalCatalog(duckdb::AttachedDatabase& db);

  // Storage-extension key, not DuckCatalog's "duckdb".
  std::string GetCatalogType() final { return std::string{kGlobalStorageType}; }
  bool SupportsForeignDependencies() const final { return true; }

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
  // The set for one of the two cluster-global kinds, or null for anything else.
  duckdb::optional_ptr<duckdb::CatalogSet> TryGetCatalogSet(
    duckdb::CatalogType type);

  // The catalog-level set a by-id root location resolves against: the two
  // cluster-global kinds are this catalog's own, and duckdb's schemas set
  // answers for the rest.
  duckdb::optional_ptr<duckdb::CatalogSet> RootEntrySet(
    duckdb::CatalogType slot) final;

 private:
  // Roles and databases are children of the instance, not of a schema, so their
  // version chains hang off the catalog the way a database's foreign servers
  // hang off its own.
  duckdb::CatalogSet _roles;
  duckdb::CatalogSet _databases;
};

// The cluster-global catalog, or null before the attach and after shutdown.
duckdb::optional_ptr<SereneDBGlobalCatalog> TryGlobalCatalog(
  duckdb::ClientContext& context);
duckdb::optional_ptr<SereneDBGlobalCatalog> TryGlobalCatalog();

// The cluster-wide catalog log, or null before it is open. duckdb's own
// WriteAheadLog over a serenedb path: the records, the serializer, the file
// writer, the allocator and the WAL-size bookkeeping are all duckdb's, and
// what serenedb supplies is the file and the handful of records duckdb has
// none of. It is not a database's WAL -- the catalog outlives every database
// and is there before any of them -- so it hangs off the cluster-global
// attachment's storage manager rather than belonging to it.
duckdb::optional_ptr<duckdb::WriteAheadLog> ClusterCatalogWal();

// Ends the run of catalog records a committing transaction wrote: they are made
// durable and the log is released, or -- `committed` false -- the run is
// rewound and released without a marker, discarding one whose commit threw. A
// no-op unless this thread wrote a record. This is where the crash windows a
// statement has are modelled; a background reclamation writes records too, and
// no client is waiting on those.
void EndCommittingCatalogRun(bool committed);

// Parks the artifact half of a removal this transaction performed, run once the
// removal records are durable: it must not run for a drop whose commit can
// still be refused, and a refused commit discards it. Runs the action inline
// when there is no transaction to park it on -- the removal was inline and
// already ordered.
void DeferDropAction(duckdb::ClientContext* context,
                     std::function<void()> action);

// What a transaction that wrote leaves for after its commit: the artifacts of
// what it removed, and the rewrite of the log where the size-driven one would
// have happened -- a rewrite takes the catalog for itself, so it cannot run
// under the commit. `committed` false discards the artifact half. The artifacts
// are only the ones `context` parked: a nested statement of its own -- the
// index build's -- commits inside it and decides nothing it deferred.
void EndCommittingWrites(duckdb::ClientContext& context, bool committed);

class SereneDBGlobalTransactionManager final
  : public duckdb::DuckTransactionManager {
 public:
  using duckdb::DuckTransactionManager::DuckTransactionManager;

  // This catalog owns no storage: its entries are made durable by the cluster
  // catalog log, which every serenedb attachment shares -- a transaction that
  // writes a role and a table writes one log, and duckdb lets it. Such a
  // statement is ordinary -- postgres puts its shared catalogs in the same
  // transaction as everything else -- so this attachment must not take the
  // single-writable-database slot.
  duckdb::optional_ptr<duckdb::WriteAheadLog> CatalogLog() final {
    return ClusterCatalogWal();
  }

  // The run this commit wrote ends where the walk that wrote it does, so what
  // follows -- a wait on another committer -- never runs with the log held.
  void FlushCatalogLog() final { EndCommittingCatalogRun(/*committed=*/true); }

  duckdb::ErrorData CommitTransaction(duckdb::ClientContext& context,
                                      duckdb::Transaction& transaction) final;

  void RollbackTransaction(duckdb::Transaction& transaction) final;
};

void RegisterSereneDBGlobalStorage(duckdb::DBConfig& config);

// Opens the cluster catalog log over the main attachment, which duckdb built
// from ConfigureServerDBConfig before anything else exists.
void InitClusterCatalogWal();

// Releases it. The log holds duckdb's own writer and allocator, so it has to go
// before the instance it was built over -- nothing may outlive that.
void CloseClusterCatalogWal();

// The log, taken by the first catalog record of a commit and held until it
// ends, so one transaction's records are contiguous and the flush that makes
// them durable covers that transaction alone. Every record locates its own
// object by id, so no record needs to say which database it belongs to. Null
// before the log is open.
duckdb::optional_ptr<duckdb::WriteAheadLog> ScopedCatalogWal();

// The catalog written out: the log is replaced by one create per live object,
// which is what lets it stop growing. `fill` writes that snapshot into a log of
// its own, which then takes the place of the one in use. The caller holds
// whatever lock makes its snapshot answer for every record being replaced.
void RewriteClusterCatalogWal(
  absl::FunctionRef<void(duckdb::WriteAheadLog&)> fill);

// State a catalog keeps that is not an entry, in the one record duckdb hands
// back to whoever wrote it. Tagged, so a new kind of state costs no further
// duckdb patch.
enum class CatalogState : uint8_t {
  OidHorizon = 1,
  SequenceValue = 2,
  SequenceDropped = 3,
};

// Writes down that every id up to `horizon` is spent, and makes it durable
// before any of them is handed out: an id can name something that outlives the
// transaction meant to record it -- a database file, an index directory -- so
// reissuing one after a crash would collide with what is already on disk.
// False when the log is not up to take the record -- boot's own replay-time
// allocations -- in which case the horizon must not be treated as raised.
bool WriteOidHorizon(uint64_t horizon);

// Marks a stretch where the object id horizon may be written by waiting for the
// cluster log lock. Only valid where no catalog lock is held: everywhere else
// the allocator refuses rather than waits, because the log lock sits outside
// the catalog locks and waiting for it there would invert the order.
class OidHorizonWaitScope {
 public:
  OidHorizonWaitScope();
  ~OidHorizonWaitScope();
  OidHorizonWaitScope(const OidHorizonWaitScope&) = delete;
  OidHorizonWaitScope& operator=(const OidHorizonWaitScope&) = delete;

 private:
  bool _previous;
};
// The same records into a log of the caller's own, for the rewrite that folds
// the state back into a fresh one.
void WriteOidHorizonTo(duckdb::WriteAheadLog& wal, uint64_t horizon);
void WriteSequenceValueTo(duckdb::WriteAheadLog& wal, ObjectId sequence_id,
                          uint64_t value, bool max_merge);

// A sequence's counter. `max_merge` is a create's seed and a nextval horizon,
// which are floors and never rewind; setval is an ordered assign. Returns once
// durable: the value has to outlive the call that handed it out.
void WriteSequenceValue(ObjectId sequence_id, uint64_t value, bool max_merge);
void WriteSequenceDropped(ObjectId sequence_id);

// How a table's rows got to the shape its definition states, for the one step
// the definition cannot express: ALTER COLUMN ... TYPE ... USING says what the
// old values become, and nothing in the resulting definition does. Written into
// the same run as that definition, and read back at boot by the database whose
// file is behind it. Nothing else is recorded here: every other reshape is the
// difference between the two definitions.
std::vector<duckdb::unique_ptr<duckdb::AlterInfo>> TakeRowRecipes(
  ObjectId table_id);

// Says the open run carries a version of an object, which is what makes the
// flush that ends it the moment the catalog decided.
void MarkCatalogDecision();
// Says the open run drops an object: artifacts survive it that no transaction
// rolls back, and the window between the durable removal and their sweep is
// where the drop fault sits.
void MarkCatalogDrop();

// As EndCommittingCatalogRun without the crash windows: flushes what the
// commit's own flush did not already make durable -- or, `committed` false,
// rewinds the run -- and releases the log.
void EndClusterCatalogWal(bool committed);

// Attributes a cluster-global write to that attachment: starts the
// transaction's DuckTransaction there and marks it written, instead of leaving
// the write unattributed or charged to the database the statement runs in.
// No-op before the attach and outside a transaction.
void ModifyGlobalDatabase(duckdb::ClientContext& context,
                          duckdb::DatabaseModificationType modification);

}  // namespace sdb::catalog
