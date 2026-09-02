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

#include "catalog/log/duckdb_global_catalog.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <duckdb/transaction/transaction.hpp>
#include <utility>

#include "basics/debugging.h"
#include "basics/duckdb_engine.h"
#include "basics/file_utils.h"
#include "basics/log.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/log/store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/read/duckdb_dependency.h"
#include "connector/duckdb_storage_extension.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

duckdb::unique_ptr<duckdb::Catalog> AttachGlobal(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::string& name, duckdb::AttachInfo& info,
  duckdb::AttachOptions& options) {
  // AttachedDatabase reads info.path after this returns and builds a
  // SingleFileStorageManager from it. Empty means IN_MEMORY_PATH, which is what
  // makes this attachment storage-less: no data file, no data WAL, and
  // storage_options.Initialize is skipped so no storage version applies.
  info.path.clear();
  return duckdb::make_uniq<SereneDBGlobalCatalog>(db);
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateGlobalTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<SereneDBGlobalTransactionManager>(db);
}

class SereneDBGlobalStorageExtension final : public duckdb::StorageExtension {
 public:
  SereneDBGlobalStorageExtension() {
    attach = AttachGlobal;
    create_transaction_manager = CreateGlobalTransactionManager;
  }
};

}  // namespace

SereneDBGlobalCatalog::SereneDBGlobalCatalog(duckdb::AttachedDatabase& db)
  : duckdb::DuckCatalog{db},
    // Case-sensitive for the same reason the schema sets are: serenedb folds an
    // unquoted identifier at parse time and then matches exactly.
    _roles{*this, nullptr, /*case_sensitive=*/true},
    _databases{*this, nullptr, /*case_sensitive=*/true} {
  _roles.EnableOidLookup(nullptr, duckdb::CatalogType::ROLE_ENTRY);
  _databases.EnableOidLookup(nullptr, duckdb::CatalogType::DATABASE_ENTRY);
}

duckdb::optional_ptr<duckdb::CatalogSet>
SereneDBGlobalCatalog::TryGetCatalogSet(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::ROLE_ENTRY:
      return &_roles;
    case duckdb::CatalogType::DATABASE_ENTRY:
      return &_databases;
    default:
      return nullptr;
  }
}

duckdb::optional_ptr<duckdb::CatalogSet> SereneDBGlobalCatalog::RootEntrySet(
  duckdb::CatalogType slot) {
  if (auto set = TryGetCatalogSet(slot)) {
    return set;
  }
  return duckdb::DuckCatalog::RootEntrySet(slot);
}

namespace {

duckdb::optional_ptr<SereneDBGlobalCatalog> AsGlobalCatalog(
  duckdb::optional_ptr<duckdb::AttachedDatabase> db) {
  if (!db) {
    return nullptr;
  }
  auto& catalog = db->GetCatalog();
  if (catalog.GetCatalogType() != kGlobalStorageType) {
    return nullptr;
  }
  return &catalog.Cast<SereneDBGlobalCatalog>();
}

}  // namespace

duckdb::CatalogEntryInfo SereneDBGlobalCatalog::GetDependencyInfo(
  const duckdb::CatalogEntry& entry) const {
  // A role and a database are the only kinds this catalog holds, and both
  // are addressed by their stable id. Anything else -- duckdb's own
  // dependency entries -- keeps duckdb's name-keyed address.
  return IsHostedEntry(entry) ? DependencyInfo(catalog::IdOf(entry))
                              : duckdb::Catalog::GetDependencyInfo(entry);
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBGlobalCatalog::GetDependencyEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::CatalogEntryInfo& info) {
  const auto id = DependencyInfoId(info);
  return id.isSet() ? GetEntryById(transaction, id.id())
                    : duckdb::Catalog::GetDependencyEntry(transaction, info);
}

duckdb::optional_ptr<SereneDBGlobalCatalog> TryGlobalCatalog(
  duckdb::ClientContext& context) {
  return AsGlobalCatalog(duckdb::DatabaseManager::Get(context)
                           .GetDatabase(duckdb::Identifier{kGlobalDatabaseName})
                           .get());
}

duckdb::optional_ptr<SereneDBGlobalCatalog> TryGlobalCatalog() {
  auto db = duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
              .GetDatabase(duckdb::Identifier{kGlobalDatabaseName});
  return AsGlobalCatalog(db.get());
}

void RegisterSereneDBGlobalStorage(duckdb::DBConfig& config) {
  auto ext = duckdb::make_shared_ptr<SereneDBGlobalStorageExtension>();
  duckdb::StorageExtension::Register(config, std::string{kGlobalStorageType},
                                     std::move(ext));
}

// Held rather than looked up: the commit walk writes to it after the committing
// transaction has left the state a name lookup needs.
duckdb::shared_ptr<duckdb::WriteAheadLog> gClusterWal;
// One log, every database: two commits interleaving their records would be made
// durable by whichever flushed first, so a commit holds the log for its run.
absl::Mutex gClusterWalMutex;
thread_local bool gHoldsClusterWal = false;
// Set only where the caller is known to hold no catalog lock, which is the one
// place waiting for the log lock cannot invert an order.
thread_local bool gOidHorizonMayWait = false;
thread_local uint64_t gClusterWalFlushedAt = 0;
// Whether the open run carries a catalog decision -- a version of an object, or
// the opening of a drop -- as opposed to only what describes one. That is where
// the crash window of a statement sits.
thread_local bool gClusterWalDecides = false;
// Whether it opened a reclamation. The window a drop has is after the entry
// records that decided it are durable and before the sweep has run, so that is
// where the fault sits.
thread_local bool gClusterWalDropped = false;
thread_local uint64_t gClusterWalSizeBeforeRun = 0;
thread_local uint64_t gClusterWalWrittenBeforeRun = 0;

void InitClusterCatalogWal() {
  auto db = duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
              .GetDatabase(duckdb::Identifier{kGlobalDatabaseName});
  SDB_ENSURE(db && db->HasStorageManager(),
             "the cluster-global attachment has no storage manager to hang the "
             "catalog log off");
  // duckdb's own replay, over serenedb's own path. A database record attaches
  // its database on the spot -- catalog only, no file -- so every record after
  // it has a set to land in.
  gClusterWal = duckdb::WriteAheadLog::Replay(
    duckdb::QueryContext{}, db->GetStorageManager(),
    basics::file_utils::BuildFilename(
      std::string{GetCatalogStore().WalDirectory()}, "catalog.wal"));
}

void SereneDBGlobalCatalog::WriteCatalogChange(
  duckdb::DuckTransaction& /*transaction*/, duckdb::CatalogEntry& entry,
  duckdb::data_ptr_t /*extra_data*/) {
  auto wal = ClusterCatalogWal();
  if (!wal) {
    return;
  }
  auto& new_entry = entry.Parent();
  // Roles and databases are what this catalog is durable for. Everything else
  // it holds -- duckdb's own dependency entries -- is rebuilt from them.
  if (!IsHostedEntry(new_entry.type == duckdb::CatalogType::DELETED_ENTRY
                       ? entry
                       : new_entry)) {
    return;
  }
  wal = ScopedCatalogWal();
  MarkCatalogDecision();
  if (new_entry.type == duckdb::CatalogType::DELETED_ENTRY) {
    MarkCatalogDrop();
    wal->WriteDropEntry(entry);
    return;
  }
  wal->WriteCreateEntry(new_entry);
}

namespace {

absl::Mutex gRowRecipesMutex;
containers::NodeHashMap<uint64_t,
                        std::vector<duckdb::unique_ptr<duckdb::AlterInfo>>>
  gRowRecipes ABSL_GUARDED_BY(gRowRecipesMutex);

void StashRowRecipe(ObjectId table_id,
                    duckdb::unique_ptr<duckdb::AlterInfo> recipe) {
  const absl::MutexLock lock{&gRowRecipesMutex};
  gRowRecipes[table_id.id()].push_back(std::move(recipe));
}

}  // namespace

std::vector<duckdb::unique_ptr<duckdb::AlterInfo>> TakeRowRecipes(
  ObjectId table_id) {
  const absl::MutexLock lock{&gRowRecipesMutex};
  const auto it = gRowRecipes.find(table_id.id());
  if (it == gRowRecipes.end()) {
    return {};
  }
  auto recipes = std::move(it->second);
  gRowRecipes.erase(it);
  return recipes;
}

void SereneDBGlobalCatalog::Alter(duckdb::CatalogTransaction /*transaction*/,
                                  duckdb::AlterInfo& info) {
  StashRowRecipe(ObjectId{info.oid}, info.Copy());
}

void CloseClusterCatalogWal() {
  {
    const absl::MutexLock lock{&gRowRecipesMutex};
    gRowRecipes.clear();
  }
  const absl::MutexLock lock{&gClusterWalMutex};
  gClusterWal.reset();
}

duckdb::ErrorData SereneDBGlobalTransactionManager::CommitTransaction(
  duckdb::ClientContext& context, duckdb::Transaction& transaction) {
  // This attachment has no rows, so it commits ahead of the database whose
  // records belong with its own: the artifacts wait for that one when there is
  // one, and this is the commit that decides them when there is not.
  // The meta transaction is not readable from every commit road, and a queue
  // left undrained would run against the next statement on this thread.
  const bool has_meta = context.transaction.HasActiveTransaction();
  const bool wrote =
    !transaction.IsReadOnly() &&
    (!has_meta || !duckdb::MetaTransaction::Get(context).ModifiedDatabase());
  // As in SereneDBTransactionManager::CommitTransaction: the run holds the
  // cluster WAL until it ends, so it ends however the commit leaves.
  bool committed = false;
  const absl::Cleanup end_run = [&] {
    EndCommittingCatalogRun(committed);
    if (wrote) {
      EndCommittingWrites(context, committed);
    }
  };
  auto error =
    duckdb::DuckTransactionManager::CommitTransaction(context, transaction);
  committed = !error.HasError();
  return error;
}

void SereneDBGlobalTransactionManager::RollbackTransaction(
  duckdb::Transaction& transaction) {
  const auto context = transaction.context.lock();
  duckdb::DuckTransactionManager::RollbackTransaction(transaction);
  if (context) {
    EndCommittingWrites(*context, /*committed=*/false);
  }
}

duckdb::optional_ptr<duckdb::WriteAheadLog> ScopedCatalogWal()
  ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto wal = ClusterCatalogWal();
  if (!wal) {
    return nullptr;
  }
  if (!gHoldsClusterWal) {
    // The failure this exists for: once the log is a consensus log, an append
    // refused by a lost leadership or a partition is routine, and the answer is
    // an aborted transaction and an ordinary error -- not a fatal.
    SDB_IF_FAILURE("catalog_append_fails") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("catalog log: could not append the transaction"));
    }
    gClusterWalMutex.Lock();
    gHoldsClusterWal = true;
    gClusterWalSizeBeforeRun = wal->GetStorageManager().GetWALSize();
    gClusterWalWrittenBeforeRun = wal->GetTotalWritten();
    gClusterWalFlushedAt = gClusterWalWrittenBeforeRun;
  }
  return wal;
}

void WriteOidHorizonTo(duckdb::WriteAheadLog& wal, uint64_t horizon) {
  uint8_t bytes[sizeof(uint8_t) + sizeof(uint64_t)];
  duckdb::MemoryStream stream{bytes, sizeof(bytes)};
  stream.Write<uint8_t>(static_cast<uint8_t>(CatalogState::OidHorizon));
  stream.Write<uint64_t>(horizon);
  wal.WriteCatalogState(bytes, stream.GetPosition());
}

void WriteSequenceValueTo(duckdb::WriteAheadLog& wal, ObjectId sequence_id,
                          uint64_t value, bool max_merge) {
  uint8_t bytes[sizeof(uint8_t) + 2 * sizeof(uint64_t) + sizeof(uint8_t)];
  duckdb::MemoryStream stream{bytes, sizeof(bytes)};
  stream.Write<uint8_t>(static_cast<uint8_t>(CatalogState::SequenceValue));
  stream.Write<uint64_t>(sequence_id.id());
  stream.Write<uint64_t>(value);
  stream.Write<uint8_t>(static_cast<uint8_t>(max_merge));
  wal.WriteCatalogState(bytes, stream.GetPosition());
}

bool WriteOidHorizon(uint64_t horizon) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto wal = ClusterCatalogWal();
  if (!wal) {
    return false;
  }
  // An id is allocated wherever a catalog entry is built, which is under the
  // catalog and set locks. The log lock sits outside those, so waiting for it
  // here would close the cycle a commit walks the other way round. The run that
  // already holds it writes straight through; anyone else takes it only if it
  // is free, and otherwise reports a conflict the statement can be retried on.
  if (gHoldsClusterWal) {
    WriteOidHorizonTo(*wal, horizon);
    wal->Flush();
    return true;
  }
  if (gOidHorizonMayWait) {
    gClusterWalMutex.Lock();
  } else if (!gClusterWalMutex.TryLock()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("catalog log: object id horizon is busy, retry the statement"));
  }
  const absl::Cleanup unlock = []() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    gClusterWalMutex.Unlock();
  };
  WriteOidHorizonTo(*wal, horizon);
  wal->Flush();
  return true;
}

void WriteSequenceValue(ObjectId sequence_id, uint64_t value, bool max_merge) {
  auto wal = ClusterCatalogWal();
  if (!wal) {
    return;
  }
  absl::MutexLock lock{&gClusterWalMutex};
  WriteSequenceValueTo(*wal, sequence_id, value, max_merge);
  wal->Flush();
}

void WriteSequenceDropped(ObjectId sequence_id) {
  auto wal = ClusterCatalogWal();
  if (!wal) {
    return;
  }
  uint8_t bytes[sizeof(uint8_t) + sizeof(uint64_t)];
  duckdb::MemoryStream stream{bytes, sizeof(bytes)};
  stream.Write<uint8_t>(static_cast<uint8_t>(CatalogState::SequenceDropped));
  stream.Write<uint64_t>(sequence_id.id());
  absl::MutexLock lock{&gClusterWalMutex};
  wal->WriteCatalogState(bytes, stream.GetPosition());
  wal->Flush();
}

void MarkCatalogDecision() ABSL_NO_THREAD_SAFETY_ANALYSIS {
  gClusterWalDecides = true;
}

void MarkCatalogDrop() ABSL_NO_THREAD_SAFETY_ANALYSIS {
  gClusterWalDropped = true;
}

void SereneDBGlobalCatalog::ReplayCatalogState(
  duckdb::ClientContext& /*context*/, duckdb::const_data_ptr_t data,
  duckdb::idx_t size) {
  duckdb::MemoryStream stream{const_cast<duckdb::data_ptr_t>(data), size};
  switch (static_cast<CatalogState>(stream.Read<uint8_t>())) {
    case CatalogState::OidHorizon:
      IdAllocator().RestoreOidReservation(stream.Read<uint64_t>());
      return;
    case CatalogState::SequenceValue: {
      const ObjectId id{stream.Read<uint64_t>()};
      const auto value = stream.Read<uint64_t>();
      GetCatalogStore().ApplySequenceValue(id, value,
                                           stream.Read<uint8_t>() != 0);
      return;
    }
    case CatalogState::SequenceDropped:
      GetCatalogStore().ApplySequenceDropped(ObjectId{stream.Read<uint64_t>()});
      return;
  }
}

void RewriteClusterCatalogWal(
  absl::FunctionRef<void(duckdb::WriteAheadLog&)> fill) {
  if (!gClusterWal || gHoldsClusterWal) {
    // A commit is mid-run: rewriting the file under it would drop the records
    // it has already written. The next append tries again.
    return;
  }
  const auto path = gClusterWal->GetPath();
  const auto next_path = path + ".rewrite";
  auto& storage = gClusterWal->GetStorageManager();
  absl::MutexLock lock{&gClusterWalMutex};
  {
    duckdb::WriteAheadLog next{storage, next_path};
    fill(next);
    next.Flush();
  }
  std::error_code ec;
  std::filesystem::rename(next_path, path, ec);
  SDB_ENSURE(!ec, "could not replace the catalog log: ", ec.message());
  gClusterWal = duckdb::make_shared_ptr<duckdb::WriteAheadLog>(storage, path);
}

namespace {

// Makes the run's records durable, the way duckdb ends one in a database's own
// WAL: a WAL_FLUSH marker and a sync. Called from inside the commit, before the
// database's own rows are flushed -- the catalog may end up ahead of the rows
// it describes, and never behind them. A no-op unless this thread wrote a
// record; the run stays open.
void FlushClusterCatalogWal() ABSL_NO_THREAD_SAFETY_ANALYSIS {
  if (!gHoldsClusterWal) {
    return;
  }
  auto wal = ClusterCatalogWal();
  if (!wal || wal->GetTotalWritten() <= gClusterWalFlushedAt) {
    return;
  }
  wal->Flush();
  gClusterWalFlushedAt = wal->GetTotalWritten();
}

}  // namespace
namespace {

// Whether the flush about to happen is the one that makes this run's records
// durable for the first time -- the crash window a statement has, as opposed to
// the later flushes of the same run.
bool DecidesTheRun() ABSL_NO_THREAD_SAFETY_ANALYSIS {
  return gHoldsClusterWal && gClusterWalDecides;
}

}  // namespace

void EndCommittingCatalogRun(bool committed) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  if (!committed || !DecidesTheRun()) {
    // One transaction ends a run per attachment it wrote, and only one of them
    // carries the decision -- so what it dropped is forgotten there, or here
    // when the commit was refused and there is nothing to sweep.
    if (!committed) {
      gClusterWalDropped = false;
    }
    EndClusterCatalogWal(committed);
    return;
  }
  // Nothing this run wrote is durable yet: replay stops at the last flush
  // marker, so a crash here leaves no half of the statement behind.
  SDB_IF_FAILURE("crash_before_catalog_commit") { SDB_IMMEDIATE_ABORT(); }
  const bool dropped = std::exchange(gClusterWalDropped, false);
  EndClusterCatalogWal(committed);
  // The catalog has decided and the rows it describes have not been written
  // out: the window a crash can hit. What is durable here is the definition,
  // and boot walks the rows up to it.
  SDB_IF_FAILURE("crash_after_catalog_before_data") { SDB_IMMEDIATE_ABORT(); }
  // The same window for a drop: its removals are durable and the artifact
  // sweep has not run, so boot's reconciliation finds and removes the orphans.
  if (dropped) {
    SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
  }
}

OidHorizonWaitScope::OidHorizonWaitScope() : _previous{gOidHorizonMayWait} {
  gOidHorizonMayWait = true;
}

OidHorizonWaitScope::~OidHorizonWaitScope() { gOidHorizonMayWait = _previous; }

void EndClusterCatalogWal(bool committed) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  if (!gHoldsClusterWal) {
    return;
  }
  if (auto wal = ClusterCatalogWal()) {
    if (committed) {
      FlushClusterCatalogWal();
    } else if (wal->GetTotalWritten() > gClusterWalWrittenBeforeRun) {
      // The run stops where it started, the way duckdb reverts a commit that
      // wrote to a database's own WAL: the next one must not flush these.
      wal->Truncate(gClusterWalSizeBeforeRun);
    }
  }
  gHoldsClusterWal = false;
  gClusterWalDecides = false;
  gClusterWalMutex.Unlock();
}

namespace {

// The removals a transaction performed, from the first of them to the commit
// that decides them: the statement and the commit that ends it are one thread's
// work, as the run of records above is. Held with the connection that parked
// them, which is what the commit deciding them is on.
thread_local std::vector<std::function<void()>> gDropActions;
thread_local duckdb::ClientContext* gDropContext = nullptr;

}  // namespace

void DeferDropAction(duckdb::ClientContext* context,
                     std::function<void()> action) {
  // Nothing is parked without a transaction to charge it to: boot, background
  // drops and connection teardown remove inline and hold no claim.
  if (context && context->transaction.HasActiveTransaction()) {
    gDropContext = context;
    gDropActions.push_back(std::move(action));
    return;
  }
  action();
}

void EndCommittingWrites(duckdb::ClientContext& context, bool committed) {
  std::vector<std::function<void()>> actions;
  if (gDropContext == &context) {
    gDropContext = nullptr;
    actions = std::exchange(gDropActions, {});
  }
  // A connection closing during teardown still commits, and by then the store
  // and the background scheduler may be gone.
  if (!committed || !CatalogStore::Available()) {
    return;
  }
  // Reclamation starts once the commit that decided it has landed, never
  // before: one that ran ahead of the commit would remove artifacts for a drop
  // whose commit could still be refused.
  for (auto& action : actions) {
    action();
  }
  // The whole file rewritten from the catalog the statement just changed, where
  // the size-driven one would have happened. A rewrite takes the catalog for
  // itself, so it runs here rather than under the commit.
  SDB_IF_FAILURE("compact_inside_ddl") {
    GetCatalog().TryExcludingMutations([] { GetCatalogStore().CompactNow(); });
  }
  GetCatalogStore().TryCompact();
}

void SereneDBGlobalCatalog::ReplayCatalogEntry(
  duckdb::ClientContext& /*context*/, duckdb::CreateInfo& info,
  const duckdb::CatalogPermissions& permissions, bool dropped) {
  // Contextless: replay runs a transaction with no statement behind it, and a
  // write attributed to one wants the client state a statement would have.
  catalog::ReplayCatalogRecord(info.Copy(), permissions, dropped);
}

duckdb::optional_ptr<duckdb::WriteAheadLog> ClusterCatalogWal() {
  return gClusterWal.get();
}

void ModifyGlobalDatabase(duckdb::ClientContext& context,
                          duckdb::DatabaseModificationType modification) {
  if (!context.transaction.HasActiveTransaction()) {
    return;
  }
  // Context-free: the attachment is there from before the first statement, and
  // a name lookup through the transaction wants a query this may not have --
  // the catalog log replays with a transaction and no statement behind it.
  auto db = duckdb::DatabaseManager::Get(context).GetDatabase(
    duckdb::Identifier{kGlobalDatabaseName});
  if (!db) {
    return;
  }
  duckdb::MetaTransaction::Get(context).ModifyDatabase(*db, modification);
}

}  // namespace sdb::catalog
