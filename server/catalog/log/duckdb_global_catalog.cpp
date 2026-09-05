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

// Guarded rather than owned by an attachment: the commit walk records changes
// after the committing transaction has left the state a name lookup needs.
duckdb::shared_ptr<duckdb::WriteAheadLog> gClusterWal;
// The storage manager the log hangs off -- the cluster-global attachment's.
// Set once at init, before anything runs concurrently, and outliving the log:
// the attachment goes down after the log is closed.
duckdb::StorageManager* gClusterWalStorage = nullptr;

// One catalog record of the run a committing transaction buffers: a create
// carries the version and its grants, a drop the identity it removes, a recipe
// how the rows of the version after it got their shape.
struct CatalogRunRecord {
  duckdb::unique_ptr<duckdb::CreateInfo> info;
  duckdb::CatalogPermissions permissions;
  duckdb::unique_ptr<duckdb::AlterInfo> recipe;
  bool dropped = false;
};

// The records of one committing transaction, buffered on its thread until the
// splice that ends the run: the walk that produces them holds catalog locks,
// and the log's lock must stay a leaf. `dropped` says the run opened a
// reclamation -- the window a drop has is after its removal records are
// durable and before the sweep has run, and that is where the fault sits.
struct CatalogRun {
  std::vector<CatalogRunRecord> records;
  bool dropped = false;
};
thread_local CatalogRun gCatalogRun;

// The failure the append fault models: once the log is a consensus log, an
// append refused by a lost leadership or a partition is routine, and the
// answer is an aborted transaction and an ordinary error -- not a fatal.
// Fired on the run's first record, inside duckdb's commit try, so the refusal
// reverts the commit.
void ThrowIfCatalogAppendRefused() {
  if (!gCatalogRun.records.empty()) {
    return;
  }
  SDB_IF_FAILURE("catalog_append_fails") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("catalog log: could not append the transaction"));
  }
}

void InitClusterCatalogWal() {
  auto db = duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
              .GetDatabase(duckdb::Identifier{kGlobalDatabaseName});
  SDB_ENSURE(db && db->HasStorageManager(),
             "the cluster-global attachment has no storage manager to hang the "
             "catalog log off");
  gClusterWalStorage = &db->GetStorageManager();
  // duckdb's own replay, over serenedb's own path. A database record attaches
  // its database on the spot -- catalog only, no file -- so every record after
  // it has a set to land in. Replayed before the log is published: a replay
  // write finding no log is what keeps it from re-recording itself.
  auto wal = duckdb::WriteAheadLog::Replay(
    duckdb::QueryContext{}, *gClusterWalStorage,
    basics::file_utils::BuildFilename(
      std::string{GetCatalogStore().WalDirectory()}, "catalog.wal"));
  const auto lock = LockClusterCatalogWal();
  gClusterWal = std::move(wal);
}

duckdb::unique_lock<duckdb::mutex> LockClusterCatalogWal() {
  // duckdb's own WAL lock of the storage manager the log hangs off: one log,
  // every database -- two commits interleaving their records would be made
  // durable by whichever flushed first, so a run's records reach the file as
  // one splice under it. The attachment is storage-less, so nothing else ever
  // takes it, and it stays a leaf: nothing under it takes another lock or
  // waits on another transaction. Empty before init, which is single-threaded.
  if (!gClusterWalStorage) {
    return {};
  }
  return gClusterWalStorage->GetWALLock();
}

void SereneDBGlobalCatalog::WriteCatalogChange(
  duckdb::DuckTransaction& /*transaction*/, duckdb::CatalogEntry& entry,
  duckdb::data_ptr_t /*extra_data*/) {
  if (!ClusterCatalogWal()) {
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
  if (new_entry.type == duckdb::CatalogType::DELETED_ENTRY) {
    BufferCatalogDrop(entry.GetInfo());
    return;
  }
  BufferCatalogCreate(new_entry.GetInfo(), new_entry.permissions);
}

namespace {

// Staged beside the log they came out of, so the log's own lock guards them.
containers::NodeHashMap<uint64_t,
                        std::vector<duckdb::unique_ptr<duckdb::AlterInfo>>>
  gRowRecipes;

void StashRowRecipe(ObjectId table_id,
                    duckdb::unique_ptr<duckdb::AlterInfo> recipe) {
  const auto lock = LockClusterCatalogWal();
  gRowRecipes[table_id.id()].push_back(std::move(recipe));
}

}  // namespace

std::vector<duckdb::unique_ptr<duckdb::AlterInfo>> TakeRowRecipes(
  ObjectId table_id) {
  const auto lock = LockClusterCatalogWal();
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
    const auto lock = LockClusterCatalogWal();
    gRowRecipes.clear();
    gClusterWal.reset();
  }
  // The lock lives on the global attachment's storage manager, which shutdown
  // destroys with the attachments: once the log is closed, no later call may
  // reach for it.
  gClusterWalStorage = nullptr;
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
  // As in SereneDBTransactionManager::CommitTransaction: the run ends however
  // the commit leaves, so a commit that threw mid-walk drops its records.
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

void BufferCatalogCreate(duckdb::unique_ptr<duckdb::CreateInfo> info,
                         const duckdb::CatalogPermissions& permissions) {
  ThrowIfCatalogAppendRefused();
  gCatalogRun.records.push_back(
    {std::move(info), permissions, nullptr, /*dropped=*/false});
}

void BufferCatalogDrop(duckdb::unique_ptr<duckdb::CreateInfo> info) {
  ThrowIfCatalogAppendRefused();
  gCatalogRun.records.push_back({std::move(info),
                                 {},
                                 nullptr,
                                 /*dropped=*/true});
  gCatalogRun.dropped = true;
}

void BufferCatalogRecipe(duckdb::unique_ptr<duckdb::AlterInfo> recipe) {
  ThrowIfCatalogAppendRefused();
  gCatalogRun.records.push_back({nullptr,
                                 {},
                                 std::move(recipe),
                                 /*dropped=*/false});
}

void WriteBootstrapEntry(const duckdb::CreateInfo& info,
                         const duckdb::CatalogPermissions& permissions) {
  const auto lock = LockClusterCatalogWal();
  SDB_ENSURE(gClusterWal, "the catalog log is not open");
  gClusterWal->WriteCreateEntry(info, permissions);
  gClusterWal->Flush();
}

ClusterCatalogWalSizes ClusterCatalogWalSize() {
  const auto lock = LockClusterCatalogWal();
  if (!gClusterWal) {
    return {};
  }
  return {gClusterWal->GetTotalWritten(),
          gClusterWal->GetStorageManager().GetWALSize()};
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

bool WriteOidHorizon(uint64_t horizon) {
  const auto lock = LockClusterCatalogWal();
  if (!gClusterWal) {
    return false;
  }
  WriteOidHorizonTo(*gClusterWal, horizon);
  gClusterWal->Flush();
  return true;
}

void WriteSequenceValue(ObjectId sequence_id, uint64_t value, bool max_merge) {
  const auto lock = LockClusterCatalogWal();
  if (!gClusterWal) {
    return;
  }
  WriteSequenceValueTo(*gClusterWal, sequence_id, value, max_merge);
  gClusterWal->Flush();
}

void WriteSequenceDropped(ObjectId sequence_id) {
  uint8_t bytes[sizeof(uint8_t) + sizeof(uint64_t)];
  duckdb::MemoryStream stream{bytes, sizeof(bytes)};
  stream.Write<uint8_t>(static_cast<uint8_t>(CatalogState::SequenceDropped));
  stream.Write<uint64_t>(sequence_id.id());
  const auto lock = LockClusterCatalogWal();
  if (!gClusterWal) {
    return;
  }
  gClusterWal->WriteCatalogState(bytes, stream.GetPosition());
  gClusterWal->Flush();
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
  uint64_t expected_written,
  absl::FunctionRef<void(duckdb::WriteAheadLog&)> fill) {
  // Under the log's lock for the duration: an append landing between what
  // `fill` writes and the swap would be lost with the file it landed in.
  // Rewrites are rare and the file is one catalog, so the appends wait.
  //
  // The caller read its snapshot with no mutation excluded, so a commit can
  // land between that read and this lock -- its records in the file, its
  // entries not in the snapshot. Its splice moved the write count past what
  // the caller saw, which is the abandon: the next append tries again.
  const auto lock = LockClusterCatalogWal();
  if (!gClusterWal || gClusterWal->GetTotalWritten() != expected_written) {
    return;
  }
  const auto path = gClusterWal->GetPath();
  const auto next_path = path + ".rewrite";
  auto& storage = gClusterWal->GetStorageManager();
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

void EndCommittingCatalogRun(bool committed) {
  const auto run = std::exchange(gCatalogRun, {});
  if (!committed || run.records.empty()) {
    // A refused commit's records are discarded before the log ever saw them:
    // there is no half-written run to rewind, and nothing to sweep.
    return;
  }
  // Nothing this run buffered is durable yet: a crash here leaves no half of
  // the statement behind.
  SDB_IF_FAILURE("crash_before_catalog_commit") { SDB_IMMEDIATE_ABORT(); }
  {
    // The splice: one transaction's records reach the file contiguous, and the
    // flush that makes them durable covers that transaction alone. Called from
    // inside the commit, before the database's own rows are flushed -- the
    // catalog may end up ahead of the rows it describes, and never behind
    // them. Every record locates its own object by id, so none says which
    // database it belongs to.
    const auto lock = LockClusterCatalogWal();
    if (gClusterWal) {
      for (const auto& record : run.records) {
        if (record.recipe) {
          gClusterWal->WriteAlter(*record.recipe);
        } else if (record.dropped) {
          gClusterWal->WriteDropEntry(*record.info);
        } else {
          gClusterWal->WriteCreateEntry(*record.info, record.permissions);
        }
      }
      gClusterWal->Flush();
    }
  }
  // The catalog has decided and the rows it describes have not been written
  // out: the window a crash can hit. What is durable here is the definition,
  // and boot walks the rows up to it.
  SDB_IF_FAILURE("crash_after_catalog_before_data") { SDB_IMMEDIATE_ABORT(); }
  // The same window for a drop: its removals are durable and the artifact
  // sweep has not run, so boot's reconciliation finds and removes the orphans.
  if (run.dropped) {
    SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
  }
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
  // the size-driven one would have happened.
  SDB_IF_FAILURE("compact_inside_ddl") { GetCatalogStore().CompactNow(); }
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
  const auto lock = LockClusterCatalogWal();
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
