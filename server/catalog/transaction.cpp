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

#include "catalog/transaction.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>

#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/sql_identifier.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/external_resources_manager.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <string>

#include "catalog/base_catalog.h"
#include "catalog/record.h"
#include "pg/connection_context.h"
#include "scheduler/background_scheduler.h"
#include "search/task.h"
#include "search/wal_recovery.h"
#include "server/utils/lifecycle.h"
#include "storage_engine/search_engine.h"

namespace sdb::catalog {
namespace {

struct Awaited {
  const std::atomic<duckdb::transaction_t>* last = nullptr;
  duckdb::transaction_t commit = 0;
};

bool Published(Awaited* awaited) {
  return awaited->last->load(std::memory_order_acquire) >= awaited->commit;
}

bool ModifiesUserDatabase(SessionTransaction& session) {
  return !session.SearchTxn().Empty() ||
         absl::c_any_of(session.Changes(), [](const Change& change) {
           const auto& db = change.Subject().ParentCatalog().GetAttached();
           return !db.IsSystem() && !db.IsTemporary();
         });
}

void RequireLog(const ReplicationLsnEntry& entry) {
  if (!entry.ParentCatalog().Cast<BaseCatalog>().Durable()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG(entry.name.GetIdentifierName(),
                            " advanced in a catalog without a log"));
  }
}

void CheckDependencies(SessionTransaction& transaction) {
  const auto view = transaction.View();
  for (const auto& change : transaction.Changes()) {
    if (change.Subject().ParentCatalog().GetAttached().IsReadOnly()) {
      continue;
    }
    switch (change.kind) {
      case ChangeKind::Create:
      case ChangeKind::Alter:
        if (change.set->Members()) {
          change.set->Oids().CheckCreated(*change.version);
        }
        break;
      case ChangeKind::Drop:
        change.set->Oids().CheckDropped(view, change.Subject());
        break;
      case ChangeKind::Rename:
        break;
    }
  }
}

}  // namespace

SessionTransaction::SessionTransaction(duckdb::transaction_t id,
                                       SessionSlot& slot)
  : _id{id},
    _slot{&slot},
    _snapshot{Transactions::Get().Acquire(slot.snapshot)},
    _catalog_epoch{Transactions::Get().CatalogEpoch(Snapshot())},
    _search{id} {}

void SessionTransaction::Refresh() {
  if (HasChanges()) {
    return;
  }
  _search.NoteConflicts(Snapshot());
  if (!_held && !_search.Empty()) {
    _held = Snapshot();
    _slot->held.store(*_held, std::memory_order_seq_cst);
  }
  auto& transactions = Transactions::Get();
  const auto previous = Snapshot();
  if (transactions.LastCommit() == previous) {
    return;
  }
  const auto snapshot = transactions.Acquire(_slot->snapshot);
  _snapshot.store(snapshot, std::memory_order_release);
  _catalog_epoch.store(transactions.CatalogEpoch(snapshot),
                       std::memory_order_release);
  transactions.Ended(previous);
}

void ApplyLsnUpdate(const LsnUpdate& update) {
  if (update.relation != 0) {
    update.entry->MarkRelationSynced(update.relation, update.remote_lsn);
  } else if (update.assign) {
    update.entry->AssignRemoteLsn(update.remote_lsn);
  } else {
    update.entry->RaiseRemoteLsn(update.remote_lsn);
  }
}

void SessionTransaction::PushReplicationLsn(ReplicationLsnEntry& entry,
                                            uint64_t remote_lsn) {
  RequireLog(entry);
  const auto it = absl::c_find_if(_lsn_updates, [&](const LsnUpdate& update) {
    return update.entry == &entry && update.relation == 0;
  });
  if (it == _lsn_updates.end()) {
    _lsn_updates.push_back({.entry = &entry, .remote_lsn = remote_lsn});
  } else if (it->assign) {
    it->remote_lsn = remote_lsn;
  } else {
    it->remote_lsn = std::max(it->remote_lsn, remote_lsn);
  }
}

void SessionTransaction::AssignReplicationLsn(ReplicationLsnEntry& entry,
                                              uint64_t remote_lsn) {
  RequireLog(entry);
  ForgetReplicationLsn(entry);
  _lsn_updates.push_back(
    {.entry = &entry, .remote_lsn = remote_lsn, .assign = true});
}

void SessionTransaction::ForgetReplicationLsn(ReplicationLsnEntry& entry) {
  std::erase_if(_lsn_updates, [&](const LsnUpdate& update) {
    return update.entry == &entry && update.relation == 0;
  });
}

void SessionTransaction::PushRelationSync(ReplicationLsnEntry& entry,
                                          duckdb::idx_t relation,
                                          uint64_t remote_lsn) {
  RequireLog(entry);
  SDB_ASSERT(relation != 0);
  _lsn_updates.push_back(
    {.entry = &entry, .relation = relation, .remote_lsn = remote_lsn});
}

Transactions& Transactions::Get() {
  static Transactions gTransactions;
  return gTransactions;
}

SessionSlot& Transactions::Join() {
  auto* slot = new SessionSlot;
  slot->next = _slots.load(std::memory_order_relaxed);
  while (!_slots.compare_exchange_weak(
    slot->next, slot, std::memory_order_release, std::memory_order_relaxed)) {
  }
  if ((_joined.fetch_add(1, std::memory_order_relaxed) & (kCollectEvery - 1)) ==
      kCollectEvery - 1) {
    RequestCollect();
  }
  return *slot;
}

void Transactions::Leave(SessionSlot& slot) noexcept {
  slot.snapshot.store(kNoSnapshot, std::memory_order_release);
  slot.held.store(kNoSnapshot, std::memory_order_release);
  slot.left.store(true, std::memory_order_release);
}

duckdb::transaction_t Transactions::Acquire(
  std::atomic<duckdb::transaction_t>& slot) const noexcept {
  auto snapshot = _last_commit.load(std::memory_order_acquire);
  for (;;) {
    slot.store(snapshot, std::memory_order_seq_cst);
    const auto published = _last_commit.load(std::memory_order_seq_cst);
    if (published == snapshot) {
      return snapshot;
    }
    snapshot = published;
  }
}

void Transactions::Ended(duckdb::transaction_t snapshot) noexcept {
  if (_garbage_pending.load(std::memory_order_relaxed) &&
      snapshot <= Oldest()) {
    RequestCollect();
  }
}

duckdb::transaction_t Transactions::RefreshOldest() {
  absl::MutexLock lock{&_walk_mutex};
  auto oldest = _last_commit.load(std::memory_order_seq_cst);
  SessionSlot* prev = nullptr;
  auto* slot = _slots.load(std::memory_order_acquire);
  while (slot) {
    auto* next = slot->next;
    if (!slot->left.load(std::memory_order_acquire)) {
      oldest = std::min({oldest, slot->snapshot.load(std::memory_order_seq_cst),
                         slot->held.load(std::memory_order_seq_cst)});
    } else if (prev) {
      prev->next = next;
      delete slot;
      slot = next;
      continue;
    } else if (auto* head = slot; _slots.compare_exchange_strong(
                 head, next, std::memory_order_acq_rel,
                 std::memory_order_acquire)) {
      delete slot;
      slot = next;
      continue;
    }
    prev = slot;
    slot = next;
  }
  auto cached = _oldest.load(std::memory_order_relaxed);
  while (cached < oldest && !_oldest.compare_exchange_weak(
                              cached, oldest, std::memory_order_release,
                              std::memory_order_relaxed)) {
  }
  return std::max(cached, oldest);
}

void Transactions::GarbageQueued() noexcept {
  _garbage_queued.fetch_add(1, std::memory_order_release);
}

void Transactions::RequestCollect() noexcept {
  if (!BackgroundScheduler::gInstance || lifecycle::IsStopping()) {
    return;
  }
  auto state = _collect_state.load(std::memory_order_acquire);
  for (;;) {
    if (state == CollectState::Again) {
      return;
    }
    const auto next =
      state == CollectState::Idle ? CollectState::Running : CollectState::Again;
    if (_collect_state.compare_exchange_weak(
          state, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
      if (next == CollectState::Again) {
        return;
      }
      break;
    }
  }
  try {
    BackgroundScheduler::instance().Run([this] { RunCollector(); }).Detach();
  } catch (...) {
    _collect_state.store(CollectState::Idle, std::memory_order_release);
  }
}

void Transactions::RunCollector() noexcept {
  for (;;) {
    try {
      CollectGarbage();
    } catch (const std::exception& e) {
      SDB_WARN(GENERAL, "collecting garbage failed: ",
               duckdb::ErrorData{e}.RawMessage());
    }
    auto state = CollectState::Running;
    if (_collect_state.compare_exchange_strong(state, CollectState::Idle,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
      return;
    }
    _collect_state.store(CollectState::Running, std::memory_order_release);
  }
}

void Transactions::CollectGarbage() {
  if (lifecycle::IsStopping()) {
    return;
  }
  _garbage_seen.store(_garbage_queued.load(std::memory_order_acquire),
                      std::memory_order_relaxed);
  _garbage_pending.store(false, std::memory_order_relaxed);
  const auto oldest = RefreshOldest();
  Collect(oldest);
  search::CleanupReleased(oldest);
  bool pending = search::ReleasedPending();
  if (!pending) {
    absl::MutexLock lock{&_garbage_mutex};
    pending = !_garbage.empty();
  }
  if (pending) {
    _garbage_pending.store(true, std::memory_order_relaxed);
  }
}

void Transactions::AfterCommit(duckdb::transaction_t commit) noexcept {
  if (_garbage_queued.load(std::memory_order_relaxed) !=
        _garbage_seen.load(std::memory_order_relaxed) ||
      (commit & (kCollectEvery - 1)) == 0) {
    RequestCollect();
  }
}

void Transactions::Collect(duckdb::transaction_t oldest) {
  {
    absl::MutexLock lock{&_garbage_mutex};
    if (_garbage.empty() || _garbage.front().commit > oldest) {
      return;
    }
  }
  std::vector<Garbage> ready;
  {
    absl::MutexLock lock{&_garbage_mutex};
    while (!_garbage.empty() && _garbage.front().commit <= oldest) {
      ready.push_back(std::move(_garbage.front()));
      _garbage.pop_front();
    }
  }
  std::vector<duckdb::shared_ptr<duckdb::AttachedDatabase>> databases;
  std::vector<duckdb::unique_ptr<duckdb::CatalogEntry>> freed;
  for (auto& garbage : ready) {
    auto database = duckdb::AttachedDatabase::TryGetReference(garbage.database);
    if (!database) {
      continue;
    }
    if (auto older = garbage.set->Trim(*garbage.version)) {
      freed.push_back(std::move(older));
    }
    databases.push_back(std::move(database));
  }
  SDB_PARK_ONCE_ON_FAILURE("catalog_collect_before_free");
}

void Transactions::Commit(SessionTransaction& transaction,
                          duckdb::ClientContext& context) {
  auto& writes = transaction.SearchTxn();
  if (transaction.Changes().empty() && transaction.LsnUpdates().empty() &&
      writes.Empty()) {
    return;
  }
  std::vector<uint8_t> record;
  Log::Range range;
  Lsn end = 0;
  duckdb::transaction_t commit = 0;
  {
    absl::MutexLock lock{&_commit_mutex};
    if (_replaying == 0) {
      CheckDependencies(transaction);
    }
    record = WriteRecord(context, transaction, writes);
    if (!record.empty() && _log && _replaying == 0) {
      SDB_IF_FAILURE("crash_before_catalog_commit") { SDB_IMMEDIATE_ABORT(); }
      SDB_IF_FAILURE("catalog_append_fails") {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_IO_ERROR),
          ERR_MSG("catalog log: could not append the transaction"));
      }
      range = _log->Reserve(record.size());
    }
    commit = ++_reserved_commit;
    for (const auto& change : transaction.Changes()) {
      change.version->timestamp = commit;
      change.set->Commit(*change.version);
    }
    {
      absl::MutexLock garbage{&_garbage_mutex};
      for (const auto& change : transaction.Changes()) {
        _garbage.push_back({
          .commit = commit,
          .set = change.set,
          .version = change.version,
          .database =
            change.Subject().ParentCatalog().GetAttached().shared_from_this(),
        });
      }
    }
    for (const auto& change : transaction.Changes()) {
      if (change.kind == ChangeKind::Drop) {
        change.version->Child().OnDrop();
      }
    }
    writes.Apply(commit, Oldest(), range.begin);
    for (const auto& update : transaction.LsnUpdates()) {
      ApplyLsnUpdate(update);
    }
    end = range.end != 0 ? range.end : ReservedEnd();
    Enqueue(commit, end, !transaction.Changes().empty());
  }
  if (range.end != 0) {
    _log->Write(range, record);
    _log->Sync(range.end);
    SDB_IF_FAILURE("crash_after_catalog_before_data") { SDB_IMMEDIATE_ABORT(); }
    SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
    SDB_WAIT_ON_FAILURE("pause_after_catalog_decision");
    MaintainLog(context, range.end);
  } else if (end != 0) {
    _log->Sync(end);
  }
  writes.BeforePublish();
  Publish(commit, true);
  writes.Finish();
  if (!transaction.Changes().empty()) {
    GarbageQueued();
  }
  AfterCommit(commit);
  std::vector<BaseCatalog*> catalogs;
  for (const auto& change : transaction.Changes()) {
    auto* catalog = &change.Subject().ParentCatalog().Cast<BaseCatalog>();
    if (!absl::c_contains(catalogs, catalog)) {
      catalogs.push_back(catalog);
    }
  }
  for (auto* catalog : catalogs) {
    catalog->OnCommit();
  }
}

void Transactions::CommitStorage(
  absl::FunctionRef<std::vector<uint8_t>()> build,
  absl::FunctionRef<void(duckdb::transaction_t, duckdb::transaction_t, Lsn)>
    apply) {
  std::vector<uint8_t> record;
  Log::Range range;
  Lsn end = 0;
  duckdb::transaction_t commit = 0;
  {
    absl::MutexLock lock{&_commit_mutex};
    record = build();
    if (!record.empty() && _log && _replaying == 0) {
      range = _log->Reserve(record.size());
    }
    commit = ++_reserved_commit;
    apply(commit, Oldest(), range.begin);
    end = range.end != 0 ? range.end : ReservedEnd();
    Enqueue(commit, end, false);
  }
  if (range.end != 0) {
    _log->Write(range, record);
  }
  if (end != 0) {
    _log->Sync(end);
  }
  Publish(commit, false);
  AfterCommit(commit);
}

void Transactions::CommitState(absl::FunctionRef<std::vector<uint8_t>()> build,
                               absl::FunctionRef<void(Lsn)> logged) {
  if (!_log || _replaying != 0) {
    return;
  }
  std::vector<uint8_t> record;
  Log::Range range;
  {
    absl::MutexLock lock{&_commit_mutex};
    record = build();
    if (record.empty()) {
      return;
    }
    range = _log->Reserve(record.size());
    logged(range.begin);
  }
  _log->Write(range, record);
  _log->Sync(range.end);
}

void Transactions::MaintainLog(duckdb::ClientContext& context, Lsn end) {
  bool rewrite = false;
  SDB_IF_FAILURE("compact_inside_ddl") { rewrite = true; }
  SDB_IF_FAILURE("compact_inside_drop") { rewrite = true; }
  if (rewrite && !search::IsBackupActive()) {
    if (const auto status = TruncateLog(true); !status.ok()) {
      SDB_WARN(GENERAL, "rewriting the log failed: ", status.message());
    }
    return;
  }
  if (!_log->HasSpare() && end - _log->NewestChunk() > Log::kChunkBytes / 2 &&
      BackgroundScheduler::gInstance &&
      !_preparing_spare.exchange(true, std::memory_order_acq_rel)) {
    BackgroundScheduler::instance()
      .Run([this] {
        absl::Cleanup done = [this] {
          _preparing_spare.store(false, std::memory_order_release);
        };
        try {
          _log->PrepareSpare();
        } catch (const std::exception& e) {
          SDB_WARN(GENERAL, "preparing the next log file failed: ",
                   duckdb::ErrorData{e}.RawMessage());
        }
      })
      .Detach();
  }
  if (end - _log->Oldest() <=
        duckdb::DBConfig::GetConfig(context).options.checkpoint_wal_size ||
      _log->NewestChunk() <= _log->Oldest()) {
    return;
  }
  TruncateInBackground(false);
}

void Transactions::SettleLog() {
  if (_log) {
    TruncateInBackground(true);
  }
}

void Transactions::TruncateInBackground(bool switch_chunk) {
  if (search::IsBackupActive() || !BackgroundScheduler::gInstance ||
      _truncating.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  BackgroundScheduler::instance()
    .Run([this, switch_chunk] {
      absl::Cleanup done = [this] {
        _truncating.store(false, std::memory_order_release);
      };
      if (const auto status = TruncateLog(switch_chunk); !status.ok()) {
        SDB_WARN(GENERAL, "log truncation failed: ", status.message());
      }
    })
    .Detach();
}

absl::Status Transactions::TruncateLog(bool switch_chunk,
                                       duckdb::CheckpointAbort abort) {
  if (!_log || _replaying != 0) {
    return absl::OkStatus();
  }
  const auto pass = _checkpoints.TryEnter();
  if (!pass) {
    return absl::FailedPreconditionError(
      "cannot checkpoint while a backup is in progress");
  }
  absl::MutexLock round{&_truncate_mutex};
  Lsn target = 0;
  {
    absl::MutexLock lock{&_commit_mutex};
    target = switch_chunk ? _log->Switch() : _log->NewestChunk();
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_checkpoint");
  if (abort == duckdb::CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER ||
      abort == duckdb::CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER_NON_FATAL) {
    return absl::AbortedError(
      "Checkpoint aborted before header write because of "
      "debug_checkpoint_abort");
  }
  if (target <= _log->Oldest()) {
    return absl::OkStatus();
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_log_truncate_before_cover");
  search::CoverLog(target);
  SDB_IF_FAILURE("crash_before_log_truncate") { SDB_IMMEDIATE_ABORT(); }
  _log->Sync(target);
  if (abort == duckdb::CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE ||
      abort == duckdb::CheckpointAbort::DEBUG_ABORT_BEFORE_WAL_FINISH) {
    return absl::AbortedError(
      "Checkpoint aborted before truncate because of debug_checkpoint_abort");
  }
  _log->Truncate(target, &CatalogPart, &CompactHistory);
  return absl::OkStatus();
}

Lsn Transactions::ReservedEnd() const {
  return _log && _replaying == 0 ? _log->Reserved() : 0;
}

Lsn Transactions::DurableEnd() const {
  return _log && _replaying == 0 ? _log->Durable()
                                 : std::numeric_limits<Lsn>::max();
}

void Transactions::Enqueue(duckdb::transaction_t commit, Lsn end,
                           bool catalog) {
  absl::MutexLock lock{&_publish_mutex};
  _pending.push_back({.commit = commit, .end = end, .catalog = catalog});
}

void Transactions::Publish(duckdb::transaction_t commit, bool wait) {
  absl::MutexLock lock{&_publish_mutex};
  const auto durable = DurableEnd();
  if (!_pending.empty() && commit >= _pending.front().commit) {
    auto& own = _pending[commit - _pending.front().commit];
    SDB_ASSERT(own.commit == commit);
    own.ready = true;
  }
  duckdb::transaction_t last = 0;
  duckdb::transaction_t catalog = 0;
  while (!_pending.empty() && _pending.front().ready &&
         _pending.front().end <= durable) {
    last = _pending.front().commit;
    if (_pending.front().catalog) {
      catalog = last;
    }
    _pending.pop_front();
  }
  if (catalog != 0) {
    _catalog_version.store(catalog, std::memory_order_release);
  }
  if (last != 0) {
    _last_commit.store(last, std::memory_order_release);
  }
  if (wait) {
    Awaited awaited{.last = &_last_commit, .commit = commit};
    _publish_mutex.Await(absl::Condition(&Published, &awaited));
  }
}

search::StoreSnapshotPtr SessionTransaction::EnsureSearchSnapshot(
  duckdb::idx_t index_id, const std::shared_ptr<search::Store>& storage) {
  SDB_ASSERT(storage);
  absl::MutexLock lock{&_mutex};
  auto& snapshot =
    _search_snapshots.try_emplace(std::pair{storage->GetDatabaseId(), index_id})
      .first->second;
  if (!snapshot) {
    snapshot = storage->Snapshot();
  }
  return snapshot;
}

void SessionTransaction::ResetSearchReaders() noexcept {
  {
    absl::MutexLock lock{&_mutex};
    _search_snapshots.clear();
  }
  _search.ResetReaders();
}

void SessionTransaction::ResetSearchReader(duckdb::idx_t database_id,
                                           duckdb::idx_t shard_id) noexcept {
  _search.ResetReader(database_id, shard_id);
}

void SessionTransaction::ClaimSearchLayout(std::shared_ptr<search::Store> table,
                                           std::string_view name,
                                           search::TableChange change) {
  if (table->AcquireLayout(_id, name, change)) {
    _layout_claims.emplace_back(std::move(table));
  }
}

void SessionTransaction::PrepareCommit(duckdb::ClientContext& context) {
  _search.PrepareCommit(context, Snapshot());
}

void SessionTransaction::Release() noexcept {
  for (const auto& table : _layout_claims) {
    table->ReleaseLayout(_id, _state == SessionState::Committed);
  }
  _layout_claims.clear();
  {
    absl::MutexLock lock{&_mutex};
    _search_snapshots.clear();
  }
  _search.Abort();
  _held.reset();
  _on_commit.clear();
}

void SessionTransaction::Commit(duckdb::ClientContext& context) {
  absl::Cleanup rollback = [this] { Rollback(); };
  Transactions::Get().Commit(*this, context);
  std::move(rollback).Cancel();
  _state = SessionState::Committed;
  absl::Cleanup release = [this] { Release(); };
  for (auto& action : _on_commit) {
    action();
  }
}

void SessionTransaction::Rollback() noexcept {
  for (auto it = _changes.rbegin(); it != _changes.rend(); ++it) {
    it->set->Undo(*it->version, it->kind);
  }
  _changes.clear();
  _change_count.store(0, std::memory_order_release);
  _lsn_updates.clear();
  Release();
  std::vector<duckdb::reference<duckdb::AttachedDatabase>> attached;
  {
    absl::MutexLock lock{&_mutex};
    attached.swap(_attached);
  }
  for (auto& db : attached) {
    auto& manager = duckdb::DatabaseManager::Get(db.get().GetDatabase());
    if (auto detached = manager.DetachInternal(db.get().name)) {
      if (auto deleter = detached->ExtractDeleter()) {
        manager.AddPendingTeardown(std::move(deleter));
      }
    }
  }
  _state = SessionState::RolledBack;
}

DatabaseTransaction::DatabaseTransaction(
  TransactionManager& manager, duckdb::ClientContext& context,
  std::shared_ptr<SessionTransaction> session)
  : duckdb::Transaction{manager, context}, _session{std::move(session)} {}

duckdb::Transaction& TransactionManager::StartTransaction(
  duckdb::ClientContext& context) {
  auto transaction = std::make_unique<DatabaseTransaction>(
    *this, context, ConnectionContext::Get(context).AcquireTransaction());
  auto& result = *transaction;
  absl::MutexLock lock{&_mutex};
  _active.emplace(&result, std::move(transaction));
  return result;
}

void TransactionManager::Remove(duckdb::Transaction& transaction) {
  std::unique_ptr<DatabaseTransaction> removed;
  absl::MutexLock lock{&_mutex};
  const auto it = _active.find(&transaction);
  removed = std::move(it->second);
  _active.erase(it);
}

duckdb::ErrorData TransactionManager::CommitTransaction(
  duckdb::ClientContext& context, duckdb::Transaction& transaction) {
  absl::Cleanup remove = [&] { Remove(transaction); };
  auto& session = transaction.Cast<DatabaseTransaction>().Session();
  if (session.State() != SessionState::Active) {
    return {};
  }
  if (duckdb::Settings::Get<duckdb::DebugForceCommitFailureSetting>(context) &&
      ModifiesUserDatabase(session)) {
    return duckdb::ErrorData{
      duckdb::ExceptionType::INVALID_INPUT,
      "Forced commit failure (debug_force_commit_failure)"};
  }
  try {
    session.Commit(context);
  } catch (const std::exception& e) {
    return duckdb::ErrorData{e};
  }
  return {};
}

void TransactionManager::RollbackTransaction(duckdb::Transaction& transaction) {
  absl::Cleanup remove = [&] { Remove(transaction); };
  auto& session = transaction.Cast<DatabaseTransaction>().Session();
  if (session.State() != SessionState::Active) {
    return;
  }
  if (auto context = transaction.context.lock();
      context && context->transaction.HasActiveTransaction()) {
    return;
  }
  session.Rollback();
}

void TransactionManager::PushAttach(duckdb::Transaction& transaction,
                                    duckdb::AttachedDatabase& attached_db) {
  transaction.Cast<DatabaseTransaction>().Session().Attached(attached_db);
}

void TransactionManager::RefreshStartTime(duckdb::Transaction& transaction) {
  transaction.Cast<DatabaseTransaction>().Session().Refresh();
}

void TransactionManager::Checkpoint(duckdb::ClientContext& context, bool) {
  if (db.IsReadOnly()) {
    return;
  }
  if (search::IsBackupActive()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("cannot checkpoint while a backup is in progress"),
                    ERR_HINT("Retry after pg_backup_stop()."));
  }
  if (const auto status = Transactions::Get().TruncateLog(
        true,
        duckdb::Settings::Get<duckdb::DebugCheckpointAbortSetting>(context));
      !status.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("Checkpoint failed for database ",
                            duckdb::SQLQuotedIdentifier::ToString(db.GetName()),
                            ": ", status.message()));
  }
}

void PushChange(duckdb::CatalogTransaction transaction, Change&& change) {
  if (transaction.transaction) {
    transaction.transaction->Cast<DatabaseTransaction>().Session().Push(
      std::move(change));
  }
}

SessionTransaction& SessionOf(duckdb::ClientContext& context,
                              duckdb::Catalog& catalog) {
  return duckdb::Transaction::Get(context, catalog)
    .Cast<DatabaseTransaction>()
    .Session();
}

}  // namespace sdb::catalog
