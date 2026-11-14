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
#include "runtime/runtime.h"
#include "search/task.h"
#include "search/wal_recovery.h"
#include "server/utils/lifecycle.h"
#include "storage_engine/search_engine.h"

namespace sdb::catalog {
namespace {

class [[nodiscard]] CommitGate final {
 public:
  CommitGate(absl::Mutex& gate, bool exclusive) ABSL_NO_THREAD_SAFETY_ANALYSIS
    : _gate{gate},
      _exclusive{exclusive} {
    if (_exclusive) {
      _gate.Lock();
    } else {
      _gate.ReaderLock();
    }
  }
  ~CommitGate() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    if (_exclusive) {
      _gate.Unlock();
    } else {
      _gate.ReaderUnlock();
    }
  }
  CommitGate(const CommitGate&) = delete;
  CommitGate& operator=(const CommitGate&) = delete;

 private:
  absl::Mutex& _gate;
  bool _exclusive;
};

class [[nodiscard]] TableLocks final {
 public:
  explicit TableLocks(std::vector<absl::Mutex*> mutexes)
    ABSL_NO_THREAD_SAFETY_ANALYSIS : _mutexes{std::move(mutexes)} {
    for (auto* mutex : _mutexes) {
      mutex->Lock();
    }
  }
  ~TableLocks() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    for (auto it = _mutexes.rbegin(); it != _mutexes.rend(); ++it) {
      (*it)->Unlock();
    }
  }
  TableLocks(const TableLocks&) = delete;
  TableLocks& operator=(const TableLocks&) = delete;

 private:
  std::vector<absl::Mutex*> _mutexes;
};

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
    _search{id, _snapshot, slot.walks} {}

void SessionTransaction::Refresh() {
  if (HasChanges()) {
    return;
  }
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

void ApplyLsnUpdate(const LsnUpdate& update, duckdb::transaction_t commit) {
  if (update.relation != 0) {
    update.entry->MarkRelationSynced(update.relation, update.remote_lsn,
                                     commit);
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
  _live_slots.fetch_add(1, std::memory_order_relaxed);
  return *slot;
}

void Transactions::Leave(SessionSlot& slot) noexcept {
  slot.snapshot.store(kNoSnapshot, std::memory_order_seq_cst);
  slot.held.store(kNoSnapshot, std::memory_order_seq_cst);
  slot.left.store(true, std::memory_order_release);
  const auto live = _live_slots.fetch_sub(1, std::memory_order_relaxed) - 1;
  const auto dead = _dead_slots.fetch_add(1, std::memory_order_relaxed) + 1;
  if (dead > std::max(kMinDeadSlots, live)) {
    RequestCollect();
  }
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
  if (_garbage_pending.load(std::memory_order_seq_cst) &&
      snapshot <= _oldest.load(std::memory_order_seq_cst)) {
    RequestCollect();
  }
}

WalkGuard::WalkGuard(WalkCounters& walks) noexcept {
  auto& transactions = Transactions::Get();
  for (;;) {
    const auto epoch = transactions.Epoch();
    auto& counter = walks.parity[epoch & 1];
    counter.fetch_add(1, std::memory_order_seq_cst);
    if (transactions.Epoch() == epoch) {
      _counter = &counter;
      return;
    }
    counter.fetch_sub(1, std::memory_order_release);
  }
}

template<typename Fn>
void Transactions::WalkSlots(Fn&& fn) {
  SessionSlot* prev = nullptr;
  auto* slot = _slots.load(std::memory_order_acquire);
  while (slot) {
    auto* next = slot->next;
    if (!slot->left.load(std::memory_order_acquire)) {
      fn(*slot);
    } else if (prev) {
      prev->next = next;
      delete slot;
      _dead_slots.fetch_sub(1, std::memory_order_relaxed);
      slot = next;
      continue;
    } else if (auto* head = slot; _slots.compare_exchange_strong(
                 head, next, std::memory_order_acq_rel,
                 std::memory_order_acquire)) {
      delete slot;
      _dead_slots.fetch_sub(1, std::memory_order_relaxed);
      slot = next;
      continue;
    }
    prev = slot;
    slot = next;
  }
}

bool Transactions::Drained(uint64_t parity) {
  if (_shared_walks.parity[parity].load(std::memory_order_seq_cst) != 0) {
    return false;
  }
  bool drained = true;
  WalkSlots([&](const SessionSlot& slot) {
    drained =
      drained && slot.walks.parity[parity].load(std::memory_order_seq_cst) == 0;
  });
  return drained;
}

void Transactions::AdvanceEpoch() {
  absl::MutexLock lock{&_walk_mutex};
  for (int flips = 0; flips < 2; ++flips) {
    const auto epoch = _epoch.load(std::memory_order_relaxed);
    if (!Drained((epoch + 1) & 1)) {
      return;
    }
    _epoch.store(epoch + 1, std::memory_order_seq_cst);
    _safe_epoch.store(epoch - 1, std::memory_order_release);
  }
}

Transactions::Running Transactions::RunningSnapshots() {
  absl::MutexLock lock{&_walk_mutex};
  Running running;
  running.published = _last_commit.load(std::memory_order_seq_cst);
  std::vector<Released> holders;
  const bool per_store = !_released.empty();
  WalkSlots([&](const SessionSlot& slot) {
    for (const auto* field : {&slot.snapshot, &slot.held}) {
      if (const auto snapshot = field->load(std::memory_order_seq_cst);
          snapshot != kNoSnapshot) {
        running.snapshots.push_back(snapshot);
        if (per_store) {
          holders.push_back({.slot = &slot, .snapshot = snapshot});
        }
      }
    }
  });
  const auto sorted = [](std::vector<duckdb::transaction_t>& snapshots) {
    absl::c_sort(snapshots);
    snapshots.erase(std::ranges::unique(snapshots).begin(), snapshots.end());
  };
  sorted(running.snapshots);
  for (const auto& released : _released) {
    if (search::FindSnapshots(running.stores, released.store)) {
      continue;
    }
    auto& own = running.stores.emplace_back(search::StoreSnapshots{
      .store = released.store, .oldest = running.published});
    for (const auto& holder : holders) {
      if (absl::c_none_of(_released, [&](const Released& mine) {
            return mine.store == released.store && mine.slot == holder.slot &&
                   mine.snapshot == holder.snapshot;
          })) {
        own.running.push_back(holder.snapshot);
        own.oldest = std::min(own.oldest, holder.snapshot);
      }
    }
    sorted(own.running);
  }
  return running;
}

void Transactions::ReleaseVersions(
  duckdb::idx_t store, const SessionSlot& slot,
  std::span<const duckdb::transaction_t> snapshots) {
  absl::MutexLock lock{&_walk_mutex};
  for (const auto snapshot : snapshots) {
    _released.push_back({.store = store, .slot = &slot, .snapshot = snapshot});
  }
}

void Transactions::KeepVersions(const SessionSlot& slot) {
  absl::MutexLock lock{&_walk_mutex};
  std::erase_if(_released, [&](const Released& released) {
    return released.slot == &slot;
  });
}

duckdb::transaction_t Transactions::RefreshOldest() {
  absl::MutexLock lock{&_walk_mutex};
  auto oldest = _last_commit.load(std::memory_order_seq_cst);
  WalkSlots([&](const SessionSlot& slot) {
    oldest = std::min({oldest, slot.snapshot.load(std::memory_order_seq_cst),
                       slot.held.load(std::memory_order_seq_cst)});
  });
  auto cached = _oldest.load(std::memory_order_relaxed);
  while (cached < oldest && !_oldest.compare_exchange_weak(
                              cached, oldest, std::memory_order_seq_cst)) {
  }
  return std::max(cached, oldest);
}

void Transactions::GarbageQueued() noexcept {
  _garbage_queued.fetch_add(1, std::memory_order_release);
  _garbage_pending.store(true, std::memory_order_seq_cst);
}

void Transactions::RequestCollect() noexcept {
  if (!Runtime::Current() || lifecycle::IsStopping()) {
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
    Runtime::Get().Run([this] { RunCollector(); }).Detach();
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
  for (auto oldest = RefreshOldest();;) {
    Collect(oldest);
    const auto running = RunningSnapshots();
    search::CleanupReleased(oldest, running.stores);
    search::PruneVersions(running.snapshots, running.published, running.stores);
    AdvanceEpoch();
    bool pending = search::FreeVersions();
    pending = pending || search::ReleasedPending();
    if (!pending) {
      absl::MutexLock lock{&_garbage_mutex};
      pending = !_garbage.empty();
    }
    _garbage_pending.store(pending, std::memory_order_seq_cst);
    if (!pending) {
      return;
    }
    const auto again = RefreshOldest();
    if (again <= oldest) {
      return;
    }
    oldest = again;
  }
}

void Transactions::AfterCommit() noexcept {
  if (_garbage_queued.load(std::memory_order_relaxed) !=
      _garbage_seen.load(std::memory_order_relaxed)) {
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
  const auto layouts = !_replaying
                         ? transaction.LayoutClaims()
                         : std::span<const std::shared_ptr<search::Store>>{};
  {
    const CommitGate gate{_commit_gate, !transaction.Changes().empty() ||
                                          !transaction.LsnUpdates().empty() ||
                                          writes.CrossTable()};
    const TableLocks tables{writes.CommitMutexes(layouts)};
    if (!_replaying) {
      CheckDependencies(transaction);
    }
    record = WriteRecord(context, transaction, writes);
    const bool logged = !record.empty() && _log && !_replaying;
    if (logged) {
      SDB_IF_FAILURE("crash_before_catalog_commit") { SDB_IMMEDIATE_ABORT(); }
      SDB_IF_FAILURE("catalog_append_fails") {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_IO_ERROR),
          ERR_MSG("catalog log: could not append the transaction"));
      }
    }
    {
      absl::MutexLock sequence{&_sequence_mutex};
      if (logged) {
        range = _log->Reserve(record.size());
      }
      commit = ++_reserved_commit;
      end = range.end != 0 ? range.end : ReservedEnd();
      _commit_end[commit % kPublishRing] = end;
    }
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
    writes.Apply(commit, layouts);
  }
  if (range.end != 0) {
    _log->Write(range, record);
    _log->Sync(range.end);
    SDB_IF_FAILURE("crash_after_catalog_before_data") { SDB_IMMEDIATE_ABORT(); }
    SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
    SDB_WAIT_ON_FAILURE("pause_after_catalog_decision");
    MaintainLog(range.end);
  } else if (end != 0) {
    _log->Sync(end);
  }
  if (!transaction.LsnUpdates().empty()) {
    SDB_PARK_ONCE_ON_FAILURE("pause_lsn_commit_before_publish");
  }
  writes.BeforePublish();
  Publish(commit, !transaction.Changes().empty(), true);
  for (const auto& update : transaction.LsnUpdates()) {
    ApplyLsnUpdate(update, commit);
  }
  writes.Finish();
  if (!transaction.Changes().empty()) {
    GarbageQueued();
  }
  AfterCommit();
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
  absl::Mutex& table, absl::FunctionRef<std::vector<uint8_t>()> build,
  absl::FunctionRef<void(duckdb::transaction_t)> apply) {
  std::vector<uint8_t> record;
  Log::Range range;
  Lsn end = 0;
  duckdb::transaction_t commit = 0;
  {
    absl::ReaderMutexLock gate{&_commit_gate};
    absl::MutexLock lock{&table};
    record = build();
    {
      absl::MutexLock sequence{&_sequence_mutex};
      if (!record.empty() && _log && !_replaying) {
        range = _log->Reserve(record.size());
      }
      commit = ++_reserved_commit;
      end = range.end != 0 ? range.end : ReservedEnd();
      _commit_end[commit % kPublishRing] = end;
    }
    apply(commit);
  }
  if (range.end != 0) {
    _log->Write(range, record);
    _log->Sync(range.end);
  }
  Publish(commit, false, false);
  AfterCommit();
}

void Transactions::MaintainLog(Lsn end) {
  bool rewrite = false;
  SDB_IF_FAILURE("compact_inside_ddl") { rewrite = true; }
  SDB_IF_FAILURE("compact_inside_drop") { rewrite = true; }
  if (rewrite && !search::IsBackupActive()) {
    if (const auto status = TruncateLog(); !status.ok()) {
      SDB_WARN(GENERAL, "rewriting the log failed: ", status.message());
    }
    return;
  }
  if (!_log->HasNext() && end - _log->NewestChunk() > Log::kChunkBytes / 2 &&
      Runtime::Current() &&
      !_preparing_next.exchange(true, std::memory_order_acq_rel)) {
    Runtime::Get()
      .Run([this] {
        absl::Cleanup done = [this] {
          _preparing_next.store(false, std::memory_order_release);
        };
        try {
          _log->PrepareNext();
        } catch (const std::exception& e) {
          SDB_WARN(GENERAL, "preparing the next log file failed: ",
                   duckdb::ErrorData{e}.RawMessage());
        }
      })
      .Detach();
  }
  const auto covered = _log->Covered();
  if (end <= covered ||
      end - covered <=
        std::max<uint64_t>(_max_log_bytes.load(std::memory_order_relaxed),
                           _log->SnapshotBytes()) ||
      _log->NewestChunk() <= _log->Oldest()) {
    return;
  }
  TruncateInBackground();
}

void Transactions::SettleLog() {
  if (!_log) {
    return;
  }
  const auto covered = _log->Covered();
  const auto end = _log->End();
  if (end > covered && end - covered >= _log->SnapshotBytes()) {
    TruncateInBackground();
  }
}

void Transactions::TruncateInBackground() {
  if (search::IsBackupActive() || !Runtime::Current() ||
      _truncating.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  Runtime::Get()
    .Run([this] {
      absl::Cleanup done = [this] {
        _truncating.store(false, std::memory_order_release);
      };
      if (const auto status = TruncateLog(); !status.ok()) {
        SDB_WARN(GENERAL, "log truncation failed: ", status.message());
      }
    })
    .Detach();
}

absl::Status Transactions::TruncateLog(duckdb::CheckpointAbort abort) {
  if (!_log || _replaying) {
    return absl::OkStatus();
  }
  const auto pass = _checkpoints.TryEnter();
  if (!pass) {
    return absl::FailedPreconditionError(
      "cannot checkpoint while a backup is in progress");
  }
  absl::MutexLock round{&_truncate_mutex};
  SDB_PARK_ONCE_ON_FAILURE("pause_checkpoint");
  if (abort == duckdb::CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER ||
      abort == duckdb::CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER_NON_FATAL) {
    return absl::AbortedError(
      "Checkpoint aborted before header write because of "
      "debug_checkpoint_abort");
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_log_truncate_before_cover");
  Lsn covered = 0;
  std::vector<search::TableSnapshot> tables;
  {
    const CommittedView view;
    const auto end = CommitEnd(view.Snapshot());
    if (!end || *end <= _log->Covered()) {
      return absl::OkStatus();
    }
    covered = *end;
    SDB_PARK_ONCE_ON_FAILURE("pause_cover_before_collect");
    tables = search::CaptureSnapshot(view);
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_cover_after_collect");
  const auto records = search::SnapshotRecords(tables);
  tables.clear();
  _log->Sync(covered);
  if (abort == duckdb::CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE ||
      abort == duckdb::CheckpointAbort::DEBUG_ABORT_BEFORE_WAL_FINISH) {
    return absl::AbortedError(
      "Checkpoint aborted before truncate because of debug_checkpoint_abort");
  }
  _log->WriteSnapshot(covered, records, &CatalogPart, &CompactHistory);
  SDB_IF_FAILURE("crash_before_log_truncate") { SDB_IMMEDIATE_ABORT(); }
  _log->DropCovered();
  return absl::OkStatus();
}

Lsn Transactions::ReservedEnd() const {
  return _log && !_replaying ? _log->End() : 0;
}

void Transactions::Open(std::unique_ptr<Log> log) {
  absl::MutexLock sequence{&_sequence_mutex};
  _opened_commit = _reserved_commit;
  _opened_end = log->End();
  _log = std::move(log);
}

void Transactions::Close() {
  absl::MutexLock sequence{&_sequence_mutex};
  _log.reset();
}

std::optional<Lsn> Transactions::CommitEnd(duckdb::transaction_t commit) {
  absl::MutexLock sequence{&_sequence_mutex};
  if (commit <= _opened_commit) {
    return _opened_end;
  }
  if (_reserved_commit - commit >= kPublishRing) {
    return std::nullopt;
  }
  return _commit_end[commit % kPublishRing];
}

void Transactions::Publish(duckdb::transaction_t commit, bool catalog,
                           bool wait) {
  SDB_ASSERT(commit - LastCommit() < kPublishRing);
  _ready[commit % kPublishRing].store(commit << 1 | uint64_t{catalog},
                                      std::memory_order_seq_cst);
  auto published = _last_commit.load(std::memory_order_seq_cst);
  bool advanced = false;
  for (;;) {
    const auto next = published + 1;
    const auto mark =
      _ready[next % kPublishRing].load(std::memory_order_seq_cst);
    if (mark >> 1 != next) {
      break;
    }
    if (mark & 1) {
      auto version = _catalog_version.load(std::memory_order_relaxed);
      while (version < next && !_catalog_version.compare_exchange_weak(
                                 version, next, std::memory_order_release,
                                 std::memory_order_relaxed)) {
      }
    }
    if (_last_commit.compare_exchange_weak(published, next,
                                           std::memory_order_seq_cst)) {
      published = next;
      advanced = true;
    }
  }
  if (advanced) {
    _last_commit.notify_all();
  }
  if (wait) {
    AwaitPublished(commit);
  }
}

void Transactions::AwaitPublished(duckdb::transaction_t commit) const {
  for (auto seen = _last_commit.load(std::memory_order_acquire); seen < commit;
       seen = _last_commit.load(std::memory_order_acquire)) {
    _last_commit.wait(seen, std::memory_order_acquire);
  }
}

void SessionTransaction::ClaimSearchLayout(std::shared_ptr<search::Store> table,
                                           std::string_view name,
                                           search::TableChange change) {
  if (table->AcquireLayout(_id, name, change)) {
    _layout_claims.emplace_back(std::move(table));
  }
}

std::shared_ptr<const InvertedIndexConfig> SessionTransaction::ScanConfig(
  duckdb::ClientContext& context, search::Store& store) {
  const auto view = _search.Read(context, store);
  if (absl::c_any_of(
        _layout_claims,
        [&](const auto& claimed) { return claimed.get() == &store; }) &&
      view.state->tick >= store.GetTable().LastRewrite()) {
    if (auto own = store.OwnScanConfig(_id)) {
      return own;
    }
  }
  return view.state->config;
}

void SessionTransaction::ReleaseVersions(
  const std::shared_ptr<search::Store>& store) {
  _search.Adopting(store);
  std::vector<duckdb::transaction_t> snapshots{Snapshot()};
  if (_held) {
    snapshots.push_back(*_held);
  }
  Transactions::Get().ReleaseVersions(store->GetId(), *_slot, snapshots);
  _released_versions = true;
}

void SessionTransaction::PrepareCommit(duckdb::ClientContext& context) {
  _search.PrepareCommit(context);
}

void SessionTransaction::Release() noexcept {
  if (std::exchange(_released_versions, false)) {
    Transactions::Get().KeepVersions(*_slot);
  }
  for (const auto& table : _layout_claims) {
    table->ReleaseLayout(_id, _state == SessionState::Committed);
  }
  _layout_claims.clear();
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
