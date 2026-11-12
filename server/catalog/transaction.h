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

#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/functional/any_invocable.h>
#include <absl/functional/function_ref.h>
#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <duckdb/common/enums/checkpoint_abort.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/transaction/transaction.hpp>
#include <duckdb/transaction/transaction_manager.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/replication_lsn.h"
#include "catalog/entry_set.h"
#include "catalog/log.h"
#include "catalog/walk.h"
#include "search/store.h"
#include "search/writes.h"
#include "utils/drain_gate.h"

namespace sdb::catalog {

struct Change {
  duckdb::CatalogEntry& Subject() const {
    return kind == ChangeKind::Drop || kind == ChangeKind::Rename
             ? version->Child()
             : *version;
  }

  EntrySet* set = nullptr;
  duckdb::CatalogEntry* version = nullptr;
  ChangeKind kind = ChangeKind::Create;
  std::string alter;
};

struct LsnUpdate {
  ReplicationLsnEntry* entry = nullptr;
  duckdb::idx_t relation = 0;
  uint64_t remote_lsn = 0;
  bool assign = false;
};

void ApplyLsnUpdate(const LsnUpdate& update, duckdb::transaction_t commit);

enum class SessionState : uint8_t {
  Active,
  Committed,
  RolledBack,
};

inline constexpr duckdb::transaction_t kNoSnapshot =
  std::numeric_limits<duckdb::transaction_t>::max();

struct alignas(64) SessionSlot {
  std::atomic<duckdb::transaction_t> snapshot{kNoSnapshot};
  std::atomic<duckdb::transaction_t> held{kNoSnapshot};
  WalkCounters walks;
  std::atomic<bool> left{false};
  SessionSlot* next = nullptr;
};

class SessionTransaction final {
 public:
  SessionTransaction(duckdb::transaction_t id, SessionSlot& slot);

  duckdb::transaction_t Id() const noexcept { return _id; }

  duckdb::transaction_t Snapshot() const noexcept {
    return _snapshot.load(std::memory_order_acquire);
  }

  duckdb::SnapshotView View() const {
    return duckdb::SnapshotView{_id,
                                duckdb::VisibilityBound::Through(Snapshot())};
  }

  duckdb::idx_t CatalogEpoch() const noexcept {
    const auto changes = _change_count.load(std::memory_order_acquire);
    return changes == 0 ? _catalog_epoch.load(std::memory_order_acquire)
                        : duckdb::TRANSACTION_ID_START + changes;
  }

  void Refresh();

  void Push(Change change) {
    _changes.push_back(std::move(change));
    _change_count.store(_changes.size(), std::memory_order_release);
  }
  bool HasChanges() const noexcept {
    return _change_count.load(std::memory_order_acquire) != 0;
  }
  const std::vector<Change>& Changes() const noexcept { return _changes; }

  void PushReplicationLsn(ReplicationLsnEntry& entry, uint64_t remote_lsn);
  void AssignReplicationLsn(ReplicationLsnEntry& entry, uint64_t remote_lsn);
  void ForgetReplicationLsn(ReplicationLsnEntry& entry);
  void PushRelationSync(ReplicationLsnEntry& entry, duckdb::idx_t relation,
                        uint64_t remote_lsn);
  const std::vector<LsnUpdate>& LsnUpdates() const noexcept {
    return _lsn_updates;
  }

  void Attached(duckdb::AttachedDatabase& db) {
    absl::MutexLock lock{&_mutex};
    _attached.emplace_back(db);
  }

  search::Writes& SearchTxn() noexcept { return _search; }

  void ResetSearchPins(bool all) noexcept { _search.ResetPins(all); }

  void DeferToCommit(absl::AnyInvocable<void()> action) {
    _on_commit.push_back(std::move(action));
  }

  void ClaimSearchLayout(std::shared_ptr<search::Store> table,
                         std::string_view name, search::TableChange change);
  std::span<const std::shared_ptr<search::Store>> LayoutClaims()
    const noexcept {
    return _layout_claims;
  }
  std::shared_ptr<const InvertedIndexConfig> ScanConfig(
    duckdb::ClientContext& context, search::Store& store);
  void ReleaseVersions(const std::shared_ptr<search::Store>& store);

  SessionState State() const noexcept { return _state; }

  void PrepareCommit(duckdb::ClientContext& context);
  void Commit(duckdb::ClientContext& context);
  void Rollback() noexcept;

 private:
  void Release() noexcept;

  duckdb::transaction_t _id;
  SessionSlot* _slot;
  std::atomic<duckdb::transaction_t> _snapshot;
  std::atomic<duckdb::idx_t> _catalog_epoch;
  std::optional<duckdb::transaction_t> _held;
  std::vector<Change> _changes;
  std::atomic<size_t> _change_count{0};
  std::vector<LsnUpdate> _lsn_updates;
  absl::Mutex _mutex;
  std::vector<duckdb::reference<duckdb::AttachedDatabase>> _attached
    ABSL_GUARDED_BY(_mutex);
  search::Writes _search;
  std::vector<absl::AnyInvocable<void()>> _on_commit;
  std::vector<std::shared_ptr<search::Store>> _layout_claims;
  bool _released_versions = false;
  SessionState _state = SessionState::Active;
};

class Transactions final {
 public:
  static Transactions& Get();

  duckdb::transaction_t LastCommit() const noexcept {
    return _last_commit.load(std::memory_order_acquire);
  }

  duckdb::idx_t CatalogVersion() const noexcept {
    return _catalog_version.load(std::memory_order_acquire);
  }

  duckdb::idx_t CatalogEpoch(duckdb::transaction_t snapshot) const noexcept {
    const auto version = CatalogVersion();
    return version <= snapshot ? version : snapshot;
  }

  duckdb::transaction_t NextId() noexcept {
    return _next_id.fetch_add(1, std::memory_order_relaxed);
  }

  void Open(std::unique_ptr<Log> log) ABSL_LOCKS_EXCLUDED(_sequence_mutex);
  Log* GetLog() noexcept { return _log.get(); }

  void SetReplaying(bool replaying) noexcept { _replaying = replaying; }
  std::optional<Lsn> CommitEnd(duckdb::transaction_t commit)
    ABSL_LOCKS_EXCLUDED(_sequence_mutex);

  void Commit(SessionTransaction& transaction, duckdb::ClientContext& context);
  void CommitStorage(absl::Mutex& table,
                     absl::FunctionRef<std::vector<uint8_t>()> build,
                     absl::FunctionRef<void(duckdb::transaction_t)> apply);
  void AwaitPublished(duckdb::transaction_t commit) const;

  static constexpr uint64_t kDefaultMaxLogBytes = 1'000'000'000;

  void SetMaxLogBytes(uint64_t bytes) noexcept {
    _max_log_bytes.store(bytes, std::memory_order_relaxed);
  }

  absl::Status TruncateLog(
    duckdb::CheckpointAbort abort = duckdb::CheckpointAbort::NO_ABORT);
  void SettleLog();
  [[nodiscard]] utils::DrainGate::Closed CloseCheckpoints(
    duckdb::ClientContext& context) {
    return _checkpoints.Close(context);
  }

  SessionSlot& Join();
  void Leave(SessionSlot& slot) noexcept;
  duckdb::transaction_t Acquire(
    std::atomic<duckdb::transaction_t>& slot) const noexcept;
  void Ended(duckdb::transaction_t snapshot) noexcept;

  duckdb::transaction_t Oldest() const noexcept {
    return _oldest.load(std::memory_order_acquire);
  }
  duckdb::transaction_t RefreshOldest() ABSL_LOCKS_EXCLUDED(_walk_mutex);
  void GarbageQueued() noexcept;
  void CollectGarbage();

  struct Running {
    duckdb::transaction_t published = 0;
    std::vector<duckdb::transaction_t> snapshots;
    std::vector<search::StoreSnapshots> stores;
  };
  Running RunningSnapshots() ABSL_LOCKS_EXCLUDED(_walk_mutex);
  void ReleaseVersions(duckdb::idx_t store, const SessionSlot& slot,
                       std::span<const duckdb::transaction_t> snapshots)
    ABSL_LOCKS_EXCLUDED(_walk_mutex);
  void KeepVersions(const SessionSlot& slot) ABSL_LOCKS_EXCLUDED(_walk_mutex);

  uint64_t Epoch() const noexcept {
    return _epoch.load(std::memory_order_seq_cst);
  }
  uint64_t SafeEpoch() const noexcept {
    return _safe_epoch.load(std::memory_order_acquire);
  }
  WalkCounters& SharedWalks() noexcept { return _shared_walks; }
  void AdvanceEpoch() ABSL_LOCKS_EXCLUDED(_walk_mutex);
  void RequestCollect() noexcept;

 private:
  struct Garbage {
    duckdb::transaction_t commit;
    EntrySet* set;
    duckdb::CatalogEntry* version;
    duckdb::weak_ptr<duckdb::AttachedDatabase> database;
  };
  enum class CollectState : uint8_t {
    Idle,
    Running,
    Again,
  };
  struct Released {
    duckdb::idx_t store = 0;
    const SessionSlot* slot = nullptr;
    duckdb::transaction_t snapshot = 0;
  };
  static constexpr uint64_t kPublishRing = uint64_t{1} << 16;

  Lsn ReservedEnd() const;
  void Publish(duckdb::transaction_t commit, bool catalog, bool wait);
  void MaintainLog(Lsn end);
  void TruncateInBackground();

  static constexpr int64_t kMinDeadSlots = 64;

  void RunCollector() noexcept;
  void AfterCommit() noexcept;
  void Collect(duckdb::transaction_t oldest);
  template<typename Fn>
  void WalkSlots(Fn&& fn) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_walk_mutex);
  bool Drained(uint64_t parity) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_walk_mutex);

  std::atomic<uint64_t> _epoch{2};
  std::atomic<uint64_t> _safe_epoch{0};
  WalkCounters _shared_walks;
  std::atomic<SessionSlot*> _slots{nullptr};
  std::atomic<int64_t> _live_slots{0};
  std::atomic<int64_t> _dead_slots{0};
  absl::Mutex _walk_mutex;
  std::vector<Released> _released ABSL_GUARDED_BY(_walk_mutex);
  std::atomic<duckdb::transaction_t> _oldest{
    duckdb::SYSTEM_TRANSACTION_TIMESTAMP};
  std::atomic<uint64_t> _garbage_queued{0};
  std::atomic<uint64_t> _garbage_seen{0};
  std::atomic<bool> _garbage_pending{false};
  std::atomic<CollectState> _collect_state{CollectState::Idle};
  absl::Mutex _garbage_mutex;
  std::deque<Garbage> _garbage ABSL_GUARDED_BY(_garbage_mutex);
  absl::Mutex _commit_gate;
  absl::Mutex _sequence_mutex;
  duckdb::transaction_t _reserved_commit ABSL_GUARDED_BY(_sequence_mutex) =
    duckdb::SYSTEM_TRANSACTION_TIMESTAMP;
  std::array<Lsn, kPublishRing> _commit_end ABSL_GUARDED_BY(_sequence_mutex){};
  duckdb::transaction_t _opened_commit ABSL_GUARDED_BY(_sequence_mutex) = 0;
  Lsn _opened_end ABSL_GUARDED_BY(_sequence_mutex) = 0;
  std::array<std::atomic<uint64_t>, kPublishRing> _ready{};
  std::atomic<duckdb::transaction_t> _last_commit{
    duckdb::SYSTEM_TRANSACTION_TIMESTAMP};
  std::atomic<duckdb::transaction_t> _next_id{duckdb::TRANSACTION_ID_START};
  std::atomic<duckdb::idx_t> _catalog_version{
    duckdb::SYSTEM_TRANSACTION_TIMESTAMP};
  std::unique_ptr<Log> _log;
  bool _replaying = false;
  std::atomic<uint64_t> _max_log_bytes{kDefaultMaxLogBytes};
  std::atomic<bool> _truncating{false};
  std::atomic<bool> _preparing_next{false};
  absl::Mutex _truncate_mutex;
  utils::DrainGate _checkpoints;
};

class CommittedView final {
 public:
  CommittedView()
    : _slot{Transactions::Get().Join()},
      _snapshot{Transactions::Get().Acquire(_slot.snapshot)} {}
  ~CommittedView() {
    auto& transactions = Transactions::Get();
    transactions.Leave(_slot);
    transactions.Ended(_snapshot);
  }
  CommittedView(const CommittedView&) = delete;
  CommittedView& operator=(const CommittedView&) = delete;

  duckdb::transaction_t Snapshot() const noexcept { return _snapshot; }
  WalkCounters& Walks() const noexcept { return _slot.walks; }

  duckdb::SnapshotView View() const {
    return duckdb::SnapshotView{duckdb::TRANSACTION_ID_START - 1,
                                duckdb::VisibilityBound::Through(_snapshot)};
  }

  duckdb::CatalogTransaction Transaction(duckdb::DatabaseInstance& db) const {
    return duckdb::CatalogTransaction{
      db, duckdb::TRANSACTION_ID_START - 1,
      duckdb::VisibilityBound::Through(_snapshot)};
  }

 private:
  SessionSlot& _slot;
  duckdb::transaction_t _snapshot;
};

class TransactionManager;

class DatabaseTransaction final : public duckdb::Transaction {
 public:
  DatabaseTransaction(TransactionManager& manager,
                      duckdb::ClientContext& context,
                      std::shared_ptr<SessionTransaction> session);

  duckdb::SnapshotView GetSnapshotView() const final {
    return _session->View();
  }

  SessionTransaction& Session() const noexcept { return *_session; }

 private:
  std::shared_ptr<SessionTransaction> _session;
};

class TransactionManager final : public duckdb::TransactionManager {
 public:
  explicit TransactionManager(duckdb::AttachedDatabase& db)
    : duckdb::TransactionManager{db} {}

  duckdb::Transaction& StartTransaction(duckdb::ClientContext& context) final;
  duckdb::ErrorData CommitTransaction(duckdb::ClientContext& context,
                                      duckdb::Transaction& transaction) final;
  void RollbackTransaction(duckdb::Transaction& transaction) final;
  void Checkpoint(duckdb::ClientContext& context, bool force) final;
  bool CommitsAcrossDatabases() final { return true; }
  void PushAttach(duckdb::Transaction& transaction,
                  duckdb::AttachedDatabase& attached_db) final;
  void RefreshStartTime(duckdb::Transaction& transaction) final;

 private:
  void Remove(duckdb::Transaction& transaction);

  absl::Mutex _mutex;
  absl::flat_hash_map<const duckdb::Transaction*,
                      std::unique_ptr<DatabaseTransaction>>
    _active ABSL_GUARDED_BY(_mutex);
};

void PushChange(duckdb::CatalogTransaction transaction, Change&& change);

SessionTransaction& SessionOf(duckdb::ClientContext& context,
                              duckdb::Catalog& catalog);

}  // namespace sdb::catalog
