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
#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/functional/any_invocable.h>
#include <absl/functional/function_ref.h>
#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <duckdb/common/enums/checkpoint_abort.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/transaction/transaction.hpp>
#include <duckdb/transaction/transaction_manager.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/replication_lsn.h"
#include "catalog/entry_set.h"
#include "catalog/log.h"
#include "search/store.h"
#include "search/writes.h"

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

void ApplyLsnUpdate(const LsnUpdate& update);

enum class SessionState : uint8_t {
  Active,
  Committed,
  RolledBack,
};

class SessionTransaction final {
 public:
  explicit SessionTransaction(duckdb::transaction_t id);
  ~SessionTransaction();

  duckdb::transaction_t Id() const noexcept { return _id; }

  duckdb::transaction_t Snapshot() const noexcept {
    return _snapshot.load(std::memory_order_acquire);
  }

  duckdb::SnapshotView View() const {
    return duckdb::SnapshotView{_id,
                                duckdb::VisibilityBound::Through(Snapshot())};
  }

  void Refresh();

  void Push(Change change) { _changes.push_back(std::move(change)); }
  const std::vector<Change>& Changes() const noexcept { return _changes; }

  void PushReplicationLsn(ReplicationLsnEntry& entry, uint64_t remote_lsn);
  void AssignReplicationLsn(ReplicationLsnEntry& entry, uint64_t remote_lsn);
  void ForgetReplicationLsn(ReplicationLsnEntry& entry);
  void PushRelationSync(ReplicationLsnEntry& entry, duckdb::idx_t relation,
                        uint64_t remote_lsn);
  const std::vector<LsnUpdate>& LsnUpdates() const noexcept {
    return _lsn_updates;
  }

  void Attached(duckdb::AttachedDatabase& db) { _attached.emplace_back(db); }

  search::Writes& SearchTxn() {
    if (!_search) {
      _search.emplace(_id);
    }
    return *_search;
  }
  search::Writes* SearchWrites() noexcept {
    return _search ? &*_search : nullptr;
  }

  search::StoreSnapshotPtr EnsureSearchSnapshot(
    duckdb::idx_t index_id, const std::shared_ptr<search::Store>& storage);

  void ResetSearchReaders() noexcept;
  void ResetSearchViews() noexcept {
    if (_search) {
      _search->ResetReaders();
    }
  }
  void ResetSearchReader(duckdb::idx_t database_id,
                         duckdb::idx_t shard_id) noexcept;

  void DeferToCommit(absl::AnyInvocable<void()> action) {
    _on_commit.push_back(std::move(action));
  }

  void ClaimSearchLayout(std::shared_ptr<search::Store> table,
                         std::string_view name, search::TableChange change);

  SessionState State() const noexcept { return _state; }

  void PrepareCommit(duckdb::ClientContext& context);
  void Commit(duckdb::ClientContext& context);
  void Rollback() noexcept;

 private:
  void Release() noexcept;

  duckdb::transaction_t _id;
  std::atomic<duckdb::transaction_t> _snapshot;
  std::vector<Change> _changes;
  std::vector<LsnUpdate> _lsn_updates;
  std::vector<duckdb::reference<duckdb::AttachedDatabase>> _attached;
  irs::containers::FlatHashMap<std::pair<duckdb::idx_t, duckdb::idx_t>,
                               search::StoreSnapshotPtr>
    _search_snapshots;
  std::optional<search::Writes> _search;
  std::vector<absl::AnyInvocable<void()>> _on_commit;
  std::vector<std::shared_ptr<search::Store>> _layout_claims;
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

  duckdb::transaction_t NextId() noexcept {
    return _next_id.fetch_add(1, std::memory_order_relaxed);
  }

  void Open(std::unique_ptr<Log> log) { _log = std::move(log); }
  Log* GetLog() noexcept { return _log.get(); }

  void SetReplaying(Lsn lsn) noexcept { _replaying = lsn; }
  Lsn Replaying() const noexcept { return _replaying; }

  void Commit(SessionTransaction& transaction, duckdb::ClientContext& context);
  void CommitStorage(
    std::span<const uint8_t> record,
    absl::FunctionRef<void(duckdb::transaction_t, duckdb::transaction_t, Lsn)>
      apply);
  void CommitState(absl::FunctionRef<std::vector<uint8_t>()> build,
                   absl::FunctionRef<void(Lsn)> logged);

  absl::Status TruncateLog(
    bool switch_chunk,
    duckdb::CheckpointAbort abort = duckdb::CheckpointAbort::NO_ABORT);

  duckdb::transaction_t Begin();
  duckdb::transaction_t Advance(duckdb::transaction_t snapshot);
  void End(duckdb::transaction_t snapshot);
  duckdb::transaction_t OldestSnapshot() ABSL_LOCKS_EXCLUDED(_snapshots_mutex);

 private:
  struct Garbage {
    duckdb::transaction_t commit;
    EntrySet* set;
    std::weak_ptr<const void> set_alive;
    duckdb::CatalogEntry* version;
    duckdb::weak_ptr<duckdb::AttachedDatabase> database;
  };
  struct Pending {
    duckdb::transaction_t commit = 0;
    Lsn end = 0;
    bool catalog = false;
    bool ready = false;
  };

  Lsn DurableEnd() const;
  Lsn ReservedEnd() const;
  void Enqueue(duckdb::transaction_t commit, Lsn end, bool catalog)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_commit_mutex);
  void Publish(duckdb::transaction_t commit, bool wait);
  void MaintainLog(duckdb::ClientContext& context, Lsn end);

  duckdb::transaction_t Oldest() const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_snapshots_mutex);
  void Release(duckdb::transaction_t snapshot)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_snapshots_mutex);
  void Collect();

  absl::Mutex _collect_mutex;
  absl::Mutex _snapshots_mutex;
  absl::btree_map<duckdb::transaction_t, uint32_t> _snapshots
    ABSL_GUARDED_BY(_snapshots_mutex);
  std::deque<Garbage> _garbage ABSL_GUARDED_BY(_snapshots_mutex);
  absl::Mutex _commit_mutex;
  duckdb::transaction_t _reserved_commit ABSL_GUARDED_BY(_commit_mutex) =
    duckdb::SYSTEM_TRANSACTION_TIMESTAMP;
  absl::Mutex _publish_mutex;
  std::deque<Pending> _pending ABSL_GUARDED_BY(_publish_mutex);
  std::atomic<duckdb::transaction_t> _last_commit{
    duckdb::SYSTEM_TRANSACTION_TIMESTAMP};
  std::atomic<duckdb::transaction_t> _next_id{duckdb::TRANSACTION_ID_START};
  std::atomic<duckdb::idx_t> _catalog_version{0};
  std::unique_ptr<Log> _log;
  Lsn _replaying = 0;
  std::atomic<bool> _truncating{false};
  std::atomic<bool> _preparing_spare{false};
  absl::Mutex _truncate_mutex;
};

class CommittedView final {
 public:
  CommittedView() : _snapshot{Transactions::Get().Begin()} {}
  ~CommittedView() { Transactions::Get().End(_snapshot); }
  CommittedView(const CommittedView&) = delete;
  CommittedView& operator=(const CommittedView&) = delete;

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
