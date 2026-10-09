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

#include <atomic>
#include <cstdint>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/parser/parsed_data/create_subscription_info.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>
#include <yaclib/algo/one_shot_event.hpp>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/task.hpp>

#include "replication/conninfo.h"
#include "replication/pgoutput.h"
#include "replication/publisher_session.h"
#include "replication/repl_source.h"
#include "replication/repl_stream.h"
#include "replication/spill_buffer.h"
#include "replication/sync_session.h"

namespace sdb::replication {

struct ConflictCounters {
  std::atomic<uint64_t> insert_exists{0};
  std::atomic<uint64_t> update_exists{0};
  std::atomic<uint64_t> update_missing{0};
  std::atomic<uint64_t> delete_missing{0};
  std::atomic<uint64_t> multiple_unique_conflicts{0};

  void Reset() noexcept {
    insert_exists.store(0, std::memory_order_relaxed);
    update_exists.store(0, std::memory_order_relaxed);
    update_missing.store(0, std::memory_order_relaxed);
    delete_missing.store(0, std::memory_order_relaxed);
    multiple_unique_conflicts.store(0, std::memory_order_relaxed);
  }
};

class PgReplicationClient final : public SyncSession {
 public:
  PgReplicationClient(network::IoExecutor& exec, ReplicationTarget target,
                      size_t host_index);

  yaclib::Task<> RunClient();
  void StopClient() { this->Stop(); }

  const ReplicationTarget& Target() const noexcept { return _target; }
  bool Connected() const noexcept {
    return _connected.load(std::memory_order_acquire);
  }
  bool SyncFailed() const noexcept {
    return _sync_failed.load(std::memory_order_acquire);
  }
  uint64_t ReceivedLsn() const noexcept {
    return _received_lsn.load(std::memory_order_relaxed);
  }
  uint64_t FlushedLsn() const noexcept {
    return _flushed_lsn.load(std::memory_order_relaxed);
  }
  int64_t LastSendTime() const noexcept {
    return _last_send_time.load(std::memory_order_relaxed);
  }
  int64_t LastReceiptTime() const noexcept {
    return _last_receipt_time.load(std::memory_order_relaxed);
  }
  int64_t LatestEndTime() const noexcept {
    return _latest_end_time.load(std::memory_order_relaxed);
  }
  const ConflictCounters& Conflicts() const noexcept { return _conflicts; }
  void ResetConflicts() noexcept { _conflicts.Reset(); }

 private:
  yaclib::Task<bool> SyncTables();
  yaclib::Task<bool> DescribeTables();
  yaclib::Task<bool> SyncSequential();
  yaclib::Task<bool> SyncParallel(size_t workers);
  yaclib::Task<bool> StartReplication();
  yaclib::Task<> Feeder();
  yaclib::Future<> FeedbackLoop();
  yaclib::Task<bool> Publish(std::span<const PgOutputMessage> messages);
  bool Stage(std::string_view payload, bool in_stream);
  yaclib::Task<bool> Flush();
  yaclib::Task<bool> Enqueue(std::string_view payload, bool in_stream);
  yaclib::Task<bool> Enqueue(PgOutputMessage message);
  yaclib::Task<bool> ReplayStream(uint32_t xid,
                                  const StreamCommitMessage& commit);
  void SendFeedback(bool reply);

  yaclib::Future<> ReplicationMain();
  yaclib::Task<> ReplicationLoop();
  void UpdateDefinition(
    absl::FunctionRef<void(duckdb::CreateSubscriptionInfo&)> edit);
  const RelInfo* Relation(uint32_t relation_id) const;
  void OnRelation(const RelationMessage& message);
  void CheckRelation(const RelInfo& relation) const;
  yaclib::Task<bool> MultipleUniqueConflicts(
    duckdb::ColumnDataCollection& rows);
  bool ApplyChanges(const RelInfo& relation) const;
  yaclib::Task<bool> ApplyMessage(const PgOutputMessage& message);
  void BeginTxn(const BeginMessage& message);
  void PushRemoteLsn(uint64_t end_lsn);
  bool CommitTxn(const CommitMessage& message);
  bool FlushCommit();
  bool Reorder(std::span<const PgOutputMessage> messages);
  void GroupCommit(const CommitMessage& message);
  bool PassThrough(const PgOutputMessage& message);
  yaclib::Task<> ApplyTruncate(const TruncateMessage& message);
  std::string ApplyContext() const;
  yaclib::Task<bool> RunBatch();

  ReplStream _stream;
  std::atomic<bool> _connected{false};
  std::atomic<bool> _sync_failed{false};
  std::atomic<bool> _apply_idle{true};
  std::atomic<uint64_t> _received_lsn{0};
  std::atomic<uint64_t> _flushed_lsn{0};
  std::atomic<int64_t> _last_send_time{0};
  std::atomic<int64_t> _last_receipt_time{0};
  std::atomic<int64_t> _latest_end_time{0};
  ConflictCounters _conflicts;
  bool _publishing = false;
  bool _published = false;
  int64_t _last_activity = 0;
  std::optional<asio_ns::steady_timer> _feedback_timer;
  std::string _arena;
  std::vector<PgOutputMessage> _outbox;

  std::vector<SyncTable> _sync_tables;

  irs::containers::NodeHashMap<uint32_t, SpillBuffer> _spools;
  irs::containers::FlatHashMap<uint32_t,
                               std::vector<std::pair<uint32_t, size_t>>>
    _subxacts;
  std::optional<uint32_t> _streamed_xid;

  irs::containers::NodeHashMap<uint32_t, RelInfo> _relations;
  bool _skipping = false;
  uint64_t _final_lsn = 0;
  bool _remote_txn = false;
  size_t _hold_commits = 0;
  std::vector<PgOutputMessage> _reordered;
  uint64_t _pending_lsn = 0;
  int64_t _pending_since = 0;
  size_t _pending_txns = 0;
  uint32_t _xid = 0;
  ReplBatch _batch;

  struct StmtKey {
    char op = 0;
    uint32_t relid = 0;
    std::vector<size_t> keys;
    std::vector<size_t> cols;
    bool full = false;

    bool operator==(const StmtKey& o) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const StmtKey& k) {
      return H::combine(std::move(h), k.op, k.relid, k.keys, k.cols, k.full);
    }
  };

  irs::containers::NodeHashMap<StmtKey,
                               duckdb::unique_ptr<duckdb::PreparedStatement>>
    _stmts;
};

}  // namespace sdb::replication
