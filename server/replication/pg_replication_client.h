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

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <yaclib/algo/one_shot_event.hpp>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/task.hpp>

#include "basics/containers/node_hash_map.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "network/pg/pg_wire_session.h"
#include "replication/pgoutput.h"
#include "replication/repl_source.h"
#include "replication/repl_stream.h"

namespace sdb::replication {

// Everything a subscriber apply loop needs, copied out of the catalog at launch
// so the client is independent of concurrent catalog edits.
struct ReplicationTarget {
  ObjectId subscription_id;
  ObjectId database_id;       // local database to apply into
  std::string database_name;  // local database name
  std::string subscription_name;
  // Parsed from the libpq conninfo the subscription stored.
  std::string host;
  std::string port = "5432";
  std::string user;
  std::string password;
  std::string dbname;  // remote (publisher) database
  std::vector<std::string> publications;
  std::string slot_name;
  bool binary = false;    // WITH (binary = true): request binary format
  bool copy_data = true;  // WITH (copy_data = false): skip initial copy
  bool disable_on_error =
    false;                     // WITH (disable_on_error): disable on apply err
  std::string origin = "any";  // WITH (origin = any|none): change filtering
  bool create_slot =
    true;  // WITH (create_slot = false): expect an existing slot
  // The subscription owner: apply runs with this role's privileges (PG-like
  // RBAC), instead of a privileged internal user.
  ObjectId owner_id;
  std::string owner_name;
};

ReplicationTarget ParseConnInfo(std::string conninfo);

// An outbound PostgreSQL logical-replication subscriber built on SereneDB's own
// async pg-wire stack: it is a pg-wire CLIENT that reuses PgWireSession's
// transport, frame assembler and local-apply drive, substituting a client
// startup + SCRAM handshake and a "consume the pushed CopyBoth stream and apply
// it locally" loop for the server negotiation and command loop. Runs entirely
// on an io-pool worker + the DuckDB scheduler; no dedicated threads.
class PgReplicationClient final
  : public network::pg::PgWireSession<network::SocketKind::Tcp> {
 public:
  PgReplicationClient(network::IoExecutor& exec, ReplicationTarget target);

  // Run one full session (connect + SCRAM + START_REPLICATION + consume) to
  // completion. Returns when the stream ends (drop, error, or StopClient). The
  // engine's per-subscription supervisor co_awaits this and reconnects. Owns
  // itself via shared_from_this while running.
  yaclib::Task<> RunClient();

  // Launch a one-shot coroutine that connects, drops the publisher's logical
  // slot (DROP_REPLICATION_SLOT ... WAIT), and exits. Used on DROP SUBSCRIPTION
  // so the slot doesn't leak and pin WAL on the publisher. Owns itself via
  // shared_from_this until it unwinds.
  void StartSlotDrop();

  // Idempotent teardown: breaks the consume loop and closes the socket so the
  // coroutine unwinds. Safe to call from another thread (Transport::Stop uses
  // atomics + gate kicks).
  void StopClient() { this->Stop(); }

  ObjectId SubscriptionId() const { return _target.subscription_id; }
  const ReplicationTarget& Target() const { return _target; }
  // True when the apply hit an error and the subscription opted into
  // disable_on_error: the supervisor disables it instead of reconnecting.
  bool DisableRequested() const {
    return _disable_requested.load(std::memory_order_acquire);
  }

  // Observability snapshots (pg_stat_subscription / pg_subscription_rel), read
  // from catalog scans on other threads.
  uint64_t ReceivedLsn() const {
    return _received_lsn.load(std::memory_order_relaxed);
  }
  uint64_t FlushedLsn() const {
    return _flushed_lsn.load(std::memory_order_relaxed);
  }
  // Local table OIDs this subscription has replicated (initial copy + streamed
  // relations). A copy, taken under _stat_mu.
  std::vector<ObjectId> ReplicatedTables() const;

 private:
  // io-side one-shot: connect+auth, then DROP_REPLICATION_SLOT ... WAIT.
  yaclib::Task<> RunSlotDrop();
  yaclib::Task<bool> SendSlotDrop();

  // io-side: resolve+connect, send StartupMessage, run the SCRAM exchange.
  // Returns false on any failure (the caller tears down).
  yaclib::Task<bool> ConnectAndAuthenticate();
  yaclib::Task<bool> Authenticate();

  // io-side: run the initial table sync if needed (new slot), then send
  // START_REPLICATION and wait for CopyBothResponse. Returns false if the
  // publisher rejects it.
  yaclib::Task<bool> Handshake();

  // io-side: if the publisher slot does not exist yet, run PostgreSQL's initial
  // data sync -- BEGIN REPEATABLE READ, CREATE_REPLICATION_SLOT USE_SNAPSHOT,
  // COPY each published table under that snapshot, COMMIT -- then streaming
  // resumes from the slot's consistent point. If the slot already exists the
  // sync is assumed done and this is a no-op.
  yaclib::Task<> MaybeInitialCopy();
  // io-side: COPY one table from the publisher (COPY ... TO STDOUT) and pipe
  // its CopyData straight into a local COPY ... FROM STDIN on the duck side
  // (via a CopyInBridge), exactly as a normal wire session ingests a client
  // COPY.
  yaclib::Task<> CopyOneTable(std::string_view schema, std::string_view table,
                              const std::vector<std::string>& columns,
                              const std::optional<std::string>& row_filter);
  // io-side: run a simple query and collect its DataRows (each field null or
  // the raw text bytes). Returns false if the publisher replied with an error.
  yaclib::Task<bool> RunQueryRows(
    std::string_view sql,
    std::vector<std::vector<std::optional<std::string>>>& rows);
  // io-side: run a simple command, draining to ReadyForQuery. False on error.
  yaclib::Task<bool> RunSimpleCommand(std::string_view sql);
  // duck-side: prepare + execute the current local COPY ... FROM STDIN AST (it
  // blocks reading the bridge the io side feeds). Returns false on apply error.
  yaclib::Task<bool> RunLocalCopy();
  // io-side: read the CopyBoth stream, handle keepalives/feedback, decode each
  // pgoutput message, and hand it to the duck-side apply through _stream.
  yaclib::Task<> Feeder();
  // duck-side: create the local apply connection, then run the control loop --
  // dispatch Begin/Commit/Relation and execute one batched DML per row run.
  yaclib::Future<> ReplicationMain();

  // Build the local DuckDB connection + ConnectionContext used to apply rows.
  bool SetupApplyConnection();
  const RelInfo* Relation(uint32_t relation_id) const;
  void OnRelation(const RelationMessage& msg);

  // Build (or reuse) and publish the prepared DML for the current _batch. The
  // control loop sets _batch's op/relation/keys/cols first; this fills in the
  // snapshot and the statement. Returns false if the statement failed to
  // prepare (skip the row).
  bool StartBatch();
  // Execute the batch: repl_src's scan drains the matching row run off _stream
  // (deserializing inside execution). Returns false on an apply error.
  yaclib::Task<bool> RunBatchExec();

  yaclib::Task<> BeginTxn();
  yaclib::Task<> CommitTxn();
  // Apply a pgoutput Truncate: TRUNCATE each mapped local table in the message.
  yaclib::Task<> ApplyTruncate(const TruncateMessage& msg);
  void SendFeedback();
  // Post a standby-status ('r') feedback send onto the io thread (the only
  // thread that touches _send), so the duck-side control loop can request one
  // after a commit without racing the feeder's writes.
  void PostFeedback();
  // Note a replicated local table for observability (idempotent). Written
  // io-side (initial copy + OnRelation), read via ReplicatedTables().
  void RecordReplicatedTable(ObjectId table_id);

  ReplicationTarget _target;
  // Publisher major version, parsed from the server_version startup parameter;
  // gates binary initial COPY (PG uses text pre-v16 even with binary = true).
  int _server_version = 0;

  // The io feeder -> duck apply rendezvous for decoded pgoutput messages.
  ReplStream _stream;

  containers::NodeHashMap<uint32_t, RelInfo> _relations;
  bool _in_txn = false;
  std::atomic<uint64_t> _received_lsn{0};  // highest wal_end seen (feeder)
  std::atomic<uint64_t> _flushed_lsn{0};   // highest committed end_lsn (apply)
  std::atomic<bool> _disable_requested{
    false};  // apply error + disable_on_error

  // Replicated local table OIDs (observability only). Written io-side, read
  // from catalog scans on other threads, so guarded.
  mutable absl::Mutex _stat_mu;
  std::vector<ObjectId> _replicated_tables ABSL_GUARDED_BY(_stat_mu);

  // Initial-sync handoff: the io side (which owns the socket) directs the duck
  // side (which runs the local COPY). _copy_phase's release/acquire fences the
  // plain _copy_stmt/_copy_ok fields; the two OneShotEvents wake across
  // threads.
  enum class CopyPhase : int { Waiting, Job, Stream };
  std::atomic<CopyPhase> _copy_phase{CopyPhase::Waiting};
  // The local COPY ... FROM STDIN for the current table, built as an AST (never
  // SQL text): io builds it before posting a Job, duck consumes it.
  duckdb::unique_ptr<duckdb::SQLStatement> _copy_stmt;
  bool _copy_ok = false;   // duck writes before signalling done, io reads
  bool _setup_ok = false;  // SetupApplyConnection result (duck -> io)
  yaclib::OneShotEvent _setup_done;  // duck: apply connection ready (or failed)
  yaclib::OneShotEvent _copy_done;   // duck: one table's local COPY finished

  // The batch the control loop is applying; repl_src's scan reads it (both on
  // the duck task, never concurrently).
  ReplBatch _batch;
  duckdb::PreparedStatement* _batch_prepared = nullptr;

  // Cache key for the per-(op, relation, column-set) prepared statements.
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

  // Parse+bind-once cache of the per-(op, relation, column-set) statements.
  // Each re-executes against a fresh row run.
  containers::NodeHashMap<StmtKey,
                          duckdb::unique_ptr<duckdb::PreparedStatement>>
    _stmts;
};

}  // namespace sdb::replication
