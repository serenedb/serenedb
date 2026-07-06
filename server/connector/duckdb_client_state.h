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
#include <duckdb/main/client_context_state.hpp>
#include <memory>

#include "catalog/identifiers/object_id.h"
#include "pg/progress_registry.h"

namespace sdb {

class ConnectionContext;
}

namespace sdb::network::pg {

class WireSinkContext;
}

namespace sdb::connector {

inline constexpr const char* kSereneDBClientStateKey = "serenedb";

// Registered in DuckDB's ClientContext to provide access to SereneDB's
// ConnectionContext (which holds config, transactions, catalog snapshots).
// Accessible from any DuckDB function/operator via:
//   context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
// The ConnectionContext whose transaction is currently committing on this
// thread (between TransactionPreCommit and TransactionCommit/Rollback);
// nullptr outside a commit (e.g. WAL replay).
ConnectionContext* CurrentCommittingContext() noexcept;

class SereneDBClientState final : public duckdb::ClientContextState {
 public:
  // Creates a SereneDBClientState, registers it in the ClientContext, and wires
  // up per-connection hooks (e.g. transaction isolation level validator).
  // Returns the registered state so the caller can cache it and skip the keyed
  // (locked, case-insensitive) registry lookup on every query.
  static SereneDBClientState& Register(
    duckdb::ClientContext& client_ctx,
    std::shared_ptr<ConnectionContext> connection_ctx);

  explicit SereneDBClientState(
    std::shared_ptr<ConnectionContext> connection_ctx)
    : _connection_ctx{std::move(connection_ctx)} {}

  ~SereneDBClientState() final {
    if (progress_source) {
      progress_source->Detach();
      pg::ProgressRegistry::Instance().Unregister(progress_source.get());
    }
  }

  ConnectionContext& GetConnectionContext() const { return *_connection_ctx; }

  // The connection's row in sdb_progress. Execution paths write the
  // command-specific counters straight into Progress(); the registry snapshots
  // them from any thread.
  std::shared_ptr<pg::ProgressSource> progress_source;

  pg::ProgressMetrics& Progress() { return progress_source->metrics; }

  // COPY FROM STDIN state shared across handles within a single query. The
  // non-seekable wire stream is read SINGLE-PASS: our binary/text functions
  // stream straight off the bridge, and DuckDB's csv/json readers sniff+scan
  // over one buffer-manager pass (no re-open). So there is no replay buffer.
  // open_count lets the handle reject an unexpected re-open; done
  // short-circuits reads after CopyDone.
  int copy_stdin_open_count = 0;
  bool copy_stdin_done = false;

  // Armed by the pg-wire session around PendingQuery for row-returning
  // statements: the get_result_collector hook reads it to install the
  // encode-in-Sink wire collector. Shared so the executor's sink state keeps
  // it alive on every error/teardown path.
  std::shared_ptr<network::pg::WireSinkContext> wire_sink;

  void TransactionPreCommit(duckdb::MetaTransaction& transaction,
                            duckdb::ClientContext& context) final;

  void TransactionPreCheckpoint(duckdb::AttachedDatabase& db,
                                duckdb::ClientContext& context) final;

  void TransactionPreRollback(
    duckdb::MetaTransaction& transaction, duckdb::ClientContext& context,
    duckdb::optional_ptr<duckdb::ErrorData> error) final;

  void TransactionCommit(duckdb::MetaTransaction& transaction,
                         duckdb::ClientContext& context) final;

  void TransactionRollback(duckdb::MetaTransaction& transaction,
                           duckdb::ClientContext& context) final;

  void QueryBegin(duckdb::ClientContext& context) final;
  void QueryEnd(duckdb::ClientContext& context) final;

  // COPY classification for the NEXT query, staged by the wire session before
  // PendingQuery. QueryBegin resets the metrics of the previous statement, so
  // the session cannot write them directly; QueryBegin applies these after the
  // reset and clears them.
  pg::ProgressCommand pending_copy_command = pg::ProgressCommand::None;
  pg::ProgressIoType pending_copy_io = pg::ProgressIoType::None;
  ObjectId pending_copy_relid;

  // Transaction-scoped compensation for work staged on a side transaction
  // (CTAS): registered when the side transaction starts, run in
  // TransactionPreRollback while the MetaTransaction is alive, cleared by the
  // owner right before its commit point. Never runs from a destructor -- a
  // sink state outlives the statement (it dies with the cached plan), so a
  // destructor-time MetaTransaction reference is a use-after-free.
  std::function<void(duckdb::MetaTransaction&)> transaction_abort_cleanup;

 private:
  std::shared_ptr<ConnectionContext> _connection_ctx;
};

// Helper to get the ConnectionContext from a DuckDB ClientContext.
ConnectionContext* GetSereneDBContextPtr(duckdb::ClientContext& context);
ConnectionContext& GetSereneDBContext(duckdb::ClientContext& context);

}  // namespace sdb::connector
