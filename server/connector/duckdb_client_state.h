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

#include "pg/progress_tracker.h"

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

  ConnectionContext& GetConnectionContext() const { return *_connection_ctx; }

  std::unique_ptr<pg::ProgressReporter> progress;

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

  void ArmCopyProgress(ObjectId table_id, pg::copy_progress::Command command,
                       pg::copy_progress::Type type);
  void EnsureCopyProgress(ObjectId table_id, pg::copy_progress::Command command,
                          pg::copy_progress::Type type);
  void EnsureCreateIndexProgress(ObjectId datid, ObjectId relid,
                                 duckdb::idx_t estimated_cardinality);
  void EnsureCreateTableAsProgress(ObjectId datid, ObjectId relid,
                                   duckdb::idx_t estimated_cardinality);
  void EnsureAnalyzeProgress(ObjectId datid, ObjectId relid);
  void EnsureVacuumProgress(ObjectId datid, ObjectId relid);

  // Set by the wire session before PendingQuery; consumed by the Ensure*
  // arming paths to tell COPY apart from plain DML sharing the same plan
  // shape. Reset in QueryEnd.
  duckdb::StatementType current_statement_type =
    duckdb::StatementType::INVALID_STATEMENT;

 private:
  std::shared_ptr<ConnectionContext> _connection_ctx;
};

// Helper to get the ConnectionContext from a DuckDB ClientContext.
ConnectionContext* GetSereneDBContextPtr(duckdb::ClientContext& context);
ConnectionContext& GetSereneDBContext(duckdb::ClientContext& context);

}  // namespace sdb::connector
