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

namespace sdb::connector {

inline constexpr const char* kSereneDBClientStateKey = "serenedb";

// Registered in DuckDB's ClientContext to provide access to SereneDB's
// ConnectionContext (which holds config, transactions, catalog snapshots).
// Accessible from any DuckDB function/operator via:
//   context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
class SereneDBClientState final : public duckdb::ClientContextState {
 public:
  // Creates a SereneDBClientState, registers it in the ClientContext, and wires
  // up per-connection hooks (e.g. transaction isolation level validator).
  static void Register(duckdb::ClientContext& client_ctx,
                       std::shared_ptr<ConnectionContext> connection_ctx);

  explicit SereneDBClientState(
    std::shared_ptr<ConnectionContext> connection_ctx)
    : _connection_ctx{std::move(connection_ctx)} {}

  ConnectionContext& GetConnectionContext() const { return *_connection_ctx; }

  std::unique_ptr<pg::ProgressReporter> progress;

  // COPY FROM STDIN state shared across handles within a single query.
  // DuckDB may open /dev/stdin more than once (CSV sniff then real read);
  // the buffer captures what the first open drains so later opens replay
  // it, and the done flag short-circuits reads after CopyDone.
  std::shared_ptr<std::string> copy_stdin_buffer;
  int copy_stdin_open_count = 0;
  bool copy_stdin_done = false;
  // Binary COPY opens /dev/stdin exactly once (no CSV sniff -> no re-open), so
  // it must not accumulate the replay buffer, which would duplicate the entire
  // input in memory.
  bool copy_stdin_no_replay = false;

  void TransactionPreCommit(duckdb::MetaTransaction& transaction,
                            duckdb::ClientContext& context) final;

  void TransactionPreRollback(
    duckdb::MetaTransaction& transaction, duckdb::ClientContext& context,
    duckdb::optional_ptr<duckdb::ErrorData> error) final;

  void TransactionCommit(duckdb::MetaTransaction& transaction,
                         duckdb::ClientContext& context) final;

  void TransactionRollback(duckdb::MetaTransaction& transaction,
                           duckdb::ClientContext& context) final;

  duckdb::RebindQueryInfo OnExecutePrepared(
    duckdb::ClientContext& context, duckdb::PreparedStatementCallbackInfo& info,
    duckdb::RebindQueryInfo current_rebind) final;

  void QueryEnd(duckdb::ClientContext& context) final;

 private:
  std::shared_ptr<ConnectionContext> _connection_ctx;
};

// Helper to get the ConnectionContext from a DuckDB ClientContext.
ConnectionContext& GetSereneDBContext(duckdb::ClientContext& context);

}  // namespace sdb::connector
