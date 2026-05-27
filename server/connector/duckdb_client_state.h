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

namespace sdb::message {

class Buffer;
}

namespace sdb::pg {

class CopyMessagesQueue;
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

  // Set by PG layer before query execution.
  // Read by SereneDBCopyFileSystem to bridge PG protocol into DuckDB I/O.
  pg::CopyMessagesQueue* copy_queue = nullptr;
  message::Buffer* send_buffer = nullptr;

  std::unique_ptr<pg::ProgressReporter> progress;

  // Buffer for COPY FROM STDIN data. DuckDB opens /dev/stdin multiple
  // times (for sniffing and reading). Data read on first opens is saved
  // here and replayed on the final open.
  std::shared_ptr<std::string> copy_stdin_buffer;
  int copy_stdin_open_count = 0;

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
    duckdb::ClientContext& context,
    duckdb::PreparedStatementCallbackInfo& info,
    duckdb::RebindQueryInfo current_rebind) final;

  void QueryEnd(duckdb::ClientContext& context) final;

 private:
  std::shared_ptr<ConnectionContext> _connection_ctx;
};

// Helper to get the ConnectionContext from a DuckDB ClientContext.
ConnectionContext& GetSereneDBContext(duckdb::ClientContext& context);

}  // namespace sdb::connector
