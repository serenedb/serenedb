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
  explicit SereneDBClientState(
    std::shared_ptr<ConnectionContext> connection_ctx)
    : _connection_ctx{std::move(connection_ctx)} {}

  ConnectionContext& GetConnectionContext() const { return *_connection_ctx; }

  // Transaction lifecycle callbacks
  void TransactionCommit(duckdb::MetaTransaction& transaction,
                         duckdb::ClientContext& context) override;
  void TransactionRollback(duckdb::MetaTransaction& transaction,
                           duckdb::ClientContext& context) override;
  void QueryEnd(duckdb::ClientContext& context) override;

 private:
  std::shared_ptr<ConnectionContext> _connection_ctx;
};

// Helper to get the ConnectionContext from a DuckDB ClientContext.
ConnectionContext& GetSereneDBContext(duckdb::ClientContext& context);

}  // namespace sdb::connector
