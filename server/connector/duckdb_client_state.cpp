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

#include "connector/duckdb_client_state.h"

#include <duckdb/main/client_context.hpp>

#include "basics/assert.h"
#include "pg/connection_context.h"

namespace sdb::connector {

void SereneDBClientState::TransactionCommit(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context) {
  auto r = _connection_ctx->Commit();
  if (!r.ok()) {
    throw duckdb::TransactionException("SereneDB commit failed: %s",
                                       std::string{r.errorMessage()});
  }
}

void SereneDBClientState::TransactionRollback(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context) {
  auto r = _connection_ctx->Rollback();
  if (!r.ok()) {
    throw duckdb::TransactionException("SereneDB rollback failed: %s",
                                       std::string{r.errorMessage()});
  }
}

void SereneDBClientState::QueryEnd(duckdb::ClientContext& context) {
  _connection_ctx->OnNewStatement();
}

ConnectionContext& GetSereneDBContext(duckdb::ClientContext& context) {
  auto state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  SDB_ASSERT(state, "SereneDB client state not registered");
  return state->GetConnectionContext();
}

}  // namespace sdb::connector
