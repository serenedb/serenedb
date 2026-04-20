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

#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/client_context.hpp>

#include "basics/assert.h"
#include "basics/containers/trivial_map.h"
#include "basics/system-compiler.h"
#include "pg/connection_context.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::connector {

void SereneDBClientState::Register(
  duckdb::ClientContext& client_ctx,
  std::shared_ptr<ConnectionContext> connection_ctx) {
  auto state =
    duckdb::make_shared_ptr<SereneDBClientState>(std::move(connection_ctx));
  client_ctx.registered_state->Insert(kSereneDBClientStateKey,
                                      std::move(state));
  client_ctx.warning_handler = [](duckdb::ClientContext& ctx,
                                  const char* message) {
    GetSereneDBContext(ctx).AddNotice(
      SQL_ERROR_DATA(ERR_CODE(ERRCODE_WARNING), ERR_MSG(message)));
    return true;
  };

  client_ctx.setting_change_handler = [](duckdb::ClientContext& ctx,
                                         const std::string& name,
                                         duckdb::SetScope scope) {
    // SET GLOBAL changes the DB-instance default and is not rolled back with
    // the transaction -- only session/local changes are tracked.
    if (scope == duckdb::SetScope::GLOBAL) {
      return;
    }
    GetSereneDBContext(ctx).OnSet(name, scope == duckdb::SetScope::LOCAL);
  };

  client_ctx.setting_visibility = [](duckdb::ClientContext&,
                                     const std::string& name) {
    // Internal knobs -- hidden from SHOW ALL / pg_settings / duckdb_settings().
    // Still settable/readable by name.
    static constexpr containers::TrivialSet kHidden = [](auto selector) {
      return selector().Case("sdb_faults").Case("debug_verification");
    };
    return !kHidden.Contains(name);
  };

  client_ctx.isolation_level_validator =
    [](duckdb::ClientContext& ctx, duckdb::TransactionIsolationLevel level) {
      if (level != duckdb::TransactionIsolationLevel::READ_COMMITTED &&
          level != duckdb::TransactionIsolationLevel::REPEATABLE_READ) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("transaction isolation level \"",
                  duckdb::EnumUtil::ToChars(level), "\" is not supported"),
          ERR_HINT("Available values: repeatable read, read committed."));
      }
      auto& conn_ctx = GetSereneDBContext(ctx);
      if (conn_ctx.IsExplicitTransaction() &&
          (conn_ctx.HasRocksDBRead() || conn_ctx.HasRocksDBWrite()) &&
          level != conn_ctx.GetIsolationLevel()) {
        throw duckdb::InvalidInputException(
          "SET TRANSACTION ISOLATION LEVEL must be called before any query");
      }
    };
}

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
  copy_queue = nullptr;
  send_buffer = nullptr;
  copy_stdin_buffer.reset();
  copy_stdin_open_count = 0;
  _connection_ctx->OnNewStatement();
}

ConnectionContext& GetSereneDBContext(duckdb::ClientContext& context) {
  auto state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  SDB_ASSERT(state, "SereneDB client state not registered");
  return state->GetConnectionContext();
}

}  // namespace sdb::connector
