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

#include <absl/strings/match.h>

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/log.h"
#include "basics/system-compiler.h"
#include "catalog/store/store.h"
#include "pg/connection_context.h"
#include "pg/copy_messages_queue.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

SereneDBClientState& SereneDBClientState::Register(
  duckdb::ClientContext& client_ctx,
  std::shared_ptr<ConnectionContext> connection_ctx) {
  auto state =
    duckdb::make_shared_ptr<SereneDBClientState>(std::move(connection_ctx));
  auto& registered = *state;

  auto source = std::make_shared<pg::ProgressSource>();
  source->pid = registered._connection_ctx->GetBackendPid();
  if (const auto& db = registered._connection_ctx->GetDatabasePtr()) {
    source->datid = static_cast<int64_t>(db->GetId().id());
  }
  source->user = registered._connection_ctx->user();
  source->database = registered._connection_ctx->GetDatabase();
  source->backend_start_us = duckdb::Timestamp::GetCurrentTimestamp().value;
  source->ctx = &client_ctx;
  registered.progress_source = source;
  pg::ProgressRegistry::Instance().Register(std::move(source));
  client_ctx.registered_state->Insert(kSereneDBClientStateKey,
                                      std::move(state));
  client_ctx.warning_handler = [](duckdb::ClientContext& ctx,
                                  const char* message) {
    if (absl::StrContains(message, "no transaction is active")) {
      GetSereneDBContext(ctx).AddNotice(
        SQL_ERROR_DATA(ERR_CODE(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
                       ERR_MSG("there is no transaction in progress")));
      return true;
    }
    GetSereneDBContext(ctx).AddNotice(
      SQL_ERROR_DATA(ERR_CODE(ERRCODE_WARNING), ERR_MSG(message)));
    return true;
  };

  client_ctx.setting_change_handler = [](duckdb::ClientContext& ctx,
                                         const std::string& name,
                                         duckdb::SetScope scope,
                                         const duckdb::Value* new_value) {
    // Resolve AUTOMATIC against the setting's target scope so the downstream
    // check works uniformly regardless of how the user wrote the SET.
    if (scope == duckdb::SetScope::AUTOMATIC) {
      auto& db_config = duckdb::DBConfig::GetConfig(ctx);
      auto name_ref = duckdb::String::Reference(name.data(), name.size());
      duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
      if (db_config.TryGetSettingIndex(name_ref, option).IsValid() && option) {
        scope = (option->scope == duckdb::SettingScopeTarget::GLOBAL_ONLY ||
                 option->scope == duckdb::SettingScopeTarget::GLOBAL_DEFAULT)
                  ? duckdb::SetScope::GLOBAL
                  : duckdb::SetScope::SESSION;
      } else {
        duckdb::ExtensionOption ext;
        if (db_config.TryGetExtensionOption(name_ref, ext)) {
          scope = ext.default_scope;
        }
      }
    }
    // SET GLOBAL changes the DB-instance default (lives only in DBConfig, not
    // user_settings / custom session store) and is not rolled back with the
    // transaction -- only session/local changes are tracked.
    if (scope == duckdb::SetScope::GLOBAL) {
      return;
    }
    auto& sdb_ctx = GetSereneDBContext(ctx);
    // A reported GUC may have changed -- flag it so the wire layer re-emits
    // ParameterStatus at the next ReadyForQuery (a cheap version bump; the GUC
    // poll itself is skipped entirely when nothing changed).
    sdb_ctx.MarkSettingsChanged();
    // Outside an explicit transaction there's nothing to roll back --
    // the map stays empty.
    if (!sdb_ctx.IsExplicitTransaction()) {
      return;
    }
    duckdb::Value old_value;
    ctx.TryGetCurrentSetting(name, old_value);
    sdb_ctx.OnSet(name, scope == duckdb::SetScope::LOCAL, std::move(old_value),
                  new_value);
  };

  client_ctx.setting_visibility = [](duckdb::ClientContext&,
                                     const std::string& name) {
    // Internal knobs -- hidden from SHOW ALL / pg_settings / duckdb_settings().
    // Still settable/readable by name.
    static const containers::FlatHashSet<std::string_view> kHidden = {
      "sdb_faults", "debug_verification"};
    return !kHidden.contains(name);
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
          conn_ctx.HadQueryInTransaction() &&
          level != conn_ctx.GetIsolationLevel()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
          ERR_MSG(
            "SET TRANSACTION ISOLATION LEVEL must be called before any query"));
      }
    };
  return registered;
}

namespace {

// Published for the duration of the storage commit: BoundIndex appends run
// on the committing connection's thread inside LocalStorage::Flush, after
// TransactionPreCommit and before TransactionCommit/Rollback, with no
// ClientContext parameter of their own.
thread_local ConnectionContext* tls_committing_ctx = nullptr;

}  // namespace

ConnectionContext* CurrentCommittingContext() noexcept {
  return tls_committing_ctx;
}

void SereneDBClientState::TransactionPreCommit(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context) {
  // Pre-durability crash point: fires before the engine commit, so the
  // transaction must be absent after restart. Only write transactions
  // crash (the fault-arming SET itself must survive). Name historical.
  SDB_IF_FAILURE("crash_before_search_commit") {
    if (transaction.ModifiedDatabase()) {
      SDB_IMMEDIATE_ABORT();
    }
  }
  // Revert SET LOCAL variables while the DuckDB transaction is still active
  // so catalog lookups performed by custom-impl settings (e.g. search_path)
  // can succeed via their normal set_local path.
  _connection_ctx->PreCommit();
  tls_committing_ctx = _connection_ctx.get();
}

void SereneDBClientState::TransactionPreCheckpoint(duckdb::AttachedDatabase& db,
                                                   duckdb::ClientContext&) {
  // Commit the search-index leg synchronously with the store table changes:
  // this fires after the store database's changes are durable but before the
  // in-commit checkpoint, so the checkpoint's force-refresh never waits on an
  // un-committed in-flight batch. Only the store database carries indexed
  // tables, so settle on its commit.
  if (db.GetName().GetIdentifierName() != catalog::kStoreDatabaseName) {
    return;
  }
  _connection_ctx->CommitSearch();
}

void SereneDBClientState::TransactionPreRollback(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context,
  duckdb::optional_ptr<duckdb::ErrorData> error) {
  if (auto cleanup = std::exchange(transaction_abort_cleanup, nullptr)) {
    try {
      cleanup(transaction);
    } catch (const std::exception& e) {
      SDB_WARN(GENERAL, "transaction abort cleanup failed: ", e.what());
    }
  }
  _connection_ctx->PreRollback();
}

void SereneDBClientState::TransactionCommit(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context) {
  // Post-durability crash point: the engine commit is durable, search
  // ticks are not yet -- recovery must rebuild the storage. Only write
  // transactions crash. Name historical.
  SDB_IF_FAILURE("crash_after_search_commit") {
    if (transaction.ModifiedDatabase()) {
      SDB_IMMEDIATE_ABORT();
    }
  }
  tls_committing_ctx = nullptr;
  auto r = _connection_ctx->Commit();
  if (!r.ok()) {
    throw duckdb::TransactionException("SereneDB commit failed: %s",
                                       r.errorMessage());
  }
}

void SereneDBClientState::TransactionRollback(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context) {
  tls_committing_ctx = nullptr;
  auto r = _connection_ctx->Rollback();
  if (!r.ok()) {
    throw duckdb::TransactionException("SereneDB rollback failed: %s",
                                       r.errorMessage());
  }
}

void SereneDBClientState::QueryBegin(duckdb::ClientContext& context) {
  _connection_ctx->OnStatementBegin();
  progress_source->BeginQuery(context.GetCurrentQuery());
  if (pending_copy_command != pg::ProgressCommand::None) {
    auto& metrics = progress_source->metrics;
    metrics.SetCommand(pending_copy_command);
    metrics.SetIoType(pending_copy_io);
    pg::ProgressMetrics::Set(metrics.relid,
                             static_cast<int64_t>(pending_copy_relid.id()));
    pending_copy_command = pg::ProgressCommand::None;
    pending_copy_io = pg::ProgressIoType::None;
    pending_copy_relid = {};
  }
}

void SereneDBClientState::QueryEnd(duckdb::ClientContext& context) {
  if (auto* queue = _connection_ctx->GetCopyQueue()) {
    queue->CloseListening();
  }
  copy_stdin_open_count = 0;
  copy_stdin_done = false;
  progress_source->EndQuery();
  _connection_ctx->OnStatementEnd();
}

ConnectionContext* GetSereneDBContextPtr(duckdb::ClientContext& context) {
  auto state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  if (!state) {
    return nullptr;
  }
  return &state->GetConnectionContext();
}

ConnectionContext& GetSereneDBContext(duckdb::ClientContext& context) {
  auto* ctx = GetSereneDBContextPtr(context);
  SDB_ASSERT(ctx, "SereneDB client state not registered; active query: ",
             context.GetCurrentQuery());
  return *ctx;
}

}  // namespace sdb::connector
