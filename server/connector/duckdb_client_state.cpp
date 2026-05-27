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
#include <duckdb/main/prepared_statement_data.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/containers/trivial_map.h"
#include "basics/system-compiler.h"
#include "catalog/database.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_physical_sst_insert.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/progress_tracker.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

std::unique_ptr<pg::ProgressReporter> MakeProgressReporter(
  ObjectId datid, const duckdb::PreparedStatementData& prepared) {
  if (!prepared.physical_plan) {
    return nullptr;
  }
  const auto& root = prepared.physical_plan->Root();

  if (prepared.statement_type == duckdb::StatementType::COPY_STATEMENT) {
    const auto* sst = dynamic_cast<const SereneDBPhysicalSSTInsert*>(&root);
    if (!sst) {
      return nullptr;
    }
    auto reporter = std::make_unique<pg::ProgressReporter>(
      datid, sst->TargetTableId(), pg::ProgressCommand::Copy);
    reporter->SetCommand(pg::copy_progress::Command::CopyFrom);
    reporter->SetType(pg::copy_progress::Type::File);
    return reporter;
  }

  if (prepared.statement_type == duckdb::StatementType::CREATE_STATEMENT) {
    const auto* ci = dynamic_cast<const SereneDBPhysicalCreateIndex*>(&root);
    if (!ci) {
      return nullptr;
    }
    auto reporter = std::make_unique<pg::ProgressReporter>(
      ci->DatabaseId(), ci->TargetRelationId(),
      pg::ProgressCommand::CreateIndex);
    reporter->SetCommand(pg::create_index_progress::Command::CreateIndex);
    reporter->SetPhase(pg::create_index_progress::Phase::Initializing);
    if (ci->estimated_cardinality > 0) {
      reporter->Set(pg::create_index_progress::Param::TuplesTotal,
                    static_cast<int64_t>(ci->estimated_cardinality));
    }
    return reporter;
  }

  return nullptr;
}

}  // namespace

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
      if (conn_ctx.IsExplicitTransaction() && conn_ctx.HasRocksDBSnapshot() &&
          level != conn_ctx.GetIsolationLevel()) {
        throw duckdb::InvalidInputException(
          "SET TRANSACTION ISOLATION LEVEL must be called before any query");
      }
    };
}

void SereneDBClientState::TransactionPreCommit(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context) {
  // Revert SET LOCAL variables while the DuckDB transaction is still active
  // so catalog lookups performed by custom-impl settings (e.g. search_path)
  // can succeed via their normal set_local path.
  _connection_ctx->PreCommit();
}

void SereneDBClientState::TransactionPreRollback(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context,
  duckdb::optional_ptr<duckdb::ErrorData> error) {
  _connection_ctx->PreRollback();
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

duckdb::RebindQueryInfo SereneDBClientState::OnExecutePrepared(
  duckdb::ClientContext& context, duckdb::PreparedStatementCallbackInfo& info,
  duckdb::RebindQueryInfo current_rebind) {
  if (const auto& db = _connection_ctx->GetDatabasePtr()) {
    progress = MakeProgressReporter(db->GetId(), info.prepared_statement);
  }
  return current_rebind;
}

void SereneDBClientState::QueryEnd(duckdb::ClientContext& context) {
  copy_queue = nullptr;
  send_buffer = nullptr;
  copy_stdin_buffer.reset();
  copy_stdin_open_count = 0;
  progress.reset();
  _connection_ctx->OnNewStatement();
}

ConnectionContext& GetSereneDBContext(duckdb::ClientContext& context) {
  auto state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  SDB_ASSERT(state, "SereneDB client state not registered");
  return state->GetConnectionContext();
}

}  // namespace sdb::connector
