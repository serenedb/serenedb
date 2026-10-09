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
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table/index_entry.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/local_storage.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <utility>

#include "auth/enforce.h"
#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "connector/inverted_store_index.h"
#include "pg/connection_context.h"
#include "query/config.h"

namespace sdb::connector {

SereneDBClientState& SereneDBClientState::Register(
  duckdb::ClientContext& client_ctx,
  std::shared_ptr<ConnectionContext> connection_ctx) {
  auto state =
    duckdb::make_shared_ptr<SereneDBClientState>(std::move(connection_ctx));
  auto& registered = *state;

  auto source = std::make_shared<pg::ProgressSource>();
  source->pid = registered._connection_ctx->GetBackendPid();
  source->datid = registered._connection_ctx->GetDatabaseId();
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

  client_ctx.setting_change_handler =
    [](duckdb::ClientContext& ctx, const std::string& name,
       duckdb::SetScope scope, const duckdb::Value* new_value) {
      // Refused the way PG refuses a postmaster-scoped GUC. Checked before the
      // SetScope::GLOBAL return below, since some of these are global and would
      // otherwise slip past it. DuckDB routes SET and RESET, for built-in
      // settings and extension options alike, through this handler, and does so
      // before invoking an option's own set_function -- so an entry here needs
      // no callback of its own.
      if (IsUnchangeableSetting(name)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                        ERR_MSG("parameter \"", name, "\" cannot be changed"));
      }
      if (new_value && IsCompatSetting(name)) {
        NoticeIfChanged(ctx, name, *new_value);
      }
      // Resolve AUTOMATIC against the setting's target scope so the downstream
      // check works uniformly regardless of how the user wrote the SET.
      if (scope == duckdb::SetScope::AUTOMATIC) {
        auto& db_config = duckdb::DBConfig::GetConfig(ctx);
        const duckdb::Identifier setting{name};
        duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
        if (db_config.TryGetSettingIndex(setting, option).IsValid() && option) {
          scope = (option->scope == duckdb::SettingScopeTarget::GLOBAL_ONLY ||
                   option->scope == duckdb::SettingScopeTarget::GLOBAL_DEFAULT)
                    ? duckdb::SetScope::GLOBAL
                    : duckdb::SetScope::SESSION;
        } else {
          duckdb::ExtensionOption ext;
          if (db_config.TryGetExtensionOption(setting, ext)) {
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
      // ParameterStatus at the next ReadyForQuery (a cheap version bump; the
      // GUC poll itself is skipped entirely when nothing changed).
      sdb_ctx.MarkSettingsChanged();
      // Outside an explicit transaction there's nothing to roll back --
      // the map stays empty.
      if (!sdb_ctx.IsExplicitTransaction()) {
        return;
      }
      duckdb::Value old_value;
      ctx.TryGetCurrentSetting(duckdb::Identifier{name}, old_value);
      sdb_ctx.OnSet(name, scope == duckdb::SetScope::LOCAL,
                    std::move(old_value), new_value);
    };

  client_ctx.setting_visibility = [](duckdb::ClientContext&,
                                     const std::string& name) {
    static const irs::containers::FlatHashSet<std::string_view> kHidden = {
      "sdb_faults", "debug_verification", "is_superuser", "role",
      "session_authorization"};
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
  // crash (the fault-arming SET itself must survive).
  SDB_IF_FAILURE("crash_before_commit") {
    if (transaction.ModifiedDatabase()) {
      SDB_IMMEDIATE_ABORT();
    }
  }
  // Revert SET LOCAL variables while the DuckDB transaction is still active
  // so catalog lookups performed by custom-impl settings (e.g. search_path)
  // can succeed via their normal set_local path.
  _connection_ctx->PreCommit();
  std::vector<std::reference_wrapper<duckdb::AttachedDatabase>> written;
  for (auto& db : transaction.OpenedTransactions()) {
    if (db.get().GetCatalog().GetCatalogType() !=
        catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    auto opened = transaction.TryGetTransaction(db);
    if (opened && opened->IsDuckTransaction() &&
        opened->Cast<duckdb::DuckTransaction>().ChangesMade()) {
      written.emplace_back(db);
    }
  }
  for (auto& db : written) {
    const auto& name = db.get().GetName();
    if (db.get().GetCatalog().IsDropped()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                      ERR_MSG("database \"", name.GetIdentifierName(),
                              "\" was dropped by another transaction"));
    }
    auto& cluster = catalog::ClusterOf(context);
    if (!cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
           .GetEntry(cluster.GetCatalogTransaction(context), name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
        ERR_MSG("database \"", name.GetIdentifierName(), "\" does not exist"));
    }
  }
  if (InvertedStoreIndex::AnyBound()) {
    const auto opened_databases = transaction.OpenedTransactions();
    for (auto& db : opened_databases) {
      if (db.get().GetCatalog().GetCatalogType() !=
          catalog::SereneDBCatalog::kStorageType) {
        continue;
      }
      auto opened = transaction.TryGetTransaction(db);
      if (!opened || !opened->IsDuckTransaction()) {
        continue;
      }
      auto& local =
        duckdb::LocalStorage::Get(opened->Cast<duckdb::DuckTransaction>());
      for (auto& table : local.GetTables()) {
        const auto rows = local.AddedRows(table);
        if (rows == 0) {
          continue;
        }
        for (auto entry :
             table.get().GetDataTableInfo()->GetIndexes().IndexEntries()) {
          if (entry->GetBindState() == duckdb::IndexBindState::BOUND &&
              entry->GetIndexType() == InvertedStoreIndex::kTypeName) {
            auto index = entry->GetWriteHandle<InvertedStoreIndex>();
            index->PrepareFeed(*_connection_ctx, context, rows);
          }
        }
      }
    }
  }
  tls_committing_ctx = _connection_ctx.get();
}

void SereneDBClientState::TransactionPreCheckpoint(
  duckdb::AttachedDatabase& db, duckdb::ClientContext&,
  duckdb::idx_t wal_generation, duckdb::idx_t wal_end_offset) {
  if (db.GetCatalog().GetCatalogType() !=
      catalog::SereneDBCatalog::kStorageType) {
    return;
  }
  // This commit's exact WAL position, captured under the WAL lock by the
  // engine: with overlapping commits, reading the WAL size here would include
  // later transactions' bytes and over-claim the recovery cursor (skipping
  // their re-stream after a crash).
  _connection_ctx->CommitSearch(
    search::WalCursor{wal_generation, wal_end_offset}, db.oid);
}

void SereneDBClientState::TransactionPreWalWrite(duckdb::AttachedDatabase& db,
                                                 duckdb::ClientContext&) {
  _connection_ctx->RefreshCreatedIndexes(db.oid);
}

void SereneDBClientState::TransactionPreRollback(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context,
  duckdb::optional_ptr<duckdb::ErrorData> error) {
  _connection_ctx->PreRollback();
}

void SereneDBClientState::TransactionCommit(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context) {
  // Post-durability crash point: the engine commit is durable, search
  // ticks are not yet -- recovery must rebuild the storage. Only write
  // transactions crash.
  SDB_IF_FAILURE("crash_after_commit") {
    if (transaction.ModifiedDatabase()) {
      SDB_IMMEDIATE_ABORT();
    }
  }
  tls_committing_ctx = nullptr;
  _connection_ctx->Commit();
  if (transaction.ModifiedDatabase()) {
    catalog::ClusterOf(*context.db).MaybeCompactCatalogLog();
  }
}

void SereneDBClientState::TransactionRollback(
  duckdb::MetaTransaction& transaction, duckdb::ClientContext& context) {
  tls_committing_ctx = nullptr;
  _connection_ctx->Rollback();
}

void SereneDBClientState::QueryBegin(duckdb::ClientContext& context) {
  _connection_ctx->OnStatementBegin();
  progress_source->BeginQuery(context.GetCurrentQuery());
  if (pending_copy_command != pg::ProgressCommand::None) {
    auto& metrics = progress_source->metrics;
    metrics.SetCommand(pending_copy_command);
    if (pending_copy_command == pg::ProgressCommand::CreateTableAs) {
      metrics.SetPhase(pg::progress_phase::CreateTableAs::Ingesting);
    }
    metrics.SetIoType(pending_copy_io);
    pg::ProgressMetrics::Set(metrics.relid,
                             static_cast<int64_t>(pending_copy_relid));
    pending_copy_command = pg::ProgressCommand::None;
    pending_copy_io = pg::ProgressIoType::None;
    pending_copy_relid = {};
  }
}

void SereneDBClientState::QueryEnd(duckdb::ClientContext& context) {
  copy_stdin_open_count = 0;
  copy_stdin_done = false;
  progress_source->EndQuery();
  _connection_ctx->OnStatementEnd();
}

void SereneDBClientState::OnBoundPlan(duckdb::ClientContext& context,
                                      duckdb::Binder& binder,
                                      duckdb::LogicalOperator& plan) {
  auth::EnforcePlan(context, *_connection_ctx, binder, plan);
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

void SetDefaultSearchPath(duckdb::ClientContext& context,
                          std::string_view database) {
  const duckdb::Identifier catalog{database};
  std::vector<duckdb::CatalogSearchEntry> paths{
    duckdb::CatalogSearchEntry{catalog, duckdb::Identifier{"$user"}},
    duckdb::CatalogSearchEntry{catalog, duckdb::Identifier{"public"}},
  };
  auto& search_path = *context.client_data->catalog_search_path;
  search_path.SetDefaultPaths(std::vector{paths});
  search_path.Set(std::move(paths), duckdb::CatalogSetPathType::SET_DIRECTLY);
}

SystemConnection MakeSystemConnection(std::string_view user, duckdb::idx_t role,
                                      std::string_view database,
                                      duckdb::idx_t database_id) {
  SystemConnection system{.conn =
                            irs::DuckDBEngine::Instance().CreateConnection()};
  auto& context = *system.conn->context;
  system.ctx = std::make_shared<ConnectionContext>(
    context, user, role, database, database_id, nullptr, 0, nullptr);
  SereneDBClientState::Register(context, system.ctx);
  context.session_user.assign(irs::StaticStrings::kDefaultUser);
  SetDefaultSearchPath(context, database);
  return system;
}

}  // namespace sdb::connector
