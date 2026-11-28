////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/connection_context.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/extra_drop_info.hpp>
#include <duckdb/parser/statement/drop_statement.hpp>
#include <duckdb/planner/operator/logical_attach.hpp>
#include <duckdb/planner/operator/logical_create.hpp>
#include <duckdb/planner/operator/logical_drop.hpp>
#include <duckdb/planner/operator/logical_execute.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/error_mapping.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <magic_enum/magic_enum.hpp>
#include <utility>

#include "backup/backup_session.h"
#include "backup/commit_gate.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "connector/duckdb_storage_extension.h"
#include "connector/iresearch_replacement_scan.h"
#include "pg/catalog/oids.h"
#include "pg/progress_registry.h"

namespace sdb::pg {

LoginCheck RequireLoginRole(std::string_view user, std::string_view dbname) {
  // No ClientContext yet -- the connection is still being established -- so
  // this reads the committed cluster state.
  struct {
    duckdb::idx_t oid = 0;
    bool can_login = false;
    bool superuser = false;
    int32_t conn_limit = -1;
    std::string name;
  } role;
  if (!catalog::ReadEntry<catalog::RoleCatalogEntry>(
        user, [&](const catalog::RoleCatalogEntry& entry) {
          role.oid = entry.oid;
          role.can_login = entry.CanLogin();
          role.superuser = entry.IsSuperuser();
          role.conn_limit = entry.ConnLimit();
          role.name = entry.name.GetIdentifierName();
        })) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
              ERR_MSG("role \"", user, "\" does not exist"))};
  }
  if (!role.can_login) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
              ERR_MSG("role \"", user, "\" is not permitted to log in"))};
  }
  const auto roles = catalog::RolesOf(nullptr);
  const auto principal = roles->Access(role.oid);
  struct {
    duckdb::idx_t oid = 0;
    bool connect = false;
    bool allow_connections = true;
    int32_t conn_limit = -1;
  } database;
  if (!catalog::ReadEntry<catalog::DatabaseCatalogEntry>(
        dbname,
        [&](const catalog::DatabaseCatalogEntry& entry) {
          database.oid = entry.oid;
          database.connect =
            principal &&
            duckdb::Check<duckdb::AclMode::Connect>(*principal, entry);
          database.allow_connections = entry.AllowConnections();
          database.conn_limit = entry.ConnLimit();
        }) ||
      !database.connect) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INVALID_CATALOG_NAME),
              ERR_MSG("database \"", dbname, "\" does not exist"))};
  }
  if (!database.allow_connections) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
              ERR_MSG("database \"", dbname,
                      "\" is not currently accepting connections"))};
  }
  return {.role = role.oid,
          .database = database.oid,
          .superuser = role.superuser,
          .role_conn_limit = role.conn_limit,
          .database_conn_limit = database.conn_limit,
          .role_name = std::move(role.name)};
}

std::optional<irs::pg::SqlErrorData> ConnectionLimitError(
  const LoginCheck& login, std::string_view dbname) {
  if (login.superuser) {
    return std::nullopt;
  }
  auto& registry = ProgressRegistry::Instance();
  if (login.role_conn_limit >= 0 &&
      registry.SessionsOf(login.role) >
        static_cast<size_t>(login.role_conn_limit)) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_TOO_MANY_CONNECTIONS),
      ERR_MSG("too many connections for role \"", login.role_name, "\""));
  }
  if (login.database_conn_limit >= 0 &&
      registry.SessionsIn(login.database) >
        static_cast<size_t>(login.database_conn_limit)) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_TOO_MANY_CONNECTIONS),
      ERR_MSG("too many connections for database \"", dbname, "\""));
  }
  return std::nullopt;
}

bool IsHbaMember(std::string_view user, std::string_view group) {
  duckdb::idx_t user_oid = 0;
  duckdb::idx_t group_oid = 0;
  if (!catalog::ReadEntry<catalog::RoleCatalogEntry>(
        user,
        [&](const catalog::RoleCatalogEntry& role) { user_oid = role.oid; }) ||
      !catalog::ReadEntry<catalog::RoleCatalogEntry>(
        group,
        [&](const catalog::RoleCatalogEntry& role) { group_oid = role.oid; })) {
    return false;
  }
  const auto roles = catalog::RolesOf(nullptr);
  const auto user_access = roles->Access(user_oid);
  return user_access && user_access->BelongsTo(group_oid);
}

}  // namespace sdb::pg
namespace sdb {
namespace {

template<typename T>
T GetEnumValue(std::string_view value) noexcept {
  const auto r = magic_enum::enum_cast<T>(value, magic_enum::case_insensitive);
  SDB_ASSERT(r, "enum value is not validated");
  return *r;
}

std::string RoleName(const duckdb::SecuritySnapshot& roles, duckdb::idx_t role,
                     std::string_view fallback) {
  auto name = roles.NameOf(role);
  return std::string{name.empty() ? fallback : name};
}

void LogRollbackFailure(std::string_view database,
                        const std::exception& e) noexcept try {
  SDB_ERROR(GENERAL,
            "rolling back the transaction of a server session in database \"",
            database, "\" failed: ", duckdb::ErrorData{e}.RawMessage());
} catch (...) {
}

}  // namespace

ConnectionContext::ConnectionContext(duckdb::DatabaseInstance& db,
                                     const SessionIdentity& identity)
  : duckdb::ClientContext{db.shared_from_this(), identity.temporary},
    _user{identity.user},
    _system_user{identity.system_user},
    _database_id{identity.database_id},
    _backend_pid{identity.backend_pid},
    _cancel_registry{identity.cancel_registry},
    _send_buffer{identity.send_buffer},
    _login_role_id{identity.role},
    _progress{std::make_shared<pg::ProgressSource>()},
    _slot{catalog::Transactions::Get().Join()} {
  session_user = _user;
  session_role = identity.role;
  SetCurrentRole(identity.role);
  PublishSuperuser();
  _progress->pid = _backend_pid;
  _progress->datid = static_cast<int64_t>(_database_id);
  _progress->user = _user;
  _progress->database = identity.database;
  _progress->client_backend = identity.client_backend;
  _progress->role = identity.role;
  _progress->backend_start_us = duckdb::Timestamp::GetCurrentTimestamp().value;
  _progress->status.Write({.state_change_us = _progress->backend_start_us});
  _progress->ctx = this;
  if (identity.database.empty()) {
    return;
  }
  const duckdb::Identifier name{identity.database};
  std::vector<duckdb::CatalogSearchEntry> paths;
  if (auto attached = db.GetDatabaseManager().GetDatabase(name);
      attached && !catalog::IsSereneDB(attached->GetCatalog())) {
    paths.emplace_back(name, duckdb::Identifier::DefaultSchema());
    paths.emplace_back(
      duckdb::Identifier{irs::StaticStrings::kDefaultDatabase},
      duckdb::Identifier{irs::StaticStrings::kPgCatalogSchema});
  } else {
    paths.emplace_back(name, duckdb::Identifier{"$user"});
    paths.emplace_back(name, duckdb::Identifier{"public"});
  }
  client_data->default_database = name;
  auto& search_path = *client_data->catalog_search_path;
  search_path.SetDefaultPaths(std::vector{paths});
  search_path.Set(std::move(paths), duckdb::CatalogSetPathType::SET_DIRECTLY);
}

ConnectionContext::~ConnectionContext() {
  _commit_pass.reset();
  if (!duckdb::Exception::UncaughtException()) {
    try {
      Destroy();
    } catch (...) {
    }
    if (_progress->client_backend) {
      client_data->temporary_objects->OnDetach(*this);
    }
  }
  ConsumeNotices([](auto&) {});
  backup::BackupSession::Instance().AbortIfOwnedBy(this);
  pg::ProgressRegistry::Instance().CollectTransactions(*_progress);
  ReleaseSnapshot();
  catalog::Transactions::Get().Leave(_slot);
}

std::string ConnectionContext::EffectiveUserName() {
  return RoleName(*catalog::RolesOf(this), GetRoleId(), _user);
}

std::string ConnectionContext::SessionUserName() {
  return RoleName(*catalog::RolesOf(this), GetSessionRoleId(), _user);
}

void ConnectionContext::ApplyRoleSettings() {
  duckdb::vector<std::string> database_config;
  duckdb::vector<std::string> database_role_config;
  catalog::ReadEntry<catalog::DatabaseCatalogEntry>(
    GetDatabase(), [&](const catalog::DatabaseCatalogEntry& database) {
      if (const auto config = database.SettingsOf(0)) {
        database_config = *config;
      }
      if (const auto config = database.SettingsOf(_login_role_id)) {
        database_role_config = *config;
      }
    });
  duckdb::vector<std::string> role_config;
  catalog::ReadEntry<catalog::RoleCatalogEntry>(
    _user, [&](const catalog::RoleCatalogEntry& role) {
      role_config = role.Config();
    });
  for (const auto* config :
       {&database_config, &role_config, &database_role_config}) {
    for (const std::string_view entry : *config) {
      try {
        SetSettingChecked(duckdb::Settings::ConfigKey(entry),
                          std::string{duckdb::Settings::ConfigValue(entry)});
      } catch (const std::exception& exception) {
        AddNotice(irs::pg::ToSqlError(exception));
      }
    }
  }
}

void ConnectionContext::PreventInTransactionBlock(
  std::string_view statement) const {
  if (_in_transaction_block) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
      ERR_MSG(statement, " cannot run inside a transaction block"));
  }
}

int8_t ConnectionContext::GetExtraFloatDigits() const {
  duckdb::Value value;
  auto ok = TryGetCurrentSetting("extra_float_digits", value);
  SDB_ASSERT(ok);
  return static_cast<int8_t>(value.GetValue<int32_t>());
}

ByteaOutput ConnectionContext::GetByteaOutput() {
  auto value = Get("bytea_output");
  SDB_ASSERT(value);
  return GetEnumValue<ByteaOutput>(*value);
}

std::string ConnectionContext::GetTimeZone() const {
  duckdb::Value value;
  auto ok = TryGetCurrentSetting("TimeZone", value);
  SDB_ASSERT(ok && !value.IsNull());
  return value.ToString();
}

std::optional<std::string> ConnectionContext::Get(std::string_view key) {
  duckdb::Value value;
  if (TryGetCurrentSetting(duckdb::Identifier{key}, value)) {
    return duckdb::Settings::FormatDisplayValue(*this, value).ToString();
  }
  return std::nullopt;
}

void ConnectionContext::SetSetting(std::string_view key, std::string value,
                                   bool) {
  auto& db_config = duckdb::DBConfig::GetConfig(*db);
  const duckdb::Identifier setting{key};
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  auto setting_index = db_config.TryGetSettingIndex(setting, option);
  if (!setting_index.IsValid()) {
    return;
  }
  config.user_settings.SetUserSetting(setting_index.GetIndex(),
                                      duckdb::Value{std::move(value)});
  duckdb::ExtensionOption ext;
  if (option ? option->rebind
             : db_config.TryGetExtensionOption(setting, ext) && ext.rebind) {
    BumpBindingGeneration();
  }
}

void ConnectionContext::SetSettingChecked(std::string_view key,
                                          std::string value) {
  duckdb::PhysicalSet::SetVariable(*this, duckdb::Identifier{key},
                                   duckdb::SetScope::SESSION,
                                   duckdb::Value{std::move(value)});
}

void ConnectionContext::RefreshCatalogEpoch() noexcept {
  if (_transaction) {
    _catalog_epoch = _transaction->CatalogEpoch();
    return;
  }
  auto& transactions = catalog::Transactions::Get();
  _catalog_epoch = transactions.CatalogEpoch(transactions.LastCommit());
}

std::shared_ptr<catalog::SessionTransaction>
ConnectionContext::AcquireTransaction() {
  const auto owner = duckdb::MetaTransaction::Get(*this).global_transaction_id;
  absl::MutexLock lock{&_transaction_mutex};
  if (!_transaction || _transaction_owner != owner) {
    SDB_PARK_ONCE_ON_FAILURE("session_transaction_before_snapshot");
    ReleaseSnapshot();
    _transaction = std::make_shared<catalog::SessionTransaction>(
      catalog::Transactions::Get().NextId(), _slot);
    _transaction_owner = owner;
  }
  return _transaction;
}

bool ConnectionContext::IsStableSnapshot() const {
  return IsExplicitTransaction() &&
         GetIsolationLevel() == IsolationLevel::REPEATABLE_READ;
}

void ConnectionContext::ReleaseSnapshot() noexcept {
  const auto snapshot = std::min(_slot.snapshot.load(std::memory_order_relaxed),
                                 _slot.held.load(std::memory_order_relaxed));
  if (snapshot == catalog::kNoSnapshot) {
    return;
  }
  _slot.held.store(catalog::kNoSnapshot, std::memory_order_seq_cst);
  _slot.snapshot.store(catalog::kNoSnapshot, std::memory_order_seq_cst);
  catalog::Transactions::Get().Ended(snapshot);
}

void ConnectionContext::EndTransaction() noexcept {
  ReleaseSnapshot();
  _transaction.reset();
  _transaction_owner = duckdb::MAX_TRANSACTION_ID;
  _had_query_in_transaction = false;
}

void ConnectionContext::OnSet(std::string_view name, bool is_local,
                              duckdb::Value old_value) {
  auto canonical = GetOriginalName(name);
  std::string_view key_view = canonical.data() ? canonical : name;
  auto [it, inserted] = _variables.try_emplace(key_view);
  if (inserted) {
    it->second.rollback_restore = old_value;
    if (is_local) {
      it->second.commit_restore = std::move(old_value);
    }
    return;
  }
  if (is_local) {
    if (!it->second.commit_restore) {
      it->second.commit_restore = std::move(old_value);
    }
  } else {
    it->second.commit_restore.reset();
  }
}

void ConnectionContext::RestoreValue(std::string_view key,
                                     duckdb::Value value) noexcept {
  auto& db_config = duckdb::DBConfig::GetConfig(*db);
  const duckdb::Identifier setting{key};
  const bool is_reset = value.IsNull();
  try {
    duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
    auto setting_index = db_config.TryGetSettingIndex(setting, option);
    if (option) {
      if (is_reset) {
        if (option->reset_local) {
          option->reset_local(*this);
        } else if (setting_index.IsValid()) {
          config.user_settings.ClearSetting(setting_index.GetIndex());
        }
      } else if (option->set_local) {
        auto parameter_type =
          duckdb::DBConfig::ParseLogicalType(option->parameter_type);
        auto typed = value.CastAs(*this, parameter_type);
        option->set_local(*this, typed);
      } else if (setting_index.IsValid()) {
        config.user_settings.SetUserSetting(setting_index.GetIndex(),
                                            std::move(value));
      }
      if (option->rebind) {
        BumpBindingGeneration();
      }
      return;
    }
    duckdb::ExtensionOption ext;
    if (!db_config.TryGetExtensionOption(setting, ext)) {
      if (duckdb::Settings::IsValidCustomName(key)) {
        config.custom_settings[key] =
          is_reset ? duckdb::Value{std::string{}} : std::move(value);
      }
      return;
    }
    if (is_reset) {
      if (ext.reset_function) {
        ext.reset_function(*this, duckdb::SetScope::SESSION);
      }
      if (ext.setting_index.IsValid()) {
        config.user_settings.ClearSetting(ext.setting_index.GetIndex());
      }
    } else {
      auto typed = value.CastAs(*this, ext.type);
      if (ext.set_function) {
        ext.set_function(*this, duckdb::SetScope::SESSION, typed);
      }
      if (ext.setting_index.IsValid()) {
        config.user_settings.SetUserSetting(ext.setting_index.GetIndex(),
                                            std::move(typed));
      }
    }
    if (ext.rebind) {
      BumpBindingGeneration();
    }
  } catch (const std::exception& e) {
    WarnUser(absl::StrCat("could not restore setting \"", key,
                          "\": ", duckdb::ErrorData{e}.RawMessage()));
  }
}

void ConnectionContext::RollbackVariables() noexcept {
  if (_variables.empty()) {
    return;
  }
  for (auto&& [key, var] : _variables) {
    RestoreValue(key, std::move(var.rollback_restore));
  }
  _variables.clear();
  MarkSettingsChanged();
}

void ConnectionContext::CommitVariables() noexcept {
  bool restored = false;
  for (auto&& [key, var] : _variables) {
    if (var.commit_restore) {
      RestoreValue(key, std::move(*var.commit_restore));
      restored = true;
    }
  }
  _variables.clear();
  if (restored) {
    MarkSettingsChanged();
  }
}

void ConnectionContext::ValidateIsolationLevel(
  duckdb::TransactionIsolationLevel level) {
  if (level != duckdb::TransactionIsolationLevel::READ_COMMITTED &&
      level != duckdb::TransactionIsolationLevel::REPEATABLE_READ) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("transaction isolation level \"",
              duckdb::EnumUtil::ToChars(level), "\" is not supported"),
      ERR_HINT("Available values: repeatable read, read committed."));
  }
  if (IsExplicitTransaction() && HadQueryInTransaction() &&
      level != GetIsolationLevel()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
      ERR_MSG(
        "SET TRANSACTION ISOLATION LEVEL must be called before any query"));
  }
}

bool ConnectionContext::EmitWarning(const char* message,
                                    std::string_view subtype) {
  auto code = ERRCODE_WARNING;
  if (subtype == "NO_ACTIVE_SQL_TRANSACTION") {
    code = ERRCODE_NO_ACTIVE_SQL_TRANSACTION;
  } else if (subtype == "ACTIVE_SQL_TRANSACTION") {
    code = ERRCODE_ACTIVE_SQL_TRANSACTION;
  } else if (subtype == "PRIVILEGE_NOT_GRANTED") {
    code = ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED;
  } else if (subtype == "PRIVILEGE_NOT_REVOKED") {
    code = ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED;
  }
  AddNotice(SQL_ERROR_DATA(ERR_CODE(code), ERR_MSG(message)));
  return true;
}

void ConnectionContext::OnSettingChange(const std::string& name,
                                        duckdb::SetScope scope,
                                        const duckdb::Value& previous) {
  if (scope == duckdb::SetScope::GLOBAL) {
    return;
  }
  MarkSettingsChanged();
  if (IsNonTransactionalSetting(name)) {
    return;
  }
  OnSet(name, scope == duckdb::SetScope::LOCAL, previous);
}

void ConnectionContext::OnQueryBegin() {
  RefreshCatalogEpoch();
  if (!IsStableSnapshot() && transaction.HasActiveTransaction() &&
      !transaction.IsAutoCommit()) {
    transaction.ActiveTransaction().RefreshStartTime();
  }
  if (_internal_statements != 0) {
    return;
  }
  _query_in_transaction = true;
  _progress->BeginQuery(GetCurrentQuery());
  if (pending_copy_command != pg::ProgressCommand::None) {
    auto& metrics = _progress->metrics;
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

void ConnectionContext::OnQueryEnd() {
  copy_stdin_open_count = 0;
  copy_stdin_done = false;
  if (!_progress->client_backend && _internal_statements == 0) {
    ReportIdle();
  }
  RefreshCatalogEpoch();
  if (_transaction) {
    _transaction->ResetSearchPins(!IsStableSnapshot());
  }
}

void ConnectionContext::ReportIdle() {
  _progress->ReportIdle(pg::IdleState(transaction));
}

void ConnectionContext::OnTransactionPreCommit(duckdb::MetaTransaction& meta) {
  if (_transaction) {
    _transaction->PrepareCommit(*this);
  }
  SDB_IF_FAILURE("crash_before_commit") {
    if (meta.ModifiedDatabase()) {
      SDB_IMMEDIATE_ABORT();
    }
  }
  std::vector<std::reference_wrapper<duckdb::AttachedDatabase>> written;
  for (auto& db : meta.OpenedTransactions()) {
    if (db.get().IsTemporary() || db.get().IsReadOnly() ||
        !catalog::IsSereneDB(db.get().GetCatalog())) {
      continue;
    }
    if (auto opened = meta.TryGetTransaction(db);
        opened && !opened->IsReadOnly()) {
      written.emplace_back(db);
    }
  }
  for (auto& db : written) {
    const auto& name = db.get().GetName();
    if (db.get().GetCatalog().Cast<catalog::SereneDBCatalog>().IsDropped()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                      ERR_MSG("database \"", name.GetIdentifierName(),
                              "\" was dropped by another transaction"));
    }
    auto& cluster = catalog::ClusterOf(*this);
    if (!cluster.Set(duckdb::CatalogType::DATABASE_ENTRY)
           .Get(cluster.GetCatalogTransaction(*this), name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
        ERR_MSG("database \"", name.GetIdentifierName(), "\" does not exist"));
    }
  }
  CommitVariables();
  if (meta.ModifiedDatabase() || !written.empty()) {
    if (!_commit_pass) {
      _commit_pass = backup::CommitGate().Enter(*this);
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_commit_in_gate");
  }
}

void ConnectionContext::OnTransactionPreRollback(duckdb::MetaTransaction&) {
  RollbackVariables();
}

void ConnectionContext::OnTransactionCommit(duckdb::MetaTransaction& meta) {
  SDB_IF_FAILURE("crash_after_commit") {
    if (meta.ModifiedDatabase()) {
      SDB_IMMEDIATE_ABORT();
    }
  }
  _commit_pass.reset();
  if (std::exchange(_query_in_transaction, false)) {
    _progress->xact_commit.fetch_add(1, std::memory_order_relaxed);
  }
  EndTransaction();
  RefreshCatalogEpoch();
}

void ConnectionContext::OnTransactionRollback(duckdb::MetaTransaction&) {
  _commit_pass.reset();
  if (std::exchange(_query_in_transaction, false)) {
    _progress->xact_rollback.fetch_add(1, std::memory_order_relaxed);
  }
  RollbackVariables();
  EndTransaction();
  RefreshCatalogEpoch();
}

void ConnectionContext::OnBoundPlan(duckdb::Binder& binder,
                                    duckdb::LogicalOperator& plan) {
  connector::FilterPartialIndexScans(*this, plan);
  const auto discard_all = [](const duckdb::DropInfo& info) {
    return info.type == duckdb::CatalogType::PREPARED_STATEMENT &&
           info.extra_drop_info &&
           info.extra_drop_info->Cast<duckdb::ExtraDropDiscardInfo>().target ==
             duckdb::DiscardTarget::ALL;
  };
  auto* root = &plan;
  if ((plan.type == duckdb::LogicalOperatorType::LOGICAL_PREPARE ||
       plan.type == duckdb::LogicalOperatorType::LOGICAL_EXECUTE) &&
      !plan.children.empty()) {
    root = plan.children[0].get();
  }
  switch (root->type) {
    case duckdb::LogicalOperatorType::LOGICAL_ATTACH:
      connector::RequireDatabaseNotBeingCreated(
        *this, root->Cast<duckdb::LogicalAttach>().info->name);
      break;
    case duckdb::LogicalOperatorType::LOGICAL_CREATE_DATABASE:
      connector::RequireDatabaseNotBeingCreated(
        *this,
        root->Cast<duckdb::LogicalCreate>().info->GetQualifiedName().Name());
      break;
    case duckdb::LogicalOperatorType::LOGICAL_DROP:
      if (discard_all(*root->Cast<duckdb::LogicalDrop>().info)) {
        PreventInTransactionBlock("DISCARD ALL");
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_CREATE_VIEW:
      SDB_IF_FAILURE("view_without_name_bindings") {
        root->Cast<duckdb::LogicalCreate>()
          .info->Cast<duckdb::CreateViewInfo>()
          .name_bindings.clear();
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_EXECUTE: {
      const auto& unbound =
        root->Cast<duckdb::LogicalExecute>().prepared->unbound_statement;
      if (unbound && unbound->type == duckdb::StatementType::DROP_STATEMENT &&
          discard_all(*unbound->Cast<duckdb::DropStatement>().info)) {
        PreventInTransactionBlock("DISCARD ALL");
      }
      break;
    }
    default:
      break;
  }
}

duckdb::shared_ptr<duckdb::ClientContext> CreateClientContext(
  duckdb::DatabaseInstance& db) {
  auto& manager = db.GetDatabaseManager();
  const auto name = manager.GetDefaultDatabase();
  duckdb::idx_t oid = 0;
  if (!name.GetIdentifierName().empty()) {
    if (auto attached = manager.GetDatabase(name)) {
      oid = attached->oid;
    }
  }
  return duckdb::make_shared_ptr<ConnectionContext>(
    db, SessionIdentity{.user = irs::StaticStrings::kDefaultUser,
                        .role = pg::kRootUser,
                        .database = name.GetIdentifierName(),
                        .database_id = oid});
}

duckdb::unique_ptr<duckdb::Connection> MakeConnection(
  duckdb::DatabaseInstance& db, const SessionIdentity& identity) {
  return duckdb::make_uniq<duckdb::Connection>(
    duckdb::make_shared_ptr<ConnectionContext>(db, identity));
}

void HandNotices(ConnectionContext& ctx, const NoticeTarget& target) {
  if (target.forward_to) {
    ctx.ConsumeNotices([&](irs::pg::SqlErrorData& notice) {
      target.forward_to->AddNotice(std::move(notice));
    });
    return;
  }
  ctx.ConsumeNotices([&](const irs::pg::SqlErrorData& notice) {
    SDB_IF_FAILURE("log_notice_fails") {
      THROW_SQL_ERROR(ERR_MSG("injected notice log failure"));
    }
    SDB_WARN(GENERAL, target.what, " \"", target.name, "\": ", notice.errmsg);
  });
}

SystemConnection::SystemConnection(const SystemSession& session,
                                   const NoticeTarget& notices)
  : conn{MakeConnection(irs::DuckDBEngine::Instance().instance(),
                        {.user = session.user,
                         .system_user = session.system_user,
                         .role = session.role,
                         .database = session.database,
                         .database_id = session.database_id,
                         .backend_pid = session.backend_pid,
                         .temporary = session.temporary})},
    progress{ConnectionContext::Get(*conn->context).ProgressSource()},
    notices{notices} {}

SystemConnection::~SystemConnection() {
  auto& context = ConnectionContext::Get(*conn->context);
  if (context.transaction.HasActiveTransaction()) {
    try {
      context.transaction.Rollback(nullptr);
    } catch (const std::exception& e) {
      LogRollbackFailure(context.GetDatabase(), e);
    }
  }
  try {
    HandNotices(context, notices);
  } catch (...) {
  }
}

}  // namespace sdb
