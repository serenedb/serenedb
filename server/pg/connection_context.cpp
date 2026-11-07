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
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <magic_enum/magic_enum.hpp>
#include <utility>

#include "auth/enforce.h"
#include "auth/role_closure.h"
#include "backup/backup_session.h"
#include "backup/commit_gate.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "connector/functions/ai/common.h"
#include "connector/iresearch_replacement_scan.h"
#include "pg/catalog/oids.h"
#include "server/utils/app_server.h"

namespace sdb::pg {

LoginCheck RequireLoginRole(std::string_view user, std::string_view dbname,
                            const duckdb::Permissions& perm) {
  // No ClientContext yet -- the connection is still being established -- so
  // this reads the committed cluster state.
  struct {
    duckdb::idx_t oid = 0;
    bool can_login = false;
    bool superuser = false;
  } role;
  if (!catalog::ReadRole(user, [&](const catalog::RoleCatalogEntry& entry) {
        role.oid = entry.oid;
        role.can_login = entry.CanLogin();
        role.superuser = entry.IsSuperuser();
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
  if (!auth::ClosureFor(nullptr, role.oid)
         ->Can(duckdb::CatalogType::DATABASE_ENTRY, perm,
               duckdb::AclMode::Connect)) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
              ERR_MSG("permission denied for database \"", dbname, "\""),
              ERR_DETAIL("User does not have CONNECT privilege."))};
  }
  return {.role = role.oid, .superuser = role.superuser};
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

std::string RoleName(const auth::RoleGraph& roles, duckdb::idx_t role,
                     std::string_view fallback) {
  auto name = roles.NameOf(role);
  return std::string{name.empty() ? fallback : name};
}

}  // namespace

ConnectionContext::ConnectionContext(duckdb::DatabaseInstance& db,
                                     const SessionIdentity& identity)
  : duckdb::ClientContext{db.shared_from_this()},
    _user{identity.user},
    _system_user{identity.system_user},
    _database_name{identity.database},
    _database_id{identity.database_id},
    _backend_pid{identity.backend_pid},
    _cancel_registry{identity.cancel_registry},
    _send_buffer{identity.send_buffer},
    _login_role_id{identity.role},
    _session_role_id{identity.role},
    _effective_role_id{identity.role},
    _progress{std::make_shared<pg::ProgressSource>()},
    _ai{std::make_unique<connector::ai::AIQuery>()} {
  session_user = _user;
  _progress->pid = _backend_pid;
  _progress->datid = static_cast<int64_t>(_database_id);
  _progress->user = _user;
  _progress->database = _database_name;
  _progress->client_backend = identity.client_backend;
  _progress->backend_start_us = duckdb::Timestamp::GetCurrentTimestamp().value;
  _progress->ctx = this;
  if (_database_name.empty()) {
    return;
  }
  const duckdb::Identifier name{_database_name};
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
  if (!duckdb::Exception::UncaughtException()) {
    try {
      Destroy();
    } catch (...) {
    }
  }
  ConsumeNotices([](auto&) {});
  backup::BackupSession::Instance().AbortIfOwnedBy(this);
  pg::ProgressRegistry::Instance().CollectTransactions(*_progress);
}

std::string ConnectionContext::EffectiveUserName() {
  return RoleName(*auth::RolesOf(this), _effective_role_id, _user);
}

std::string ConnectionContext::SessionUserName() {
  return RoleName(*auth::RolesOf(this), _session_role_id, _user);
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
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  auto setting_index =
    db_config.TryGetSettingIndex(duckdb::Identifier{key}, option);
  if (setting_index.IsValid()) {
    config.user_settings.SetUserSetting(setting_index.GetIndex(),
                                        duckdb::Value{std::move(value)});
  }
}

void ConnectionContext::SetSettingChecked(std::string_view key,
                                          std::string value, bool is_local) {
  duckdb::PhysicalSet::SetVariable(
    *this, duckdb::Identifier{key},
    is_local ? duckdb::SetScope::LOCAL : duckdb::SetScope::SESSION,
    duckdb::Value{std::move(value)});
}

void ConnectionContext::RefreshCatalogEpoch() noexcept {
  if (_transaction && !_transaction->Changes().empty()) {
    _catalog_epoch =
      duckdb::TRANSACTION_ID_START + _transaction->Changes().size();
    return;
  }
  _catalog_epoch = catalog::Transactions::Get().CatalogVersion();
}

std::shared_ptr<catalog::SessionTransaction>
ConnectionContext::AcquireTransaction() {
  const auto owner = duckdb::MetaTransaction::Get(*this).global_transaction_id;
  absl::MutexLock lock{&_transaction_mutex};
  if (!_transaction || _transaction_owner != owner) {
    _transaction = std::make_shared<catalog::SessionTransaction>(
      catalog::Transactions::Get().NextId());
    _transaction_owner = owner;
  }
  return _transaction;
}

bool ConnectionContext::IsStableSnapshot() const {
  return IsExplicitTransaction() &&
         GetIsolationLevel() == IsolationLevel::REPEATABLE_READ;
}

bool ConnectionContext::TryDropSearchReader(duckdb::idx_t database_id,
                                            duckdb::idx_t shard_id) {
  if (IsStableSnapshot() || !_transaction || !_transaction->SearchWrites()) {
    return false;
  }
  _transaction->ResetSearchReader(database_id, shard_id);
  return true;
}

void ConnectionContext::EndTransaction() noexcept {
  _transaction.reset();
  _transaction_owner = duckdb::MAX_TRANSACTION_ID;
  _had_query_in_transaction = false;
  _statement_is_dml = false;
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
      return;
    }
    duckdb::ExtensionOption ext;
    if (!db_config.TryGetExtensionOption(setting, ext)) {
      return;
    }
    if (is_reset) {
      if (ext.reset_function) {
        ext.reset_function(*this, duckdb::SetScope::SESSION);
      }
      if (ext.setting_index.IsValid()) {
        config.user_settings.ClearSetting(ext.setting_index.GetIndex());
      }
      return;
    }
    auto typed = value.CastAs(*this, ext.type);
    if (ext.set_function) {
      ext.set_function(*this, duckdb::SetScope::SESSION, typed);
    }
    if (ext.setting_index.IsValid()) {
      config.user_settings.SetUserSetting(ext.setting_index.GetIndex(),
                                          std::move(typed));
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

bool ConnectionContext::EmitWarning(const char* message) {
  if (absl::StrContains(message, "no transaction is active")) {
    AddNotice(SQL_ERROR_DATA(ERR_CODE(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
                             ERR_MSG("there is no transaction in progress")));
    return true;
  }
  AddNotice(SQL_ERROR_DATA(ERR_CODE(ERRCODE_WARNING), ERR_MSG(message)));
  return true;
}

bool ConnectionContext::IsSettingVisible(const std::string& name) {
  static const irs::containers::FlatHashSet<std::string_view> kHidden = {
    "sdb_faults", "debug_verification", "is_superuser", "role",
    "session_authorization"};
  return !kHidden.contains(name);
}

void ConnectionContext::OnSettingChange(const std::string& name,
                                        duckdb::SetScope scope) {
  if (IsUnchangeableSetting(name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                    ERR_MSG("parameter \"", name, "\" cannot be changed"));
  }
  if (scope == duckdb::SetScope::AUTOMATIC) {
    auto& db_config = duckdb::DBConfig::GetConfig(*this);
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
  if (scope == duckdb::SetScope::GLOBAL) {
    RequireSuperuserToSetGlobal(*this, name);
    return;
  }
  MarkSettingsChanged();
  if (!IsExplicitTransaction() || IsNonTransactionalSetting(name)) {
    return;
  }
  duckdb::Value old_value;
  TryGetCurrentSetting(duckdb::Identifier{name}, old_value);
  OnSet(name, scope == duckdb::SetScope::LOCAL, std::move(old_value));
}

void ConnectionContext::OnQueryBegin() {
  RefreshCatalogEpoch();
  if (!IsStableSnapshot() && transaction.HasActiveTransaction() &&
      !transaction.IsAutoCommit()) {
    transaction.ActiveTransaction().RefreshStartTime();
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
  _ai->End();
  _progress->EndQuery();
  RefreshCatalogEpoch();
  const bool dml = std::exchange(_statement_is_dml, false);
  if (_transaction && !IsStableSnapshot()) {
    _transaction->ResetSearchReaders();
  } else if (_transaction && dml) {
    _transaction->ResetSearchViews();
  }
  _commit_pass.reset();
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
    if (db.get().IsTemporary() || !catalog::IsSereneDB(db.get().GetCatalog())) {
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
  if (std::exchange(_query_in_transaction, false)) {
    _progress->xact_commit.fetch_add(1, std::memory_order_relaxed);
  }
  EndTransaction();
  RefreshCatalogEpoch();
}

void ConnectionContext::OnTransactionRollback(duckdb::MetaTransaction&) {
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
  auth::EnforcePlan(*this, *this, binder, plan);
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

SystemConnection MakeSystemConnection(std::string_view user, duckdb::idx_t role,
                                      std::string_view database,
                                      duckdb::idx_t database_id) {
  auto conn = MakeConnection(irs::DuckDBEngine::Instance().instance(),
                             {.user = user,
                              .role = role,
                              .database = database,
                              .database_id = database_id});
  auto& context = ConnectionContext::Get(*conn->context);
  return {.conn = std::move(conn),
          .progress = pg::ProgressRegistration{context.ProgressSource()}};
}

}  // namespace sdb
