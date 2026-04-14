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

#include "query/config.h"

#include <absl/strings/match.h>

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <optional>

#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/catalog.h"
#include "pg/isolation_level.h"

namespace sdb {

void Config::SetInternal(std::string_view key, std::string value) {
  auto& db_config = duckdb::DBConfig::GetConfig(*_client_ctx.db);
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  auto setting_index = db_config.TryGetSettingIndex(
    duckdb::String::Reference(key.data(), key.size()), option);
  if (setting_index.IsValid()) {
    _client_ctx.config.user_settings.SetUserSetting(
      setting_index.GetIndex(), duckdb::Value{std::move(value)});
  }
}

std::optional<std::string> Config::Get(std::string_view key) const {
  duckdb::Value value;
  if (_client_ctx.TryGetCurrentSetting(std::string{key}, value)) {
    return value.ToString();
  }
  return std::nullopt;
}

std::shared_ptr<const catalog::Snapshot> Config::EnsureCatalogSnapshot() const {
  if (_snapshot) {
    return _snapshot;
  }
  _snapshot = SerenedServer::Instance()
                .getFeature<catalog::CatalogFeature>()
                .Global()
                .GetCatalogSnapshot();
  SDB_ASSERT(_snapshot);
  return _snapshot;
}

void Config::OnSet(std::string_view name, bool is_local) {
  auto canonical = GetOriginalName(name);
  if (!canonical.data()) {
    return;
  }
  auto context = VariableContext::Session;
  if (is_local) {
    context = VariableContext::Local;
  } else if (!IsAutoCommit()) {
    context = VariableContext::Transaction;
  }
  // transaction_isolation is always Local — reverts at txn end
  if (canonical == pg::kTransactionIsolation && !IsAutoCommit()) {
    context = VariableContext::Local;
  }
  SaveForRollback(canonical, context);
}

void Config::SetSetting(std::string_view key, std::string value,
                        bool is_local) {
  OnSet(key, is_local);
  SetInternal(key, std::move(value));
}

void Config::Set(VariableContext context, std::string_view key,
                 std::string value) {
  SaveForRollback(key, context);
  SetInternal(key, std::move(value));
}

void Config::SaveForRollback(std::string_view key, VariableContext context) {
  if (context != VariableContext::Transaction &&
      context != VariableContext::Local) {
    return;
  }
  if (auto it = _transaction.find(key); it != _transaction.end()) {
    // Already tracking this key — SET LOCAL overrides a prior SET
    if (context == VariableContext::Local) {
      it->second.action = TxnAction::Revert;
    }
  } else {
    // First SET for this key in this txn — save old value
    duckdb::Value old_value;
    _client_ctx.TryGetCurrentSetting(std::string{key}, old_value);
    _transaction[key] = {
      context == VariableContext::Local ? TxnAction::Revert : TxnAction::Keep,
      std::move(old_value),
    };
  }
}

void Config::ResetAll() {
  _transaction.clear();

  _client_ctx.config.user_settings = {};
}

bool Config::IsAutoCommit() const {
  return _client_ctx.transaction.IsAutoCommit();
}

void Config::Reset(std::string_view key) {
  _transaction.erase(key);

  auto& db_config = duckdb::DBConfig::GetConfig(*_client_ctx.db);
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  auto setting_index = db_config.TryGetSettingIndex(
    duckdb::String::Reference(key.data(), key.size()), option);
  if (setting_index.IsValid()) {
    _client_ctx.config.user_settings.ClearSetting(setting_index.GetIndex());
  }
}

void Config::RestoreValue(std::string_view key, duckdb::Value value) noexcept {
  auto& db_config = duckdb::DBConfig::GetConfig(*_client_ctx.db);
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  auto setting_index = db_config.TryGetSettingIndex(
    duckdb::String::Reference(key.data(), key.size()), option);
  if (setting_index.IsValid()) {
    // TODO(gnusi): SetUserSetting can throw
    _client_ctx.config.user_settings.SetUserSetting(setting_index.GetIndex(),
                                                    std::move(value));
  }
}

void Config::RollbackVariables() noexcept {
  for (auto&& [key, var] : _transaction) {
    RestoreValue(key, std::move(var.old_value));
  }
  _transaction.clear();
}

void Config::CommitVariables() noexcept {
  // If default_transaction_isolation was changed in this txn (Keep = commit),
  // also propagate to transaction_isolation for the next transaction.
  // Litmus: SET default_transaction_isolation = A; BEGIN;
  //   SET default_transaction_isolation = B; SHOW transaction_isolation == A;
  // COMMIT; SHOW transaction_isolation == B;
  if (auto it = _transaction.find(pg::kDefaultTransactionIsolation);
      it != _transaction.end() && it->second.action == TxnAction::Keep) {
    duckdb::Value current;
    _client_ctx.TryGetCurrentSetting(
      std::string{pg::kDefaultTransactionIsolation}, current);
    SetInternal(pg::kTransactionIsolation, current.ToString());
  }

  for (auto&& [key, var] : _transaction) {
    if (var.action == TxnAction::Revert) {
      RestoreValue(key, std::move(var.old_value));
    }
  }
  _transaction.clear();
}

}  // namespace sdb
