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

#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/config.hpp>
#include <magic_enum/magic_enum.hpp>
#include <optional>

#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/catalog.h"
#include "catalog/virtual_table.h"
#include "pg/isolation_level.h"

namespace sdb {
namespace {

template<typename T>
T GetEnumValue(std::string_view value) noexcept {
  const auto r = magic_enum::enum_cast<T>(value, magic_enum::case_insensitive);
  SDB_ASSERT(r, "enum value is not validated");
  return *r;
}

}  // namespace

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

std::vector<std::string> Config::GetSearchPath() const {
  // DuckDB stores search_path as (catalog, schema) entries and serializes
  // them as "catalog.schema" when read as a string setting. Use the
  // structured API so we return just the schema names.
  const auto& entries =
    duckdb::ClientData::Get(_client_ctx).catalog_search_path->Get();
  std::vector<std::string> result;
  result.reserve(entries.size());
  for (const auto& entry : entries) {
    result.emplace_back(entry.schema);
  }
  return result;
}

int8_t Config::GetExtraFloatDigits() const {
  duckdb::Value value;
  auto ok = _client_ctx.TryGetCurrentSetting("extra_float_digits", value);
  SDB_ASSERT(ok);
  return static_cast<int8_t>(value.GetValue<int32_t>());
}

ByteaOutput Config::GetByteaOutput() const {
  auto value = Get("bytea_output");
  SDB_ASSERT(value);
  return GetEnumValue<ByteaOutput>(*value);
}

IsolationLevel Config::GetIsolationLevel() const {
  return _client_ctx.transaction.GetIsolationLevel();
}

WriteConflictPolicy Config::GetWriteConflictPolicy() const {
  auto value = Get("sdb_write_conflict_policy");
  SDB_ASSERT(value);
  return GetEnumValue<WriteConflictPolicy>(*value);
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
  // transaction_isolation is always Local -- reverts at txn end
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
    // Already tracking this key -- SET LOCAL overrides a prior SET
    if (context == VariableContext::Local) {
      it->second.action = TxnAction::Revert;
    }
  } else {
    // First SET for this key in this txn -- save old value
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
  for (auto&& [key, var] : _transaction) {
    if (var.action == TxnAction::Revert) {
      RestoreValue(key, std::move(var.old_value));
    }
  }
  _transaction.clear();
}

}  // namespace sdb
