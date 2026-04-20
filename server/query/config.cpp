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

#include <absl/container/node_hash_map.h>
#include <absl/strings/match.h>

#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database_manager.hpp>
#include <magic_enum/magic_enum.hpp>
#include <optional>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "catalog/catalog.h"

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

bool Config::GetReadYourOwnWrites() const {
  auto value = Get("sdb_read_your_own_writes");
  SDB_ASSERT(value);
  return *value != "false" && *value != "0";
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
  // Prefer the canonical PG-cased name (e.g. "DateStyle") when known so the
  // rollback key matches what SHOW returns; otherwise use the name as given
  // (covers native DuckDB settings like default_transaction_isolation).
  auto canonical = GetOriginalName(name);
  auto key = canonical.data() ? canonical : name;
  auto context = VariableContext::Session;
  if (is_local) {
    context = VariableContext::Local;
  } else if (IsExplicitTransaction()) {
    context = VariableContext::Transaction;
  }
  SaveForRollback(key, context);
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

bool Config::IsExplicitTransaction() const {
  return !_client_ctx.transaction.IsAutoCommit();
}

void Config::RestoreValue(std::string_view key, duckdb::Value value) noexcept {
  // Called from pre-commit / pre-rollback hooks while the DuckDB transaction
  // is still active, so catalog lookups performed by custom-impl set_local
  // callbacks work normally.
  auto& db_config = duckdb::DBConfig::GetConfig(*_client_ctx.db);
  auto name_ref = duckdb::String::Reference(key.data(), key.size());

  // Best-effort restore; swallow to keep noexcept contract.
  std::ignore = basics::SafeCall([&] {
    // Built-in options first.
    duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
    auto setting_index = db_config.TryGetSettingIndex(name_ref, option);
    if (option) {
      if (option->set_local) {
        auto parameter_type =
          duckdb::DBConfig::ParseLogicalType(option->parameter_type);
        auto typed = value.CastAs(_client_ctx, parameter_type);
        option->set_local(_client_ctx, typed);
      } else if (setting_index.IsValid()) {
        _client_ctx.config.user_settings.SetUserSetting(
          setting_index.GetIndex(), std::move(value));
      }
      return;
    }
    // Extension options: use the registered set_function so side effects
    // (e.g. sdb_faults toggling global fault-point state) are re-applied.
    duckdb::ExtensionOption ext;
    if (!db_config.TryGetExtensionOption(name_ref, ext)) {
      return;
    }
    auto typed = value.CastAs(_client_ctx, ext.type);
    // Only session/local SETs are tracked (setting_change_handler skips
    // GLOBAL), so the restore scope is always SESSION.
    if (ext.set_function) {
      ext.set_function(_client_ctx, duckdb::SetScope::SESSION, typed);
    }
    if (ext.setting_index.IsValid()) {
      _client_ctx.config.user_settings.SetUserSetting(
        ext.setting_index.GetIndex(), std::move(typed));
    }
  });
}

void Config::RollbackVariables() noexcept {
  for (auto&& [key, var] : _transaction) {
    RestoreValue(key, std::move(var.old_value));
  }
  _transaction.clear();
}

void Config::RevertLocalVariables() noexcept {
  // Called from the pre-commit hook while the transaction is still active.
  // Reverts SET LOCAL entries to their pre-SET values; leaves plain-SET
  // entries in _transaction so a subsequent rollback (if the commit fails)
  // can still revert them.
  absl::erase_if(_transaction, [this](auto& entry) {
    auto& [key, var] = entry;
    if (var.action != TxnAction::Revert) {
      return false;
    }
    RestoreValue(key, std::move(var.old_value));
    return true;
  });
}

void Config::DiscardCommittedVariables() noexcept {
  // Called from the post-commit hook once the transaction is fully committed.
  // Any remaining entries are plain-SET (Keep) values that the user wants to
  // persist for the session -- drop tracking without reverting.
  _transaction.clear();
}

}  // namespace sdb
