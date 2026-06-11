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
#include <duckdb/main/settings.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
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
  duckdb::Value value;
  auto ok = _client_ctx.TryGetCurrentSetting("sdb_read_your_own_writes", value);
  SDB_ASSERT(ok && !value.IsNull());
  return duckdb::BooleanValue::Get(value);
}

std::optional<std::string> Config::Get(std::string_view key) const {
  duckdb::Value value;
  if (_client_ctx.TryGetCurrentSetting(std::string{key}, value)) {
    return duckdb::Settings::FormatDisplayValue(_client_ctx, value).ToString();
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

void Config::OnSet(std::string_view name, bool is_local,
                   duckdb::Value old_value, const duckdb::Value* new_value) {
  // Prefer the canonical PG-cased name (e.g. "DateStyle") when known so the
  // rollback key matches what SHOW returns; otherwise use the name as given
  // (covers native DuckDB settings like default_transaction_isolation).
  auto canonical = GetOriginalName(name);
  std::string_view key_view = canonical.data() ? canonical : name;
  std::string key_str{key_view};

  auto [it, inserted] = _transaction.try_emplace(std::move(key_str));
  if (inserted) {
    // First event for this key. Skip tracking iff SET with the same value
    // (RESET events reaching here are guaranteed real -- DuckDB's gate
    // already short-circuits no-op RESETs).
    if (new_value && duckdb::Value::NotDistinctFrom(old_value, *new_value)) {
      _transaction.erase(it);
      return;
    }
    it->second.rollback_restore = old_value;
    if (is_local) {
      it->second.commit_restore = std::move(old_value);
    }
    return;
  }

  // Already tracking -- update scope bookkeeping regardless of value.
  if (is_local) {
    // Preserve the earliest LOCAL-overlay snapshot so consecutive SET
    // LOCALs revert to the same pre-first-LOCAL point.
    if (!it->second.commit_restore) {
      it->second.commit_restore = std::move(old_value);
    }
  } else {
    // Plain SET becomes the COMMIT keeper; drop any pending LOCAL revert.
    it->second.commit_restore.reset();
  }
}

void Config::SetSetting(std::string_view key, std::string value,
                        bool /*is_local*/) {
  SetInternal(key, std::move(value));
}

void Config::SetSettingChecked(std::string_view key, std::string value,
                               bool is_local) {
  duckdb::PhysicalSet::SetVariable(
    _client_ctx, duckdb::String::Reference(key.data(), key.size()),
    is_local ? duckdb::SetScope::LOCAL : duckdb::SetScope::SESSION,
    duckdb::Value{std::move(value)});
}

void Config::ResetAll() {
  _transaction.clear();

  _client_ctx.config.user_settings = {};
}

bool Config::IsExplicitTransaction() const {
  return !_client_ctx.transaction.IsAutoCommit();
}

bool Config::IsTransactionInvalidated() const {
  auto& txn = _client_ctx.transaction;
  return txn.HasActiveTransaction() &&
         duckdb::ValidChecker::IsInvalidated(txn.ActiveTransaction());
}

void Config::RestoreValue(std::string_view key, duckdb::Value value) noexcept {
  // Called from pre-commit / pre-rollback hooks while the DuckDB transaction
  // is still active, so catalog lookups performed by custom-impl set_local
  // callbacks work normally.
  auto& db_config = duckdb::DBConfig::GetConfig(*_client_ctx.db);
  auto name_ref = duckdb::String::Reference(key.data(), key.size());

  // A NULL old_value means the setting was at its default (never explicitly
  // SET), so the restore is a RESET rather than a SET. Calling set_local(NULL)
  // on settings like enable_profiling unconditionally enables profiling.
  const bool is_reset = value.IsNull();

  // Best-effort restore; swallow to keep noexcept contract.
  std::ignore = basics::SafeCall([&] {
    // Built-in options first.
    duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
    auto setting_index = db_config.TryGetSettingIndex(name_ref, option);
    if (option) {
      if (is_reset) {
        if (option->reset_local) {
          option->reset_local(_client_ctx);
        } else if (setting_index.IsValid()) {
          _client_ctx.config.user_settings.ClearSetting(
            setting_index.GetIndex());
        }
      } else if (option->set_local) {
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
    // Only session/local SETs are tracked (setting_change_handler skips
    // GLOBAL), so the restore scope is always SESSION.
    if (is_reset) {
      if (ext.reset_function) {
        ext.reset_function(_client_ctx, duckdb::SetScope::SESSION);
      }
      if (ext.setting_index.IsValid()) {
        _client_ctx.config.user_settings.ClearSetting(
          ext.setting_index.GetIndex());
      }
      return;
    }
    auto typed = value.CastAs(_client_ctx, ext.type);
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
    RestoreValue(key, std::move(var.rollback_restore));
  }
  _transaction.clear();
}

void Config::CommitVariables() noexcept {
  // Called from the pre-commit hook while the transaction is still active.
  // Any entry with a commit_restore was overlaid by a SET LOCAL whose
  // effects should not survive past the transaction; restore it. Plain SET
  // entries (no commit_restore) stay as-is.
  for (auto&& [key, var] : _transaction) {
    if (var.commit_restore) {
      RestoreValue(key, std::move(*var.commit_restore));
    }
  }
  _transaction.clear();
}

}  // namespace sdb
