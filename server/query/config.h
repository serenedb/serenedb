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

#pragma once

#include <duckdb/common/enums/set_scope.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/setting_info.hpp>
#include <duckdb/parser/parsed_data/transaction_info.hpp>
#include <magic_enum/magic_enum.hpp>
#include <string>
#include <string_view>

#include "basics/assert.h"
#include "basics/containers/node_hash_map.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/types.h"

namespace duckdb {

class ClientContext;
struct DBConfig;

}  // namespace duckdb
namespace sdb {
namespace catalog {

struct Snapshot;
class VirtualTable;
class VirtualTableSnapshot;

}  // namespace catalog

enum class ByteaOutput : uint8_t {
  Hex,
  Escape,
};

using IsolationLevel = duckdb::TransactionIsolationLevel;

struct VariableDescription {
  duckdb::LogicalTypeId type;
  std::string_view description;
  duckdb::Value (*default_value)() = nullptr;  // called at registration time
  duckdb::set_option_callback_t set_callback = nullptr;
  duckdb::reset_option_callback_t reset_callback = nullptr;
  duckdb::SetScope scope = duckdb::SetScope::AUTOMATIC;
};

std::string_view GetOriginalName(std::string_view name);

class Config {
 public:
  // Per-key tracking for a currently-open transaction. Populated only when
  // `setting_change_handler` sees a real change inside an explicit txn;
  // cleared on COMMIT / ROLLBACK.
  struct TxnVariable {
    // Value to restore on ROLLBACK. Captured on the first tracked event
    // for this key and never updated after.
    duckdb::Value rollback_restore;
    // Value to restore on COMMIT (to undo a SET LOCAL overlay that
    // should not persist past the transaction). nullopt means "the live
    // value is the commit-keeper; nothing to undo at COMMIT".
    std::optional<duckdb::Value> commit_restore;
  };

  explicit Config(duckdb::ClientContext& client_ctx)
    : _client_ctx{client_ctx} {}

  virtual ~Config() = default;

  duckdb::ClientContext& GetClientContext() const noexcept {
    return _client_ctx;
  }

  std::vector<std::string> GetSearchPath() const;
  int8_t GetExtraFloatDigits() const;
  ByteaOutput GetByteaOutput() const;
  IsolationLevel GetIsolationLevel() const;
  bool GetStrictDDL() const;
  bool IsExplicitTransaction() const;
  bool IsTransactionInvalidated() const;

  void DropCatalogSnapshot() { _snapshot.reset(); }

  // Installs the snapshot a prepared statement was bound against, so its plan
  // executes against the same catalog view that pinned its bound entries.
  void SetCatalogSnapshot(std::shared_ptr<const catalog::Snapshot> snapshot) {
    _snapshot = std::move(snapshot);
  }

  // Acquires the statement's catalog snapshot. Called only at statement
  // boundaries on the connection thread (OnStatementBegin, the wire message
  // handlers that bind or serialize before a query lifecycle starts); all
  // other code reads via CatalogSnapshot().
  std::shared_ptr<const catalog::Snapshot> AcquireCatalogSnapshot();

  // Read-only access to the snapshot acquired for the current statement.
  // Never acquires: a lazy first-acquire from an executor worker would race
  // the connection thread. The check stays in release builds -- a scope hole
  // must surface as an error, not a null dereference.
  const std::shared_ptr<const catalog::Snapshot>& CatalogSnapshot() const {
    SDB_ENSURE(_snapshot, ERROR_INTERNAL,
               "no catalog snapshot acquired for the current statement");
    return _snapshot;
  }

  std::shared_ptr<const catalog::Snapshot> GetCatalogSnapshot() const {
    SDB_ASSERT(_snapshot);
    return _snapshot;
  }

  // Returns the current value of a setting, or std::nullopt if not found.
  std::optional<std::string> Get(std::string_view key) const;

  // Record a SET / SET LOCAL event inside an explicit txn.
  //   old_value -- value currently in effect (pre-event snapshot).
  //   new_value -- pointer to the about-to-be value for SET; nullptr for
  //                RESET. Used only to skip initial insertion when a SET
  //                wouldn't actually change the value; for already-tracked
  //                keys, scope bookkeeping still runs regardless.
  void OnSet(std::string_view name, bool is_local, duckdb::Value old_value,
             const duckdb::Value* new_value);

  void SetSetting(std::string_view key, std::string value, bool is_local);

  // Same as SetSetting but routes through DuckDB's SET pipeline, so type
  // casting and the option's set_callback run as if the client had issued a
  // `SET key = value` statement. Throws on unknown settings or validation
  // failure -- callers translate to the appropriate PG error.
  void SetSettingChecked(std::string_view key, std::string value,
                         bool is_local);

  // Monotonic counter bumped whenever a session/local setting changes (via the
  // setting_change_handler) or is reverted at txn end. Lets the wire layer skip
  // the per-command ParameterStatus GUC poll unless something actually changed.
  uint64_t SettingsVersion() const noexcept { return _settings_version; }
  void MarkSettingsChanged() noexcept { ++_settings_version; }

 protected:
  // Pre-rollback hook: restore every tracked variable to its pre-SET value.
  void RollbackVariables() noexcept;
  // Pre/post-commit hook: restore SET LOCAL overlays; plain SET entries
  // stay as-is.
  void CommitVariables() noexcept;

 private:
  void SetInternal(std::string_view key, std::string value);
  void RestoreValue(std::string_view key, duckdb::Value value) noexcept;

  // Bumped on every tracked setting change; see SettingsVersion().
  uint64_t _settings_version = 0;

  // Transaction variables (commit-apply / revert semantics).

  // Owning-string keys: values may come from caller-provided buffers (e.g.
  // native DuckDB setting names forwarded from PhysicalSet), so we cannot
  // hold string_views that outlive the caller.
  // TODO: use FlatHashMap, there're now problems with ASAN build
  containers::NodeHashMap<std::string, TxnVariable> _transaction;
  std::shared_ptr<const catalog::Snapshot> _snapshot;
  duckdb::ClientContext& _client_ctx;
};

namespace connector {

void RegisterConfigVariables(duckdb::DBConfig& config);

}  // namespace connector
}  // namespace sdb
namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::IsolationLevel>(sdb::IsolationLevel value) noexcept {
  switch (value) {
    case sdb::IsolationLevel::READ_COMMITTED:
      return "read committed";
    case sdb::IsolationLevel::REPEATABLE_READ:
      return "repeatable read";
    default:
      break;
  }
  return default_tag;
}

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::ByteaOutput>(sdb::ByteaOutput value) noexcept {
  switch (value) {
    case sdb::ByteaOutput::Hex:
      return "hex";
    case sdb::ByteaOutput::Escape:
      return "escape";
  }
  return default_tag;
}

}  // namespace magic_enum
