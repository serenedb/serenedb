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
#include <string>
#include <string_view>

#include "basics/containers/node_hash_map.h"
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
  enum class VariableContext : uint8_t {
    Session = 0,
    Transaction,
    Local,
  };

  enum class TxnAction : uint8_t {
    // SET inside txn: on commit keep new value, on rollback restore old
    Keep = 0,
    // SET LOCAL: on both commit and rollback restore old value
    Revert,
  };

  struct TxnVariable {
    TxnAction action;
    duckdb::Value old_value;
  };

  explicit Config(duckdb::ClientContext& client_ctx)
    : _client_ctx{client_ctx} {}

  std::vector<std::string> GetSearchPath() const;
  int8_t GetExtraFloatDigits() const;
  ByteaOutput GetByteaOutput() const;
  IsolationLevel GetIsolationLevel() const;
  WriteConflictPolicy GetWriteConflictPolicy() const;
  bool GetReadYourOwnWrites() const;
  bool IsExplicitTransaction() const;

  void ResetAll();

  void DropCatalogSnapshot() { _snapshot.reset(); }

  std::shared_ptr<const catalog::Snapshot> EnsureCatalogSnapshot() const;

  // Returns the current value of a setting, or std::nullopt if not found.
  std::optional<std::string> Get(std::string_view key) const;

  void OnSet(std::string_view name, bool is_local);
  void SetSetting(std::string_view key, std::string value, bool is_local);

 protected:
  // Pre-rollback hook helper: restores every tracked variable to its pre-SET
  // value. Runs while the transaction is still active.
  void RollbackVariables() noexcept;
  // Pre-commit hook helper: reverts SET LOCAL entries only (action=Revert).
  // Plain-SET (Keep) entries remain in _transaction so a later rollback on
  // failed commit can still revert them.
  void RevertLocalVariables() noexcept;
  // Post-commit hook helper: drops any remaining tracked entries (Keep ones)
  // since the transaction committed successfully.
  void DiscardCommittedVariables() noexcept;

 private:
  void Set(VariableContext context, std::string_view key, std::string value);
  void SetInternal(std::string_view key, std::string value);
  void RestoreValue(std::string_view key, duckdb::Value value) noexcept;
  void SaveForRollback(std::string_view key, VariableContext context);

  // Transaction variables (commit-apply / revert semantics).

  // Owning-string keys: values may come from caller-provided buffers (e.g.
  // native DuckDB setting names forwarded from PhysicalSet), so we cannot
  // hold string_views that outlive the caller.
  // TODO: use FlatHashMap, there're now problems with ASAN build
  containers::NodeHashMap<std::string, TxnVariable> _transaction;
  mutable std::shared_ptr<const catalog::Snapshot> _snapshot;
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
