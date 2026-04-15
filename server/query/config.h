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
#include <string>
#include <string_view>

#include "catalog/types.h"

namespace duckdb {

class ClientContext;
struct DBConfig;

}  // namespace duckdb
namespace sdb {
namespace catalog {

struct Snapshot;

}  // namespace catalog

enum class ByteaOutput : uint8_t {
  Hex,
  Escape,
};

enum class IsolationLevel : uint8_t {
  ReadCommitted,
  RepeatableRead,
};

struct VariableDescription {
  duckdb::LogicalTypeId type;
  std::string_view description;
  std::string_view default_value;  // .data() == nullptr if None
  duckdb::set_option_callback_t set_callback = nullptr;
  duckdb::reset_option_callback_t reset_callback = nullptr;
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

  void Reset(std::string_view key);

  void ResetAll();

  void DropCatalogSnapshot() { _snapshot.reset(); }

  std::shared_ptr<const catalog::Snapshot> EnsureCatalogSnapshot() const;

  // Visit all the settings and call function f(setting_name, value,
  // description) value is std::string, because it could be non-default
  void VisitFullDescription(
    absl::FunctionRef<void(std::string_view, std::string_view,
                           std::string_view)>
      f) const;

  // Returns the current value of a setting, or std::nullopt if not found.
  std::optional<std::string> GetSetting(std::string_view key) const {
    return Get(key);
  }

  void OnSet(std::string_view name, bool is_local);
  void SetSetting(std::string_view key, std::string value, bool is_local);
  bool IsAutoCommit() const;

 protected:
  void CommitVariables() noexcept;
  void RollbackVariables() noexcept;

 private:
  std::optional<std::string> Get(std::string_view key) const;
  void Set(VariableContext context, std::string_view key, std::string value);
  void SetInternal(std::string_view key, std::string value);
  void RestoreValue(std::string_view key, duckdb::Value value) noexcept;
  void SaveForRollback(std::string_view key, VariableContext context);

  // Transaction variables (commit-apply / revert semantics).
  containers::FlatHashMap<std::string_view, TxnVariable> _transaction;
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
    case sdb::IsolationLevel::ReadCommitted:
      return "read committed";
    case sdb::IsolationLevel::RepeatableRead:
      return "repeatable read";
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
