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

#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <duckdb/common/enums/set_scope.hpp>
#include <duckdb/common/types/value.hpp>
#include <string>
#include <string_view>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/exceptions.h"
#include "basics/system-compiler.h"
#include "catalog/types.h"

namespace duckdb {

class ClientContext;
struct DBConfig;

}  // namespace duckdb

namespace sdb {
namespace catalog {

struct Snapshot;

}  // namespace catalog

enum class VariableType {
  Bool = 0,
  I32,
  I64,
  U8,
  U32,
  U64,
  F64,
  String,
  PgSearchPath,
  PgExtraFloatDigits,
  PgByteaOutput,
  SdbWriteConflictPolicy,
  SdbTransactionIsolation,
};

enum class ByteaOutput : uint8_t {
  Hex,
  Escape,
};

enum class IsolationLevel : uint8_t {
  ReadCommitted,
  RepeatableRead,
};

using ValidateFn = bool (*)(const duckdb::Value&);
using OnSetFn = void (*)(duckdb::ClientContext&, duckdb::SetScope,
                         const std::string&, duckdb::Value&, bool is_reset);
struct VariableDescription {
  duckdb::LogicalTypeId type;
  std::string_view description;
  std::string_view default_value;  // .data() == nullptr if None
  ValidateFn validate = nullptr;
  OnSetFn on_set = nullptr;
};

std::optional<VariableDescription> GetDefaultDescription(std::string_view name);

bool HasDefault(std::string_view name);

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

  template<VariableType T>
  auto Get(std::string_view key) const {
    // TODO(codeworse): consider to use std::string_view as return type to avoid
    // copy
    auto value_str = Get(key);
    // We use this only for system variables, so value must exist
    SDB_ASSERT(value_str);
    if constexpr (T == VariableType::PgSearchPath) {
      SDB_ASSERT(key == "search_path");
      auto value = value_str.and_then([](std::string_view str) {
        auto arr = absl::StrSplit(str, ", ");
        std::vector<std::string> result;
        for (const auto& str : arr) {
          auto value = absl::StripPrefix(absl::StripSuffix(str, "\""), "\"");
          result.emplace_back(value);
        }
        return std::optional{result};
      });
      SDB_ASSERT(value);
      return *value;
    } else if constexpr (T == VariableType::PgExtraFloatDigits) {
      SDB_ASSERT(key == "extra_float_digits");
      int8_t r = 0;
      const bool ok = absl::SimpleAtoi<int8_t>(*value_str, &r);
      SDB_ASSERT(ok, "extra_float_digits is not validated");
      return r;
    } else if constexpr (T == VariableType::PgByteaOutput) {
      SDB_ASSERT(key == "bytea_output");
      if (absl::EqualsIgnoreCase("hex", *value_str)) {
        return ByteaOutput::Hex;
      } else {
        SDB_ASSERT(absl::EqualsIgnoreCase("escape", *value_str),
                   "bytea_output is not validated");
        return ByteaOutput::Escape;
      }
    } else if constexpr (T == VariableType::SdbTransactionIsolation) {
      SDB_ASSERT(key == "default_transaction_isolation" ||
                 key == "transaction_isolation");
      if (absl::EqualsIgnoreCase("repeatable read", *value_str)) {
        return IsolationLevel::RepeatableRead;
      }
      SDB_ASSERT(absl::EqualsIgnoreCase("read committed", *value_str),
                 "default_transaction_isolation is not validated");
      return IsolationLevel::ReadCommitted;
    } else if constexpr (T == VariableType::SdbWriteConflictPolicy) {
      SDB_ASSERT(key == "sdb_write_conflict_policy");
      if (absl::EqualsIgnoreCase("emit_error", *value_str)) {
        return WriteConflictPolicy::EmitError;
      }
      if (absl::EqualsIgnoreCase("do_nothing", *value_str)) {
        return WriteConflictPolicy::DoNothing;
      }
      SDB_ASSERT(absl::EqualsIgnoreCase("replace", *value_str),
                 "sdb_write_conflict_policy is not validated");
      return WriteConflictPolicy::Replace;
    } else if constexpr (T == VariableType::U32) {
      uint32_t r = 0;
      const bool ok = absl::SimpleAtoi<uint32_t>(*value_str, &r);
      SDB_ASSERT(ok, key, " is not validated");
      return r;
    } else if constexpr (T == VariableType::Bool) {
      bool r = false;
      const bool ok = absl::SimpleAtob(*value_str, &r);
      SDB_ASSERT(ok, key, " is not validated");
      return r;
    } else {
      SDB_THROW(ERROR_NOT_IMPLEMENTED);
    }
  }

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
