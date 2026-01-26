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
#include <axiom/optimizer/OptimizerOptions.h>
#include <frozen/unordered_map.h>
#include <velox/common/config/IConfig.h>
#include <velox/type/Type.h>

#include <string>
#include <string_view>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/exceptions.h"
#include "basics/fwd.h"
#include "basics/system-compiler.h"
#include "catalog/table_options.h"

namespace sdb {

enum class VariableType {
  Bool = 0,
  I32,
  I64,
  U8,
  U32,
  U64,
  F64,
  String,
  JoinOrderAlgorithm,
  PgSearchPath,
  PgExtraFloatDigits,
  PgByteaOutput,
  SdbWriteConflictPolicy,
};

enum class ByteaOutput : uint8_t {
  Hex,
  Escape,
};

struct VariableDescription {
  VariableType type;
  std::string_view description;
  std::string_view default_value;  // .data() == nullptr if None
};

bool ValidateValue(VariableType type, std::string_view value);

std::string_view GetDefaultVariable(std::string_view name);

std::optional<VariableDescription> GetDefaultDescription(std::string_view name);

std::string_view GetOriginalName(std::string_view name);

class Config : public velox::config::IConfig {
 public:
  enum class VariableContext : uint8_t {
    Session = 0,
    Transaction,
    Local,
  };

  enum class TxnAction : uint8_t {
    Apply = 0,
    Revert,
  };

  struct TxnVariable {
    TxnAction action;
    std::string value;
  };

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
    } else if constexpr (T == VariableType::JoinOrderAlgorithm) {
      SDB_ASSERT(key == "join_order_algorithm");
      if (absl::EqualsIgnoreCase("cost", *value_str)) {
        return axiom::optimizer::JoinOrder::kCost;
      } else if (absl::EqualsIgnoreCase("greedy", *value_str)) {
        return axiom::optimizer::JoinOrder::kGreedy;
      } else {
        SDB_ASSERT(absl::EqualsIgnoreCase("syntactic", *value_str),
                   "join_order_algorithm is not validated");
        return axiom::optimizer::JoinOrder::kSyntactic;
      }
    } else if constexpr (T == VariableType::U32) {
      uint32_t r = 0;
      const bool ok = absl::SimpleAtoi<uint32_t>(*value_str, &r);
      SDB_ASSERT(ok, key, " is not validated");
      return r;
    } else {
      SDB_THROW(ERROR_NOT_IMPLEMENTED);
    }
  }

  void Set(VariableContext context, std::string_view key, std::string value);

  void Reset(std::string_view key);

  void ResetAll();

  std::unordered_map<std::string, std::string> rawConfigsCopy() const final;

  // Visit all the settings and call function f(setting_name, value,
  // description) value is std::string, because it could be non-default
  void VisitFullDescription(
    absl::FunctionRef<void(std::string_view, std::string_view,
                           std::string_view)>
      f) const;

 protected:
  // Used by TxnState(transaction state) to commit/rollback transaction
  // variables
  void CommitVariables();
  void RollbackVariables() { _transaction.clear(); }

 private:
  std::optional<std::string> Get(std::string_view key) const;
  std::optional<std::string> access(const std::string& key) const final;

  std::string_view GetNonDefault(std::string_view key) const;

  // Session variables
  containers::FlatHashMap<std::string_view, std::string> _session;

  // Transaction variable
  containers::FlatHashMap<std::string_view, TxnVariable> _transaction;
};

};  // namespace sdb
