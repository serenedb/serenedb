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
  PgSearchPath,
  PgExtraFloatDigits,
  PgByteaOutput,
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
    if constexpr (T == VariableType::PgSearchPath) {
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
      SDB_ASSERT(value_str);
      int8_t result{};
      const bool succeed = absl::SimpleAtoi<int8_t>(*value_str, &result);
      SDB_ASSERT(succeed, "extra_float_digits is not validated");

      return result;
    } else if constexpr (T == VariableType::PgByteaOutput) {
      SDB_ASSERT(value_str);
      if (absl::EqualsIgnoreCase("hex", *value_str)) {
        return ByteaOutput::Hex;
      } else {
        SDB_ASSERT(absl::EqualsIgnoreCase("escape", *value_str),
                   "bytea_output is not validated");
        return ByteaOutput::Escape;
      }
    } else {
      SDB_THROW(ERROR_NOT_IMPLEMENTED);
    }
  }

  void Set(VariableContext context, std::string_view key, std::string value);

  void Reset(std::string_view key);

  void ResetAll();

  void Begin();

  void Commit();

  void Abort();

  bool InsideTransaction() const { return _inside_transaction; }

  std::unordered_map<std::string, std::string> rawConfigsCopy() const final;

  // Visit all the settings and call function f(setting_name, value,
  // description) value is std::string, because it could be non-default
  void VisitFullDescription(
    absl::FunctionRef<void(std::string_view, std::string_view,
                           std::string_view)>
      f) const;

 private:
  std::optional<std::string> Get(std::string_view key) const;
  std::optional<std::string> access(const std::string& key) const final;

  std::string_view GetNonDefault(std::string_view key) const;

  // Session variables
  containers::FlatHashMap<std::string_view, std::string> _session;

  // Transaction variable
  containers::FlatHashMap<std::string_view, TxnVariable> _transaction;

  bool _inside_transaction = false;
};

};  // namespace sdb
