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

#include <optional>

#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"

namespace sdb {

template<std::integral T>
bool CheckIntegral(std::string_view value) {
  T var;
  return absl::SimpleAtoi<T>(value, &var);
}

bool ValidateValue(VariableType type, std::string_view value) {
  switch (type) {
    case VariableType::Bool: {
      bool var;
      return absl::SimpleAtob(value, &var);
    }
    case VariableType::I32:
      return CheckIntegral<int32_t>(value);
    case VariableType::I64:
      return CheckIntegral<int64_t>(value);
    case VariableType::U8:
      return CheckIntegral<uint8_t>(value);
    case VariableType::U32:
      return CheckIntegral<uint32_t>(value);
    case VariableType::U64:
      return CheckIntegral<uint64_t>(value);
    case VariableType::F64: {
      double var;
      return absl::SimpleAtod(value, &var);
    }
    case VariableType::String:
    case VariableType::PgSearchPath:
      return true;
    case VariableType::PgExtraFloatDigits: {
      int8_t v{};
      if (!absl::SimpleAtoi<int8_t>(value, &v)) {
        return false;
      }
      return -15 <= v && v <= 3;
    }
    case VariableType::PgByteaOutput: {
      return absl::EqualsIgnoreCase("hex", value) ||
             absl::EqualsIgnoreCase("escape", value);
    }
    default:
      SDB_UNREACHABLE();
  }
}

std::optional<std::string> Config::access(const std::string& key) const {
  return Get(key);
}

std::optional<std::string> Config::Get(std::string_view key) const {
  std::string_view value = GetNonDefault(key);
  if (value.data() != nullptr) {
    return std::string{value};
  }
  auto var = GetDefaultVariable(key);
  return var.data() != nullptr ? std::optional<std::string>{var} : std::nullopt;
}

std::string_view Config::GetNonDefault(std::string_view key) const {
  {  // get from txn variables
    auto it = _transaction.find(key);
    if (it != _transaction.end()) {
      return it->second.value;
    }
  }

  {  // get from session variables
    auto it = _session.find(key);
    if (it != _session.end()) {
      return it->second;
    }
  }
  return {};
}

void Config::Set(VariableContext context, std::string_view key,
                 std::string value) {
  switch (context) {
    case VariableContext::Session: {
      _session[key] = std::move(value);
    } break;
    case VariableContext::Transaction: {
      _transaction[key] = {
        TxnAction::Apply,
        std::move(value),
      };
    } break;
    case VariableContext::Local: {
      _transaction[key] = {
        TxnAction::Revert,
        std::move(value),
      };
    } break;
  }
}

void Config::ResetAll() {
  _session.clear();
  _transaction.clear();
}

void Config::Reset(std::string_view key) {
  _transaction.erase(key);
  _session.erase(key);
}

void Config::CommitVariables() {
  for (auto&& [key, value] : _transaction) {
    if (value.action == TxnAction::Apply) {
      _session.insert_or_assign(key, std::move(value.value));
    }
  }
  _transaction.clear();
}

}  // namespace sdb
