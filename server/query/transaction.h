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
#include <rocksdb/utilities/transaction_db.h>

#include <yaclib/async/future.hpp>

#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"

namespace sdb {

class Config;

std::shared_ptr<rocksdb::Transaction> CreateTransaction(
  rocksdb::TransactionDB& db);

class TxnState {
 public:
  enum class Action : uint8_t {
    Apply = 0,
    Revert,
  };

  struct Variable {
    Action action;
    std::string value;
  };

  TxnState(Config& config) : _config(config) {}

  yaclib::Future<Result> Begin();

  yaclib::Future<Result> Commit();

  yaclib::Future<Result> Abort();

  bool InsideTransaction() const { return _txn.get() != nullptr; }

  void Reset(std::string_view key) { _variables.erase(key); }

  void ResetAll() { _variables.clear(); }

  std::string_view Get(std::string_view key) const {
    auto it = _variables.find(key);
    return it == _variables.end() ? std::string_view{} : it->second.value;
  }

  void Set(std::string_view key, std::string value, Action action) {
    _variables[key] = {
      action,
      std::move(value),
    };
  }

  void CopyVariablesTo(
    std::unordered_map<std::string, std::string>& dest) const {
    for (const auto& [key, var] : _variables) {
      dest.emplace(key, var.value);
    }
  }

  const std::shared_ptr<rocksdb::Transaction>& GetTransaction() const noexcept {
    return _txn;
  }

 private:
  containers::FlatHashMap<std::string_view, Variable> _variables;
  std::shared_ptr<rocksdb::Transaction> _txn;
  Config& _config;
};
}  // namespace sdb
