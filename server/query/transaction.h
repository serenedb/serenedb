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
#include "query/config.h"

namespace sdb {

std::shared_ptr<rocksdb::Transaction> CreateTransaction(
  rocksdb::TransactionDB& db);

class TxnState : public Config {
 public:
  class LazyTransaction {
   public:
    void SetTransaction() { _initialized = true; }

    const std::shared_ptr<rocksdb::Transaction>& GetTransaction() const;

    bool IsInitialized() const noexcept { return _initialized; }

    void Reset() {
      _txn.reset();
      _initialized = false;
    }

   private:
    mutable std::shared_ptr<rocksdb::Transaction> _txn;
    bool _initialized = false;
  };

  yaclib::Future<Result> Begin();

  yaclib::Future<Result> Commit();

  yaclib::Future<Result> Rollback();

  bool InsideTransaction() const noexcept { return _txn.IsInitialized(); }

  const auto& GetTransaction() const { return _txn.GetTransaction(); }

 private:
  LazyTransaction _txn;
};

}  // namespace sdb
