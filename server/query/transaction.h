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
#include <folly/Lazy.h>
#include <rocksdb/utilities/transaction_db.h>

#include <yaclib/async/future.hpp>

#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"
#include "query/config.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"

namespace sdb {

std::shared_ptr<rocksdb::Transaction> CreateTransaction(
  rocksdb::TransactionDB& db);

class TxnState : public Config {
 public:
  TxnState();

  yaclib::Future<Result> Begin();

  yaclib::Future<Result> Commit();

  yaclib::Future<Result> Rollback();

  bool InsideTransaction() const noexcept { return _txn != nullptr; }

  const auto& GetTransaction() const {
    // Get is allowed only inside actual transaction
    SDB_ASSERT(_txn);
    return _txn;
  }

  const rocksdb::Snapshot* GetSnapshot() const {
    if (_txn) {
      return _txn->GetSnapshot();
    }
    if (!_lazy_snapshot()) {
      return nullptr;
    }
    auto snapshot =
      std::dynamic_pointer_cast<RocksDBSnapshot>(_lazy_snapshot());
    SDB_ASSERT(snapshot);
    return snapshot->getSnapshot();
  }

  // Used for simple queries without explicit transaction management
  // Should be called at the end of a query
  void ResetSnapshot() const noexcept { _lazy_snapshot.reset(); }

 private:
  template<typename Sig>
  using LazyWrapper = folly::detail::Lazy<absl::AnyInvocable<Sig>>;

  std::shared_ptr<rocksdb::Transaction> _txn;
  mutable LazyWrapper<std::shared_ptr<StorageSnapshot>()> _lazy_snapshot;
};

}  // namespace sdb
