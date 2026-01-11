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
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"

namespace sdb {

std::shared_ptr<rocksdb::Transaction> CreateTransaction(
  rocksdb::TransactionDB& db);

class TxnState : public Config {
 public:
  using Transaction = std::shared_ptr<rocksdb::Transaction>;
  using Snapshot = std::shared_ptr<StorageSnapshot>;

  enum class State {
    NONE,
    TRANSACTION,  // Explicit transaction
    SNAPSHOT,     // Implicit read-only transaction
    LOCAL,        // Implicit transaction for a simple query
  };

  State GetState() const noexcept { return _state; }

  Result Begin();

  Result Commit();

  Result Rollback();

  void SetLocalTransaction() noexcept {
    SDB_ASSERT(_state != State::TRANSACTION);
    _state = State::LOCAL;
  }

  void SetSnapshotOnly() noexcept {
    SDB_ASSERT(_state != State::TRANSACTION);
    _state = State::SNAPSHOT;
  }

  bool InsideTransaction() const noexcept {
    return _state == State::TRANSACTION || _state == State::LOCAL;
  }

  Transaction& GetTransaction();

  const rocksdb::Snapshot* GetSnapshot();

  // Used for simple queries without explicit transaction management
  // Should be called at the end of a query
  void ResetState() noexcept;

 private:
  void EnsureTransaction();
  void EnsureSnapshot();
  void CreateLocalTransaction() const;
  void CreateLocalSnapshot() const;

  State _state = State::NONE;
  mutable std::variant<Transaction, Snapshot> _data;
};

}  // namespace sdb
