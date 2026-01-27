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

#include <rocksdb/snapshot.h>
#include <rocksdb/utilities/transaction.h>

#include <yaclib/async/future.hpp>

#include "basics/bit_utils.hpp"
#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"
#include "catalog/catalog.h"
#include "query/config.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/table_shard.h"

namespace sdb::query {

class Transaction : public Config {
 public:
  enum class State : uint8_t {
    None = 0,
    HasRocksDBRead = 1 << 0,
    HasRocksDBWrite = 1 << 1,
    HasTransactionBegin = 1 << 2,
  };

  Result Begin();

  Result Commit();

  Result Rollback();

  auto GetCatalogSnapshot() const {
    // TODO(codeworse): manage with rocksdb snapshot
    return catalog::GetCatalog().GetSnapshot();
  }

  void UpdateNumRows(ObjectId table_id, int64_t delta) noexcept {
    _table_rows_deltas[table_id] += delta;
  }

  void AddRocksDBRead() noexcept;

  void AddRocksDBWrite() noexcept;

  bool HasTransactionBegin() const noexcept;

  rocksdb::Transaction* GetRocksDBTransaction() const noexcept;

  rocksdb::Transaction& EnsureRocksDBTransaction();

  const rocksdb::Snapshot& EnsureRocksDBSnapshot();

  void Destroy() noexcept;

  catalog::TableStats GetTableStats(ObjectId table_id) const;

 private:
  void CreateStorageSnapshot();
  void CreateRocksDBTransaction();
  void ApplyTableStatsDiffs();

  State _state = State::None;
  std::shared_ptr<StorageSnapshot> _storage_snapshot;
  std::unique_ptr<rocksdb::Transaction> _rocksdb_transaction;
  const rocksdb::Snapshot* _rocksdb_snapshot = nullptr;
  containers::FlatHashMap<ObjectId, int64_t> _table_rows_deltas;
};

ENABLE_BITMASK_ENUM(Transaction::State);

}  // namespace sdb::query
