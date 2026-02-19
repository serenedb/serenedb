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

#include <iresearch/index/index_writer.hpp>
#include <yaclib/async/future.hpp>

#include "basics/bit_utils.hpp"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/result.h"
#include "catalog/catalog.h"
#include "query/config.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"

namespace sdb::query {

class Transaction : public Config {
 public:
  enum class State : uint8_t {
    None = 0,
    HasRocksDBRead = 1 << 0,
    HasRocksDBWrite = 1 << 1,
    HasTransactionBegin = 1 << 2,
  };

#ifdef SDB_DEV
  virtual ~Transaction() {
    // Search transactions have implicit commit in destructor (historical
    // reasons) So if we get here explicit Commit/Rollback should be already
    // called. Otherwise we might have some unexpected data
    SDB_ASSERT(_search_transactions.empty());
    // RocksDB transactions aborts itself in destructor but just for consistency
    // we should do Commit/Rollback explicitly
    SDB_ASSERT(!_rocksdb_transaction);
  }
#endif

  void OnNewStatement();

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
  bool HasRocksDBRead() const noexcept;

  void AddRocksDBWrite() noexcept;
  bool HasRocksDBWrite() const noexcept;

  void AddTransactionBegin() noexcept;
  bool HasTransactionBegin() const noexcept;

  IsolationLevel GetIsolationLevel() const noexcept {
    return Get<VariableType::SdbTransactionIsolation>("transaction_isolation");
  }

  rocksdb::Transaction& GetRocksDBTransaction() const noexcept {
    SDB_ASSERT(_rocksdb_transaction);
    return *_rocksdb_transaction;
  }

  rocksdb::Transaction& EnsureRocksDBTransaction();

  const rocksdb::Snapshot& EnsureRocksDBSnapshot();

  void Destroy() noexcept;

  catalog::TableStats GetTableStats(ObjectId table_id) const;

  template<typename Visit, typename Filter = std::nullptr_t>
  void EnsureIndexesTransactions(ObjectId table_id, Visit&& visit,
                                 Filter&& filter = nullptr) {
    auto snapshot = GetCatalogSnapshot();
    SDB_ASSERT(snapshot->GetObject(table_id)->GetType() ==
               catalog::ObjectType::Table);

    for (auto index_shard : snapshot->GetIndexShardsByTable(table_id)) {
      auto index = snapshot->GetObject<catalog::Index>(index_shard->GetId());
      SDB_ASSERT(index);

      if constexpr (!std::is_same_v<std::decay_t<Filter>, std::nullptr_t>) {
        if (!filter(index->GetColumnIds())) {
          continue;
        }
      }

      if (index_shard->GetType() == IndexType::Inverted) {
        auto& inverted_index_shard =
          basics::downCast<search::InvertedIndexShard>(*index_shard);
        _search_transactions.try_emplace(inverted_index_shard.GetId(), nullptr);
        auto& transaction = _search_transactions[inverted_index_shard.GetId()];
        if (!transaction) {
          transaction = std::make_unique<irs::IndexWriter::Transaction>(
            inverted_index_shard.GetTransaction());
        }
        visit(*transaction, index->GetColumnIds());
      } else {
        visit(EnsureRocksDBTransaction(), index->GetColumnIds());
      }
    }
  }

 private:
  void ApplyTableStatsDiffs() noexcept;

  State _state = State::None;
  std::shared_ptr<StorageSnapshot> _storage_snapshot;
  std::unique_ptr<rocksdb::Transaction> _rocksdb_transaction;
  const rocksdb::Snapshot* _rocksdb_snapshot = nullptr;
  containers::FlatHashMap<ObjectId,
                          std::unique_ptr<irs::IndexWriter::Transaction>>
    _search_transactions;
  containers::FlatHashMap<ObjectId, int64_t> _table_rows_deltas;
};

ENABLE_BITMASK_ENUM(Transaction::State);

}  // namespace sdb::query
