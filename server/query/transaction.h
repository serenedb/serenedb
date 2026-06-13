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

#include <iresearch/index/index_writer.hpp>
#include <yaclib/async/future.hpp>

#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/result.h"
#include "catalog/catalog.h"
#include "query/config.h"
#include "search/inverted_index_shard.h"

namespace sdb::query {

class Transaction : public Config {
 public:
  using Config::Config;

#ifdef SDB_DEV
  virtual ~Transaction() {
    // Search transactions have implicit commit in destructor (historical
    // reasons) So if we get here explicit Commit/Rollback should be already
    // called. Otherwise we might have some unexpected data
    SDB_ASSERT(_search_transactions.empty());
  }
#endif

  void OnNewStatement();
  // Restarts a writeless explicit engine transaction under READ COMMITTED
  // so the upcoming statement sees freshly committed changes. Must run
  // outside the query lifecycle (before the statement starts).
  void RefreshReadCommittedSnapshot();

  // Pre-commit work that needs an active transaction (revert SET LOCAL for
  // custom-impl settings). Runs before the engine commit.
  void PreCommit() noexcept;
  // Pre-rollback counterpart -- restores all SET values.
  void PreRollback() noexcept;

  Result Commit();

  Result Rollback();

  void UpdateNumRows(ObjectId table_id, int64_t delta) noexcept {
    _table_rows_deltas[table_id] += delta;
  }

  // True once any statement that reads or writes the current database ran
  // inside the active explicit transaction; gates late SET TRANSACTION
  // ISOLATION LEVEL changes.
  bool HadQueryInTransaction() const noexcept {
    return _had_query_in_transaction;
  }
  void MarkQueryInTransaction() noexcept { _had_query_in_transaction = true; }

  search::InvertedIndexSnapshotPtr EnsureSearchSnapshot(ObjectId index_id);

  void EraseSearchTransaction(ObjectId shard_id) noexcept {
    _search_transactions.erase(shard_id);
  }

  void Destroy() noexcept;

  catalog::TableStats GetTableStats(ObjectId table_id) const;

  template<typename Visit, typename Filter = std::nullptr_t>
  void EnsureIndexesTransactions(ObjectId table_id, Visit&& visit,
                                 Filter&& filter = nullptr) {
    auto snapshot = EnsureCatalogSnapshot();
    SDB_ASSERT(snapshot->GetObject(table_id)->GetType() ==
               catalog::ObjectType::Table);

    for (auto index_shard : snapshot->GetIndexShardsByRelation(table_id)) {
      auto index =
        snapshot->GetObject<catalog::Index>(index_shard->GetIndexId());
      SDB_ASSERT(index);

      if constexpr (!std::is_same_v<std::decay_t<Filter>, std::nullptr_t>) {
        auto referenced = index->GetReferencedColumnIds();
        if (!filter(referenced)) {
          continue;
        }
      }

      if (index_shard->GetType() != catalog::ObjectType::InvertedIndexShard) {
        // Secondary indexes are native ART on the store table; nothing to
        // feed here.
        continue;
      }
      auto& inverted_index_shard =
        basics::downCast<search::InvertedIndexShard>(*index_shard);
      _search_transactions.try_emplace(inverted_index_shard.GetId(), nullptr);
      auto& transaction = _search_transactions[inverted_index_shard.GetId()];
      if (!transaction) {
        transaction = std::make_unique<irs::IndexWriter::Transaction>(
          inverted_index_shard.GetTransaction());
      }
      visit(*transaction, *index);
    }
  }

 private:
  void ApplyTableStatsDiffs() noexcept;

  containers::FlatHashMap<ObjectId,
                          std::unique_ptr<irs::IndexWriter::Transaction>>
    _search_transactions;
  containers::FlatHashMap<ObjectId, search::InvertedIndexSnapshotPtr>
    _search_snapshots;
  containers::FlatHashMap<ObjectId, int64_t> _table_rows_deltas;
  bool _had_query_in_transaction = false;
};

}  // namespace sdb::query
