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
#include "search/inverted_index_storage.h"

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
  void RefreshReadCommittedSnapshot();

  // Pre-commit work that needs an active transaction (revert SET LOCAL for
  // custom-impl settings). Runs before the engine commit.
  void PreCommit() noexcept;
  // Pre-rollback counterpart -- restores all SET values.
  void PreRollback() noexcept;

  void CommitSearch() noexcept;

  Result Commit();

  Result Rollback();

  bool HadQueryInTransaction() const noexcept {
    return _had_query_in_transaction;
  }
  void MarkQueryInTransaction() noexcept { _had_query_in_transaction = true; }

  search::InvertedIndexSnapshotPtr EnsureSearchSnapshot(ObjectId index_id);

  void EraseSearchTransaction(ObjectId index_id) noexcept {
    _search_transactions.erase(index_id);
  }

  void RegisterSearchFlush() noexcept {
    for (auto& [index_id, entry] : _search_transactions) {
      if (entry.transaction) {
        entry.transaction->RegisterFlush();
      }
    }
  }

  void Destroy() noexcept;

  template<typename Visit, typename Filter = std::nullptr_t>
  void EnsureIndexesTransactions(ObjectId table_id, Visit&& visit,
                                 Filter&& filter = nullptr) {
    auto snapshot = EnsureCatalogSnapshot();
    SDB_ASSERT(snapshot->GetObject(table_id)->GetType() ==
               catalog::ObjectType::Table);

    for (auto& index : snapshot->GetIndexesByRelation(table_id)) {
      SDB_ASSERT(index);

      if constexpr (!std::is_same_v<std::decay_t<Filter>, std::nullptr_t>) {
        auto referenced = index->GetReferencedColumnIds();
        if (!filter(referenced)) {
          continue;
        }
      }

      if (index->GetType() != catalog::ObjectType::InvertedIndex) {
        continue;
      }
      auto storage =
        basics::downCast<const catalog::InvertedIndex>(*index).GetData();
      SDB_ASSERT(storage);
      auto& entry =
        _search_transactions.try_emplace(index->GetId()).first->second;
      if (!entry.transaction) {
        entry.transaction = std::make_unique<irs::IndexWriter::Transaction>(
          storage->GetTransaction());
        entry.storage = storage;
      }
      visit(*entry.transaction, *index);
    }
  }

 private:
  struct SearchTransaction {
    std::unique_ptr<irs::IndexWriter::Transaction> transaction;
    std::shared_ptr<search::InvertedIndexStorage> storage;
  };

  containers::FlatHashMap<ObjectId, SearchTransaction> _search_transactions;
  containers::FlatHashMap<ObjectId, search::InvertedIndexSnapshotPtr>
    _search_snapshots;
  bool _had_query_in_transaction = false;
};

}  // namespace sdb::query
