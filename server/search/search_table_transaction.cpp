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

#include "search/search_table_transaction.h"

#include <absl/algorithm/container.h>

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/system-compiler.h"
#include "search/search_db_wal.h"
#include "search/search_table_shard.h"
#include "storage_engine/table_shard.h"

namespace sdb::search {

void SearchTableTransaction::RegisterFlush() noexcept {
  for (auto& [table_id, w] : _writes) {
    for (auto& trx : w.transactions) {
      trx->RegisterFlush();
    }
  }
}

void SearchTableTransaction::Abort() noexcept {
  for (auto& [table_id, w] : _writes) {
    for (auto& trx : w.transactions) {
      trx->Abort();
    }
  }
}

void SearchTableTransaction::Commit() {
  SDB_ASSERT(!_writes.empty());
  // Insert-only today: zero removes, so all segments share first_tick and one
  // RefreshCommit publishes them.
  SDB_ASSERT(absl::c_all_of(_writes, [](const auto& e) {
    return absl::c_all_of(e.second.transactions,
                          [](const auto& t) { return t->GetQueries() == 0; });
  }));
  SDB_IF_FAILURE("crash_before_search_wal_commit") { SDB_IMMEDIATE_ABORT(); }
  const uint64_t tick = AppendCommit();
  SDB_IF_FAILURE("crash_after_search_wal_commit") { SDB_IMMEDIATE_ABORT(); }
  for (auto& [table_id, w] : _writes) {
    for (auto& trx : w.transactions) {
      trx->Commit(tick);
    }
  }
}

uint64_t SearchTableTransaction::AppendCommit() {
  SDB_ASSERT(!_writes.empty());
  std::vector<SearchDbWal::ShardSection> sections;
  sections.reserve(_writes.size());
  std::vector<std::vector<SearchDbWal::Op>> op_lists;
  op_lists.reserve(_writes.size());
  SearchDbWal* wal =
    &basics::downCast<SearchTableShard>(*_writes.begin()->second.shard).Wal();
  for (auto& [table_id, w] : _writes) {
    SDB_ASSERT(wal == &basics::downCast<SearchTableShard>(*w.shard).Wal(),
               "all search shards in a txn must share one database WAL");
    auto& ops = op_lists.emplace_back();
    auto it = _changes.find(table_id);
    if (it != _changes.end()) {
      for (auto& buf : it->second.inserts) {
        if (buf.collection && buf.collection->Count() > 0) {
          ops.push_back(SearchDbWal::Op{
            buf.collection.get(),
            buf.pk_segments
              ? std::span<const SearchDbWal::InlinePk>{*buf.pk_segments}
              : std::span<const SearchDbWal::InlinePk>{},
            {}});
        }
      }
    }
    if (!w.seg_ids.empty()) {
      ops.push_back(
        SearchDbWal::Op{nullptr, {}, std::span<const uint64_t>{w.seg_ids}});
    }
    SDB_ASSERT(!ops.empty(),
               "search-table commit with neither chunk files nor inline rows");

    SearchDbWal::ShardSection section;
    section.table_id = table_id;
    section.ops = std::span<const SearchDbWal::Op>{ops};
    sections.push_back(section);
  }

  SDB_ASSERT(wal != nullptr);
  return wal->AppendCommit(sections);
}

}  // namespace sdb::search
