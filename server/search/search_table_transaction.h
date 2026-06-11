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

#include <absl/functional/any_invocable.h>

#include <cstdint>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "search/search_db_wal.h"
#include "search/search_table_changes.h"

namespace sdb {

class TableShard;

}  // namespace sdb
namespace sdb::search {

struct SearchShardWrites {
  std::shared_ptr<TableShard> shard;
  std::vector<std::unique_ptr<irs::IndexWriter::Transaction>> transactions;
  std::vector<SearchDbWal::PendingChunk> chunks;
};

// Holds a query::Transaction's search-table (StorageKind::kSearch) state and
// commit logic.
class SearchTableTransaction {
 public:
  // Bulk INSERT: each parallel sink thread hands off its iresearch trx + the
  // chunk file it streamed. Records the destination shard (the commit reaches
  // the per-database WAL through it); the table id comes from the shard.
  void AddParallelSearchTransaction(
    const std::shared_ptr<TableShard>& shard,
    std::unique_ptr<irs::IndexWriter::Transaction> trx,
    SearchDbWal::PendingChunk chunk);

  // Single-threaded INSERT: reuse this shard's serial trx (created via
  // `make_trx` on first use) so consecutive statements coalesce into one
  // segment. Also records the destination shard.
  irs::IndexWriter::Transaction& EnsureSerialSearchTransaction(
    const std::shared_ptr<TableShard>& shard,
    absl::AnyInvocable<irs::IndexWriter::Transaction()> make_trx);

  template<typename Factory>
  std::shared_ptr<irs::DirectoryReader> EnsureSearchTableReader(
    ObjectId shard_id, Factory&& make_reader) {
    auto it = _readers.find(shard_id);
    if (it == _readers.end()) {
      it = _readers
             .emplace(shard_id,
                      std::make_shared<irs::DirectoryReader>(make_reader()))
             .first;
    }
    return it->second;
  }

  LocalTableChanges& Changes() noexcept { return _changes; }

  bool Empty() const noexcept { return _writes.empty(); }

  void RegisterFlush() noexcept;

  void Commit();

  void Abort() noexcept;

  void ResetReaders() noexcept { _readers.clear(); }

 private:
  uint64_t AppendCommit();

  containers::NodeHashMap<ObjectId, SearchShardWrites> _writes;
  containers::FlatHashMap<ObjectId, std::shared_ptr<irs::DirectoryReader>>
    _readers;
  LocalTableChanges _changes;
};

}  // namespace sdb::search
