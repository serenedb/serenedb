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
#include <span>
#include <string>
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
  // the per-database WAL through it); the table id comes from the shard. The
  // matching manifest REFERENCE op is added once per statement via
  // AddReferences (not here), so the multi-threaded Combine path never touches
  // the manifest.
  void AddParallelSearchTransaction(
    const std::shared_ptr<TableShard>& shard,
    std::unique_ptr<irs::IndexWriter::Transaction> trx,
    SearchDbWal::PendingChunk chunk);

  // Bulk INSERT: append the statement's chunk-file refs to the shard's current
  // insert run in one batched call (the bulk Sink's Finalize, after all its
  // parallel sinks have combined). Does not seal the run.
  void AddReferences(const std::shared_ptr<TableShard>& shard,
                     std::span<const uint64_t> seg_ids);

  // Single-threaded INSERT: reuse this shard's serial trx (created via
  // `make_trx` on first use) so consecutive statements coalesce into one
  // segment. Also records the destination shard.
  irs::IndexWriter::Transaction& EnsureSerialSearchTransaction(
    const std::shared_ptr<TableShard>& shard,
    absl::AnyInvocable<irs::IndexWriter::Transaction()> make_trx);

  // Single-threaded INSERT: coalesce one input chunk into the shard's current
  // insert run in the ordered op manifest (Changes()). `pk_base` is recorded
  // per chunk only for generated-PK shards.
  void AddInlineInsertChunk(const std::shared_ptr<TableShard>& shard,
                            duckdb::BufferManager& buffer_manager,
                            const duckdb::vector<duckdb::LogicalType>& types,
                            duckdb::DataChunk& chunk, bool uses_generated_pk,
                            uint64_t pk_base);

  // DELETE: append a DELETE entry (encoded PK byte strings) to the shard's
  // ordered op manifest, sealing the current insert run. The matching iresearch
  // removal rides the shard's serial trx (EnsureSerialSearchTransaction).
  void AddSearchDeletes(const std::shared_ptr<TableShard>& shard,
                        std::span<const std::string> pks);

  void AddSearchTruncate(const std::shared_ptr<TableShard>& shard);

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
  // Builds the shard sections, reserves the tick band (width = max over shards
  // of sum-over-trxs(GetQueries()+1)), appends the record, and returns the
  // record tick (the band top) -- the tick every shard's last trx commits at.
  uint64_t AppendCommit();

  containers::NodeHashMap<ObjectId, SearchShardWrites> _writes;
  containers::FlatHashMap<ObjectId, std::shared_ptr<irs::DirectoryReader>>
    _readers;
  LocalTableChanges _changes;
};

}  // namespace sdb::search
