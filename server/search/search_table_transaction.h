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

namespace sdb::search {

class SearchTable;

struct SearchShardWrites {
  std::shared_ptr<SearchTable> shard;
  std::vector<std::unique_ptr<irs::IndexWriter::Transaction>> transactions;
  // The writer-generation slot this transaction registered in on `shard`,
  // released when the transaction settles. -1 until it registers.
  int writer_slot = -1;
};

// Holds a query::Transaction's search-table (TableEngine::Search) state and
// commit logic.
class SearchTableTransaction {
 public:
  ~SearchTableTransaction();

  // Registers this transaction as a writer of `shard`, once, before anything
  // reads the shard's index config -- so a rebuild that publishes a config can
  // tell whether this transaction predates it. Called from the DML operators'
  // GetGlobalSinkState: the bulk insert path builds its sink there, ahead of
  // the Combine that hands over its iresearch transaction, so registering any
  // later would let it straddle a swap unnoticed.
  void RegisterWriter(const std::shared_ptr<SearchTable>& shard);

  // Whether this transaction has already written to `shard`. CREATE INDEX
  // refuses to run in such a transaction: the rebuild would wait for writers
  // that predate its config swap, and this one cannot finish until the
  // statement it is running does.
  bool HasWritesFor(ObjectId shard_id) const noexcept {
    return _writes.contains(shard_id);
  }

  void AddParallelSearchTransaction(
    const std::shared_ptr<SearchTable>& shard,
    std::unique_ptr<irs::IndexWriter::Transaction> trx);

  // The segments a bulk statement flushed + fsynced, for the WAL to reference
  // instead of a second copy of the rows.
  void AddSegments(const std::shared_ptr<SearchTable>& shard,
                   std::vector<SearchDbWal::SegmentRef>&& segments);

  irs::IndexWriter::Transaction& EnsureSerialSearchTransaction(
    const std::shared_ptr<SearchTable>& shard,
    absl::AnyInvocable<irs::IndexWriter::Transaction()> make_trx);

  void AddInlineInsertChunk(const std::shared_ptr<SearchTable>& shard,
                            duckdb::BufferManager& buffer_manager,
                            const duckdb::vector<duckdb::LogicalType>& types,
                            duckdb::DataChunk& chunk, uint64_t pk_base);

  void AddSearchDeletes(const std::shared_ptr<SearchTable>& shard,
                        std::span<const std::string> pks);

  void AddSearchTruncate(const std::shared_ptr<SearchTable>& shard,
                         bool clears_shard);

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

  bool Empty() const noexcept { return _writes.empty(); }

  void RegisterFlush() noexcept;

  void Commit();

  void Abort() noexcept;

  void ResetReaders() noexcept { _readers.clear(); }

  void ResetReader(ObjectId shard_id) noexcept { _readers.erase(shard_id); }

 private:
  // Builds the shard sections, reserves the tick band (width = max over shards
  // of sum-over-trxs(GetQueries()+1)), appends the record, and returns the
  // record tick (the band top) -- the tick every shard's last trx commits at.
  uint64_t AppendCommit();

  // Releases every writer registration this transaction holds. Idempotent, so
  // Commit / Abort / the destructor can all call it.
  void ReleaseWriters() noexcept;

  containers::NodeHashMap<ObjectId, SearchShardWrites> _writes;
  containers::FlatHashMap<ObjectId, std::shared_ptr<irs::DirectoryReader>>
    _readers;
  LocalTableChanges _changes;
};

}  // namespace sdb::search
