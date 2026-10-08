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
#include <duckdb/common/identifier.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "connector/column_id.h"
#include "search/search_table_changes.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

class SearchTableEntry;

}  // namespace sdb::catalog
namespace sdb::search {

class SearchTable;

struct SearchShardWrites {
  std::shared_ptr<SearchTable> shard;
  const catalog::SearchTableEntry* table = nullptr;
  std::vector<std::unique_ptr<irs::IndexWriter::Transaction>> transactions;
  // The transaction the write buffer flushes into once it has overrun, owned by
  // `transactions` above. Exclusive, so its segments can be named in the
  // record, and flushed to disk exactly once -- at commit, which is all
  // FlushAndFsync permits. Null while the buffer has never overrun.
  irs::IndexWriter::Transaction* buffer_trx = nullptr;
  // The writer-generation slot this transaction registered in on `shard`,
  // released when the transaction settles. -1 until it registers.
  int writer_slot = -1;
  bool truncate_claim = false;
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
  void RegisterWriter(const std::shared_ptr<SearchTable>& shard,
                      const catalog::SearchTableEntry& table);

  // Whether this transaction has already written to `shard`. CREATE INDEX
  // refuses to run in such a transaction: the rebuild would wait for writers
  // that predate its config swap, and this one cannot finish until the
  // statement it is running does.
  bool HasWritesFor(duckdb::idx_t shard_id) const noexcept {
    return _writes.contains(shard_id);
  }

  void AddParallelSearchTransaction(
    const std::shared_ptr<SearchTable>& shard,
    std::unique_ptr<irs::IndexWriter::Transaction> trx);

  // The segments a bulk statement flushed + fsynced, for the WAL to reference
  // instead of a second copy of the rows.
  void AddSegments(const std::shared_ptr<SearchTable>& shard,
                   std::vector<std::string>&& segments);

  irs::IndexWriter::Transaction& EnsureSerialSearchTransaction(
    const std::shared_ptr<SearchTable>& shard,
    absl::AnyInvocable<irs::IndexWriter::Transaction()> make_trx);

  void AddInlineInsertChunk(const std::shared_ptr<SearchTable>& shard,
                            duckdb::BufferManager& buffer_manager,
                            const duckdb::vector<duckdb::LogicalType>& types,
                            std::span<const connector::ColumnId> column_ids,
                            duckdb::Catalog& catalog, duckdb::DataChunk& chunk,
                            uint64_t pk_base);

  // Bytes a flush would reclaim for `shard_id`; deletes are not counted.
  uint64_t BufferedBytes(duckdb::idx_t shard_id) const noexcept {
    auto it = _changes.find(shard_id);
    return it == _changes.end() ? 0 : it->second.BufferedBytes();
  }

  // Replays the whole buffer -- rows and removals, in issue order -- into the
  // shard's exclusive transaction, creating it on first use, and empties the
  // row buffer. The rows become that transaction's to make durable, so the
  // record stops carrying a copy of them; the removals stay buffered, since
  // the record has to carry those either way.
  void FlushBuffer(const std::shared_ptr<SearchTable>& shard,
                   duckdb::ClientContext& context);

  // Rowids to remove, in issue order with the buffered rows.
  void AddSearchDeletes(const std::shared_ptr<SearchTable>& shard,
                        std::span<const int64_t> rows);

  void AddSearchTruncate(const std::shared_ptr<SearchTable>& shard,
                         const duckdb::Identifier& table_name,
                         bool clears_shard);

  template<typename Factory>
  std::shared_ptr<irs::DirectoryReader> EnsureSearchTableReader(
    duckdb::idx_t shard_id, Factory&& make_reader) {
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

  void PrepareCommit(duckdb::ClientContext& context);

  void FlushPending(duckdb::ClientContext& context);

  void Abort() noexcept;

  void ResetReaders() noexcept { _readers.clear(); }

  void ResetReader(duckdb::idx_t shard_id) noexcept {
    _readers.erase(shard_id);
  }

 private:
  void ReleaseWriters() noexcept;

  // Replays a buffer into `trx` in issue order, rows and removals interleaved
  // by watermark. Leaves the buffer untouched; the caller decides whether the
  // rows are now somebody else's to make durable.
  static void ReplayBuffer(SearchTable& shard, LocalTableChangesEntry& entry,
                           irs::IndexWriter::Transaction& trx,
                           duckdb::ClientContext& context);

  // The shard's exclusive buffer transaction, created on first overrun.
  irs::IndexWriter::Transaction& EnsureBufferTransaction(
    const std::shared_ptr<SearchTable>& shard);

  irs::containers::NodeHashMap<duckdb::idx_t, SearchShardWrites> _writes;
  irs::containers::FlatHashMap<duckdb::idx_t,
                               std::shared_ptr<irs::DirectoryReader>>
    _readers;
  LocalTableChanges _changes;
};

}  // namespace sdb::search
