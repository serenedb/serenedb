////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <cstdint>
#include <duckdb/common/identifier.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/duckdb_sink_writer_base.h"
#include "search/search_table_changes.h"
#include "search/store.h"

namespace sdb::search {

struct Slot {
  std::unique_ptr<irs::IndexWriter::Transaction> trx;
  std::unique_ptr<connector::DuckDBSinkIndexWriter> writer;
};

struct StoreWrites {
  std::shared_ptr<Store> store;
  const catalog::SearchTableEntry* table = nullptr;
  std::vector<Slot> slots;
  LocalTableChangesEntry changes;
  irs::IndexWriter::Transaction* buffer_trx = nullptr;
  // The writer-generation slot this transaction registered in on `store`,
  // released when the transaction settles. -1 until it registers.
  int writer_slot = -1;
  bool truncate_claim = false;
  duckdb::unique_ptr<duckdb::StorageLockKey> checkpoint_lock;
  irs::SourcePosition position;
};

class Writes {
 public:
  ~Writes() { Abort(); }

  // Registers this transaction as a writer of `store`, once, before anything
  // reads the store's index config -- so a rebuild that publishes a config can
  // tell whether this transaction predates it. Called from the DML operators'
  // GetGlobalSinkState: the bulk insert path builds its sink there, ahead of
  // the Combine that hands over its iresearch transaction, so registering any
  // later would let it straddle a swap unnoticed.
  void RegisterWriter(const std::shared_ptr<Store>& store,
                      const catalog::SearchTableEntry& table);

  // Whether this transaction has already written to `store_id`. CREATE INDEX
  // refuses to run in such a transaction: the rebuild would wait for writers
  // that predate its config swap, and this one cannot finish until the
  // statement it is running does.
  bool HasWritesFor(duckdb::idx_t store_id) const noexcept {
    return _writes.contains(store_id);
  }

  void AddParallelSearchTransaction(
    const std::shared_ptr<Store>& store,
    std::unique_ptr<irs::IndexWriter::Transaction> trx);

  // The segments a bulk statement flushed + fsynced, for the WAL to reference
  // instead of a second copy of the rows.
  void AddSegments(const std::shared_ptr<Store>& store,
                   std::vector<std::string>&& segments);

  void AddInlineRows(const std::shared_ptr<Store>& store,
                     std::span<const connector::ColumnId> column_ids,
                     duckdb::Catalog& catalog,
                     std::unique_ptr<duckdb::ColumnDataCollection> rows);

  // Bytes a flush would reclaim for `store_id`; deletes are not counted.
  uint64_t BufferedBytes(duckdb::idx_t store_id) const noexcept {
    const auto it = _writes.find(store_id);
    return it == _writes.end() ? 0 : it->second.changes.BufferedBytes();
  }

  // Replays the whole buffer into the shard's exclusive transaction, creating
  // it on first use, and empties the row buffer. The rows become that
  // transaction's to make durable, so the record stops carrying a copy of
  // them.
  void FlushBuffer(const std::shared_ptr<Store>& store,
                   duckdb::ClientContext& context);
  void FlushBufferTransaction(duckdb::idx_t store_id);

  void AddRemoval(const std::shared_ptr<Store>& store,
                  irs::DocRemovalBuilder&& positions);

  void AddSearchTruncate(const std::shared_ptr<Store>& store,
                         const duckdb::Identifier& table_name,
                         bool clears_store);

  struct PinnedSearchTable {
    std::shared_ptr<irs::DirectoryReader> reader;
    std::shared_ptr<const catalog::InvertedIndexConfig> config;
  };

  static PinnedSearchTable MakePinnedSearchTable(const Store& store);

  const PinnedSearchTable& EnsureSearchTable(const Store& store);
  const irs::DirectoryReader& PinnedReader(const Store& store) const;

  void ResetReaders() noexcept { _readers.clear(); }

  void ResetReader(duckdb::idx_t database_id, duckdb::idx_t store_id) noexcept {
    _readers.erase(std::pair{database_id, store_id});
  }

  Slot& EnsureIndexSlot(
    duckdb::idx_t index_id, std::shared_ptr<Store> store,
    std::shared_ptr<const catalog::InvertedIndexConfig> config,
    size_t slot = 0);

  irs::IndexWriter::Transaction& EnsureIndexTransaction(
    duckdb::idx_t index_id, std::shared_ptr<Store> store,
    std::shared_ptr<const catalog::InvertedIndexConfig> config) {
    return *EnsureIndexSlot(index_id, std::move(store), std::move(config)).trx;
  }

  std::span<Slot> IndexSlots(duckdb::idx_t index_id) {
    const auto it = _writes.find(index_id);
    if (it == _writes.end()) {
      return {};
    }
    return it->second.slots;
  }

  const duckdb::Vector& FeedColumn(const void* database,
                                   duckdb::idx_t table_oid,
                                   duckdb::row_t first_row, duckdb::idx_t count,
                                   duckdb::idx_t column,
                                   const duckdb::Vector& source);

  bool Empty() const noexcept { return _writes.empty(); }

  void PrepareCommit(duckdb::ClientContext& context);
  void Stage(const irs::SourcePosition& position,
             duckdb::idx_t database) noexcept;
  void Publish(duckdb::idx_t database) noexcept;

  void Abort() noexcept;

 private:
  struct FeedColumns {
    const void* database = nullptr;
    duckdb::idx_t table_oid = 0;
    duckdb::row_t first_row = 0;
    duckdb::idx_t count = 0;
    std::vector<std::pair<duckdb::idx_t, duckdb::Vector>> columns;
  };

  StoreWrites& EnsureStoreWrites(const std::shared_ptr<Store>& store);

  // Node-based: the entry is well past FlatHashMap's size cut-off, and the
  // buffer is held by reference across a statement, so the nodes must not
  // move.
  irs::containers::NodeHashMap<duckdb::idx_t, StoreWrites> _writes;
  irs::containers::FlatHashMap<std::pair<duckdb::idx_t, duckdb::idx_t>,
                               PinnedSearchTable>
    _readers;
  FeedColumns _feed_columns;
};

}  // namespace sdb::search
