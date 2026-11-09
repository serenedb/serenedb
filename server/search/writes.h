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

#include <absl/container/flat_hash_map.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/common/identifier.hpp>
#include <duckdb/main/client_context.hpp>
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
#include "connector/search_sink_writer.hpp"
#include "search/search_table_changes.h"
#include "search/store.h"
#include "search/table.h"
#include "search/table_keys.h"

namespace sdb::search {

struct StoreWrites {
  mutable absl::Mutex mutex;
  std::shared_ptr<Store> store;
  const catalog::SearchTableEntry* table = nullptr;
  std::vector<std::unique_ptr<irs::IndexWriter::Transaction>> transactions;
  LocalTableChangesEntry changes;
  WriterGenerations::Writer registration;
  WriterGenerations::Truncate truncate_claim;
  std::vector<OwnSegment> own_segments;
  std::vector<irs::DocMap> own_maps;
  uint64_t rows_generation = 0;
  Tick rebuilt = 0;
  bool rebuilt_newer = false;
  std::shared_ptr<HotContext> hot;
  uint64_t hot_id = 0;
  irs::doc_id_t hot_first = 0;
  irs::doc_id_t hot_end = 0;
  duckdb::vector<std::string> log;
  bool prepared = false;
  bool dropping = false;
  // The keys the table enforces, and what this transaction's rows were
  // checked against: the keys of the rows it still buffers (once the commit
  // is prepared, of the rows it wrote into its hot unit), how many of those
  // rows and of its own segments were checked, and the committed rows the last
  // check saw.
  std::vector<KeySpec> keys;
  HotKeys buffered_keys;
  duckdb::idx_t keyed_rows = 0;
  uint64_t keyed_generation = 0;
  size_t checked_segments = 0;
  std::optional<LiveUnits> checked;
  // The unique keys of the rows this transaction removed, for every unique
  // key a foreign key references, and how many of them were checked; a key
  // first referenced after the transaction removed rows misses theirs until
  // the commit reads them.
  struct RemovedKeys {
    irs::field_id field = 0;
    std::vector<std::string> keys;
    size_t checked = 0;
    bool complete = true;
  };
  std::vector<RemovedKeys> removed_keys;
  bool removed_rows = false;
  // The committed rows of a referenced or referencing table the first check
  // against it saw, by store.
  absl::flat_hash_map<duckdb::idx_t, std::shared_ptr<const TableState>>
    related_checked;
  // The keys of the rows the commit writes into its hot unit.
  struct HotKey {
    irs::field_id field = 0;
    std::string key;
    irs::doc_id_t doc = 0;
  };
  std::vector<HotKey> hot_keys;
};

struct LogSection {
  const catalog::SearchTableEntry* table = nullptr;
  duckdb::vector<std::string> entries;
};

class Writes {
 public:
  explicit Writes(duckdb::transaction_t session) : _session{session} {}
  ~Writes() { Abort(); }

  // Registers this transaction as a writer of `store`, once, before anything
  // reads the store's index config -- so a rebuild that publishes a config can
  // tell whether this transaction predates it. Called from the DML operators'
  // GetGlobalSinkState: the bulk insert path builds its sink there, ahead of
  // the Combine that hands over its iresearch transaction, so registering any
  // later would let it straddle a swap unnoticed.
  void RegisterWriter(const std::shared_ptr<Store>& store,
                      const catalog::SearchTableEntry& table);

  const WriterGenerations::Writer* Registration(duckdb::idx_t store_id) const;

  void AdoptTable(const std::shared_ptr<Store>& store,
                  const catalog::SearchTableEntry& table);

  void RewriteOwn(duckdb::ClientContext& context,
                  const std::shared_ptr<Store>& store, duckdb::Catalog& catalog,
                  absl::FunctionRef<void(const irs::SubReader&,
                                         connector::SearchSinkInsertBaseImpl&)>
                    feed);

  void Rebuilt(const std::shared_ptr<Store>& store, Tick tick, bool newer);

  void AddOwnSegments(
    const std::shared_ptr<Store>& store,
    std::unique_ptr<irs::IndexWriter::Transaction> trx,
    std::span<const irs::IndexWriter::FlushedSegment> flushed);

  void AddInlineRows(duckdb::ClientContext& context,
                     const std::shared_ptr<Store>& store,
                     std::span<const connector::ColumnId> column_ids,
                     duckdb::Catalog& catalog,
                     std::unique_ptr<duckdb::ColumnDataCollection> rows,
                     uint64_t flush_above);

  void AddRemoval(duckdb::ClientContext& context,
                  const std::shared_ptr<Store>& store,
                  irs::DocRemovalBuilder&& positions);

  void AddSearchTruncate(const std::shared_ptr<Store>& store,
                         const catalog::SearchTableEntry& table, Tick snapshot);

  void ClaimExclusive(const std::shared_ptr<Store>& store,
                      Store::Exclusive kind);

  struct PinnedSearchTable {
    std::shared_ptr<irs::DirectoryReader> reader;
    std::shared_ptr<const catalog::InvertedIndexConfig> config;
  };

  template<typename Factory>
  PinnedSearchTable EnsureSearchTable(duckdb::idx_t database_id,
                                      duckdb::idx_t store_id, bool searchable,
                                      Factory&& make) {
    const std::pair key{database_id, store_id};
    {
      absl::ReaderMutexLock lock{&_mutex};
      if (const auto it = _readers.find(key); it != _readers.end()) {
        const auto& pinned =
          searchable ? it->second.searchable : it->second.columns;
        if (pinned.reader) {
          return pinned;
        }
      }
    }
    auto made = make();
    absl::MutexLock lock{&_mutex};
    auto& views = _readers[key];
    auto& pinned = searchable ? views.searchable : views.columns;
    if (!pinned.reader) {
      pinned = std::move(made);
      views.latest = pinned.reader;
    }
    return pinned;
  }

  void ResetReaders() noexcept {
    absl::MutexLock lock{&_mutex};
    _readers.clear();
  }

  void ResetReader(duckdb::idx_t database_id, duckdb::idx_t store_id) noexcept {
    absl::MutexLock lock{&_mutex};
    _readers.erase(std::pair{database_id, store_id});
  }

  irs::DirectoryReader View(duckdb::ClientContext& context, Store& store,
                            Tick snapshot, bool searchable);
  std::shared_ptr<irs::DirectoryReader> PinnedReader(const Store& store) const;

  bool Empty() const noexcept { return _writes.empty(); }

  // Refuses the statement whose rows of `store` hold a unique key a live row
  // already holds, theirs or another of its own, or a foreign key the
  // referenced table does not hold, and the statement that removed the last
  // row holding a key a foreign key references. Runs once the statement has
  // handed its rows over.
  void CheckKeys(duckdb::ClientContext& context, const Store& store,
                 Tick snapshot);

  // Refuses the rows of `table`, the whole table, whose reference key `spec`
  // the referenced table does not hold; the commit checks them again if the
  // referenced table changes before it.
  void VerifyReferences(duckdb::ClientContext& context,
                        const catalog::SearchTableEntry& table,
                        const KeySpec& spec);

  // Notes the rows this transaction removed that a commit after `snapshot`
  // removed too, for the commit to refuse: a statement moving the
  // transaction's snapshot past such a commit would hide the conflict from the
  // check at commit.
  void NoteConflicts(Tick snapshot);

  void PrepareCommit(duckdb::ClientContext& context, Tick snapshot);
  std::vector<LogSection> TakeLog(duckdb::ClientContext& context,
                                  Tick snapshot);
  void Apply(Tick tick, Tick oldest, Lsn lsn) noexcept;
  void BeforePublish() noexcept;
  void Finish() noexcept;

  void Abort() noexcept;

 private:
  StoreWrites& EnsureStoreWrites(const std::shared_ptr<Store>& store);
  void Adopt(StoreWrites& w, const catalog::SearchTableEntry& table);
  void Flush(duckdb::ClientContext& context, StoreWrites& w);
  StoreWrites* Related(const Store& store, StoreWrites& w);
  // The catalog as it is now, with this transaction's own changes.
  duckdb::CatalogTransaction Latest(
    const catalog::SearchTableEntry& table) const {
    return duckdb::CatalogTransaction{table.catalog.GetDatabase(), _session,
                                      duckdb::VisibilityBound::AllCommitted()};
  }
  void CheckReferences(duckdb::ClientContext& context, StoreWrites& w,
                       const KeySpec& spec, std::span<const std::string> keys);
  void CheckRemovedKeys(duckdb::ClientContext& context, StoreWrites& w,
                        bool commit);
  void CatchUpKeys(duckdb::ClientContext& context, StoreWrites& w,
                   Tick snapshot);
  std::shared_ptr<const TableState> VerifyAllReferences(
    duckdb::ClientContext& context, const catalog::SearchTableEntry& table,
    const KeySpec& spec);

  struct ReferenceCheck {
    const catalog::SearchTableEntry* table = nullptr;
    KeySpec spec;
    std::shared_ptr<const TableState> referenced;
  };

  struct PinnedViews {
    std::shared_ptr<const TableState> state;
    PinnedSearchTable columns;
    PinnedSearchTable searchable;
    std::shared_ptr<irs::DirectoryReader> latest;
  };

  std::shared_ptr<const TableState> PinState(const Store& store);

  duckdb::transaction_t _session;
  bool _conflict = false;
  mutable absl::Mutex _mutex;
  std::vector<ReferenceCheck> _reference_checks;
  // Node-based: the entry is well past FlatHashMap's size cut-off, and the
  // buffer is held by reference across a statement, so the nodes must not
  // move.
  irs::containers::NodeHashMap<duckdb::idx_t, StoreWrites> _writes;
  irs::containers::NodeHashMap<std::pair<duckdb::idx_t, duckdb::idx_t>,
                               PinnedViews>
    _readers;
};

}  // namespace sdb::search
