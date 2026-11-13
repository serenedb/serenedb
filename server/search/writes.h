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
  const duckdb::CatalogEntry* owner = nullptr;
  const catalog::SearchTableEntry* table = nullptr;
  std::vector<std::unique_ptr<irs::IndexWriter::Transaction>> transactions;
  LocalTableChangesEntry changes;
  WriterGenerations::Writer registration;
  std::vector<OwnSegment> own_segments;
  std::vector<irs::DocMap> own_maps;
  uint64_t rows_generation = 0;
  std::shared_ptr<const TableState> rebuilt;
  bool rebuilt_newer = false;
  bool adopting = false;
  uint64_t revision = 0;
  struct Pin {
    const TableState* state = nullptr;
    std::optional<irs::DirectoryReader> own;
    const FileManifest* manifest = nullptr;
    uint64_t revision = 0;

    const irs::DirectoryReader& Reader() const noexcept {
      return own ? *own : state->reader;
    }
  };
  std::optional<Pin> pin;
  std::atomic<const Pin*> pinned{nullptr};
  StoreWrites* listed = nullptr;
  std::shared_ptr<const irs::SegmentReaderImpl> piece;
  std::shared_ptr<const FileManifest> manifest;
  struct ChangedRows {
    Tick snapshot = 0;
    std::vector<std::shared_ptr<const irs::DocRemoval>> rows;
  };
  std::vector<ChangedRows> changed;
  LoggedRows rows;
  TableCommit commit;
  bool prepared = false;
  bool dropping = false;
  // The keys the table enforces, and what this transaction's rows were
  // checked against: the keys of the rows it still buffers, how many of those
  // rows and of its own segments were checked, and the committed rows the last
  // check saw.
  std::vector<KeySpec> keys;
  HotKeys buffered_keys;
  duckdb::idx_t keyed_rows = 0;
  uint64_t keyed_generation = 0;
  size_t checked_segments = 0;
  std::optional<LiveUnits> checked;
  bool merge_inserted = false;
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
};

struct LogSection {
  const duckdb::CatalogEntry* owner = nullptr;
  TableDelta delta;
};

class Writes {
 public:
  Writes(duckdb::transaction_t session, const std::atomic<Tick>& snapshot,
         catalog::WalkCounters& walks)
    : _session{session}, _snapshot{snapshot}, _walks{walks} {}
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

  void Adopting(const std::shared_ptr<Store>& store);
  void Rebuilt(const std::shared_ptr<Store>& store,
               std::shared_ptr<const TableState> state, bool newer);

  void AddOwnSegments(
    const std::shared_ptr<Store>& store,
    std::unique_ptr<irs::IndexWriter::Transaction> trx,
    std::span<const irs::IndexWriter::FlushedSegment> flushed);

  void AddInlineRows(duckdb::ClientContext& context,
                     const std::shared_ptr<Store>& store,
                     std::span<const connector::ColumnId> column_ids,
                     duckdb::Catalog& catalog,
                     std::unique_ptr<duckdb::ColumnDataCollection> rows,
                     uint64_t bytes, uint64_t flush_above);

  void AddRemoval(duckdb::ClientContext& context,
                  const std::shared_ptr<Store>& store,
                  irs::DocRemovalBuilder&& positions);

  void AddPatches(const std::shared_ptr<Store>& store,
                  std::vector<UnitPatch>&& patches);
  void CheckConflicts(const Store& store);

  void AddSearchTruncate(duckdb::ClientContext& context,
                         const std::shared_ptr<Store>& store);

  void RegisterIndex(const std::shared_ptr<Store>& store,
                     const duckdb::CatalogEntry& index);
  void SetManifest(const std::shared_ptr<Store>& store,
                   std::shared_ptr<const FileManifest> manifest);

  void ClaimExclusive(const std::shared_ptr<Store>& store,
                      Store::Exclusive kind);

  ReadView Read(duckdb::ClientContext& context, Store& store);
  Tick PlanVersion(const Store& store);
  const irs::DirectoryReader& PinnedReader(duckdb::ClientContext& context,
                                           Store& store);
  void ResetPins(bool all) noexcept;
  TableRead LatestView(duckdb::ClientContext& context, Store& store);

  bool Empty() const noexcept { return _writes.empty(); }

  // Refuses the statement whose rows of `store` hold a unique key a live row
  // already holds, theirs or another of its own, or a foreign key the
  // referenced table does not hold, and the statement that removed the last
  // row holding a key a foreign key references. Runs once the statement has
  // handed its rows over.
  void CheckKeys(duckdb::ClientContext& context, const Store& store,
                 bool merge_insert);

  // Refuses the rows of `table`, the whole table, whose reference key `spec`
  // the referenced table does not hold; the commit checks them again if the
  // referenced table changes before it.
  void VerifyReferences(duckdb::ClientContext& context,
                        const catalog::SearchTableEntry& table,
                        const KeySpec& spec);

  void PrepareCommit(duckdb::ClientContext& context);
  std::vector<LogSection> TakeLog(duckdb::ClientContext& context);
  bool CrossTable() const;
  std::vector<absl::Mutex*> CommitMutexes(
    std::span<const std::shared_ptr<Store>> layouts) const;
  void Apply(Tick tick,
             std::span<const std::shared_ptr<Store>> layouts) noexcept;
  void BeforePublish() noexcept;
  void Finish() noexcept;

  void Abort() noexcept;

 private:
  StoreWrites& EnsureStoreWrites(const std::shared_ptr<Store>& store);
  StoreWrites* FindStoreWrites(duckdb::idx_t store_id) const;
  const StoreWrites::Pin& EnsurePin(duckdb::ClientContext& context,
                                    StoreWrites& w);
  Tick Snapshot() const noexcept {
    return _snapshot.load(std::memory_order_acquire);
  }
  void Adopt(StoreWrites& w, const catalog::SearchTableEntry& table);
  void Flush(duckdb::ClientContext& context, StoreWrites& w);
  StoreWrites* Related(const Store& store, StoreWrites& w);
  // The catalog as it is now, with this transaction's own changes.
  duckdb::CatalogTransaction Latest(
    const catalog::SearchTableEntry& table) const {
    return duckdb::CatalogTransaction{table.catalog.GetDatabase(), _session,
                                      duckdb::VisibilityBound::AllCommitted()};
  }
  void CheckChanged(const StoreWrites& w,
                    std::span<const StoreWrites::ChangedRows> changed);
  [[noreturn]] void ThrowDuplicateOrConflict(duckdb::ClientContext& context,
                                             const StoreWrites& w,
                                             const KeySpec& spec,
                                             std::string_view key,
                                             bool merge_insert);
  void CheckReferences(duckdb::ClientContext& context, StoreWrites& w,
                       const KeySpec& spec, std::span<const std::string> keys);
  void CheckRemovedKeys(duckdb::ClientContext& context, StoreWrites& w,
                        bool commit);
  void CatchUpKeys(duckdb::ClientContext& context, StoreWrites& w);
  std::shared_ptr<const TableState> VerifyAllReferences(
    duckdb::ClientContext& context, const catalog::SearchTableEntry& table,
    const KeySpec& spec);

  struct ReferenceCheck {
    const catalog::SearchTableEntry* table = nullptr;
    KeySpec spec;
    std::shared_ptr<const TableState> referenced;
  };

  duckdb::transaction_t _session;
  const std::atomic<Tick>& _snapshot;
  catalog::WalkCounters& _walks;
  mutable absl::Mutex _mutex;
  std::atomic<StoreWrites*> _listed{nullptr};
  std::vector<ReferenceCheck> _reference_checks;
  Tick _conflict = 0;
  // Node-based: the entry is well past FlatHashMap's size cut-off, and the
  // buffer is held by reference across a statement, so the nodes must not
  // move.
  irs::containers::NodeHashMap<duckdb::idx_t, StoreWrites> _writes;
};

}  // namespace sdb::search
