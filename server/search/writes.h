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

#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <array>
#include <bitset>
#include <cstdint>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/string_heap.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/index/composite_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/constraint_keys.h"
#include "connector/search_sink_writer.hpp"
#include "search/hot_keys.h"
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
  std::vector<OwnSegment> own_segments;
  std::optional<uint64_t> version;
  std::vector<irs::DocMap> own_maps;
  uint64_t rows_generation = 0;
  std::shared_ptr<const TableState> rebuilt;
  bool rebuilt_newer = false;
  bool adopting = false;
  uint64_t revision = 0;
  struct Pin {
    const TableState* state = nullptr;
    std::optional<irs::CompositeReader> own;
    const FileManifest* manifest = nullptr;
    uint64_t revision = 0;

    const irs::CompositeReader& Reader() const noexcept {
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
  // rows and of its own segments were checked, and the committed rows all of
  // them were checked against.
  std::vector<KeySpec> keys;
  HotKeys buffered_keys;
  duckdb::idx_t keyed_rows = 0;
  uint64_t keyed_generation = 0;
  size_t checked_segments = 0;
  std::optional<LiveUnits> checked;
  bool merge_inserted = false;
  struct VerifiedKey {
    KeySpec spec;
    std::shared_ptr<const TableState> state;
  };
  std::vector<VerifiedKey> verified_keys;
  // The unique keys of the rows this transaction removed, for every unique
  // key a foreign key references, and how many of them were checked; a key
  // first referenced after the transaction removed rows misses theirs until
  // the commit reads them.
  struct RemovedKeys {
    irs::field_id field = 0;
    std::vector<duckdb::string_t> keys;
    size_t checked = 0;
    bool complete = true;
  };
  std::vector<RemovedKeys> removed_keys;
  duckdb::StringHeap removed_heap;
  bool removed_rows = false;
  std::vector<std::vector<duckdb::string_t>> statement_references;
  duckdb::StringHeap statement_heap;
  // The committed rows of a referenced or referencing table the first check
  // against it saw, by store.
  irs::containers::FlatHashMap<duckdb::idx_t, std::shared_ptr<const TableState>>
    related_checked;
};

enum class KeyConflict : uint8_t {
  Unique,
  Upsert,
  UpsertUpdate,
};

class ProposedKeys {
 public:
  static constexpr unsigned kShardBits = 6;
  static constexpr size_t kShards = size_t{1} << kShardBits;

  struct Shard {
    mutable absl::Mutex mutex;
    irs::containers::FlatHashSet<HashedKey, HashedKeyHash, HashedKeyEq> keys
      ABSL_GUARDED_BY(mutex);
    duckdb::StringHeap heap ABSL_GUARDED_BY(mutex);

    bool Insert(std::string_view key, size_t hash)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex);
  };

  static constexpr size_t ShardIndex(size_t hash) noexcept {
    return hash >> (64 - kShardBits);
  }

  Shard& At(size_t index) noexcept { return _shards[index]; }
  Shard& Of(size_t hash) noexcept { return At(ShardIndex(hash)); }
  bool Contains(const HashedKey& key) const;

 private:
  std::array<Shard, kShards> _shards;
};

class ConflictArbiter {
 public:
  struct Scratch {
    std::vector<connector::ConstraintKeyEncoder> encoders;
    std::vector<std::string_view> keys;
    std::vector<size_t> hashes;
    std::vector<std::pair<std::string_view, duckdb::idx_t>> rows;
    std::vector<std::string_view> sorted;
    std::vector<uint32_t> rest;
    std::vector<std::pair<duckdb::idx_t, KeyHolder>> holders;
    std::vector<uint16_t> by_shard;
  };

  ConflictArbiter(duckdb::ClientContext& context, const StoreWrites& w,
                  const irs::CompositeReader& reader,
                  std::span<const irs::field_id> fields);

  duckdb::idx_t Admit(duckdb::DataChunk& chunk,
                      std::span<const connector::ColumnId> column_ids,
                      duckdb::SelectionVector& kept, Scratch& scratch);
  void Replace(
    duckdb::DataChunk& chunk, std::span<const connector::ColumnId> column_ids,
    absl::FunctionRef<void(duckdb::idx_t row, const KeyHolder& holder)>
      replaced,
    Scratch& scratch);

  const ProposedKeys* Proposed(irs::field_id field) const;
  bool Probed(uint64_t unit) const { return _probed.contains(unit); }

 private:
  struct Arbiter {
    irs::field_id field = 0;
    catalog::ConstraintKey key;
    KeyProbe held;
    std::unique_ptr<ProposedKeys> proposed;
  };

  struct OwnRows {
    uint32_t index = 0;
    irs::doc_id_t docs = 0;
    const irs::DocumentMask* removed = nullptr;
    uint64_t generation = 0;
  };

  void Encode(duckdb::DataChunk& chunk,
              std::span<const connector::ColumnId> column_ids,
              Scratch& scratch) const;
  void ForEachHolder(
    Scratch& scratch, duckdb::idx_t count,
    absl::FunctionRef<void(duckdb::idx_t row, const KeyHolder& holder)> visit)
    const;
  bool Propose(const Scratch& scratch,
               duckdb::idx_t row) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  void ProposeByShard(
    Scratch& scratch, size_t arbiter,
    const std::bitset<STANDARD_VECTOR_SIZE>& skip, duckdb::idx_t count,
    absl::FunctionRef<void(duckdb::idx_t row, bool inserted)> proposed);

  duckdb::ClientContext& _context;
  const StoreWrites& _w;
  std::vector<Arbiter> _arbiters;
  irs::containers::FlatHashSet<uint64_t> _probed;
  std::optional<OwnRows> _own;
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

  void RegisterWriter(const std::shared_ptr<Store>& store,
                      const catalog::SearchTableEntry& table);

  void FollowVersion(const Store& store, uint64_t before);

  void AdoptTable(const std::shared_ptr<Store>& store,
                  const catalog::SearchTableEntry& table);

  void RewriteOwn(duckdb::ClientContext& context,
                  const std::shared_ptr<Store>& store, duckdb::Catalog& catalog,
                  absl::FunctionRef<void(const irs::SubReader&,
                                         connector::SearchSinkInsertBaseImpl&)>
                    feed);
  void TakeIndexedPatches(
    duckdb::ClientContext& context, Store& store,
    absl::FunctionRef<void(const irs::CompositeReader&, size_t,
                           const irs::DocumentMask&)>
      feed);

  void Adopting(const std::shared_ptr<Store>& store);
  void Rebuilt(const std::shared_ptr<Store>& store,
               std::shared_ptr<const TableState> state, bool newer);

  void AddOwnSegments(const std::shared_ptr<Store>& store,
                      std::unique_ptr<irs::IndexWriter::Transaction> trx,
                      std::span<const irs::IndexWriter::FlushedSegment> flushed,
                      uint64_t version);

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
  Tick LayoutVersion(const Store& store);
  const irs::CompositeReader& PinnedReader(duckdb::ClientContext& context,
                                           Store& store);
  void ResetPins(bool all) noexcept;
  TableRead LatestView(duckdb::ClientContext& context, Store& store);

  bool Empty() const noexcept { return _writes.empty(); }

  void CheckKeys(duckdb::ClientContext& context, const Store& store,
                 KeyConflict conflict,
                 const ConflictArbiter* arbiter = nullptr);

  std::unique_ptr<ConflictArbiter> ArbitrateConflicts(
    duckdb::ClientContext& context, const std::shared_ptr<Store>& store,
    std::span<const irs::field_id> fields);

  void CheckStatement(duckdb::ClientContext& context);

  // Refuses the rows of `table`, the whole table, whose reference key `spec`
  // the referenced table does not hold; the commit checks them again if the
  // referenced table changes before it.
  void VerifyReferences(duckdb::ClientContext& context,
                        const catalog::SearchTableEntry& table,
                        const KeySpec& spec);

  void VerifyUniqueKey(duckdb::ClientContext& context,
                       const std::shared_ptr<Store>& store,
                       const KeySpec& spec);

  void PrepareCommit(duckdb::ClientContext& context);
  std::vector<LogSection> TakeLog(duckdb::ClientContext& context);
  bool CrossTable() const;
  std::vector<absl::Mutex*> CommitMutexes(
    std::span<const std::shared_ptr<Store>> layouts) const;
  void Apply(duckdb::ClientContext& context, Tick tick,
             std::span<const std::shared_ptr<Store>> layouts) noexcept;
  void BeforePublish() noexcept;
  void Finish() noexcept;

  void Abort() noexcept;

 private:
  StoreWrites& EnsureStoreWrites(const std::shared_ptr<Store>& store);
  StoreWrites* FindStoreWrites(duckdb::idx_t store_id) const;
  bool HasOwnWrites(const Store& store);
  const StoreWrites::Pin& EnsurePin(duckdb::ClientContext& context,
                                    StoreWrites& w);
  Tick Snapshot() const noexcept {
    return _snapshot.load(std::memory_order_acquire);
  }
  void Adopt(StoreWrites& w, const catalog::SearchTableEntry& table);
  void AddIndexKeys(StoreWrites& w, const catalog::SearchTableEntry& table);
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
                                             std::string_view key, bool upsert);
  void CheckReferences(duckdb::ClientContext& context, StoreWrites& w,
                       const KeySpec& spec,
                       std::span<const duckdb::string_t> keys);
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
  std::atomic<bool> _statement_checks{false};
  std::vector<ReferenceCheck> _reference_checks;
  Tick _conflict = 0;
  // Node-based: the entry is well past FlatHashMap's size cut-off, and the
  // buffer is held by reference across a statement, so the nodes must not
  // move.
  irs::containers::NodeHashMap<duckdb::idx_t, StoreWrites> _writes;
};

}  // namespace sdb::search
