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

#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/formats/column/hot_column_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "search/hot_keys.h"
#include "search/store_stats.h"

namespace sdb::search {

using Tick = duckdb::transaction_t;
using Lsn = uint64_t;

inline constexpr Tick kAllApplied = std::numeric_limits<Tick>::max();
inline constexpr Lsn kNoLsn = std::numeric_limits<Lsn>::max();

constexpr uint64_t OwnRowsId(uint64_t generation) noexcept {
  return std::numeric_limits<uint64_t>::max() - generation;
}

struct InsertRange {
  irs::doc_id_t end = 0;
  Tick tick = 0;
};

struct DeleteGeneration {
  Tick tick = 0;
  std::shared_ptr<const irs::DocumentMask> mask;
};

irs::doc_id_t HotDocs(const irs::IndexWriter::Transaction& trx) noexcept;
uint64_t HotSegmentId(const irs::IndexWriter::Transaction& trx);

class HotContext final {
 public:
  explicit HotContext(irs::IndexWriter::Transaction&& trx) noexcept
    : _trx{std::move(trx)} {}

  absl::Mutex& Mutex() noexcept ABSL_LOCK_RETURNED(_mutex) { return _mutex; }

  irs::IndexWriter::Transaction& Trx() noexcept
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex) {
    return _trx;
  }

  irs::doc_id_t Docs() const noexcept ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex) {
    return HotDocs(_trx);
  }
  uint64_t Id() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex) {
    return HotSegmentId(_trx);
  }
  size_t Memory() const noexcept ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  std::shared_ptr<const irs::SegmentReaderImpl> Reader(
    duckdb::DatabaseInstance& db, irs::doc_id_t docs)
    ABSL_LOCKS_EXCLUDED(_mutex);

  HotKeys& Keys() noexcept { return _keys; }
  const HotKeys& Keys() const noexcept { return _keys; }

  struct CooledSegment {
    std::string filename;
    std::shared_ptr<const irs::SegmentReaderImpl> reader;
  };

  std::shared_ptr<const CooledSegment> Cooled() const noexcept {
    return std::atomic_load(&_cooled);
  }
  void SetCooled(std::shared_ptr<const CooledSegment> cooled) noexcept {
    std::atomic_store(&_cooled, std::move(cooled));
  }

 private:
  std::shared_ptr<const irs::SegmentReaderImpl> Build(
    duckdb::DatabaseInstance& db, irs::doc_id_t docs)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  absl::Mutex _mutex;
  irs::IndexWriter::Transaction _trx ABSL_GUARDED_BY(_mutex);
  irs::HotChunkStats _stats ABSL_GUARDED_BY(_mutex);
  std::shared_ptr<const irs::SegmentReaderImpl> _cached;
  std::shared_ptr<const CooledSegment> _cooled;
  HotKeys _keys;
};

struct TableUnit {
  uint64_t id = 0;
  std::shared_ptr<HotContext> hot;
  irs::IndexSegment segment;
  std::shared_ptr<const irs::SegmentReaderImpl> reader;
  irs::IndexFileRefs::ref_t meta_ref;
  irs::doc_id_t docs = 0;
  std::vector<InsertRange> inserts;
  std::vector<DeleteGeneration> deletes;
  Tick replaced_at = 0;
  Tick released_at = 0;
  std::vector<irs::DocMap> maps;
  Lsn first_lsn = kNoLsn;
};

struct TableState {
  std::vector<std::shared_ptr<const TableUnit>> units;
};

struct OwnSegment {
  irs::IndexSegment segment;
  std::shared_ptr<const irs::SegmentReaderImpl> reader;
};

struct OwnWrites {
  const duckdb::ColumnDataCollection* rows = nullptr;
  uint64_t rows_id = OwnRowsId(0);
  std::span<const irs::field_id> row_columns;
  std::span<const OwnSegment> segments;
  std::span<const std::shared_ptr<const irs::DocRemoval>> removals;
  bool truncated = false;
};

struct TableCommit {
  std::shared_ptr<HotContext> hot;
  uint64_t hot_id = 0;
  irs::doc_id_t hot_end = 0;
  std::vector<OwnSegment> segments;
  std::vector<std::shared_ptr<const irs::DocRemoval>> removals;
  bool truncate = false;
  Lsn lsn = 0;
};

struct ReplacedSegment {
  uint64_t id = 0;
  std::string meta_file;
  irs::doc_id_t docs = 0;
};

struct StateUnit {
  ReplacedSegment segment;
  std::shared_ptr<const irs::DocumentMask> deleted;
};

// The units of a table's latest applied state, each with the rows it holds,
// the rows commits deleted from it and the rows a transaction's removals take.
[[noreturn]] void ThrowConcurrentUpdate();

class LiveUnits {
 public:
  struct Unit {
    const TableUnit* unit = nullptr;
    irs::doc_id_t visible = 0;
    const irs::DocumentMask* deleted = nullptr;
    const irs::DocumentMaskBuilder* removed = nullptr;
  };

  LiveUnits(std::shared_ptr<const TableState> state,
            std::span<const std::shared_ptr<const irs::DocRemoval>> removals);

  const std::shared_ptr<const TableState>& State() const noexcept {
    return _state;
  }
  const std::vector<Unit>& Units() const noexcept { return _units; }
  const Unit* Find(uint64_t id) const noexcept;
  // The rows the removals take from `id`, a committed unit or not.
  const irs::DocumentMaskBuilder* Removed(uint64_t id) const noexcept {
    const auto it = _removed.find(id);
    return it == _removed.end() ? nullptr : &it->second;
  }

 private:
  std::shared_ptr<const TableState> _state;
  absl::flat_hash_map<uint64_t, irs::DocumentMaskBuilder> _removed;
  std::vector<Unit> _units;
};

class Table final {
 public:
  Table(irs::Directory& dir, irs::IndexReaderOptions options);

  std::shared_ptr<const TableState> State() const {
    return std::atomic_load(&_state);
  }

  irs::DirectoryReader View(Tick snapshot, const OwnWrites& own) const {
    return Assemble(State(), snapshot, own, Scope::All, false);
  }
  irs::DirectoryReader View(const std::shared_ptr<const TableState>& state,
                            Tick snapshot, const OwnWrites& own,
                            bool searchable) const {
    return Assemble(state, snapshot, own, Scope::All, searchable);
  }
  irs::DirectoryReader ColdView(Tick snapshot) const {
    return Assemble(State(), snapshot, {}, Scope::Cold, false);
  }
  irs::DirectoryReader SettledView(Tick snapshot) const {
    return Assemble(State(), snapshot, {}, Scope::Settled, false);
  }
  StoreStats Stats(Tick snapshot) const;
  std::shared_ptr<const irs::SegmentReaderImpl> UnitReader(
    const TableUnit& unit) const {
    return unit.hot ? unit.hot->Reader(*_options.db, unit.docs) : unit.reader;
  }
  OwnSegment OpenSegment(std::string meta_file) const;

  std::shared_ptr<HotContext> AcquireHot(irs::IndexWriter& writer);
  void ReleaseHot(const std::shared_ptr<HotContext>& hot, bool retire);
  void Retire(Lsn before);
  void RetireVisible(Tick snapshot);

  std::shared_ptr<const irs::DocRemoval> Resolve(
    const std::shared_ptr<const irs::DocRemoval>& removal) const;
  bool Conflicts(Tick snapshot, const irs::DocRemoval& removal) const;
  void CheckConflicts(Tick snapshot, const irs::DocRemoval& removal) const;
  Tick LastAppend() const noexcept {
    return _last_append.load(std::memory_order_acquire);
  }
  Tick LastWrite() const noexcept {
    return _last_write.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool Apply(Tick tick, Tick oldest, TableCommit&& commit);
  void Replace(Tick visible, Tick tick, Tick oldest, Lsn lsn,
               std::span<const uint64_t> replaced,
               std::vector<OwnSegment>&& segments,
               std::vector<irs::DocMap>&& maps);

  void Collect(Tick oldest);

  std::vector<std::shared_ptr<HotContext>> TakeCoolable(size_t memory_limit,
                                                        bool all);
  void Cooled(const std::shared_ptr<HotContext>& hot, OwnSegment&& written,
              Lsn lsn);
  bool HasRetiredHot() const;
  void AwaitRetiredReleased();

  std::vector<std::shared_ptr<const TableUnit>> UnsyncedSegments(
    Lsn before) const;
  bool Unsynced(uint64_t id) const;
  void Synced(std::span<const uint64_t> ids, Lsn lsn);
  bool Unsettled() const;

  std::vector<StateUnit> StateUnits() const;
  void StateLogged(Lsn lsn);
  Lsn NeededFrom() const;

  void ReplayRows(uint64_t id, irs::doc_id_t first, irs::doc_id_t count,
                  Lsn lsn);
  bool ReplayPending(uint64_t id) const;
  void ReplayHotRows(
    irs::IndexWriter& writer, uint64_t id, irs::doc_id_t first,
    absl::FunctionRef<void(irs::IndexWriter::Transaction&, HotKeys&)> append);
  void ReplayAddSegment(const ReplacedSegment& segment, Lsn lsn);
  void ReplayRemoval(const irs::DocRemoval& removal, Lsn lsn);
  void ReplayCooled(const ReplacedSegment& segment, Lsn lsn);
  void ReplayTruncate(Lsn lsn);
  void ReplayReplace(std::span<const uint64_t> replaced,
                     std::span<const ReplacedSegment> segments,
                     std::span<const irs::DocMap> maps, Lsn lsn);
  void ReplayState(std::span<const StateUnit> units, Lsn lsn);
  uint64_t ReplayTop() const noexcept { return _replay_top; }
  bool FinishReplay();

 private:
  enum class Scope : uint8_t {
    All,
    Cold,
    Settled,
  };

  struct ReplayUnit {
    std::shared_ptr<HotContext> hot;
    bool pending = false;
    Lsn first_lsn = kNoLsn;
    std::string meta_file;
    irs::doc_id_t docs = 0;
    irs::DocumentMaskBuilder removed;
  };

  struct CachedView {
    std::shared_ptr<const TableState> state;
    Tick seen = 0;
    irs::DirectoryReader reader;
  };

  irs::DirectoryReader Assemble(const std::shared_ptr<const TableState>& state,
                                Tick snapshot, const OwnWrites& own,
                                Scope scope, bool searchable) const;
  irs::DirectoryReader Build(const TableState& state, Tick snapshot,
                             const OwnWrites& own, Scope scope,
                             bool searchable) const;
  void Publish(std::shared_ptr<const TableState> state);
  void Logged(Lsn lsn) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_state_mutex);
  void Replayed(Lsn lsn);
  void Saw(uint64_t id) noexcept { _replay_top = std::max(_replay_top, id); }
  bool RetiredIdle() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_hot_mutex);

  irs::Directory& _dir;
  irs::IndexReaderOptions _options;
  std::shared_ptr<const TableState> _state;
  mutable std::shared_ptr<const CachedView> _view;
  mutable absl::Mutex _state_mutex;
  Lsn _needed ABSL_GUARDED_BY(_state_mutex) = kNoLsn;
  std::atomic<Tick> _last_append{0};
  std::atomic<Tick> _last_write{0};
  mutable absl::Mutex _hot_mutex;
  std::vector<std::shared_ptr<HotContext>> _free ABSL_GUARDED_BY(_hot_mutex);
  absl::flat_hash_set<const HotContext*> _held ABSL_GUARDED_BY(_hot_mutex);
  absl::flat_hash_set<const HotContext*> _retired ABSL_GUARDED_BY(_hot_mutex);
  absl::flat_hash_map<uint64_t, ReplayUnit> _replay;
  Lsn _replay_needed = kNoLsn;
  uint64_t _replay_top = 0;
};

}  // namespace sdb::search
