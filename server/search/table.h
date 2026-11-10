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

#include "catalog/entry/inverted_index.h"
#include "catalog/walk.h"
#include "search/store_stats.h"
#include "search/version_chain.h"

namespace sdb::search {

using Lsn = uint64_t;

inline constexpr Tick kAllApplied = std::numeric_limits<Tick>::max();
inline constexpr Lsn kNoLsn = std::numeric_limits<Lsn>::max();

constexpr uint64_t OwnRowsId(uint64_t generation) noexcept {
  return std::numeric_limits<uint64_t>::max() - generation;
}

irs::doc_id_t PieceDocs(const irs::IndexWriter::Transaction& trx) noexcept;

struct TableUnit {
  uint64_t id = 0;
  irs::IndexSegment segment;
  std::shared_ptr<const irs::SegmentReaderImpl> reader;
  irs::IndexFileRefs::ref_t meta_ref;
  irs::doc_id_t docs = 0;
  std::shared_ptr<const irs::DocumentMask> deleted;
  Lsn first_lsn = kNoLsn;
  bool piece = false;
};

struct MovedUnit {
  Tick tick = 0;
  std::vector<irs::DocMap> maps;
};

using MovedUnits =
  absl::flat_hash_map<uint64_t, std::shared_ptr<const MovedUnit>>;

struct TableState {
  TableState() = default;
  TableState(const TableState& other)
    : units{other.units},
      moved{other.moved},
      config{other.config},
      tick{other.tick} {}
  TableState& operator=(const TableState&) = delete;
  ~TableState() { delete reader.load(std::memory_order_relaxed); }

  std::vector<std::shared_ptr<const TableUnit>> units;
  std::shared_ptr<const MovedUnits> moved;
  std::shared_ptr<const catalog::InvertedIndexConfig> config;
  Tick tick = 0;
  mutable std::atomic<const irs::DirectoryReader*> reader{nullptr};
};

size_t VersionBytes(const TableState& state) noexcept;

using TableVersion = Version<TableState>;

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

inline bool HasOwn(const OwnWrites& own) noexcept {
  return !own.removals.empty() || !own.segments.empty() || own.truncated ||
         (own.rows != nullptr && own.rows->Count() != 0);
}

struct TableCommit {
  std::shared_ptr<const irs::SegmentReaderImpl> piece;
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
  Table(irs::Directory& dir, irs::IndexReaderOptions options,
        std::shared_ptr<const catalog::InvertedIndexConfig> config);
  ~Table();

  std::shared_ptr<const TableState> State() const;
  const TableVersion& VersionAt(Tick snapshot,
                                catalog::WalkCounters& walks) const {
    return _versions.At(snapshot, walks);
  }
  VersionChain<TableState>& Versions() noexcept { return _versions; }

  const irs::DirectoryReader& Reader(const TableState& state) const;
  irs::DirectoryReader View(const TableState& state,
                            const OwnWrites& own) const;
  irs::DirectoryReader View(const OwnWrites& own) const {
    return View(*State(), own);
  }
  irs::DirectoryReader ColdView() const {
    return Build(*State(), {}, Scope::Cold);
  }
  irs::DirectoryReader PieceView(
    std::span<const std::shared_ptr<const TableUnit>> pieces,
    bool keep_deleted) const;
  StoreStats Stats() const;
  uint64_t LiveDocs() const;
  uint64_t ByteSize() const;
  OwnSegment OpenSegment(std::string meta_file) const;

  std::vector<std::shared_ptr<const TableUnit>> Pieces(Lsn before) const;
  std::vector<std::shared_ptr<const TableUnit>> FullTier() const;
  uint64_t PieceBytes() const noexcept {
    return _piece_bytes.load(std::memory_order_relaxed);
  }
  bool HasFullTier() const noexcept {
    return _full_tier.load(std::memory_order_relaxed);
  }

  std::shared_ptr<const irs::DocRemoval> Resolve(
    const std::shared_ptr<const irs::DocRemoval>& removal) const;
  void CheckConflicts(const irs::DocRemoval& removal) const;
  Tick LastAppend() const noexcept {
    return _last_append.load(std::memory_order_acquire);
  }
  Tick LastWrite() const noexcept {
    return _last_write.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool Apply(Tick tick, TableCommit&& commit);
  std::shared_ptr<const TableState> Replace(Tick tick, Lsn lsn,
                                            std::span<const uint64_t> replaced,
                                            std::vector<OwnSegment>&& segments,
                                            std::vector<irs::DocMap>&& maps);
  std::shared_ptr<const TableState> MergePieces(
    Tick tick, Lsn lsn, std::span<const uint64_t> replaced,
    std::shared_ptr<const irs::SegmentReaderImpl> merged, Lsn first_lsn,
    std::vector<irs::DocMap>&& maps);

  bool Collectable(Tick oldest) const;
  void Collect(Tick tick, Tick oldest);
  void SetConfig(std::shared_ptr<const catalog::InvertedIndexConfig> config)
    ABSL_LOCKS_EXCLUDED(_state_mutex);
  void PublishConfig(Tick tick) ABSL_LOCKS_EXCLUDED(_state_mutex);

  absl::Mutex& CommitMutex() const noexcept ABSL_LOCK_RETURNED(_commit_mutex) {
    return _commit_mutex;
  }

  bool Unsettled() const;

  std::vector<StateUnit> StateUnits() const;
  void StateLogged(Lsn lsn);
  Lsn NeededFrom() const;

  void ReplayRows(uint64_t id, irs::doc_id_t first, irs::doc_id_t count,
                  Lsn lsn);
  bool ReplayPending(uint64_t id) const;
  void ReplayPieceRows(
    irs::IndexWriter& writer, uint64_t id, irs::doc_id_t first,
    absl::FunctionRef<void(irs::IndexWriter::Transaction&)> append);
  void ReplayAddSegment(const ReplacedSegment& segment, Lsn lsn);
  void ReplayRemoval(const irs::DocRemoval& removal, Lsn lsn);
  void ReplayTruncate(Lsn lsn);
  void ReplayReplace(std::span<const uint64_t> replaced,
                     std::span<const ReplacedSegment> segments,
                     std::span<const irs::DocMap> maps, Lsn lsn);
  bool ReplayMerge(uint64_t target, std::span<const uint64_t> sources, Lsn lsn);
  void ReplayState(std::span<const StateUnit> units, Lsn lsn);
  uint64_t ReplayTop() const noexcept { return _replay_top; }
  bool FinishReplay();

 private:
  enum class Scope : uint8_t {
    All,
    Cold,
  };

  struct ReplayUnit {
    std::optional<irs::IndexWriter::Transaction> trx;
    bool pending = false;
    Lsn first_lsn = kNoLsn;
    std::string meta_file;
    irs::doc_id_t docs = 0;
    irs::DocumentMaskBuilder removed;
  };

  const TableVersion& Head() const noexcept { return _versions.Head(); }

  irs::DirectoryReader Build(const TableState& state, const OwnWrites& own,
                             Scope scope) const;
  std::shared_ptr<const TableState> ReplaceUnits(
    Tick tick, Lsn lsn, std::span<const uint64_t> replaced,
    std::vector<std::shared_ptr<TableUnit>>&& added,
    std::vector<irs::DocMap>&& maps);
  std::shared_ptr<TableState> SameUnits() const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_state_mutex);
  void Publish(std::shared_ptr<TableState> state, Tick tick)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_state_mutex);
  void Logged(Lsn lsn) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_state_mutex);
  void Replayed(Lsn lsn);
  void Saw(uint64_t id) noexcept { _replay_top = std::max(_replay_top, id); }
  void CountPieces(const TableState& state) noexcept;

  irs::Directory& _dir;
  irs::IndexReaderOptions _options;
  mutable absl::Mutex _state_mutex;
  std::shared_ptr<const catalog::InvertedIndexConfig> _config
    ABSL_GUARDED_BY(_state_mutex);
  VersionChain<TableState> _versions;
  Lsn _needed ABSL_GUARDED_BY(_state_mutex) = kNoLsn;
  std::atomic<Tick> _last_append{0};
  std::atomic<Tick> _last_write{0};
  mutable absl::Mutex _commit_mutex;
  std::atomic<uint64_t> _piece_bytes{0};
  std::atomic<bool> _full_tier{false};
  absl::flat_hash_map<uint64_t, ReplayUnit> _replay;
  Lsn _replay_needed = kNoLsn;
  uint64_t _replay_top = 0;
};

}  // namespace sdb::search
