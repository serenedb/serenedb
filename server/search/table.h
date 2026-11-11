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
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "catalog/walk.h"
#include "connector/column_id.h"
#include "connector/file_manifest.h"
#include "search/store_stats.h"
#include "search/version_chain.h"

namespace sdb::search {

using Lsn = uint64_t;

inline constexpr Tick kAllApplied = std::numeric_limits<Tick>::max();
inline constexpr Lsn kNoLsn = std::numeric_limits<Lsn>::max();

inline constexpr uint32_t kPieceTierBits = 3;
inline constexpr uint32_t kPieceFanIn = uint32_t{1} << kPieceTierBits;

constexpr uint64_t OwnRowsId(uint64_t generation) noexcept {
  return std::numeric_limits<uint64_t>::max() - generation;
}

irs::doc_id_t PieceDocs(const irs::IndexWriter::Transaction& trx) noexcept;

struct TableUnit {
  uint64_t id = 0;
  irs::SegmentMeta meta;
  std::shared_ptr<const irs::SegmentReaderImpl> reader;
  irs::doc_id_t docs = 0;
  std::shared_ptr<const irs::DocumentMask> deleted;
  std::vector<irs::FieldPatch> patches;
  uint64_t first = 0;
  bool piece = false;
};

void MergePatches(std::vector<irs::FieldPatch>& patches,
                  std::span<const irs::FieldPatch> added, Tick tick);
void SlicePatches(std::span<const irs::FieldPatch> patches, uint64_t first,
                  uint64_t end, std::vector<irs::FieldPatch>& out);

struct LoggedSegment {
  uint64_t id = 0;
  uint8_t kinds = 0;
  irs::doc_id_t rows = 0;
};

struct LoggedRows {
  uint64_t first = 0;
  std::vector<connector::ColumnId> columns;
  std::vector<std::string> chunks;
};

struct LoggedDeletes {
  uint64_t segment = 0;
  std::shared_ptr<const irs::DocumentMask> docs;
};

struct LoggedUpdate {
  uint64_t segment = 0;
  irs::FieldPatch patch;
};

struct TableDelta {
  std::vector<uint64_t> drop;
  uint64_t drop_memory = 0;
  std::vector<LoggedSegment> add;
  std::vector<LoggedRows> rows;
  std::vector<LoggedDeletes> deletes;
  uint64_t memory_base = 0;
  std::shared_ptr<const irs::DocumentMask> memory_deletes;
  std::vector<LoggedUpdate> updates;
  std::vector<irs::FieldPatch> memory_updates;
  std::shared_ptr<const FileManifest> manifest;
};

bool Empty(const TableDelta& delta) noexcept;

class MemoryDeletes {
 public:
  void Add(uint64_t first, std::shared_ptr<const irs::DocumentMask> docs) {
    _pieces.push_back({first, std::move(docs)});
  }
  void Finish(TableDelta& delta) &&;

 private:
  struct Piece {
    uint64_t first = 0;
    std::shared_ptr<const irs::DocumentMask> docs;
  };

  std::vector<Piece> _pieces;
};

struct UnitPatch {
  uint64_t unit = 0;
  std::vector<irs::FieldPatch> fields;
};

void AddUnitPatch(std::vector<UnitPatch>& patches, uint64_t unit,
                  irs::field_id field, const duckdb::LogicalType& type,
                  std::span<const uint64_t> rows, const duckdb::Vector& values);
irs::DocumentMask PatchedDocs(const UnitPatch& patch);
void MergeUnitPatches(std::vector<UnitPatch>& into,
                      std::vector<UnitPatch>&& added);
void CarryUnitPatches(std::vector<UnitPatch>& patches, uint64_t source,
                      std::span<const irs::DocMap> maps);

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
      manifest{other.manifest},
      tick{other.tick},
      memory_end{other.memory_end} {}
  TableState& operator=(const TableState&) = delete;

  std::vector<std::shared_ptr<const TableUnit>> units;
  std::shared_ptr<const MovedUnits> moved;
  std::shared_ptr<const catalog::InvertedIndexConfig> config;
  std::shared_ptr<const FileManifest> manifest;
  Tick tick = 0;
  uint64_t memory_end = 0;
  irs::DirectoryReader reader;
};

struct TableRead {
  std::shared_ptr<const TableState> state;
  std::optional<irs::DirectoryReader> own;

  const irs::DirectoryReader& Reader() const noexcept {
    return own ? *own : state->reader;
  }
};

size_t VersionBytes(const TableState& state) noexcept;

using TableVersion = Version<TableState>;

struct OwnSegment {
  irs::SegmentMeta meta;
  std::shared_ptr<const irs::SegmentReaderImpl> reader;
};

struct OwnWrites {
  const duckdb::ColumnDataCollection* rows = nullptr;
  uint64_t rows_id = OwnRowsId(0);
  std::span<const irs::field_id> row_columns;
  std::span<const OwnSegment> segments;
  std::span<const std::shared_ptr<const irs::DocRemoval>> removals;
  std::span<const UnitPatch> patches;
};

inline bool HasOwn(const OwnWrites& own) noexcept {
  return !own.removals.empty() || !own.segments.empty() ||
         !own.patches.empty() ||
         (own.rows != nullptr && own.rows->Count() != 0);
}

struct TableCommit {
  std::shared_ptr<const irs::SegmentReaderImpl> piece;
  std::vector<OwnSegment> segments;
  std::vector<std::shared_ptr<const irs::DocRemoval>> removals;
  std::vector<UnitPatch> patches;
  std::shared_ptr<const FileManifest> manifest;
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
    const irs::DocumentMask* removed = nullptr;
  };

  LiveUnits(std::shared_ptr<const TableState> state,
            std::span<const std::shared_ptr<const irs::DocRemoval>> removals);

  const std::shared_ptr<const TableState>& State() const noexcept {
    return _state;
  }
  const std::vector<Unit>& Units() const noexcept { return _units; }
  const Unit* Find(uint64_t id) const noexcept;
  // The rows the removals take from `id`, a committed unit or not.
  const irs::DocumentMask* Removed(uint64_t id) const noexcept {
    const auto it = _removed.find(id);
    return it == _removed.end() ? nullptr : &it->second;
  }

 private:
  std::shared_ptr<const TableState> _state;
  irs::containers::NodeHashMap<uint64_t, irs::DocumentMask> _removed;
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

  static const irs::DirectoryReader& Reader(const TableState& state) noexcept {
    return state.reader;
  }
  irs::DirectoryReader View(const TableState& state,
                            const OwnWrites& own) const;
  TableRead Read(const OwnWrites& own) const;
  irs::DirectoryReader ColdView() const {
    return Build(*State(), {}, Scope::Cold);
  }
  irs::DirectoryReader PieceView(
    std::span<const std::shared_ptr<const TableUnit>> pieces,
    bool keep_deleted) const;
  StoreStats Stats() const;
  uint64_t LiveDocs() const;
  uint64_t ByteSize() const;
  OwnSegment OpenSegment(uint64_t id, uint8_t kinds, irs::doc_id_t rows) const;

  struct Memory {
    std::vector<std::shared_ptr<const TableUnit>> pieces;
    uint64_t end = 0;
  };
  Memory InMemory() const;
  std::vector<std::shared_ptr<const TableUnit>> MergeRun() const;
  uint64_t PieceBytes() const noexcept {
    return _piece_bytes.load(std::memory_order_relaxed);
  }
  uint64_t PatchBytes() const noexcept {
    return _patch_bytes.load(std::memory_order_relaxed);
  }
  bool HasMergeRun() const noexcept {
    return _merge_run.load(std::memory_order_relaxed);
  }

  std::shared_ptr<const irs::DocRemoval> Resolve(
    const std::shared_ptr<const irs::DocRemoval>& removal) const;
  std::vector<UnitPatch> Resolve(std::vector<UnitPatch>&& patches) const;
  void CheckConflicts(const irs::DocRemoval& removal, Tick snapshot) const;
  Tick LastWrite() const noexcept {
    return _last_write.load(std::memory_order_acquire);
  }
  Tick LastRewrite() const noexcept {
    return _last_rewrite.load(std::memory_order_acquire);
  }
  void Rewritten(Tick tick) noexcept {
    _last_rewrite.store(tick, std::memory_order_release);
  }

  TableDelta CommitDelta(const TableCommit& commit, LoggedRows&& rows) const;
  [[nodiscard]] bool Apply(Tick tick, TableCommit&& commit);

  struct Replacement {
    std::vector<uint64_t> replaced;
    std::vector<std::shared_ptr<TableUnit>> added;
    std::vector<irs::DocMap> maps;
    TableDelta delta;
  };
  Replacement PrepareReplace(std::span<const uint64_t> replaced,
                             std::span<const Tick> folded,
                             std::vector<std::shared_ptr<TableUnit>>&& added,
                             std::vector<irs::DocMap>&& maps,
                             uint64_t memory_below) const;
  std::shared_ptr<const TableState> Replace(Tick tick,
                                            Replacement&& replacement);
  std::shared_ptr<TableUnit> SegmentUnit(OwnSegment&& segment) const;

  bool Collectable(Tick oldest) const;
  void Collect(Tick tick, Tick oldest);
  void SetConfig(std::shared_ptr<const catalog::InvertedIndexConfig> config)
    ABSL_LOCKS_EXCLUDED(_state_mutex);
  void PublishConfig(Tick tick) ABSL_LOCKS_EXCLUDED(_state_mutex);

  absl::Mutex& CommitMutex() const noexcept ABSL_LOCK_RETURNED(_commit_mutex) {
    return _commit_mutex;
  }

  bool Unsettled() const;

  std::shared_ptr<const TableState> HeadState() const
    ABSL_LOCKS_EXCLUDED(_state_mutex);

  void Recover(std::vector<std::shared_ptr<TableUnit>> units,
               uint64_t memory_end,
               std::shared_ptr<const FileManifest> manifest);

 private:
  enum class Scope : uint8_t {
    All,
    Cold,
  };

  const TableVersion& Head() const noexcept { return _versions.Head(); }

  irs::DirectoryReader Build(const TableState& state, const OwnWrites& own,
                             Scope scope) const;
  irs::DirectoryReader BuildReader(const TableState& prev,
                                   const TableState& next) const;
  std::shared_ptr<TableState> SameUnits() const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_state_mutex);
  void Publish(std::shared_ptr<TableState> state, Tick tick)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_state_mutex);
  void CountPieces(const TableState& state) noexcept;

  irs::Directory& _dir;
  irs::IndexReaderOptions _options;
  mutable absl::Mutex _state_mutex;
  std::shared_ptr<const catalog::InvertedIndexConfig> _config
    ABSL_GUARDED_BY(_state_mutex);
  VersionChain<TableState> _versions;
  std::atomic<Tick> _last_write{0};
  std::atomic<Tick> _last_rewrite{0};
  mutable absl::Mutex _commit_mutex;
  std::atomic<uint64_t> _piece_bytes{0};
  std::atomic<uint64_t> _patch_bytes{0};
  std::atomic<bool> _merge_run{false};
};

std::shared_ptr<TableUnit> PieceUnit(
  std::shared_ptr<const irs::SegmentReaderImpl> reader, uint64_t first);

}  // namespace sdb::search
