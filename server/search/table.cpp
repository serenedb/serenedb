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

#include "search/table.h"

#include <absl/algorithm/container.h>
#include <roaring/roaring.h>

#include <algorithm>
#include <array>
#include <bit>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/hot_column_reader.hpp>
#include <iresearch/formats/segment_meta_reader.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/segment_writer.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "catalog/transaction.h"

namespace sdb::search {
namespace {

using Units = std::vector<std::shared_ptr<const TableUnit>>;

constexpr uint32_t kPieceTierBits = 3;
constexpr uint32_t kPieceFanIn = uint32_t{1} << kPieceTierBits;
constexpr size_t kPieceTiers =
  (std::numeric_limits<irs::doc_id_t>::digits - 1) / kPieceTierBits + 1;

size_t PieceTier(irs::doc_id_t docs) noexcept {
  SDB_ASSERT(docs != 0);
  return static_cast<size_t>(std::bit_width(docs) - 1) / kPieceTierBits;
}

size_t IndexOf(const Units& units, uint64_t id) noexcept {
  for (size_t i = 0; i < units.size(); ++i) {
    if (units[i]->id == id) {
      return i;
    }
  }
  return units.size();
}

const TableUnit* FindUnit(const TableState& state, uint64_t id) noexcept {
  const auto i = IndexOf(state.units, id);
  return i == state.units.size() ? nullptr : state.units[i].get();
}

const MovedUnit* FindMoved(const TableState& state, uint64_t id) noexcept {
  if (!state.moved) {
    return nullptr;
  }
  const auto it = state.moved->find(id);
  return it == state.moved->end() ? nullptr : it->second.get();
}

void SetDeletes(TableUnit& unit, irs::DocumentMaskBuilder&& mask) {
  unit.deleted =
    std::make_shared<const irs::DocumentMask>(std::move(mask).Finish());
  if (unit.reader) {
    unit.reader = unit.reader->WithDocsMask(unit.deleted);
  }
}

void AddDeletes(TableUnit& unit, const irs::DocumentMask& docs) {
  irs::DocumentMaskBuilder mask;
  if (unit.deleted) {
    mask.Merge(*unit.deleted);
  }
  mask.Merge(docs);
  SetDeletes(unit, std::move(mask));
}

void DeleteAll(TableUnit& unit) {
  if (unit.docs == 0) {
    return;
  }
  irs::DocumentMaskBuilder mask;
  mask.AddRange(irs::doc_limits::min(),
                static_cast<irs::doc_id_t>(irs::doc_limits::min() + unit.docs));
  SetDeletes(unit, std::move(mask));
}

bool Dead(const TableUnit& unit) noexcept {
  return unit.deleted && unit.deleted->Count() >= unit.docs;
}

bool LeaveDead(TableState& state, Tick tick) {
  std::shared_ptr<MovedUnits> moved;
  std::shared_ptr<const MovedUnit> deleted;
  std::erase_if(state.units, [&](const auto& unit) {
    if (!Dead(*unit)) {
      return false;
    }
    if (!moved) {
      moved = state.moved ? std::make_shared<MovedUnits>(*state.moved)
                          : std::make_shared<MovedUnits>();
      deleted = std::make_shared<const MovedUnit>(MovedUnit{.tick = tick});
    }
    moved->insert_or_assign(unit->id, deleted);
    return true;
  });
  if (!moved) {
    return false;
  }
  state.moved = std::move(moved);
  return true;
}

size_t VersionBytes(const TableVersion& version) noexcept {
  return sizeof(TableVersion) + sizeof(TableState) +
         version.state->units.size() * sizeof(std::shared_ptr<const TableUnit>);
}

std::shared_ptr<const irs::SegmentReaderImpl> Masked(
  const std::shared_ptr<const irs::SegmentReaderImpl>& base,
  irs::DocumentMaskBuilder&& mask) {
  if (mask.Empty()) {
    return base->docs_mask() ? base->WithDocsMask(nullptr) : base;
  }
  return base->WithDocsMask(
    std::make_shared<const irs::DocumentMask>(std::move(mask).Finish()));
}

std::shared_ptr<const irs::SegmentReaderImpl> OwnRowsReader(
  duckdb::DatabaseInstance& db, uint64_t id,
  const duckdb::ColumnDataCollection& rows,
  std::span<const irs::field_id> columns) {
  const auto& types = rows.Types();
  std::vector<irs::StagedColumn> staged;
  staged.reserve(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    staged.push_back(irs::StagedColumn{.id = columns[i], .type = types[i]});
  }
  for (duckdb::idx_t c = 0; c < rows.ChunkCount(); ++c) {
    duckdb::DataChunk scanned;
    rows.InitializeScanChunk(scanned);
    rows.FetchChunk(c, scanned);
    duckdb::DataChunk owned;
    owned.Initialize(duckdb::Allocator::DefaultAllocator(), types);
    scanned.Copy(owned);
    for (size_t i = 0; i < staged.size(); ++i) {
      staged[i].chunks.push_back(
        irs::WriteChunk{duckdb::Vector::Ref(owned.data[i]), owned.size()});
    }
  }
  std::vector<std::unique_ptr<irs::ColumnReader>> readers;
  readers.reserve(staged.size());
  for (auto& column : staged) {
    readers.push_back(
      irs::HotColumnReader::Make(db, std::move(column), nullptr));
  }
  irs::SegmentMeta meta;
  meta.docs_count = static_cast<irs::doc_id_t>(rows.Count());
  meta.live_docs_count = meta.docs_count;
  return irs::SegmentReaderImpl::OpenInMemory(
    meta, id, std::make_unique<irs::ColReader>(db, std::move(readers)));
}

using Mapped = absl::flat_hash_map<uint64_t, irs::DocumentMaskBuilder>;

void Follow(const TableState& state, uint64_t id, const irs::DocumentMask& docs,
            Mapped& out) {
  const auto* moved = FindUnit(state, id) ? nullptr : FindMoved(state, id);
  if (!moved) {
    out[id].Merge(docs);
    return;
  }
  for (const auto& map : moved->maps) {
    if (irs::DocumentMaskBuilder mapped; irs::MapDocs(map, docs, mapped)) {
      Follow(state, map.target, std::move(mapped).Finish(), out);
    }
  }
}

bool Intersects(const irs::DocumentMask& lhs, const irs::DocumentMask& rhs) {
  return roaring::api::roaring_bitmap_intersect(&lhs.Bitmap(), &rhs.Bitmap());
}

bool Conflicting(const TableState& state, uint64_t id,
                 const irs::DocumentMask& docs) {
  if (const auto* unit = FindUnit(state, id)) {
    return unit->deleted && Intersects(docs, *unit->deleted);
  }
  const auto* moved = FindMoved(state, id);
  if (!moved) {
    return false;
  }
  if (moved->maps.empty()) {
    return true;
  }
  return absl::c_any_of(moved->maps, [&](const irs::DocMap& map) {
    if (map.mask && Intersects(docs, *map.mask)) {
      return true;
    }
    irs::DocumentMaskBuilder mapped;
    return irs::MapDocs(map, docs, mapped) &&
           Conflicting(state, map.target, std::move(mapped).Finish());
  });
}

}  // namespace

irs::doc_id_t PieceDocs(const irs::IndexWriter::Transaction& trx) noexcept {
  const auto* writer = trx.HotWriter();
  if (writer == nullptr || !writer->initialized()) {
    return 0;
  }
  return static_cast<irs::doc_id_t>(writer->buffered_docs());
}

LiveUnits::LiveUnits(
  std::shared_ptr<const TableState> state,
  std::span<const std::shared_ptr<const irs::DocRemoval>> removals)
  : _state{std::move(state)} {
  for (const auto& removal : removals) {
    for (const auto& group : removal->groups) {
      Follow(*_state, group.segment, group.docs, _removed);
    }
  }
  _units.reserve(_state->units.size());
  for (const auto& unit : _state->units) {
    const auto removed = _removed.find(unit->id);
    _units.push_back({
      .unit = unit.get(),
      .visible = unit->docs,
      .deleted = unit->deleted.get(),
      .removed = removed == _removed.end() ? nullptr : &removed->second,
    });
  }
}

const LiveUnits::Unit* LiveUnits::Find(uint64_t id) const noexcept {
  const auto it = absl::c_find_if(
    _units, [&](const Unit& unit) { return unit.unit->id == id; });
  return it == _units.end() ? nullptr : &*it;
}

Table::Table(irs::Directory& dir, irs::IndexReaderOptions options)
  : _dir{dir},
    _options{std::move(options)},
    _head{new TableVersion{std::make_shared<const TableState>(), nullptr}},
    _tail{_head.load(std::memory_order_relaxed)} {}

Table::~Table() {
  auto* version = _head.load(std::memory_order_relaxed);
  while (version) {
    delete std::exchange(version,
                         version->prev.load(std::memory_order_relaxed));
  }
}

std::shared_ptr<const TableState> Table::State() const {
  return StateAt(kAllApplied, catalog::Transactions::Get().SharedWalks());
}

std::shared_ptr<const TableState> Table::StateAt(
  Tick snapshot, catalog::WalkCounters& walks) const {
  const catalog::WalkGuard guard{walks};
  const auto* version = _head.load(std::memory_order_acquire);
  while (version->tick > snapshot) {
    const auto* older = version->prev.load(std::memory_order_acquire);
    if (!older) {
      break;
    }
    version = older;
  }
  return version->state;
}

void Table::Publish(std::shared_ptr<TableState> state, Tick tick) {
  auto* head = _head.load(std::memory_order_relaxed);
  SDB_ASSERT(tick >= head->tick);
  if (tick == head->tick) {
    Republish(std::move(state));
    return;
  }
  state->tick = tick;
  CountPieces(*state);
  auto* version = new TableVersion{std::move(state), head};
  head->next = version;
  _head.store(version, std::memory_order_release);
  _older_versions.fetch_add(1, std::memory_order_relaxed);
  const auto oldest = catalog::Transactions::Get().Oldest();
  while (_tail != version && _tail->next->tick <= oldest) {
    Unlink(_tail);
  }
  AddGarbage(*head);
}

void Table::CountPieces(const TableState& state) noexcept {
  uint64_t bytes = 0;
  std::array<uint32_t, kPieceTiers> tiers{};
  bool full = false;
  for (const auto& unit : state.units) {
    if (unit->piece) {
      bytes += unit->reader->Meta().byte_size;
      full |= ++tiers[PieceTier(unit->docs)] == kPieceFanIn;
    }
  }
  _piece_bytes.store(bytes, std::memory_order_relaxed);
  _full_tier.store(full, std::memory_order_relaxed);
}

void Table::Republish(std::shared_ptr<TableState> state) {
  auto* head = _head.load(std::memory_order_relaxed);
  state->tick = head->tick;
  CountPieces(*state);
  auto* older = head->prev.load(std::memory_order_relaxed);
  auto* version = new TableVersion{std::move(state), older};
  if (older) {
    older->next = version;
  } else {
    _tail = version;
  }
  _head.store(version, std::memory_order_release);
  RetireVersion(head);
  AddGarbage(*head);
}

void Table::AddGarbage(const TableVersion& version) {
  _garbage_bytes += VersionBytes(version);
  if (_garbage_bytes > _garbage_trigger) {
    _garbage_trigger = std::numeric_limits<size_t>::max();
    catalog::Transactions::Get().RequestCollect();
  }
}

void Table::Unlink(TableVersion* version) {
  auto* newer = version->next;
  auto* older = version->prev.load(std::memory_order_relaxed);
  newer->prev.store(older, std::memory_order_release);
  if (older) {
    older->next = newer;
  } else {
    _tail = newer;
  }
  _older_versions.fetch_sub(1, std::memory_order_relaxed);
  RetireVersion(version);
}

void Table::RetireVersion(TableVersion* version) {
  _retired_versions.push_back(
    {.epoch = catalog::Transactions::Get().Epoch(),
     .version = std::unique_ptr<TableVersion>{version}});
  _retired_count.store(static_cast<uint32_t>(_retired_versions.size()),
                       std::memory_order_relaxed);
}

void Table::FreeRetiredVersions() {
  const auto safe = catalog::Transactions::Get().SafeEpoch();
  size_t freed = 0;
  std::erase_if(_retired_versions, [&](const RetiredVersion& retired) {
    if (retired.epoch > safe) {
      return false;
    }
    freed += VersionBytes(*retired.version);
    return true;
  });
  _garbage_bytes -= freed;
  _retired_count.store(static_cast<uint32_t>(_retired_versions.size()),
                       std::memory_order_relaxed);
  _garbage_trigger =
    std::max({kMinGarbageBytes, VersionBytes(Head()), 2 * _garbage_bytes});
}

bool Table::FreeRetired() {
  if (_retired_count.load(std::memory_order_relaxed) == 0) {
    return false;
  }
  absl::MutexLock lock{&_state_mutex};
  FreeRetiredVersions();
  return !_retired_versions.empty();
}

void Table::Prune(std::span<const Tick> running, Tick published) {
  if (_older_versions.load(std::memory_order_relaxed) == 0) {
    return;
  }
  absl::MutexLock lock{&_state_mutex};
  auto* newer = _head.load(std::memory_order_relaxed);
  auto* version = newer->prev.load(std::memory_order_relaxed);
  while (version) {
    auto* older = version->prev.load(std::memory_order_relaxed);
    const auto seen = absl::c_lower_bound(running, version->tick);
    if (newer->tick > published ||
        (seen != running.end() && *seen < newer->tick)) {
      newer = version;
    } else {
      Unlink(version);
    }
    version = older;
  }
  FreeRetiredVersions();
}

void Table::Logged(Lsn lsn) {
  if (lsn != 0 && _needed == kNoLsn) {
    _needed = lsn;
  }
}

void Table::Replayed(Lsn lsn) {
  if (_replay_needed == kNoLsn) {
    _replay_needed = lsn;
  }
}

OwnSegment Table::OpenSegment(std::string meta_file) const {
  OwnSegment opened;
  opened.segment.filename = std::move(meta_file);
  irs::segment_meta::Read(_dir, opened.segment.meta, opened.segment.filename);
  opened.reader =
    irs::SegmentReaderImpl::Open(_dir, opened.segment.meta, _options);
  return opened;
}

irs::DirectoryReader Table::Assemble(
  const std::shared_ptr<const TableState>& state, const OwnWrites& own,
  Scope scope) const {
  if (scope != Scope::All || !own.removals.empty() || !own.segments.empty() ||
      own.truncated || (own.rows != nullptr && own.rows->Count() != 0)) {
    return Build(*state, own, scope);
  }
  if (const auto* cached = state->reader.load(std::memory_order_acquire)) {
    return *cached;
  }
  auto built =
    std::make_unique<const irs::DirectoryReader>(Build(*state, own, scope));
  const irs::DirectoryReader* installed = nullptr;
  if (state->reader.compare_exchange_strong(installed, built.get(),
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
    return *built.release();
  }
  return *installed;
}

irs::DirectoryReader Table::Build(const TableState& state, const OwnWrites& own,
                                  Scope scope) const {
  auto& db = *_options.db;
  Mapped own_removed;
  for (const auto& removal : own.removals) {
    for (const auto& group : removal->groups) {
      Follow(state, group.segment, group.docs, own_removed);
    }
  }
  irs::DirectoryMeta meta;
  std::vector<irs::SegmentReader> readers;
  meta.index_meta.segments.reserve(state.units.size() + own.segments.size() +
                                   1);
  readers.reserve(meta.index_meta.segments.capacity());
  const auto add = [&](const std::string& filename,
                       std::shared_ptr<const irs::SegmentReaderImpl> reader) {
    irs::IndexSegment segment;
    segment.filename = filename;
    static_cast<irs::SegmentInfo&>(segment.meta) = reader->Meta();
    meta.index_meta.segments.push_back(std::move(segment));
    readers.emplace_back(std::move(reader));
  };
  const auto with_own =
    [&](uint64_t id, const irs::DocumentMask* deleted,
        const std::shared_ptr<const irs::SegmentReaderImpl>& base) {
      const auto own_it = own_removed.find(id);
      if (own_it == own_removed.end() && deleted == base->docs_mask()) {
        return base;
      }
      irs::DocumentMaskBuilder mask;
      if (deleted != nullptr) {
        mask.Merge(*deleted);
      }
      if (own_it != own_removed.end()) {
        mask.Merge(own_it->second);
      }
      return Masked(base, std::move(mask));
    };
  for (const auto& unit : state.units) {
    if (own.truncated || (scope == Scope::Cold && unit->piece)) {
      continue;
    }
    add(unit->segment.filename,
        with_own(unit->id, unit->deleted.get(), unit->reader));
  }
  for (const auto& segment : own.segments) {
    add(segment.segment.filename,
        with_own(segment.reader->Id(), segment.reader->docs_mask(),
                 segment.reader));
  }
  if (own.rows != nullptr && own.rows->Count() != 0) {
    const auto reader =
      OwnRowsReader(db, own.rows_id, *own.rows, own.row_columns);
    add({}, with_own(own.rows_id, nullptr, reader));
  }
  return irs::DirectoryReader{std::make_shared<const irs::DirectoryReaderImpl>(
    _dir, _options, std::move(meta), std::move(readers))};
}

StoreStats Table::Stats() const {
  const auto state = State();
  const auto cold = Build(*state, {}, Scope::Cold);
  auto stats = StoreStats::FromReader(cold);
  stats.numBufferedDocs =
    Assemble(state, {}, Scope::All).live_docs_count() - cold.live_docs_count();
  for (const auto& unit : state->units) {
    if (!unit->piece) {
      stats.numFiles += unit->segment.meta.files.size();
    }
  }
  return stats;
}

irs::DirectoryReader Table::PieceView(
  std::span<const std::shared_ptr<const TableUnit>> pieces,
  bool keep_deleted) const {
  TableState state;
  state.units.reserve(pieces.size());
  for (const auto& piece : pieces) {
    if (!keep_deleted || !piece->deleted) {
      state.units.push_back(piece);
      continue;
    }
    auto unit = std::make_shared<TableUnit>(*piece);
    unit->deleted.reset();
    unit->reader = piece->reader->WithDocsMask(nullptr);
    state.units.push_back(std::move(unit));
  }
  return Build(state, {}, Scope::All);
}

std::vector<std::shared_ptr<const TableUnit>> Table::Pieces(Lsn before) const {
  std::vector<std::shared_ptr<const TableUnit>> pieces;
  for (const auto& unit : State()->units) {
    if (unit->piece && (before == kNoLsn || unit->first_lsn < before)) {
      pieces.push_back(unit);
    }
  }
  return pieces;
}

std::vector<std::shared_ptr<const TableUnit>> Table::FullTier() const {
  const auto state = State();
  std::array<uint32_t, kPieceTiers> tiers{};
  for (const auto& unit : state->units) {
    if (unit->piece) {
      ++tiers[PieceTier(unit->docs)];
    }
  }
  const auto full = static_cast<size_t>(
    absl::c_find_if(tiers,
                    [](uint32_t count) { return count >= kPieceFanIn; }) -
    tiers.begin());
  std::vector<std::shared_ptr<const TableUnit>> pieces;
  if (full == tiers.size()) {
    return pieces;
  }
  for (const auto& unit : state->units) {
    if (unit->piece && PieceTier(unit->docs) == full) {
      pieces.push_back(unit);
    }
  }
  return pieces;
}

std::shared_ptr<const irs::DocRemoval> Table::Resolve(
  const std::shared_ptr<const irs::DocRemoval>& removal) const {
  const auto state = State();
  const bool current = absl::c_all_of(removal->groups, [&](const auto& group) {
    return FindUnit(*state, group.segment) || !FindMoved(*state, group.segment);
  });
  if (current) {
    return removal;
  }
  Mapped mapped;
  for (const auto& group : removal->groups) {
    Follow(*state, group.segment, group.docs, mapped);
  }
  auto resolved = std::make_shared<irs::DocRemoval>();
  resolved->groups.reserve(mapped.size());
  for (auto& [id, docs] : mapped) {
    if (!docs.Empty()) {
      resolved->groups.push_back(
        {.segment = id,
         .docs = std::move(docs).Finish(irs::DocumentMaskBuilder::kCanonical)});
    }
  }
  return resolved;
}

void ThrowConcurrentUpdate() {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent update"));
}

void Table::CheckConflicts(const irs::DocRemoval& removal) const {
  const auto state = State();
  if (absl::c_any_of(removal.groups, [&](const auto& group) {
        return Conflicting(*state, group.segment, group.docs);
      })) {
    ThrowConcurrentUpdate();
  }
}

bool Table::Apply(Tick tick, TableCommit&& commit) {
  absl::MutexLock lock{&_state_mutex};
  Logged(commit.lsn);
  auto next = std::make_shared<TableState>(*Head().state);
  auto& units = next->units;
  const auto rewrite = [&](size_t i, auto&& change) {
    auto unit = std::make_shared<TableUnit>(*units[i]);
    change(*unit);
    units[i] = std::move(unit);
  };
  if (commit.truncate) {
    for (size_t i = 0; i < units.size(); ++i) {
      rewrite(i, [](TableUnit& unit) { DeleteAll(unit); });
    }
  }
  if (commit.piece || !commit.segments.empty()) {
    _last_append.store(tick, std::memory_order_release);
  }
  if (commit.piece || !commit.segments.empty() || !commit.removals.empty() ||
      commit.truncate) {
    _last_write.store(tick, std::memory_order_release);
  }
  if (commit.piece) {
    auto unit = std::make_shared<TableUnit>();
    unit->id = commit.piece->Id();
    unit->docs = commit.piece->Meta().docs_count;
    unit->segment.meta.name = commit.piece->Meta().name;
    unit->deleted = commit.piece->SharedDocsMask();
    unit->reader = std::move(commit.piece);
    unit->first_lsn = commit.lsn != 0 ? commit.lsn : kNoLsn;
    unit->piece = true;
    units.push_back(std::move(unit));
  }
  for (auto& segment : commit.segments) {
    auto unit = std::make_shared<TableUnit>();
    unit->id = segment.reader->Id();
    unit->docs = segment.reader->Meta().docs_count;
    unit->meta_ref =
      irs::directory_utils::Reference(_dir, segment.segment.filename);
    unit->deleted = segment.reader->SharedDocsMask();
    unit->segment = std::move(segment.segment);
    unit->reader = std::move(segment.reader);
    units.push_back(std::move(unit));
  }
  for (const auto& removal : commit.removals) {
    for (const auto& group : removal->groups) {
      if (const auto i = IndexOf(units, group.segment); i != units.size()) {
        rewrite(i, [&](TableUnit& unit) { AddDeletes(unit, group.docs); });
      }
    }
  }
  const bool emptied = LeaveDead(*next, tick);
  Publish(std::move(next), tick);
  return emptied;
}

std::shared_ptr<const TableState> Table::Replace(
  Tick tick, Lsn lsn, std::span<const uint64_t> replaced,
  std::vector<OwnSegment>&& segments, std::vector<irs::DocMap>&& maps) {
  std::vector<std::shared_ptr<TableUnit>> added;
  added.reserve(segments.size());
  for (auto& segment : segments) {
    auto unit = std::make_shared<TableUnit>();
    unit->id = segment.reader->Id();
    unit->docs = segment.reader->Meta().docs_count;
    unit->meta_ref =
      irs::directory_utils::Reference(_dir, segment.segment.filename);
    unit->deleted = segment.reader->SharedDocsMask();
    unit->segment = std::move(segment.segment);
    unit->reader = std::move(segment.reader);
    added.push_back(std::move(unit));
  }
  return ReplaceUnits(tick, lsn, replaced, std::move(added), std::move(maps));
}

std::shared_ptr<const TableState> Table::MergePieces(
  Tick tick, Lsn lsn, std::span<const uint64_t> replaced,
  std::shared_ptr<const irs::SegmentReaderImpl> merged, Lsn first_lsn,
  std::vector<irs::DocMap>&& maps) {
  auto unit = std::make_shared<TableUnit>();
  unit->id = merged->Id();
  unit->docs = merged->Meta().docs_count;
  unit->segment.meta.name = merged->Meta().name;
  unit->deleted = merged->SharedDocsMask();
  unit->reader = std::move(merged);
  unit->first_lsn = first_lsn;
  unit->piece = true;
  std::vector<std::shared_ptr<TableUnit>> added;
  added.push_back(std::move(unit));
  return ReplaceUnits(tick, lsn, replaced, std::move(added), std::move(maps));
}

std::shared_ptr<const TableState> Table::ReplaceUnits(
  Tick tick, Lsn lsn, std::span<const uint64_t> replaced,
  std::vector<std::shared_ptr<TableUnit>>&& added,
  std::vector<irs::DocMap>&& maps) {
  absl::MutexLock lock{&_state_mutex};
  Logged(lsn);
  auto next = std::make_shared<TableState>(*Head().state);
  auto& units = next->units;
  Mapped carried;
  for (const auto& map : maps) {
    const auto i = IndexOf(units, map.source);
    if (i == units.size()) {
      carried[map.target].AddRange(
        map.base,
        static_cast<irs::doc_id_t>(map.base + (map.rank_end - map.rank_begin)));
    } else if (const auto& deleted = units[i]->deleted) {
      irs::MapDocs(map, *deleted, carried[map.target]);
    }
  }
  auto moved = next->moved ? std::make_shared<MovedUnits>(*next->moved)
                           : std::make_shared<MovedUnits>();
  for (const auto id : replaced) {
    const auto i = IndexOf(units, id);
    if (i == units.size()) {
      continue;
    }
    MovedUnit source{.tick = tick};
    for (const auto& map : maps) {
      if (map.source == id) {
        source.maps.push_back(map);
      }
    }
    moved->insert_or_assign(
      id, std::make_shared<const MovedUnit>(std::move(source)));
    units.erase(units.begin() + i);
  }
  next->moved = std::move(moved);
  for (auto& unit : added) {
    if (const auto it = carried.find(unit->id);
        it != carried.end() && !it->second.Empty()) {
      irs::DocumentMaskBuilder mask;
      if (unit->deleted) {
        mask.Merge(*unit->deleted);
      }
      mask.Merge(it->second);
      SetDeletes(*unit, std::move(mask));
    }
    units.push_back(std::move(unit));
  }
  LeaveDead(*next, tick);
  Republish(next);
  return next;
}

void Table::Collect(Tick oldest) {
  absl::MutexLock lock{&_state_mutex};
  const auto& state = Head().state;
  const auto settled = [&](const auto& entry) {
    return entry.second->tick <= oldest;
  };
  if (!state->moved || absl::c_none_of(*state->moved, settled)) {
    return;
  }
  auto next = std::make_shared<TableState>(*state);
  auto moved = std::make_shared<MovedUnits>(*state->moved);
  absl::erase_if(*moved, settled);
  if (moved->empty()) {
    next->moved.reset();
  } else {
    next->moved = std::move(moved);
  }
  Republish(std::move(next));
}

bool Table::Unsettled() const {
  return absl::c_any_of(State()->units,
                        [](const auto& unit) { return unit->piece; });
}

std::vector<StateUnit> Table::StateUnits() const {
  const auto state = State();
  std::vector<StateUnit> units;
  units.reserve(state->units.size());
  for (const auto& unit : state->units) {
    if (unit->piece) {
      continue;
    }
    units.push_back({.segment = {.id = unit->id,
                                 .meta_file = unit->segment.filename,
                                 .docs = unit->docs},
                     .deleted = unit->deleted});
  }
  return units;
}

void Table::StateLogged(Lsn lsn) {
  absl::MutexLock lock{&_state_mutex};
  _needed = lsn;
}

Lsn Table::NeededFrom() const {
  absl::MutexLock lock{&_state_mutex};
  auto needed = _needed;
  for (const auto& unit : Head().state->units) {
    needed = std::min(needed, unit->first_lsn);
  }
  return needed;
}

void Table::ReplayRows(uint64_t id, irs::doc_id_t first, irs::doc_id_t count,
                       Lsn lsn) {
  Replayed(lsn);
  Saw(id);
  auto& unit = _replay[id];
  SDB_ENSURE(unit.meta_file.empty(), "search log: rows for written segment ",
             id);
  unit.pending = true;
  unit.docs = first + count;
  unit.first_lsn = std::min(unit.first_lsn, lsn);
}

bool Table::ReplayPending(uint64_t id) const {
  const auto it = _replay.find(id);
  return it != _replay.end() && it->second.pending;
}

void Table::ReplayPieceRows(
  irs::IndexWriter& writer, uint64_t id, irs::doc_id_t first,
  absl::FunctionRef<void(irs::IndexWriter::Transaction&)> append) {
  auto& unit = _replay[id];
  SDB_ASSERT(unit.pending);
  if (!unit.trx) {
    unit.trx.emplace(writer.GetBatch(/*exclusive_segment=*/true));
    unit.trx->SetHot(id);
  }
  const auto docs = PieceDocs(*unit.trx);
  SDB_ENSURE(docs == first, "search log: rows of segment ", id, " start at ",
             first, ", expected ", docs);
  append(*unit.trx);
}

void Table::ReplayAddSegment(const ReplacedSegment& segment, Lsn lsn) {
  Replayed(lsn);
  Saw(segment.id);
  auto& unit = _replay[segment.id];
  unit.meta_file = segment.meta_file;
  unit.docs = segment.docs;
}

void Table::ReplayRemoval(const irs::DocRemoval& removal, Lsn lsn) {
  Replayed(lsn);
  for (const auto& group : removal.groups) {
    Saw(group.segment);
    _replay[group.segment].removed.Merge(group.docs);
  }
}

void Table::ReplayTruncate(Lsn lsn) {
  Replayed(lsn);
  absl::erase_if(_replay,
                 [](const auto& entry) { return !entry.second.pending; });
  for (auto& [id, unit] : _replay) {
    if (unit.docs != 0) {
      unit.removed.AddRange(
        irs::doc_limits::min(),
        static_cast<irs::doc_id_t>(irs::doc_limits::min() + unit.docs));
    }
  }
}

void Table::ReplayReplace(std::span<const uint64_t> replaced,
                          std::span<const ReplacedSegment> segments,
                          std::span<const irs::DocMap> maps, Lsn lsn) {
  Replayed(lsn);
  absl::flat_hash_map<uint64_t, irs::DocumentMaskBuilder> carried;
  for (const auto& map : maps) {
    const auto it = _replay.find(map.source);
    if (it == _replay.end() ||
        (!it->second.pending && it->second.meta_file.empty())) {
      carried[map.target].AddRange(
        map.base,
        static_cast<irs::doc_id_t>(map.base + (map.rank_end - map.rank_begin)));
    } else if (!it->second.removed.Empty()) {
      const auto docs = irs::DocumentMaskBuilder{it->second.removed}.Finish();
      irs::MapDocs(map, docs, carried[map.target]);
    }
  }
  for (const auto id : replaced) {
    Saw(id);
    _replay.erase(id);
  }
  for (const auto& segment : segments) {
    Saw(segment.id);
    auto& unit = _replay[segment.id];
    unit.meta_file = segment.meta_file;
    unit.docs = segment.docs;
    if (const auto it = carried.find(segment.id); it != carried.end()) {
      unit.removed.Merge(it->second);
    }
  }
}

void Table::ReplayMerge(uint64_t target, std::span<const uint64_t> sources,
                        Lsn lsn) {
  Replayed(lsn);
  Saw(target);
  ReplayUnit merged;
  merged.pending = true;
  irs::doc_id_t offset = 0;
  for (const auto id : sources) {
    Saw(id);
    const auto it = _replay.find(id);
    SDB_ENSURE(it != _replay.end() && it->second.pending,
               "search log: merged piece ", id, " has no rows");
    auto& source = it->second;
    merged.first_lsn = std::min(merged.first_lsn, source.first_lsn);
    if (!source.removed.Empty()) {
      irs::DocumentMaskBuilder{source.removed}.Finish().ForEachRun(
        nullptr, [&](irs::doc_id_t first, irs::doc_id_t last) {
          merged.removed.AddRange(static_cast<irs::doc_id_t>(first + offset),
                                  static_cast<irs::doc_id_t>(last + offset));
          return true;
        });
    }
    offset = static_cast<irs::doc_id_t>(offset + source.docs);
    _replay.erase(it);
  }
  merged.docs = offset;
  _replay.insert_or_assign(target, std::move(merged));
}

void Table::ReplayState(std::span<const StateUnit> units, Lsn lsn) {
  _replay_needed = lsn;
  absl::erase_if(_replay,
                 [](const auto& entry) { return !entry.second.pending; });
  for (const auto& state : units) {
    Saw(state.segment.id);
    auto& unit = _replay[state.segment.id];
    unit.meta_file = state.segment.meta_file;
    unit.docs = state.segment.docs;
    unit.removed = irs::DocumentMaskBuilder{};
    if (state.deleted) {
      unit.removed.Merge(*state.deleted);
    }
  }
}

bool Table::FinishReplay() {
  absl::MutexLock lock{&_state_mutex};
  _needed = std::exchange(_replay_needed, kNoLsn);
  const bool replayed_records = _needed != kNoLsn;
  auto next = std::make_shared<TableState>(*Head().state);
  for (auto& [id, replayed] : _replay) {
    auto unit = std::make_shared<TableUnit>();
    unit->id = id;
    SDB_ENSURE(!replayed.pending || replayed.trx,
               "search log: rows of segment ", id, " were never inverted");
    if (replayed.trx) {
      const auto docs = PieceDocs(*replayed.trx);
      if (docs == 0) {
        replayed.trx->Abort();
        continue;
      }
      unit->reader = replayed.trx->SealPiece(id);
      replayed.trx->Abort();
      unit->first_lsn = replayed.first_lsn;
      unit->docs = docs;
      unit->segment.meta.name = unit->reader->Meta().name;
      unit->piece = true;
    } else {
      if (replayed.meta_file.empty() ||
          (replayed.docs != 0 && replayed.removed.Count() >= replayed.docs)) {
        continue;
      }
      auto opened = OpenSegment(std::move(replayed.meta_file));
      unit->meta_ref =
        irs::directory_utils::Reference(_dir, opened.segment.filename);
      unit->docs = opened.reader->Meta().docs_count;
      unit->segment = std::move(opened.segment);
      unit->reader = std::move(opened.reader);
    }
    unit->deleted = unit->reader->SharedDocsMask();
    if (const auto* base = unit->reader->docs_mask()) {
      replayed.removed.Merge(*base);
    }
    if (!replayed.removed.Empty()) {
      SetDeletes(*unit, std::move(replayed.removed));
    }
    if (!Dead(*unit)) {
      next->units.push_back(std::move(unit));
    }
  }
  _replay.clear();
  Republish(std::move(next));
  return replayed_records;
}

}  // namespace sdb::search
