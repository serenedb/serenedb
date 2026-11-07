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
#include <duckdb/common/allocator.hpp>
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

namespace sdb::search {
namespace {

using Units = std::vector<std::shared_ptr<const TableUnit>>;

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

irs::doc_id_t VisibleEnd(const TableUnit& unit, Tick snapshot) noexcept {
  if (unit.replaced_at != 0 && unit.replaced_at <= snapshot) {
    return 0;
  }
  if (unit.inserts.empty()) {
    return unit.docs;
  }
  irs::doc_id_t end = 0;
  for (const auto& range : unit.inserts) {
    if (range.tick > snapshot) {
      break;
    }
    end = range.end;
  }
  return end;
}

const irs::DocumentMask* DeletesAt(const TableUnit& unit,
                                   Tick snapshot) noexcept {
  const irs::DocumentMask* mask = nullptr;
  for (const auto& generation : unit.deletes) {
    if (generation.tick > snapshot) {
      break;
    }
    mask = generation.mask.get();
  }
  return mask;
}

Tick SeenAt(const TableState& state, Tick snapshot) noexcept {
  Tick seen = 0;
  const auto see = [&](Tick tick) {
    if (tick <= snapshot) {
      seen = std::max(seen, tick);
    }
  };
  for (const auto& unit : state.units) {
    if (unit->replaced_at != 0) {
      see(unit->replaced_at);
    }
    for (const auto& range : unit->inserts) {
      see(range.tick);
    }
    for (const auto& generation : unit->deletes) {
      see(generation.tick);
    }
  }
  return seen;
}

const irs::DocumentMask* LatestDeletes(const TableUnit& unit) noexcept {
  return unit.deletes.empty() ? nullptr : unit.deletes.back().mask.get();
}

void PushDeletes(TableUnit& unit, Tick tick, irs::DocumentMaskBuilder&& mask) {
  auto finished =
    std::make_shared<const irs::DocumentMask>(std::move(mask).Finish());
  if (!unit.deletes.empty() && unit.deletes.back().tick == tick) {
    unit.deletes.back().mask = std::move(finished);
    return;
  }
  unit.deletes.push_back({tick, std::move(finished)});
}

void AddDeletes(TableUnit& unit, Tick tick, const irs::DocumentMask& docs) {
  irs::DocumentMaskBuilder mask;
  if (const auto* latest = LatestDeletes(unit)) {
    mask.Merge(*latest);
  }
  mask.Merge(docs);
  PushDeletes(unit, tick, std::move(mask));
}

void DeleteAll(TableUnit& unit, Tick tick) {
  if (unit.docs == 0) {
    return;
  }
  irs::DocumentMaskBuilder mask;
  mask.AddRange(irs::doc_limits::min(),
                static_cast<irs::doc_id_t>(irs::doc_limits::min() + unit.docs));
  PushDeletes(unit, tick, std::move(mask));
}

void CollectStamps(TableUnit& unit, Tick oldest) {
  const auto seen_insert = std::find_if(
    unit.inserts.rbegin(), unit.inserts.rend(),
    [&](const InsertRange& range) { return range.tick <= oldest; });
  if (seen_insert != unit.inserts.rend()) {
    unit.inserts.erase(unit.inserts.begin(), seen_insert.base() - 1);
    unit.inserts.front().tick = 0;
    if (!unit.hot && unit.inserts.size() == 1 &&
        unit.inserts.front().end == unit.docs) {
      unit.inserts.clear();
    }
  }
  const auto seen_delete =
    std::find_if(unit.deletes.rbegin(), unit.deletes.rend(),
                 [&](const DeleteGeneration& generation) {
                   return generation.tick <= oldest;
                 });
  if (seen_delete != unit.deletes.rend()) {
    unit.deletes.erase(unit.deletes.begin(), seen_delete.base() - 1);
    unit.deletes.front().tick = 0;
  }
}

bool Collectable(const TableUnit& unit, Tick oldest) noexcept {
  const auto stale = [&](const auto& stamps) {
    return (stamps.size() > 1 && stamps[1].tick <= oldest) ||
           (!stamps.empty() && stamps.front().tick != 0 &&
            stamps.front().tick <= oldest);
  };
  return stale(unit.inserts) || stale(unit.deletes);
}

bool Gone(const TableUnit& unit, Tick oldest) noexcept {
  if (unit.replaced_at != 0) {
    return unit.released_at <= oldest;
  }
  if (unit.hot || !unit.inserts.empty() || unit.deletes.size() != 1 ||
      unit.deletes.front().tick != 0) {
    return false;
  }
  return unit.deletes.front().mask->Count() >= unit.docs;
}

void Sweep(Units& units, Tick oldest) {
  std::erase_if(units, [&](const auto& unit) { return Gone(*unit, oldest); });
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
            Tick snapshot, Mapped& out) {
  const auto* unit = FindUnit(state, id);
  if (!unit || unit->replaced_at == 0 || unit->replaced_at > snapshot) {
    out[id].Merge(docs);
    return;
  }
  for (const auto& map : unit->maps) {
    if (irs::DocumentMaskBuilder moved; irs::MapDocs(map, docs, moved)) {
      Follow(state, map.target, std::move(moved).Finish(), snapshot, out);
    }
  }
}

bool Intersects(const irs::DocumentMask& docs, const irs::DocumentMask* now,
                const irs::DocumentMask* seen) {
  if (now == nullptr || now == seen) {
    return false;
  }
  if (seen == nullptr) {
    return roaring::api::roaring_bitmap_intersect(&now->Bitmap(),
                                                  &docs.Bitmap());
  }
  auto* fresh =
    roaring::api::roaring_bitmap_andnot(&now->Bitmap(), &seen->Bitmap());
  const bool conflict =
    roaring::api::roaring_bitmap_intersect(fresh, &docs.Bitmap());
  roaring::api::roaring_bitmap_free(fresh);
  return conflict;
}

}  // namespace

irs::doc_id_t HotDocs(const irs::IndexWriter::Transaction& trx) noexcept {
  const auto* writer = trx.HotWriter();
  if (writer == nullptr || !writer->initialized()) {
    return 0;
  }
  return static_cast<irs::doc_id_t>(writer->buffered_docs());
}

uint64_t HotSegmentId(const irs::IndexWriter::Transaction& trx) {
  const auto* writer = trx.HotWriter();
  SDB_ASSERT(writer != nullptr && writer->initialized());
  uint64_t id = 0;
  [[maybe_unused]] const bool parsed = irs::ParseSegmentId(writer->name(), id);
  SDB_ASSERT(parsed);
  return id;
}

size_t HotContext::Memory() const noexcept {
  const auto* writer = _trx.HotWriter();
  if (writer == nullptr) {
    return 0;
  }
  return writer->memory_active() + writer->StagedColumnBytes();
}

std::shared_ptr<const irs::SegmentReaderImpl> HotContext::Build(
  duckdb::DatabaseInstance& db, irs::doc_id_t docs) {
  const auto* writer = _trx.HotWriter();
  SDB_ASSERT(writer != nullptr && writer->initialized());
  std::vector<std::unique_ptr<irs::ColumnReader>> readers;
  if (const auto* columns = writer->GetColWriter()) {
    for (auto& column : columns->Snapshot(docs)) {
      readers.push_back(
        irs::HotColumnReader::Make(db, std::move(column), &_stats));
    }
  }
  irs::SegmentMeta meta;
  meta.name = writer->name();
  meta.docs_count = docs;
  meta.live_docs_count = docs;
  return irs::SegmentReaderImpl::OpenInMemory(
    meta, Id(), std::make_unique<irs::ColReader>(db, std::move(readers)));
}

std::shared_ptr<const irs::SegmentReaderImpl> HotContext::Reader(
  duckdb::DatabaseInstance& db, irs::doc_id_t docs) {
  if (auto cached = std::atomic_load(&_cached);
      cached && cached->Meta().docs_count >= docs) {
    return cached;
  }
  absl::MutexLock lock{&_mutex};
  if (auto cached = std::atomic_load(&_cached);
      cached && cached->Meta().docs_count >= docs) {
    return cached;
  }
  auto built = Build(db, docs);
  std::atomic_store(&_cached, built);
  return built;
}

LiveUnits::LiveUnits(
  std::shared_ptr<const TableState> state,
  std::span<const std::shared_ptr<const irs::DocRemoval>> removals)
  : _state{std::move(state)} {
  for (const auto& removal : removals) {
    for (const auto& group : removal->groups) {
      Follow(*_state, group.segment, group.docs, kAllApplied, _removed);
    }
  }
  _units.reserve(_state->units.size());
  for (const auto& unit : _state->units) {
    const auto visible = VisibleEnd(*unit, kAllApplied);
    if (visible == 0) {
      continue;
    }
    const auto removed = _removed.find(unit->id);
    _units.push_back({
      .unit = unit.get(),
      .visible = visible,
      .deleted = DeletesAt(*unit, kAllApplied),
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
    _state{std::make_shared<const TableState>()} {}

void Table::Publish(std::shared_ptr<const TableState> state) {
  std::atomic_store(&_state, std::move(state));
  std::atomic_store(&_view, std::shared_ptr<const CachedView>{});
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

irs::DirectoryReader Table::Assemble(Tick snapshot, const OwnWrites& own,
                                     Scope scope) const {
  const auto state = State();
  if (scope != Scope::All || !own.removals.empty() || !own.segments.empty() ||
      own.truncated || (own.rows != nullptr && own.rows->Count() != 0)) {
    return Build(*state, snapshot, own, scope);
  }
  const auto seen = SeenAt(*state, snapshot);
  if (const auto cached = std::atomic_load(&_view);
      cached && cached->state == state && cached->seen == seen) {
    return cached->reader;
  }
  auto reader = Build(*state, snapshot, own, scope);
  if (State() == state) {
    std::atomic_store(
      &_view, std::make_shared<const CachedView>(
                CachedView{.state = state, .seen = seen, .reader = reader}));
  }
  return reader;
}

irs::DirectoryReader Table::Build(const TableState& state, Tick snapshot,
                                  const OwnWrites& own, Scope scope) const {
  auto& db = *_options.db;
  Mapped own_removed;
  for (const auto& removal : own.removals) {
    for (const auto& group : removal->groups) {
      Follow(state, group.segment, group.docs, snapshot, own_removed);
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
    [&](uint64_t id, const irs::DocumentMask* deleted, irs::doc_id_t visible,
        const std::shared_ptr<const irs::SegmentReaderImpl>& base) {
      const auto own_it = own_removed.find(id);
      const auto docs = base->Meta().docs_count;
      if (visible >= docs && own_it == own_removed.end() &&
          deleted == base->docs_mask()) {
        return base;
      }
      irs::DocumentMaskBuilder mask;
      if (deleted != nullptr) {
        mask.Merge(*deleted);
      }
      if (visible < docs) {
        mask.AddRange(
          static_cast<irs::doc_id_t>(irs::doc_limits::min() + visible),
          static_cast<irs::doc_id_t>(irs::doc_limits::min() + docs));
      }
      if (own_it != own_removed.end()) {
        mask.Merge(own_it->second);
      }
      return Masked(base, std::move(mask));
    };
  for (const auto& unit : state.units) {
    const auto visible = own.truncated ? 0 : VisibleEnd(*unit, snapshot);
    if (visible == 0 || (scope != Scope::All && unit->hot) ||
        (scope == Scope::Settled &&
         (visible != unit->docs || unit->replaced_at != 0))) {
      continue;
    }
    const auto base =
      unit->hot ? unit->hot->Reader(db, unit->docs) : unit->reader;
    add(unit->segment.filename,
        with_own(unit->id, DeletesAt(*unit, snapshot), visible, base));
  }
  for (const auto& segment : own.segments) {
    add(segment.segment.filename,
        with_own(segment.reader->Id(), segment.reader->docs_mask(),
                 segment.reader->Meta().docs_count, segment.reader));
  }
  if (own.rows != nullptr && own.rows->Count() != 0) {
    const auto reader =
      OwnRowsReader(db, own.rows_id, *own.rows, own.row_columns);
    add({}, with_own(own.rows_id, nullptr, reader->Meta().docs_count, reader));
  }
  return irs::DirectoryReader{std::make_shared<const irs::DirectoryReaderImpl>(
    _dir, _options, std::move(meta), std::move(readers))};
}

irs::doc_id_t Table::BufferedDocs(Tick snapshot) const {
  irs::doc_id_t docs = 0;
  for (const auto& unit : State()->units) {
    if (unit->hot) {
      docs += VisibleEnd(*unit, snapshot);
    }
  }
  return docs;
}

size_t Table::DataFiles(Tick snapshot) const {
  size_t files = 0;
  for (const auto& unit : State()->units) {
    if (!unit->hot && VisibleEnd(*unit, snapshot) != 0) {
      files += unit->segment.meta.files.size();
    }
  }
  return files;
}

std::shared_ptr<HotContext> Table::AcquireHot(irs::IndexWriter& writer) {
  absl::MutexLock lock{&_hot_mutex};
  while (!_free.empty()) {
    auto hot = std::move(_free.back());
    _free.pop_back();
    if (!_retired.contains(hot.get())) {
      _held.insert(hot.get());
      return hot;
    }
  }
  auto trx = writer.GetBatch(/*exclusive_segment=*/true);
  trx.SetHot(0);
  auto hot = std::make_shared<HotContext>(std::move(trx));
  _held.insert(hot.get());
  return hot;
}

void Table::ReleaseHot(const std::shared_ptr<HotContext>& hot, bool retire) {
  const auto state = State();
  const bool live = absl::c_any_of(
    state->units, [&](const auto& unit) { return unit->hot == hot; });
  absl::MutexLock lock{&_hot_mutex};
  _held.erase(hot.get());
  if (!live) {
    _retired.erase(hot.get());
    if (!retire) {
      _free.push_back(hot);
    }
    return;
  }
  if (retire) {
    _retired.insert(hot.get());
  }
  if (!_retired.contains(hot.get())) {
    _free.push_back(hot);
  }
}

void Table::Retire(Lsn before) {
  const auto state = State();
  absl::MutexLock lock{&_hot_mutex};
  if (before == kNoLsn) {
    for (const auto& hot : _free) {
      _retired.insert(hot.get());
    }
    _free.clear();
    for (const auto* hot : _held) {
      _retired.insert(hot);
    }
  }
  for (const auto& unit : state->units) {
    if (unit->hot && (before == kNoLsn || unit->first_lsn < before)) {
      _retired.insert(unit->hot.get());
    }
  }
}

std::shared_ptr<const irs::DocRemoval> Table::Resolve(
  const std::shared_ptr<const irs::DocRemoval>& removal) const {
  const auto state = State();
  const bool current = absl::c_all_of(removal->groups, [&](const auto& group) {
    const auto* unit = FindUnit(*state, group.segment);
    return !unit || unit->replaced_at == 0;
  });
  if (current) {
    return removal;
  }
  Mapped mapped;
  for (const auto& group : removal->groups) {
    Follow(*state, group.segment, group.docs, kAllApplied, mapped);
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

bool Table::Conflicts(Tick snapshot, const irs::DocRemoval& removal) const {
  const auto state = State();
  return absl::c_any_of(removal.groups, [&](const auto& group) {
    const auto* unit = FindUnit(*state, group.segment);
    return unit && Intersects(group.docs, LatestDeletes(*unit),
                              DeletesAt(*unit, snapshot));
  });
}

void Table::CheckConflicts(Tick snapshot,
                           const irs::DocRemoval& removal) const {
  if (Conflicts(snapshot, removal)) {
    ThrowConcurrentUpdate();
  }
}

void Table::Apply(Tick tick, Tick oldest, TableCommit&& commit) {
  absl::MutexLock lock{&_state_mutex};
  Logged(commit.lsn);
  auto next = std::make_shared<TableState>(*State());
  auto& units = next->units;
  const auto rewrite = [&](size_t i, auto&& change) {
    auto unit = std::make_shared<TableUnit>(*units[i]);
    change(*unit);
    CollectStamps(*unit, oldest);
    units[i] = std::move(unit);
  };
  if (commit.truncate) {
    for (size_t i = 0; i < units.size(); ++i) {
      if (units[i]->replaced_at == 0) {
        rewrite(i, [&](TableUnit& unit) { DeleteAll(unit, tick); });
      }
    }
  }
  if (commit.hot || !commit.segments.empty()) {
    _last_append.store(tick, std::memory_order_release);
  }
  if (commit.hot || !commit.segments.empty() || !commit.removals.empty() ||
      commit.truncate) {
    _last_write.store(tick, std::memory_order_release);
  }
  for (auto& segment : commit.segments) {
    auto unit = std::make_shared<TableUnit>();
    unit->id = segment.reader->Id();
    unit->docs = segment.reader->Meta().docs_count;
    unit->meta_ref =
      irs::directory_utils::Reference(_dir, segment.segment.filename);
    unit->inserts.push_back({unit->docs, tick});
    if (auto mask = segment.reader->SharedDocsMask()) {
      unit->deletes.push_back({tick, std::move(mask)});
    }
    unit->segment = std::move(segment.segment);
    unit->reader = std::move(segment.reader);
    CollectStamps(*unit, oldest);
    units.push_back(std::move(unit));
  }
  for (const auto& removal : commit.removals) {
    for (const auto& group : removal->groups) {
      if (const auto i = IndexOf(units, group.segment); i != units.size()) {
        rewrite(i,
                [&](TableUnit& unit) { AddDeletes(unit, tick, group.docs); });
      }
    }
  }
  if (commit.hot) {
    const auto i = IndexOf(units, commit.hot_id);
    if (i == units.size()) {
      auto unit = std::make_shared<TableUnit>();
      unit->id = commit.hot_id;
      unit->hot = commit.hot;
      unit->docs = commit.hot_end;
      unit->inserts.push_back({commit.hot_end, tick});
      unit->first_lsn = commit.lsn != 0 ? commit.lsn : kNoLsn;
      CollectStamps(*unit, oldest);
      units.push_back(std::move(unit));
    } else {
      SDB_ASSERT(units[i]->hot == commit.hot);
      rewrite(i, [&](TableUnit& unit) {
        unit.docs = commit.hot_end;
        unit.inserts.push_back({commit.hot_end, tick});
      });
    }
  }
  Sweep(units, oldest);
  Publish(std::move(next));
}

void Table::Replace(Tick visible, Tick tick, Tick oldest, Lsn lsn,
                    std::span<const uint64_t> replaced,
                    std::vector<OwnSegment>&& segments,
                    std::vector<irs::DocMap>&& maps) {
  absl::MutexLock lock{&_state_mutex};
  Logged(lsn);
  auto next = std::make_shared<TableState>(*State());
  auto& units = next->units;
  std::vector<Tick> ticks{visible};
  for (const auto& map : maps) {
    const auto i = IndexOf(units, map.source);
    if (i == units.size() || units[i]->replaced_at != 0) {
      continue;
    }
    for (const auto& generation : units[i]->deletes) {
      if (generation.tick > visible) {
        ticks.push_back(generation.tick);
      }
    }
  }
  absl::c_sort(ticks);
  ticks.erase(std::ranges::unique(ticks).begin(), ticks.end());
  std::vector<Mapped> carried(ticks.size());
  for (const auto& map : maps) {
    const auto i = IndexOf(units, map.source);
    const bool moved = i == units.size() || units[i]->replaced_at != 0;
    for (size_t k = 0; k < ticks.size(); ++k) {
      if (moved) {
        carried[k][map.target].AddRange(
          map.base, static_cast<irs::doc_id_t>(
                      map.base + (map.rank_end - map.rank_begin)));
      } else if (const auto* deleted = DeletesAt(*units[i], ticks[k])) {
        irs::MapDocs(map, *deleted, carried[k][map.target]);
      }
    }
  }
  for (const auto id : replaced) {
    const auto i = IndexOf(units, id);
    if (i == units.size() || units[i]->replaced_at != 0) {
      continue;
    }
    auto unit = std::make_shared<TableUnit>(*units[i]);
    unit->replaced_at = visible;
    unit->released_at = tick;
    for (const auto& map : maps) {
      if (map.source == id) {
        unit->maps.push_back(map);
      }
    }
    units[i] = std::move(unit);
  }
  for (auto& segment : segments) {
    auto unit = std::make_shared<TableUnit>();
    unit->id = segment.reader->Id();
    unit->docs = segment.reader->Meta().docs_count;
    unit->meta_ref =
      irs::directory_utils::Reference(_dir, segment.segment.filename);
    unit->inserts.push_back({unit->docs, visible});
    for (size_t k = 0; k < ticks.size(); ++k) {
      irs::DocumentMaskBuilder mask;
      if (const auto* base = segment.reader->docs_mask()) {
        mask.Merge(*base);
      }
      if (const auto it = carried[k].find(unit->id); it != carried[k].end()) {
        mask.Merge(it->second);
      }
      if (!mask.Empty()) {
        PushDeletes(*unit, ticks[k], std::move(mask));
      }
    }
    unit->segment = std::move(segment.segment);
    unit->reader = std::move(segment.reader);
    CollectStamps(*unit, oldest);
    units.push_back(std::move(unit));
  }
  Sweep(units, oldest);
  Publish(std::move(next));
}

void Table::Collect(Tick oldest) {
  absl::MutexLock lock{&_state_mutex};
  const auto state = State();
  auto next = std::make_shared<TableState>();
  next->units.reserve(state->units.size());
  bool changed = false;
  for (const auto& unit : state->units) {
    if (!Collectable(*unit, oldest)) {
      next->units.push_back(unit);
      continue;
    }
    auto collected = std::make_shared<TableUnit>(*unit);
    CollectStamps(*collected, oldest);
    next->units.push_back(std::move(collected));
    changed = true;
  }
  const auto before = next->units.size();
  Sweep(next->units, oldest);
  if (changed || next->units.size() != before) {
    Publish(std::move(next));
  }
}

std::vector<std::shared_ptr<HotContext>> Table::TakeCoolable(
  size_t memory_limit, bool all) {
  const auto state = State();
  std::vector<std::shared_ptr<HotContext>> taken;
  absl::MutexLock lock{&_hot_mutex};
  for (const auto& unit : state->units) {
    const auto& hot = unit->hot;
    if (!hot || _held.contains(hot.get())) {
      continue;
    }
    bool due = all || _retired.contains(hot.get());
    if (!due && memory_limit != 0) {
      absl::MutexLock hot_lock{&hot->Mutex()};
      due = hot->Memory() >= memory_limit;
    }
    if (!due) {
      continue;
    }
    std::erase(_free, hot);
    _retired.insert(hot.get());
    _held.insert(hot.get());
    taken.push_back(hot);
  }
  return taken;
}

bool Table::HasRetiredHot() const {
  const auto state = State();
  absl::MutexLock lock{&_hot_mutex};
  for (const auto& unit : state->units) {
    if (unit->hot && _retired.contains(unit->hot.get())) {
      return true;
    }
  }
  return false;
}

bool Table::RetiredIdle() const {
  for (const auto* hot : _held) {
    if (_retired.contains(hot)) {
      return false;
    }
  }
  return true;
}

void Table::AwaitRetiredReleased() {
  absl::MutexLock lock{&_hot_mutex};
  _hot_mutex.Await(absl::Condition{this, &Table::RetiredIdle});
}

void Table::Cooled(const std::shared_ptr<HotContext>& hot, OwnSegment&& written,
                   Lsn lsn) {
  {
    absl::MutexLock lock{&_state_mutex};
    Logged(lsn);
    auto next = std::make_shared<TableState>(*State());
    auto& units = next->units;
    const auto it = absl::c_find_if(
      units, [&](const auto& unit) { return unit->hot == hot; });
    SDB_ASSERT(it != units.end());
    if (!written.reader) {
      units.erase(it);
      Publish(std::move(next));
    } else {
      auto unit = std::make_shared<TableUnit>(**it);
      unit->hot.reset();
      unit->first_lsn = kNoLsn;
      unit->meta_ref =
        irs::directory_utils::Reference(_dir, written.segment.filename);
      unit->docs = written.reader->Meta().docs_count;
      if (const auto* base = written.reader->docs_mask()) {
        for (auto& generation : unit->deletes) {
          irs::DocumentMaskBuilder mask{*base};
          mask.Merge(*generation.mask);
          generation.mask =
            std::make_shared<const irs::DocumentMask>(std::move(mask).Finish());
        }
        if (unit->deletes.empty()) {
          unit->deletes.push_back({0, written.reader->SharedDocsMask()});
        }
      }
      if (unit->inserts.size() == 1 && unit->inserts.front().tick == 0 &&
          unit->inserts.front().end == unit->docs) {
        unit->inserts.clear();
      }
      unit->segment = std::move(written.segment);
      unit->reader = std::move(written.reader);
      *it = std::move(unit);
      Publish(std::move(next));
    }
  }
  absl::MutexLock lock{&_hot_mutex};
  _held.erase(hot.get());
  _retired.erase(hot.get());
}

std::vector<StateUnit> Table::StateUnits() const {
  const auto state = State();
  std::vector<StateUnit> units;
  units.reserve(state->units.size());
  for (const auto& unit : state->units) {
    if (unit->hot || unit->replaced_at != 0) {
      continue;
    }
    units.push_back(
      {.segment = {.id = unit->id,
                   .meta_file = unit->segment.filename,
                   .docs = unit->docs},
       .deleted = unit->deletes.empty() ? nullptr : unit->deletes.back().mask});
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
  for (const auto& unit : State()->units) {
    if (unit->hot) {
      needed = std::min(needed, unit->first_lsn);
    }
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

void Table::ReplayHotRows(
  irs::IndexWriter& writer, uint64_t id, irs::doc_id_t first,
  absl::FunctionRef<void(irs::IndexWriter::Transaction&, HotKeys&)> append) {
  auto& unit = _replay[id];
  SDB_ASSERT(unit.pending);
  if (!unit.hot) {
    auto trx = writer.GetBatch(/*exclusive_segment=*/true);
    trx.SetHot(id);
    unit.hot = std::make_shared<HotContext>(std::move(trx));
  }
  absl::MutexLock lock{&unit.hot->Mutex()};
  const auto docs = unit.hot->Docs();
  SDB_ENSURE(docs == first, "search log: rows of segment ", id, " start at ",
             first, ", expected ", docs);
  append(unit.hot->Trx(), unit.hot->Keys());
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

void Table::ReplayCooled(const ReplacedSegment& segment, Lsn lsn) {
  Replayed(lsn);
  Saw(segment.id);
  auto& unit = _replay[segment.id];
  unit.hot.reset();
  unit.pending = false;
  unit.first_lsn = kNoLsn;
  unit.meta_file = segment.meta_file;
  unit.docs = segment.docs;
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

void Table::FinishReplay() {
  absl::MutexLock lock{&_state_mutex};
  _needed = std::exchange(_replay_needed, kNoLsn);
  auto next = std::make_shared<TableState>(*State());
  for (auto& [id, replayed] : _replay) {
    auto unit = std::make_shared<TableUnit>();
    unit->id = id;
    SDB_ENSURE(!replayed.pending || replayed.hot,
               "search log: rows of segment ", id, " were never inverted");
    if (replayed.hot) {
      unit->first_lsn = replayed.first_lsn;
      irs::doc_id_t docs = 0;
      {
        absl::MutexLock hot_lock{&replayed.hot->Mutex()};
        docs = replayed.hot->Docs();
      }
      if (docs == 0) {
        continue;
      }
      unit->hot = replayed.hot;
      unit->docs = docs;
      unit->inserts.push_back({docs, 0});
      absl::MutexLock hot_lock{&_hot_mutex};
      _free.push_back(replayed.hot);
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
      if (const auto* base = unit->reader->docs_mask()) {
        replayed.removed.Merge(*base);
      }
    }
    if (!replayed.removed.Empty()) {
      PushDeletes(*unit, 0, std::move(replayed.removed));
    }
    if (!Gone(*unit, 0)) {
      next->units.push_back(std::move(unit));
    }
  }
  _replay.clear();
  Publish(std::move(next));
}

}  // namespace sdb::search
