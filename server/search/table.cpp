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
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/docs_mask/docs_mask.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/segment_writer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "catalog/transaction.h"
#include "search/version_chain.tpp"

namespace sdb::search {
namespace {

using Units = std::vector<std::shared_ptr<const TableUnit>>;

size_t PieceTier(irs::doc_id_t docs) noexcept {
  SDB_ASSERT(docs != 0);
  return static_cast<size_t>(std::bit_width(docs) - 1) / kPieceTierBits;
}

bool Adjacent(const TableUnit& lhs, const TableUnit& rhs) noexcept {
  return lhs.first + lhs.docs == rhs.first;
}

bool Mergeable(const TableUnit& piece) noexcept {
  return piece.docs < irs::doc_limits::kMaxDocs / (2 * kPieceFanIn);
}

class PieceRuns {
 public:
  bool Next(const TableUnit& piece) noexcept {
    const auto* previous = std::exchange(_previous, &piece);
    if (!previous || !Adjacent(*previous, piece) || !Mergeable(*previous) ||
        !Mergeable(piece)) {
      _same = 1;
      return false;
    }
    const auto tier = PieceTier(piece.docs);
    const auto before = PieceTier(previous->docs);
    if (tier > before) {
      return true;
    }
    _same = tier == before ? _same + 1 : 1;
    return _same == kPieceFanIn;
  }

 private:
  const TableUnit* _previous = nullptr;
  uint32_t _same = 0;
};

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

void SetDeletes(TableUnit& unit, irs::DocumentMask&& mask) {
  unit.deleted = std::make_shared<const irs::DocumentMask>(std::move(mask));
  unit.deleted_bytes = unit.deleted->ByteSize();
  if (unit.reader) {
    unit.reader = unit.reader->WithDocsMask(unit.deleted);
  }
}

void AddDeletes(TableUnit& unit, const irs::DocumentMask& docs) {
  SetDeletes(unit, unit.deleted ? irs::DocumentMask::Union(unit.deleted, docs)
                                : irs::DocumentMaskBuilder{docs}.Finish());
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
    duckdb::DataChunk chunk;
    rows.InitializeScanChunk(duckdb::Allocator::DefaultAllocator(), chunk);
    rows.FetchChunk(c, chunk);
    for (size_t i = 0; i < staged.size(); ++i) {
      staged[i].chunks.push_back(
        irs::WriteChunk{duckdb::Vector::Ref(chunk.data[i]), chunk.size()});
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

bool PatchedAfter(const TableUnit& unit, const irs::DocumentMask& docs,
                  Tick snapshot) {
  for (const auto& [field, patch] : unit.patches) {
    if (patch->MaxTick() <= snapshot) {
      continue;
    }
    bool found = false;
    if (docs.Count() < patch->Count()) {
      const auto newer = [&](uint64_t, const irs::ColumnPatch::Chunk& chunk,
                             size_t i) {
        found = found || chunk.ticks[i] > snapshot;
      };
      irs::DocumentMask::Cursor cursor{docs};
      std::array<irs::MaskRun, 64> runs;
      while (const auto count = cursor.ReadRuns(runs)) {
        for (const auto& run : std::span{runs}.first(count)) {
          patch->ForEach(run.min - irs::doc_limits::min(),
                         run.max - irs::doc_limits::min() + 1, newer);
          if (found) {
            return true;
          }
        }
      }
    } else {
      auto mask = irs::MakeGenericDocsMask(&docs);
      patch->ForEach(
        0, unit.docs,
        [&](uint64_t row, const irs::ColumnPatch::Chunk& chunk, size_t i) {
          found = found || (chunk.ticks[i] > snapshot &&
                            mask.Test(static_cast<irs::doc_id_t>(
                              row + irs::doc_limits::min())));
        });
      if (found) {
        return true;
      }
    }
  }
  return false;
}

bool Conflicting(const TableState& state, uint64_t id,
                 const irs::DocumentMask& docs, Tick snapshot) {
  if (const auto* unit = FindUnit(state, id)) {
    return (unit->deleted && docs.Intersects(*unit->deleted)) ||
           PatchedAfter(*unit, docs, snapshot);
  }
  const auto* moved = FindMoved(state, id);
  if (!moved) {
    return false;
  }
  if (moved->maps.empty()) {
    return true;
  }
  return absl::c_any_of(moved->maps, [&](const irs::DocMap& map) {
    if (map.mask && docs.Intersects(*map.mask)) {
      return true;
    }
    irs::DocumentMaskBuilder mapped;
    return irs::MapDocs(map, docs, mapped) &&
           Conflicting(state, map.target, std::move(mapped).Finish(), snapshot);
  });
}

void MergeField(std::vector<irs::FieldPatch>& fields, irs::field_id field,
                const duckdb::LogicalType& type, std::span<const uint64_t> rows,
                const duckdb::Vector& values, std::span<const uint64_t> ticks) {
  const auto it = absl::c_lower_bound(
    fields, field, [](const irs::FieldPatch& entry, irs::field_id id) {
      return entry.field < id;
    });
  if (it != fields.end() && it->field == field) {
    it->patch =
      irs::ColumnPatch::Merge(it->patch.get(), type, rows, values, ticks);
  } else {
    fields.insert(it, {.field = field,
                       .patch = irs::ColumnPatch::Merge(nullptr, type, rows,
                                                        values, ticks)});
  }
}

void FollowPatch(const TableState& state, uint64_t id, irs::field_id field,
                 const duckdb::LogicalType& type,
                 std::span<const uint64_t> rows, const duckdb::Vector& values,
                 std::vector<UnitPatch>& out) {
  const auto* moved = FindUnit(state, id) ? nullptr : FindMoved(state, id);
  if (!moved) {
    AddUnitPatch(out, id, field, type, rows, values);
    return;
  }
  std::vector<irs::MappedRow> mapped;
  for (const auto& map : moved->maps) {
    mapped.clear();
    irs::MapRows(map, rows, mapped);
    if (mapped.empty()) {
      continue;
    }
    std::vector<uint64_t> target_rows;
    target_rows.reserve(mapped.size());
    duckdb::SelectionVector sel{mapped.size()};
    for (size_t k = 0; k < mapped.size(); ++k) {
      target_rows.push_back(mapped[k].row);
      sel.set_index(k, mapped[k].index);
    }
    const duckdb::Vector picked{values, sel, mapped.size()};
    FollowPatch(state, map.target, field, type, target_rows, picked, out);
  }
}

bool PatchesCurrent(const TableState& state,
                    std::span<const UnitPatch> patches) {
  return absl::c_all_of(patches, [&](const UnitPatch& patch) {
    return FindUnit(state, patch.unit) || !FindMoved(state, patch.unit);
  });
}

std::vector<UnitPatch> FollowPatches(const TableState& state,
                                     std::span<const UnitPatch> patches) {
  std::vector<UnitPatch> followed;
  for (const auto& patch : patches) {
    for (const auto& [field, column] : patch.fields) {
      std::vector<uint64_t> rows;
      std::vector<uint64_t> ticks;
      duckdb::Vector values{column->Type(), column->Count()};
      column->Entries(rows, values, ticks);
      FollowPatch(state, patch.unit, field, column->Type(), rows, values,
                  followed);
    }
  }
  return followed;
}

template<typename Keep>
void CarryPatch(irs::field_id field, const irs::ColumnPatch& patch,
                const irs::DocMap& map, const Keep& keep,
                std::vector<irs::FieldPatch>& out) {
  std::vector<uint64_t> rows;
  std::vector<uint64_t> ticks;
  duckdb::Vector values{patch.Type(), patch.Count()};
  patch.Entries(rows, values, ticks);
  std::vector<uint64_t> kept_rows;
  std::vector<uint64_t> kept_ticks;
  duckdb::SelectionVector kept_sel{rows.size()};
  for (size_t k = 0; k < rows.size(); ++k) {
    if (keep(ticks[k])) {
      kept_sel.set_index(kept_rows.size(), k);
      kept_rows.push_back(rows[k]);
      kept_ticks.push_back(ticks[k]);
    }
  }
  std::vector<irs::MappedRow> mapped;
  irs::MapRows(map, kept_rows, mapped);
  if (mapped.empty()) {
    return;
  }
  const duckdb::Vector kept{values, kept_sel, kept_rows.size()};
  std::vector<uint64_t> target_rows;
  std::vector<uint64_t> target_ticks;
  duckdb::SelectionVector sel{mapped.size()};
  for (size_t k = 0; k < mapped.size(); ++k) {
    target_rows.push_back(mapped[k].row);
    target_ticks.push_back(kept_ticks[mapped[k].index]);
    sel.set_index(k, mapped[k].index);
  }
  MergeField(out, field, patch.Type(), target_rows,
             duckdb::Vector{kept, sel, mapped.size()}, target_ticks);
}

void ShiftPatches(std::span<const irs::FieldPatch> patches, uint64_t offset,
                  std::vector<irs::FieldPatch>& out) {
  for (const auto& [field, patch] : patches) {
    std::vector<uint64_t> rows;
    std::vector<uint64_t> ticks;
    duckdb::Vector values{patch->Type(), patch->Count()};
    patch->Entries(rows, values, ticks);
    for (auto& row : rows) {
      row += offset;
    }
    MergeField(out, field, patch->Type(), rows, values, ticks);
  }
}

void ApplyPatches(TableUnit& unit, std::span<const irs::FieldPatch> added,
                  Tick tick) {
  MergePatches(unit.patches, added, tick);
  unit.reader = unit.reader->WithPatches(unit.patches);
}

std::shared_ptr<const TableState> InitialState(
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  auto initial = std::make_shared<TableState>();
  initial->config = std::move(config);
  return initial;
}

bool DeletesAll(const irs::DocumentMask* deleted, const irs::DocumentMask& docs,
                irs::doc_id_t rows) {
  auto count = docs.Count();
  if (deleted) {
    count += deleted->Count() - deleted->AndCount(docs);
  }
  return count >= rows;
}

}  // namespace

void MergePatches(std::vector<irs::FieldPatch>& patches,
                  std::span<const irs::FieldPatch> added, Tick tick) {
  for (const auto& [field, patch] : added) {
    const auto it = absl::c_lower_bound(
      patches, field, [](const irs::FieldPatch& entry, irs::field_id id) {
        return entry.field < id;
      });
    if (it != patches.end() && it->field == field) {
      it->patch = irs::ColumnPatch::Merge(it->patch.get(), *patch, tick);
    } else {
      patches.insert(it,
                     {.field = field,
                      .patch = irs::ColumnPatch::Merge(nullptr, *patch, tick)});
    }
  }
}

void SlicePatches(std::span<const irs::FieldPatch> patches, uint64_t first,
                  uint64_t end, std::vector<irs::FieldPatch>& out) {
  for (const auto& [field, patch] : patches) {
    if (!patch->Overlaps(first, end)) {
      continue;
    }
    std::vector<uint64_t> rows;
    std::vector<uint64_t> ticks;
    duckdb::Vector values{patch->Type(), patch->Count()};
    patch->Entries(rows, values, ticks);
    const auto begin = absl::c_lower_bound(rows, first) - rows.begin();
    const auto stop = absl::c_lower_bound(rows, end) - rows.begin();
    if (begin == stop) {
      continue;
    }
    std::vector<uint64_t> sliced_rows;
    std::vector<uint64_t> sliced_ticks;
    duckdb::SelectionVector sel{static_cast<duckdb::idx_t>(stop - begin)};
    for (auto i = begin; i < stop; ++i) {
      sel.set_index(sliced_rows.size(), i);
      sliced_rows.push_back(rows[i] - first);
      sliced_ticks.push_back(ticks[i]);
    }
    MergeField(out, field, patch->Type(), sliced_rows,
               duckdb::Vector{values, sel, sliced_rows.size()}, sliced_ticks);
  }
}

bool Empty(const TableDelta& delta) noexcept {
  return delta.drop.empty() && delta.drop_memory == 0 && delta.add.empty() &&
         delta.rows.empty() && delta.deletes.empty() && !delta.memory_deletes &&
         delta.updates.empty() && delta.memory_updates.empty() &&
         !delta.manifest;
}

void MemoryDeletes::Finish(TableDelta& delta) && {
  if (_pieces.empty()) {
    return;
  }
  const auto base =
    absl::c_min_element(_pieces, [](const Piece& lhs, const Piece& rhs) {
      return lhs.first < rhs.first;
    })->first;
  irs::DocumentMaskBuilder docs;
  for (const auto& piece : _pieces) {
    const auto offset = piece.first - base;
    SDB_ENSURE(
      offset + piece.docs->Max() < std::numeric_limits<irs::doc_id_t>::max(),
      "the in-memory rows of a table span more rows than a mask "
      "can number");
    docs.AddShifted(*piece.docs, static_cast<irs::doc_id_t>(offset));
  }
  delta.memory_base = base;
  delta.memory_deletes = std::make_shared<const irs::DocumentMask>(
    std::move(docs).Finish(irs::DocumentMaskBuilder::kCanonical));
}

std::shared_ptr<TableUnit> PieceUnit(
  std::shared_ptr<const irs::SegmentReaderImpl> reader, uint64_t first) {
  auto unit = std::make_shared<TableUnit>();
  unit->id = reader->Id();
  unit->docs = reader->Meta().docs_count;
  unit->meta.name = reader->Meta().name;
  unit->deleted = reader->SharedDocsMask();
  unit->deleted_bytes = unit->deleted ? unit->deleted->ByteSize() : 0;
  unit->reader = std::move(reader);
  unit->first = first;
  unit->piece = true;
  return unit;
}

void AddUnitPatch(std::vector<UnitPatch>& patches, uint64_t unit,
                  irs::field_id field, const duckdb::LogicalType& type,
                  std::span<const uint64_t> rows,
                  const duckdb::Vector& values) {
  auto it = absl::c_find_if(
    patches, [&](const UnitPatch& entry) { return entry.unit == unit; });
  if (it == patches.end()) {
    it = patches.insert(patches.end(), UnitPatch{.unit = unit});
  }
  const uint64_t own = 0;
  MergeField(it->fields, field, type, rows, values, {&own, 1});
}

irs::DocumentMask PatchedDocs(const UnitPatch& patch) {
  irs::DocumentMaskBuilder docs;
  std::vector<irs::doc_id_t> rows;
  for (const auto& [field, column] : patch.fields) {
    rows.clear();
    column->ForEach(0, std::numeric_limits<uint64_t>::max(),
                    [&](uint64_t row, const irs::ColumnPatch::Chunk&, size_t) {
                      rows.push_back(static_cast<irs::doc_id_t>(
                        row + irs::doc_limits::min()));
                    });
    docs.AddSorted(rows);
  }
  return std::move(docs).Finish(irs::DocumentMaskBuilder::kCanonical);
}

void MergeUnitPatches(std::vector<UnitPatch>& into,
                      std::vector<UnitPatch>&& added) {
  for (auto& patch : added) {
    const auto it = absl::c_find_if(
      into, [&](const UnitPatch& entry) { return entry.unit == patch.unit; });
    if (it == into.end()) {
      into.push_back(std::move(patch));
    } else {
      MergePatches(it->fields, patch.fields, 0);
    }
  }
}

void CarryUnitPatches(std::vector<UnitPatch>& patches, uint64_t source,
                      std::span<const irs::DocMap> maps) {
  const auto it = absl::c_find_if(
    patches, [&](const UnitPatch& entry) { return entry.unit == source; });
  if (it == patches.end()) {
    return;
  }
  const auto fields = std::move(it->fields);
  patches.erase(it);
  std::vector<UnitPatch> carried;
  for (const auto& map : maps) {
    if (map.source != source) {
      continue;
    }
    UnitPatch target{.unit = map.target};
    for (const auto& [field, patch] : fields) {
      CarryPatch(
        field, *patch, map, [](uint64_t) { return true; }, target.fields);
    }
    if (!target.fields.empty()) {
      carried.push_back(std::move(target));
    }
  }
  MergeUnitPatches(patches, std::move(carried));
}

size_t VersionBytes(const TableState& state) noexcept {
  return sizeof(TableState) +
         state.units.size() * sizeof(std::shared_ptr<const TableUnit>);
}

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
  Mapped removed;
  for (const auto& removal : removals) {
    for (const auto& group : removal->groups) {
      Follow(*_state, group.segment, group.docs, removed);
    }
  }
  _removed.reserve(removed.size());
  for (auto& [id, docs] : removed) {
    _removed.emplace(id, std::move(docs).Finish());
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
  absl::c_sort(_units, [](const Unit& lhs, const Unit& rhs) {
    return lhs.unit->id < rhs.unit->id;
  });
}

const LiveUnits::Unit* LiveUnits::Find(uint64_t id) const noexcept {
  const auto it = absl::c_lower_bound(
    _units, id,
    [](const Unit& unit, uint64_t key) { return unit.unit->id < key; });
  return it != _units.end() && it->unit->id == id ? &*it : nullptr;
}

Table::Table(irs::Directory& dir, irs::IndexReaderOptions options,
             std::shared_ptr<const catalog::InvertedIndexConfig> config)
  : _dir{dir},
    _options{std::move(options)},
    _config{config},
    _versions{InitialState(std::move(config))} {}

Table::~Table() = default;

std::shared_ptr<const TableState> Table::State() const {
  return _versions.Latest(catalog::Transactions::Get().SharedWalks());
}

void Table::Publish(std::shared_ptr<TableState> state, Tick tick) {
  state->tick = tick;
  state->config = _config;
  CountPieces(*state);
  state->reader = BuildReader(*Head().state, *state);
  _versions.Publish(std::move(state), tick);
}

irs::DirectoryReader Table::BuildReader(const TableState& prev,
                                        const TableState& next) const {
  std::vector<irs::SegmentReader> readers;
  readers.reserve(next.units.size());
  for (const auto& unit : next.units) {
    readers.emplace_back(unit->reader);
  }
  if (prev.units.empty()) {
    return {_dir, _options, std::move(readers), /*stats=*/true};
  }
  irs::containers::FlatHashMap<uint64_t, const TableUnit*> before;
  before.reserve(prev.units.size());
  for (const auto& unit : prev.units) {
    before.emplace(unit->id, unit.get());
  }
  std::vector<irs::SegmentReader> added;
  std::vector<irs::SegmentReader> patched;
  for (const auto& unit : next.units) {
    const auto it = before.find(unit->id);
    if (it == before.end()) {
      added.emplace_back(unit->reader);
      continue;
    }
    if (it->second->reader->PatchTick() != unit->reader->PatchTick()) {
      patched.emplace_back(unit->reader);
    }
    before.erase(it);
  }
  std::vector<irs::SegmentReader> removed;
  removed.reserve(before.size());
  for (const auto& [id, unit] : before) {
    removed.emplace_back(unit->reader);
  }
  return {prev.reader, std::move(readers), added, patched, removed};
}

void Table::CountPieces(const TableState& state) noexcept {
  uint64_t bytes = 0;
  uint64_t dirty = 0;
  PieceRuns runs;
  bool run = false;
  for (const auto& unit : state.units) {
    uint64_t inline_bytes = unit->deleted_bytes;
    for (const auto& patch : unit->patches) {
      inline_bytes += patch.patch->Bytes();
    }
    if (unit->piece) {
      bytes += unit->reader->Meta().byte_size + inline_bytes;
      run = runs.Next(*unit) || run;
    } else {
      dirty += inline_bytes;
    }
  }
  _piece_bytes.store(bytes, std::memory_order_relaxed);
  _dirty_bytes.store(dirty, std::memory_order_relaxed);
  _merge_run.store(run, std::memory_order_relaxed);
}

std::shared_ptr<TableState> Table::SameUnits() const {
  return std::make_shared<TableState>(*Head().state);
}

OwnSegment Table::OpenSegment(uint64_t id, uint8_t kinds,
                              irs::doc_id_t rows) const {
  OwnSegment opened;
  auto& meta = opened.meta;
  meta.name = irs::FileName(id);
  meta.kinds = kinds;
  meta.docs_count = rows;
  meta.live_docs_count = rows;
  for (const auto& file : irs::SegmentFiles(meta.name, kinds)) {
    uint64_t size = 0;
    if (!_dir.length(size, file)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FILE),
                      ERR_MSG("segment file '", file, "' is missing"));
    }
    meta.byte_size += size;
  }
  opened.reader = irs::SegmentReaderImpl::Open(_dir, meta, _options);
  return opened;
}

irs::DirectoryReader Table::View(const TableState& state,
                                 const OwnWrites& own) const {
  SDB_ASSERT(HasOwn(own));
  return Build(state, own, Scope::All);
}

TableRead Table::Read(const OwnWrites& own) const {
  TableRead read{.state = State()};
  if (HasOwn(own)) {
    read.own.emplace(Build(*read.state, own, Scope::All));
  }
  return read;
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
  std::vector<irs::SegmentReader> readers;
  readers.reserve(state.units.size() + own.segments.size() + 1);
  const auto add = [&](std::shared_ptr<const irs::SegmentReaderImpl> reader) {
    readers.emplace_back(std::move(reader));
  };
  const auto with_own =
    [&](uint64_t id, const std::shared_ptr<const irs::DocumentMask>& deleted,
        const std::shared_ptr<const irs::SegmentReaderImpl>& base) {
      const auto own_it = own_removed.find(id);
      if (own_it == own_removed.end() && deleted.get() == base->docs_mask()) {
        return base;
      }
      if (deleted && own_it != own_removed.end()) {
        return base->WithDocsMask(
          std::make_shared<const irs::DocumentMask>(irs::DocumentMask::Union(
            deleted, std::move(own_it->second).Finish())));
      }
      irs::DocumentMaskBuilder mask;
      if (deleted) {
        mask.Merge(*deleted);
      }
      if (own_it != own_removed.end()) {
        mask.Merge(own_it->second);
      }
      return Masked(base, std::move(mask));
    };
  std::vector<irs::SegmentReader> added;
  std::vector<irs::SegmentReader> patched;
  std::vector<UnitPatch> followed;
  auto own_patches = own.patches;
  if (!PatchesCurrent(state, own_patches)) {
    followed = FollowPatches(state, own_patches);
    own_patches = followed;
  }
  const auto with_patches =
    [&](uint64_t id, std::span<const irs::FieldPatch> committed,
        std::shared_ptr<const irs::SegmentReaderImpl> reader) {
      const auto it = absl::c_find_if(
        own_patches, [&](const UnitPatch& patch) { return patch.unit == id; });
      if (it == own_patches.end()) {
        return reader;
      }
      std::vector<irs::FieldPatch> fields{committed.begin(), committed.end()};
      MergePatches(fields, it->fields, 0);
      auto patched_reader = reader->WithPatches(fields);
      patched.emplace_back(patched_reader);
      return patched_reader;
    };
  for (const auto& unit : state.units) {
    if (scope == Scope::Cold && unit->piece) {
      continue;
    }
    add(with_patches(unit->id, unit->patches,
                     with_own(unit->id, unit->deleted, unit->reader)));
  }
  for (const auto& segment : own.segments) {
    const auto id = segment.reader->Id();
    auto reader = with_patches(
      id, {}, with_own(id, segment.reader->SharedDocsMask(), segment.reader));
    added.emplace_back(reader);
    add(std::move(reader));
  }
  if (own.rows != nullptr && own.rows->Count() != 0) {
    auto reader = with_patches(
      own.rows_id, {},
      with_own(own.rows_id, nullptr,
               OwnRowsReader(db, own.rows_id, *own.rows, own.row_columns)));
    added.emplace_back(reader);
    add(std::move(reader));
  }
  if (scope == Scope::Cold) {
    return {_dir, _options, std::move(readers), /*stats=*/false};
  }
  if (state.units.empty()) {
    return {_dir, _options, std::move(readers), /*stats=*/true};
  }
  return {state.reader, std::move(readers), added, patched, {}};
}

StoreStats Table::Stats() const {
  const auto state = State();
  const auto cold = Build(*state, {}, Scope::Cold);
  auto stats = StoreStats::FromReader(cold);
  stats.numBufferedDocs =
    state->reader.live_docs_count() - cold.live_docs_count();
  for (const auto& unit : state->units) {
    if (!unit->piece) {
      stats.numFiles += std::popcount(unit->meta.kinds);
    }
  }
  return stats;
}

uint64_t Table::LiveDocs() const {
  uint64_t live = 0;
  for (const auto& unit : State()->units) {
    live += unit->docs - (unit->deleted ? unit->deleted->Count() : 0);
  }
  return live;
}

uint64_t Table::ByteSize() const {
  uint64_t bytes = 0;
  for (const auto& unit : State()->units) {
    bytes += unit->reader->Meta().byte_size;
  }
  return bytes;
}

irs::DirectoryReader Table::PieceView(
  std::span<const std::shared_ptr<const TableUnit>> pieces,
  bool keep_deleted) const {
  std::vector<irs::SegmentReader> readers;
  readers.reserve(pieces.size());
  for (const auto& piece : pieces) {
    readers.emplace_back(keep_deleted && piece->deleted
                           ? piece->reader->WithDocsMask(nullptr)
                           : piece->reader);
  }
  return {_dir, _options, std::move(readers), /*stats=*/false};
}

Table::Memory Table::InMemory() const {
  const auto state = State();
  Memory memory{.end = state->memory_end};
  for (const auto& unit : state->units) {
    if (unit->piece) {
      memory.pieces.push_back(unit);
    }
  }
  return memory;
}

std::vector<std::shared_ptr<const TableUnit>> Table::MergeRun() const {
  auto pieces = InMemory().pieces;
  PieceRuns runs;
  for (size_t i = 0; i < pieces.size(); ++i) {
    if (!runs.Next(*pieces[i])) {
      continue;
    }
    const auto tier = PieceTier(pieces[i]->docs);
    size_t begin = 0;
    if (tier > PieceTier(pieces[i - 1]->docs)) {
      begin = i - 1;
      while (begin != 0 && Adjacent(*pieces[begin - 1], *pieces[begin]) &&
             PieceTier(pieces[begin - 1]->docs) < tier) {
        --begin;
      }
    } else {
      begin = i + 1 - kPieceFanIn;
    }
    uint64_t docs = 0;
    for (size_t j = begin; j <= i; ++j) {
      docs += pieces[j]->docs;
    }
    for (; docs > irs::doc_limits::kMaxDocs; ++begin) {
      docs -= pieces[begin]->docs;
    }
    if (begin == i) {
      continue;
    }
    auto end = i + 1;
    for (; end != pieces.size() && Adjacent(*pieces[end - 1], *pieces[end]) &&
           Mergeable(*pieces[end]) && PieceTier(pieces[end]->docs) <= tier &&
           docs + pieces[end]->docs <= irs::doc_limits::kMaxDocs;
         ++end) {
      docs += pieces[end]->docs;
    }
    pieces.erase(pieces.begin() + end, pieces.end());
    pieces.erase(pieces.begin(), pieces.begin() + begin);
    return pieces;
  }
  return {};
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

Tick Table::Conflict(const irs::DocRemoval& removal, Tick snapshot) const {
  const auto state = State();
  return absl::c_any_of(removal.groups,
                        [&](const auto& group) {
                          return Conflicting(*state, group.segment, group.docs,
                                             snapshot);
                        })
           ? state->tick
           : 0;
}

std::vector<UnitPatch> Table::Resolve(std::vector<UnitPatch>&& patches) const {
  const auto state = State();
  if (PatchesCurrent(*state, patches)) {
    return std::move(patches);
  }
  return FollowPatches(*state, patches);
}

TableDelta Table::CommitDelta(const TableCommit& commit,
                              LoggedRows&& rows) const {
  absl::MutexLock lock{&_state_mutex};
  const auto& state = *Head().state;
  TableDelta delta;
  Mapped removed;
  for (const auto& removal : commit.removals) {
    for (const auto& group : removal->groups) {
      removed[group.segment].Merge(group.docs);
    }
  }
  irs::containers::FlatHashMap<uint64_t,
                               std::shared_ptr<const irs::DocumentMask>>
    deletes;
  deletes.reserve(removed.size());
  for (auto& [id, docs] : removed) {
    if (!docs.Empty()) {
      deletes.emplace(
        id, std::make_shared<const irs::DocumentMask>(
              std::move(docs).Finish(irs::DocumentMaskBuilder::kCanonical)));
    }
  }
  irs::containers::FlatHashSet<uint64_t> own;
  irs::containers::FlatHashSet<uint64_t> dead;
  for (const auto& segment : commit.segments) {
    const auto id = segment.reader->Id();
    const auto rows = segment.reader->Meta().docs_count;
    own.insert(id);
    if (const auto it = deletes.find(id);
        it != deletes.end() &&
        DeletesAll(segment.reader->docs_mask(), *it->second, rows)) {
      dead.insert(id);
      continue;
    }
    delta.add.push_back({.id = id, .kinds = segment.meta.kinds, .rows = rows});
  }
  if (commit.piece) {
    SDB_ASSERT(!rows.chunks.empty());
    rows.first = state.memory_end;
    delta.rows.push_back(std::move(rows));
  }
  MemoryDeletes memory;
  for (auto& [id, docs] : deletes) {
    if (own.contains(id)) {
      if (!dead.contains(id)) {
        delta.deletes.push_back({.segment = id, .docs = std::move(docs)});
      }
      continue;
    }
    const auto* unit = FindUnit(state, id);
    if (!unit) {
      continue;
    }
    if (unit->piece) {
      memory.Add(unit->first, std::move(docs));
    } else if (DeletesAll(unit->deleted.get(), *docs, unit->docs)) {
      delta.drop.push_back(id);
      dead.insert(id);
    } else {
      delta.deletes.push_back({.segment = id, .docs = std::move(docs)});
    }
  }
  std::move(memory).Finish(delta);
  for (const auto& patch : commit.patches) {
    if (dead.contains(patch.unit)) {
      continue;
    }
    if (commit.piece && patch.unit == commit.piece->Id()) {
      ShiftPatches(patch.fields, state.memory_end, delta.memory_updates);
      continue;
    }
    if (!own.contains(patch.unit)) {
      const auto* unit = FindUnit(state, patch.unit);
      if (!unit) {
        continue;
      }
      if (unit->piece) {
        ShiftPatches(patch.fields, unit->first, delta.memory_updates);
        continue;
      }
    }
    for (const auto& field : patch.fields) {
      delta.updates.push_back({.segment = patch.unit, .patch = field});
    }
  }
  delta.manifest = commit.manifest;
  return delta;
}

bool Table::Apply(Tick tick, TableCommit&& commit) {
  absl::MutexLock lock{&_state_mutex};
  auto next = std::make_shared<TableState>(*Head().state);
  auto& units = next->units;
  const auto rewrite = [&](size_t i, auto&& change) {
    auto unit = std::make_shared<TableUnit>(*units[i]);
    change(*unit);
    units[i] = std::move(unit);
  };
  if (commit.manifest) {
    next->manifest = std::move(commit.manifest);
  }
  if (commit.piece || !commit.segments.empty() || !commit.removals.empty() ||
      !commit.patches.empty()) {
    _last_write.store(tick, std::memory_order_release);
  }
  if (commit.piece) {
    auto unit = PieceUnit(std::move(commit.piece), next->memory_end);
    next->memory_end += unit->docs;
    units.push_back(std::move(unit));
  }
  for (auto& segment : commit.segments) {
    units.push_back(SegmentUnit(std::move(segment)));
  }
  for (const auto& removal : commit.removals) {
    for (const auto& group : removal->groups) {
      if (const auto i = IndexOf(units, group.segment); i != units.size()) {
        rewrite(i, [&](TableUnit& unit) { AddDeletes(unit, group.docs); });
      }
    }
  }
  for (const auto& patch : commit.patches) {
    if (const auto i = IndexOf(units, patch.unit); i != units.size()) {
      rewrite(i,
              [&](TableUnit& unit) { ApplyPatches(unit, patch.fields, tick); });
    }
  }
  const bool emptied = LeaveDead(*next, tick);
  Publish(std::move(next), tick);
  return emptied;
}

std::shared_ptr<TableUnit> Table::SegmentUnit(OwnSegment&& segment) const {
  auto unit = std::make_shared<TableUnit>();
  unit->id = segment.reader->Id();
  unit->docs = segment.reader->Meta().docs_count;
  unit->deleted = segment.reader->SharedDocsMask();
  unit->deleted_bytes = unit->deleted ? unit->deleted->ByteSize() : 0;
  unit->meta = std::move(segment.meta);
  unit->reader = std::move(segment.reader);
  return unit;
}

Table::Replacement Table::PrepareReplace(
  std::span<const uint64_t> replaced, std::span<const Tick> folded,
  std::vector<std::shared_ptr<TableUnit>>&& added,
  std::vector<irs::DocMap>&& maps, uint64_t memory_below) const {
  SDB_ASSERT(folded.size() == replaced.size());
  absl::MutexLock lock{&_state_mutex};
  const auto& state = *Head().state;
  Replacement replacement{.added = std::move(added), .maps = std::move(maps)};
  auto& delta = replacement.delta;
  delta.drop_memory = memory_below;
  Mapped carried;
  for (const auto& map : replacement.maps) {
    const auto* source = FindUnit(state, map.source);
    if (!source) {
      carried[map.target].AddRange(
        map.base,
        static_cast<irs::doc_id_t>(map.base + (map.rank_end - map.rank_begin)));
    } else if (source->deleted) {
      irs::MapDocs(map, *source->deleted, carried[map.target]);
    }
  }
  absl::flat_hash_map<uint64_t, std::vector<irs::FieldPatch>> carried_patches;
  irs::containers::FlatHashMap<uint64_t, std::vector<irs::FieldPatch>>
    logged_patches;
  const auto oldest = catalog::Transactions::Get().RefreshOldest();
  for (size_t r = 0; r < replaced.size(); ++r) {
    const auto* source = FindUnit(state, replaced[r]);
    if (!source) {
      continue;
    }
    replacement.replaced.push_back(replaced[r]);
    if (!source->piece) {
      delta.drop.push_back(replaced[r]);
    }
    const auto bound = std::min<Tick>(folded[r], oldest);
    for (const auto& [field, patch] : source->patches) {
      if (patch->MaxTick() <= bound) {
        continue;
      }
      for (const auto& map : replacement.maps) {
        if (map.source != replaced[r]) {
          continue;
        }
        CarryPatch(
          field, *patch, map, [&](uint64_t tick) { return tick > bound; },
          carried_patches[map.target]);
        if (patch->MaxTick() > folded[r]) {
          CarryPatch(
            field, *patch, map, [&](uint64_t tick) { return tick > folded[r]; },
            logged_patches[map.target]);
        }
      }
    }
  }
  for (auto& unit : replacement.added) {
    if (const auto it = carried.find(unit->id);
        it != carried.end() && !it->second.Empty()) {
      irs::DocumentMaskBuilder mask;
      if (unit->deleted) {
        mask.Merge(*unit->deleted);
      }
      mask.Merge(it->second);
      SetDeletes(*unit, std::move(mask).Finish());
    }
    if (auto it = carried_patches.find(unit->id);
        it != carried_patches.end() && !it->second.empty()) {
      SDB_ASSERT(unit->patches.empty());
      unit->patches = std::move(it->second);
      unit->reader = unit->reader->WithPatches(unit->patches);
    }
    if (unit->piece || Dead(*unit)) {
      continue;
    }
    delta.add.push_back(
      {.id = unit->id, .kinds = unit->meta.kinds, .rows = unit->docs});
    if (unit->deleted && !unit->deleted->Empty()) {
      delta.deletes.push_back({.segment = unit->id, .docs = unit->deleted});
    }
    if (auto it = logged_patches.find(unit->id); it != logged_patches.end()) {
      for (auto& field : it->second) {
        delta.updates.push_back(
          {.segment = unit->id, .patch = std::move(field)});
      }
    }
  }
  return replacement;
}

std::shared_ptr<const TableState> Table::Replace(Tick tick,
                                                 Replacement&& replacement) {
  absl::MutexLock lock{&_state_mutex};
  auto next = std::make_shared<TableState>(*Head().state);
  auto& units = next->units;
  auto moved = next->moved ? std::make_shared<MovedUnits>(*next->moved)
                           : std::make_shared<MovedUnits>();
  auto at = units.size();
  for (const auto id : replacement.replaced) {
    const auto i = IndexOf(units, id);
    if (i == units.size()) {
      continue;
    }
    MovedUnit source{.tick = tick};
    for (const auto& map : replacement.maps) {
      if (map.source == id) {
        source.maps.push_back(map);
      }
    }
    moved->insert_or_assign(
      id, std::make_shared<const MovedUnit>(std::move(source)));
    units.erase(units.begin() + i);
    at = std::min(at, i);
  }
  next->moved = std::move(moved);
  for (auto& unit : replacement.added) {
    if (unit->piece) {
      units.insert(units.begin() + at++, std::move(unit));
    } else {
      units.push_back(std::move(unit));
    }
  }
  LeaveDead(*next, tick);
  Publish(next, tick);
  return next;
}

bool Table::Collectable(Tick oldest) const {
  const auto state = State();
  return state->moved && absl::c_any_of(*state->moved, [&](const auto& entry) {
           return entry.second->tick <= oldest;
         });
}

void Table::Collect(Tick tick, Tick oldest) {
  absl::MutexLock lock{&_state_mutex};
  const auto& state = *Head().state;
  const auto settled = [&](const auto& entry) {
    return entry.second->tick <= oldest;
  };
  if (!state.moved || absl::c_none_of(*state.moved, settled)) {
    return;
  }
  auto next = SameUnits();
  auto moved = std::make_shared<MovedUnits>(*state.moved);
  absl::erase_if(*moved, settled);
  if (moved->empty()) {
    next->moved.reset();
  } else {
    next->moved = std::move(moved);
  }
  Publish(std::move(next), tick);
}

void Table::SetConfig(
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  absl::MutexLock lock{&_state_mutex};
  _config = std::move(config);
}

void Table::PublishConfig(Tick tick) {
  absl::MutexLock lock{&_state_mutex};
  if (Head().state->config != _config) {
    Publish(SameUnits(), tick);
  }
}

bool Table::Unsettled() const {
  return absl::c_any_of(State()->units,
                        [](const auto& unit) { return unit->piece; });
}

std::shared_ptr<const TableState> Table::HeadState() const {
  absl::MutexLock lock{&_state_mutex};
  return Head().state;
}

void Table::Recover(std::vector<std::shared_ptr<TableUnit>> units,
                    uint64_t memory_end,
                    std::shared_ptr<const FileManifest> manifest) {
  absl::MutexLock lock{&_state_mutex};
  auto next = std::make_shared<TableState>(*Head().state);
  next->units.assign(std::make_move_iterator(units.begin()),
                     std::make_move_iterator(units.end()));
  next->memory_end = memory_end;
  next->manifest = std::move(manifest);
  next->config = _config;
  CountPieces(*next);
  next->reader = BuildReader(*Head().state, *next);
  _versions.ReplaceInitial(std::move(next));
}

}  // namespace sdb::search
