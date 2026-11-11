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

#include "search/table_replay.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/vector_size.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

namespace sdb::search {
namespace {

constexpr uint64_t kPieceRowsMin = uint64_t{kPieceFanIn} * STANDARD_VECTOR_SIZE;

}  // namespace

TableReplay::Segment& TableReplay::Find(uint64_t id) {
  const auto it = _segments.find(id);
  SDB_ENSURE(it != _segments.end(), "the log changes a missing segment ", id);
  return it->second;
}

void TableReplay::Drop(std::span<const uint64_t> segments,
                       uint64_t memory_below) {
  _replayed = true;
  for (const auto id : segments) {
    _top = std::max(_top, id);
    SDB_ENSURE(_segments.erase(id) != 0, "the log drops a missing segment ",
               id);
  }
  const auto kept = absl::c_find_if(_chunks, [&](const Chunk& chunk) {
    return chunk.first + chunk.rows->size() > memory_below;
  });
  _chunks.erase(_chunks.begin(), kept);
  SDB_ENSURE(_chunks.empty() || _chunks.front().first >= memory_below,
             "the log drops the in-memory rows below ", memory_below,
             ", inside the chunk of rows from ", _chunks.front().first);
}

void TableReplay::Add(const LoggedSegment& segment) {
  _replayed = true;
  _top = std::max(_top, segment.id);
  auto& added = _segments[segment.id];
  added.meta_file = segment.meta_file;
  added.rows = segment.rows;
}

void TableReplay::Append(
  duckdb::Catalog& catalog, uint64_t first,
  std::shared_ptr<const std::vector<connector::ColumnId>> columns,
  duckdb::unique_ptr<duckdb::DataChunk> rows) {
  _replayed = true;
  SDB_ENSURE(first >= _memory_end, "the log appends rows at ", first,
             ", below the end of the rows before them at ", _memory_end);
  if (rows->size() == 0) {
    return;
  }
  _catalog = &catalog;
  _memory_end = first + rows->size();
  _chunks.push_back(
    {.first = first, .rows = std::move(rows), .columns = std::move(columns)});
}

void TableReplay::Delete(uint64_t segment, const irs::DocumentMask& docs) {
  _replayed = true;
  Find(segment).deleted.Merge(docs);
}

void TableReplay::DeleteMemory(uint64_t base, const irs::DocumentMask& docs) {
  _replayed = true;
  docs.ForEachRun(nullptr, [&](irs::doc_id_t first, irs::doc_id_t last) {
    const auto from = base + first - irs::doc_limits::min();
    const auto to = base + last - irs::doc_limits::min();
    auto it = absl::c_upper_bound(
      _chunks, from,
      [](uint64_t row, const Chunk& chunk) { return row < chunk.first; });
    if (it != _chunks.begin()) {
      --it;
    }
    for (; it != _chunks.end() && it->first < to; ++it) {
      const auto begin = std::max(from, it->first);
      const auto end = std::min(to, it->first + it->rows->size());
      if (begin < end) {
        it->deleted.AddRange(
          static_cast<irs::doc_id_t>(begin - it->first +
                                     irs::doc_limits::min()),
          static_cast<irs::doc_id_t>(end - it->first + irs::doc_limits::min()));
      }
    }
    return true;
  });
}

void TableReplay::Update(uint64_t segment, const irs::FieldPatch& patch) {
  _replayed = true;
  MergePatches(Find(segment).patches, {&patch, 1}, 0);
}

void TableReplay::UpdateMemory(const irs::FieldPatch& patch) {
  _replayed = true;
  MergePatches(_memory_patches, {&patch, 1}, 0);
}

std::vector<TableReplay::Piece>& TableReplay::Cut() {
  _pieces.clear();
  const auto live = [](const Chunk& chunk) {
    return chunk.deleted.Count() < chunk.rows->size();
  };
  uint64_t rows = 0;
  for (const auto& chunk : _chunks) {
    if (live(chunk)) {
      rows += chunk.rows->size();
    }
  }
  const auto pieces =
    std::clamp<uint64_t>(rows / kPieceRowsMin, 1, kPieceFanIn - 1);
  const auto target = (rows + pieces - 1) / pieces;
  Piece* open = nullptr;
  uint64_t taken = 0;
  uint64_t end = 0;
  for (size_t i = 0; i < _chunks.size(); ++i) {
    const auto& chunk = _chunks[i];
    if (!live(chunk)) {
      open = nullptr;
      continue;
    }
    if (!open || chunk.first != end || taken >= target) {
      open = &_pieces.emplace_back(Piece{.begin = i});
      taken = 0;
    }
    open->end = i + 1;
    taken += chunk.rows->size();
    end = chunk.first + chunk.rows->size();
  }
  return _pieces;
}

std::vector<std::shared_ptr<TableUnit>> TableReplay::Units(
  const Table& table) && {
  std::vector<std::shared_ptr<TableUnit>> units;
  units.reserve(_segments.size() + _pieces.size());
  const auto finish = [&](std::shared_ptr<TableUnit> unit,
                          irs::DocumentMaskBuilder& deleted,
                          std::vector<irs::FieldPatch>& patches) {
    if (!deleted.Empty()) {
      if (unit->deleted) {
        deleted.Merge(*unit->deleted);
      }
      unit->deleted =
        std::make_shared<const irs::DocumentMask>(std::move(deleted).Finish());
      unit->reader = unit->reader->WithDocsMask(unit->deleted);
    }
    if (unit->deleted && unit->deleted->Count() >= unit->docs) {
      return;
    }
    if (!patches.empty()) {
      unit->patches = std::move(patches);
      unit->reader = unit->reader->WithPatches(unit->patches);
    }
    units.push_back(std::move(unit));
  };
  for (auto& [id, segment] : _segments) {
    SDB_ENSURE(segment.opened.reader, "segment ", id,
               " of the log was never opened");
    auto unit = table.SegmentUnit(std::move(segment.opened));
    SDB_ENSURE(unit->id == id && unit->docs == segment.rows,
               "the log gives segment ", id, " ", segment.rows,
               " rows, its files segment ", unit->id, " ", unit->docs);
    finish(std::move(unit), segment.deleted, segment.patches);
  }
  for (auto& piece : _pieces) {
    const auto chunks = Chunks(piece);
    auto unit = PieceUnit(std::move(piece.reader), chunks.front().first);
    irs::DocumentMaskBuilder deleted;
    irs::doc_id_t offset = 0;
    for (auto& chunk : chunks) {
      if (!chunk.deleted.Empty()) {
        std::move(chunk.deleted)
          .Finish()
          .ForEachRun(nullptr, [&](irs::doc_id_t first, irs::doc_id_t last) {
            deleted.AddRange(offset + first, offset + last);
            return true;
          });
      }
      offset += static_cast<irs::doc_id_t>(chunk.rows->size());
    }
    std::vector<irs::FieldPatch> patches;
    SlicePatches(_memory_patches, unit->first, unit->first + unit->docs,
                 patches);
    finish(std::move(unit), deleted, patches);
  }
  return units;
}

}  // namespace sdb::search
