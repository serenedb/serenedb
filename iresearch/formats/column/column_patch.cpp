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

#include "iresearch/formats/column/column_patch.hpp"

#include <absl/algorithm/container.h>

#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/storage/table/column_segment.hpp>

#include "iresearch/formats/column/hot_column_reader.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {
namespace {

constexpr uint32_t kGroupRowShift =
  ColumnPatch::kChunkShift + ColumnPatch::kGroupShift;

struct Pick {
  const duckdb::Vector* source;
  duckdb::idx_t index;
};

std::shared_ptr<const ColumnPatch::Chunk> MergeChunk(
  const duckdb::LogicalType& type, const ColumnPatch::Chunk* old,
  uint64_t chunk_row, std::span<const uint64_t> rows,
  const duckdb::Vector& values, duckdb::idx_t values_at,
  std::span<const uint64_t> ticks) {
  const size_t old_count = old ? old->offsets.size() : 0;
  size_t count = 0;
  for (size_t a = 0, b = 0; a < old_count || b < rows.size(); ++count) {
    const bool take_new =
      b < rows.size() &&
      (a == old_count || rows[b] - chunk_row <= old->offsets[a]);
    if (take_new) {
      if (a < old_count && rows[b] - chunk_row == old->offsets[a]) {
        ++a;
      }
      ++b;
    } else {
      ++a;
    }
  }
  auto chunk = std::make_shared<ColumnPatch::Chunk>(type, count);
  chunk->offsets.reserve(count);
  chunk->ticks.reserve(count);
  std::array<duckdb::sel_t, ColumnPatch::kChunkRows> picked;
  const duckdb::Vector* source = nullptr;
  size_t run = 0;
  const auto flush = [&] {
    if (run != 0) {
      chunk->values.Append(
        *source, duckdb::SelectionVector{picked.data(), picked.size()}, run);
      run = 0;
    }
  };
  const auto pick = [&](const duckdb::Vector& from, duckdb::idx_t index) {
    if (source != &from) {
      flush();
      source = &from;
    }
    picked[run++] = static_cast<duckdb::sel_t>(index);
  };
  for (size_t a = 0, b = 0; a < old_count || b < rows.size();) {
    const bool take_new =
      b < rows.size() &&
      (a == old_count || rows[b] - chunk_row <= old->offsets[a]);
    if (take_new) {
      if (a < old_count && rows[b] - chunk_row == old->offsets[a]) {
        ++a;
      }
      chunk->offsets.push_back(static_cast<uint16_t>(rows[b] - chunk_row));
      chunk->ticks.push_back(ticks.size() == 1 ? ticks[0] : ticks[b]);
      pick(values, values_at + b);
      ++b;
    } else {
      chunk->offsets.push_back(old->offsets[a]);
      chunk->ticks.push_back(old->ticks[a]);
      pick(old->values, a);
      ++a;
    }
  }
  flush();
  SDB_ASSERT(chunk->values.size() == count);
  if (old) {
    chunk->stats = old->stats.Copy();
  }
  const duckdb::Vector added{values, values_at, values_at + rows.size()};
  chunk->stats.Merge(ChunkStatistics(type, added, rows.size()));
  chunk->bytes = chunk->values.GetAllocationSize() +
                 count * (sizeof(uint16_t) + sizeof(uint64_t));
  return chunk;
}

template<typename Visit>
void ForEachRun(const ColumnPatch& patch, uint64_t first, uint64_t last,
                const Visit& visit) {
  const ColumnPatch::Chunk* chunk = nullptr;
  size_t index = 0;
  uint64_t row = 0;
  size_t length = 0;
  patch.ForEach(first, last,
                [&](uint64_t at, const ColumnPatch::Chunk& in, size_t i) {
                  if (length != 0 && &in == chunk && i == index + length &&
                      at == row + length) {
                    ++length;
                    return;
                  }
                  if (length != 0) {
                    visit(*chunk, index, row, length);
                  }
                  chunk = &in;
                  index = i;
                  row = at;
                  length = 1;
                });
  if (length != 0) {
    visit(*chunk, index, row, length);
  }
}

void CopyEntries(const ColumnPatch::Chunk& chunk, size_t index,
                 duckdb::Vector& out, duckdb::idx_t at, size_t length) {
  out.Copy(chunk.values, *duckdb::FlatVector::IncrementalSelectionVector(),
           chunk.values.size(), index, at, length);
}

void ApplyRange(const ColumnPatch& patch, uint64_t first, uint64_t count,
                duckdb::Vector& out, duckdb::idx_t at) {
  ForEachRun(patch, first, first + count,
             [&](const ColumnPatch::Chunk& chunk, size_t index, uint64_t row,
                 size_t length) {
               CopyEntries(chunk, index, out, at + (row - first), length);
             });
}

void Own(duckdb::Vector& vector, duckdb::idx_t count) {
  duckdb::Vector owned{vector.GetType(), count};
  owned.Copy(vector, *duckdb::FlatVector::IncrementalSelectionVector(), count,
             0, 0, count);
  duckdb::FlatVector::SetSize(owned, count);
  vector.Reference(owned);
}

std::vector<ColumnBlockMeta> PatchedBlocks(const ColumnReader& base,
                                           const ColumnPatch& patch) {
  const auto metas = base.DataBlocks();
  std::vector<ColumnBlockMeta> blocks;
  blocks.reserve(metas.size());
  for (size_t b = 0; b < metas.size(); ++b) {
    auto& block = blocks.emplace_back(ColumnBlockMeta{
      .statistics = metas[b].statistics.Copy(),
      .tuple_count = metas[b].tuple_count,
      .file_offset = metas[b].file_offset,
      .byte_size = metas[b].byte_size,
      .codec = metas[b].codec,
    });
    const auto first = base.DataBlockFirstRow(b);
    patch.MergeStats(first, first + metas[b].tuple_count, block.statistics);
  }
  return blocks;
}

std::vector<ColumnBlockMeta> NullBlocks(duckdb::DatabaseInstance& db,
                                        const duckdb::LogicalType& type,
                                        uint64_t rows) {
  const auto codec = duckdb::DBConfig::GetConfig(db).TryGetCompressionFunction(
    duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType());
  SDB_ASSERT(codec);
  auto stats = duckdb::BaseStatistics::CreateEmpty(type);
  stats.SetHasNull();
  std::vector<ColumnBlockMeta> blocks;
  blocks.push_back(ColumnBlockMeta{
    .statistics = std::move(stats),
    .tuple_count = rows,
    .codec = codec.get(),
  });
  return blocks;
}

}  // namespace

ColumnPatch::Chunk::Chunk(const duckdb::LogicalType& type,
                          duckdb::idx_t capacity)
  : values{type, capacity}, stats{duckdb::BaseStatistics::CreateEmpty(type)} {}

ColumnPatch::Group::Group(const duckdb::LogicalType& type)
  : stats{duckdb::BaseStatistics::CreateEmpty(type)} {}

ColumnPatch::ColumnPatch(duckdb::LogicalType type)
  : _type{std::move(type)},
    _stats{duckdb::BaseStatistics::CreateEmpty(_type)} {}

bool ColumnPatch::Patchable(const duckdb::LogicalType& type) noexcept {
  if (type.IsNested() || type.id() == duckdb::LogicalTypeId::VARIANT) {
    return false;
  }
  const auto physical = type.InternalType();
  return physical == duckdb::PhysicalType::VARCHAR ||
         duckdb::TypeIsConstantSize(physical);
}

std::shared_ptr<const ColumnPatch> ColumnPatch::Merge(
  const ColumnPatch* base, const duckdb::LogicalType& type,
  std::span<const uint64_t> rows, const duckdb::Vector& values,
  std::span<const uint64_t> ticks) {
  SDB_ASSERT(rows.size() == values.size());
  SDB_ASSERT(ticks.size() == 1 || ticks.size() == rows.size());
  auto next = std::make_shared<ColumnPatch>(type);
  if (base) {
    next->_groups = base->_groups;
    next->_count = base->_count;
    next->_bytes = base->_bytes;
    next->_max_tick = base->_max_tick;
    next->_stats = base->_stats.Copy();
  }
  next->_max_tick = std::max(next->_max_tick, *absl::c_max_element(ticks));
  for (size_t i = 0; i < rows.size();) {
    SDB_ASSERT(i == 0 || rows[i] > rows[i - 1]);
    const uint64_t group_id = rows[i] >> kGroupRowShift;
    auto it = absl::c_lower_bound(
      next->_groups, group_id,
      [](const auto& entry, uint64_t id) { return entry.first < id; });
    const bool existing = it != next->_groups.end() && it->first == group_id;
    auto group = std::make_shared<Group>(type);
    if (existing) {
      group->chunks = it->second->chunks;
      group->stats = it->second->stats.Copy();
    }
    while (i < rows.size() && rows[i] >> kGroupRowShift == group_id) {
      const uint64_t chunk_id = rows[i] >> kChunkShift;
      size_t end = i + 1;
      while (end < rows.size() && rows[end] >> kChunkShift == chunk_id) {
        ++end;
      }
      auto& slot = group->chunks[chunk_id & (kGroupChunks - 1)];
      const auto* old = slot.get();
      auto merged = MergeChunk(
        type, old, chunk_id << kChunkShift, rows.subspan(i, end - i), values, i,
        ticks.size() == 1 ? ticks : ticks.subspan(i, end - i));
      next->_count =
        next->_count - (old ? old->offsets.size() : 0) + merged->offsets.size();
      next->_bytes = next->_bytes - (old ? old->bytes : 0) + merged->bytes;
      group->stats.Merge(merged->stats);
      slot = std::move(merged);
      i = end;
    }
    next->_stats.Merge(group->stats);
    if (existing) {
      it->second = std::move(group);
    } else {
      next->_groups.emplace(it, group_id, std::move(group));
    }
  }
  return next;
}

std::shared_ptr<const ColumnPatch> ColumnPatch::Merge(const ColumnPatch* base,
                                                      const ColumnPatch& added,
                                                      uint64_t tick) {
  std::vector<uint64_t> rows;
  std::vector<uint64_t> ticks;
  duckdb::Vector values{added._type, added._count};
  added.Entries(rows, values, ticks);
  return Merge(base, added._type, rows, values, tick);
}

void ColumnPatch::Entries(std::vector<uint64_t>& rows, duckdb::Vector& values,
                          std::vector<uint64_t>& ticks) const {
  rows.reserve(rows.size() + _count);
  ticks.reserve(ticks.size() + _count);
  for (const auto& [group_id, group] : _groups) {
    for (size_t c = 0; c < kGroupChunks; ++c) {
      const auto& chunk = group->chunks[c];
      if (!chunk) {
        continue;
      }
      const uint64_t chunk_row =
        (group_id << kGroupRowShift) + (uint64_t{c} << kChunkShift);
      for (const auto offset : chunk->offsets) {
        rows.push_back(chunk_row + offset);
      }
      absl::c_copy(chunk->ticks, std::back_inserter(ticks));
      values.Append(chunk->values, chunk->values.size());
    }
  }
}

const ColumnPatch::Chunk* ColumnPatch::FindChunk(
  uint64_t chunk) const noexcept {
  const uint64_t group_id = chunk >> kGroupShift;
  const auto it = absl::c_lower_bound(
    _groups, group_id,
    [](const auto& entry, uint64_t id) { return entry.first < id; });
  if (it == _groups.end() || it->first != group_id) {
    return nullptr;
  }
  return it->second->chunks[chunk & (kGroupChunks - 1)].get();
}

bool ColumnPatch::Overlaps(uint64_t first, uint64_t last) const noexcept {
  if (first >= last || _groups.empty()) {
    return false;
  }
  for (uint64_t c = first >> kChunkShift; c <= (last - 1) >> kChunkShift; ++c) {
    const auto* chunk = FindChunk(c);
    if (!chunk) {
      continue;
    }
    const uint64_t chunk_row = c << kChunkShift;
    const auto from =
      static_cast<uint16_t>(first > chunk_row ? first - chunk_row : 0);
    const auto it = absl::c_lower_bound(chunk->offsets, from);
    if (it != chunk->offsets.end() && chunk_row + *it < last) {
      return true;
    }
  }
  return false;
}

void ColumnPatch::MergeStats(uint64_t first, uint64_t last,
                             duckdb::BaseStatistics& stats) const {
  if (first >= last) {
    return;
  }
  auto it = absl::c_lower_bound(
    _groups, first >> kGroupRowShift,
    [](const auto& entry, uint64_t id) { return entry.first < id; });
  for (; it != _groups.end(); ++it) {
    const uint64_t group_row = it->first << kGroupRowShift;
    if (group_row >= last) {
      return;
    }
    if (group_row >= first &&
        group_row + (uint64_t{1} << kGroupRowShift) <= last) {
      stats.Merge(it->second->stats);
      continue;
    }
    for (size_t c = 0; c < kGroupChunks; ++c) {
      const auto& chunk = it->second->chunks[c];
      const uint64_t chunk_row = group_row + (uint64_t{c} << kChunkShift);
      if (chunk && chunk_row < last && chunk_row + kChunkRows > first) {
        stats.Merge(chunk->stats);
      }
    }
  }
}

NullColumnReader::NullColumnReader(duckdb::DatabaseInstance& db, field_id id,
                                   const duckdb::LogicalType& type,
                                   uint64_t rows)
  : ColumnReader{id, type, NullBlocks(db, type, rows), nullptr, {}} {}

duckdb::idx_t NullColumnReader::Scan(ScanState& s, duckdb::Vector& result,
                                     duckdb::idx_t count) const {
  duckdb::ConstantVector::SetNull(result, duckdb::count_t{count});
  s.st.offset_in_column += count;
  return count;
}

duckdb::idx_t NullColumnReader::ScanCount(ScanState& s, duckdb::Vector& result,
                                          duckdb::idx_t count,
                                          duckdb::idx_t result_offset) const {
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    validity.SetInvalid(result_offset + i);
  }
  if (result.size() < result_offset + count) {
    duckdb::FlatVector::SetSize(result, result_offset + count);
  }
  s.st.offset_in_column += count;
  return count;
}

duckdb::idx_t NullColumnReader::GatherFilter(
  ScanState& s, uint64_t anchor, duckdb::idx_t span,
  duckdb::SelectionVector& sel, duckdb::idx_t sel_count,
  const duckdb::TableFilter&, duckdb::TableFilterState& filter_state,
  NullCheckKind, duckdb::Vector& scratch) const {
  SDB_ASSERT(anchor >= s.st.offset_in_column,
             "GatherFilter requires ascending rows");
  s.st.offset_in_column = anchor + span;
  duckdb::ConstantVector::SetNull(scratch, duckdb::count_t{span});
  duckdb::idx_t approved = sel_count;
  duckdb::ColumnSegment::FilterSelection(sel, scratch, filter_state, span,
                                         approved);
  return approved;
}

bool NullColumnReader::FetchPoint(PointReader&, uint64_t, duckdb::Vector& out,
                                  duckdb::idx_t out_offset) const {
  duckdb::FlatVector::SetNull(out, out_offset, true);
  return false;
}

PatchedColumnReader::PatchedColumnReader(
  std::unique_ptr<const ColumnReader> base,
  std::shared_ptr<const ColumnPatch> patch)
  : PatchedColumnReader{*base, std::move(patch)} {
  _owned = std::move(base);
}

PatchedColumnReader::PatchedColumnReader(
  const ColumnReader& base, std::shared_ptr<const ColumnPatch> patch)
  : ColumnReader{base.Id(),
                 base.Type(),
                 PatchedBlocks(base, *patch),
                 nullptr,
                 {}},
    _base{base},
    _patch{std::move(patch)} {
  _validity_view = base.Validity();
  _row_count = base.RowCount();
  auto stats = base.MergedStatistics().Copy();
  stats.Merge(_patch->Stats(), duckdb::StatsMergeType::EXPAND_BOUNDS);
  _stats = stats.ToUnique();
  _distinct = SharedDistinct(base);
  _array_size = base.ArraySize();
}

duckdb::idx_t PatchedColumnReader::Scan(ScanState& s, duckdb::Vector& result,
                                        duckdb::idx_t count) const {
  const auto first = _base.GatherCursor(s);
  const auto n = _base.Scan(s, result, count);
  if (n != 0 && _patch->Overlaps(first, first + n)) {
    Own(result, n);
    ApplyRange(*_patch, first, n, result, 0);
  }
  return n;
}

duckdb::idx_t PatchedColumnReader::ScanCount(
  ScanState& s, duckdb::Vector& result, duckdb::idx_t count,
  duckdb::idx_t result_offset) const {
  const auto first = _base.GatherCursor(s);
  const auto n = _base.ScanCount(s, result, count, result_offset);
  if (n != 0 && _patch->Overlaps(first, first + n)) {
    ApplyRange(*_patch, first, n, result, result_offset);
  }
  return n;
}

void PatchedColumnReader::ApplySelected(uint64_t anchor,
                                        const duckdb::SelectionVector& sel,
                                        duckdb::idx_t hits, duckdb::Vector& out,
                                        duckdb::idx_t at) const {
  duckdb::idx_t i = 0;
  ForEachRun(*_patch, anchor + sel.get_index(0),
             anchor + sel.get_index(hits - 1) + 1,
             [&](const ColumnPatch::Chunk& chunk, size_t index, uint64_t row,
                 size_t length) {
               for (size_t k = 0; k < length; ++k) {
                 const uint64_t target = row + k;
                 while (i < hits && anchor + sel.get_index(i) < target) {
                   ++i;
                 }
                 if (i == hits) {
                   return;
                 }
                 if (anchor + sel.get_index(i) == target) {
                   CopyEntries(chunk, index + k, out, at + i, 1);
                 }
               }
             });
}

void PatchedColumnReader::GatherScatter(ScanState& s, uint64_t anchor,
                                        const duckdb::SelectionVector& sel,
                                        duckdb::idx_t hits, duckdb::Vector& out,
                                        duckdb::idx_t at) const {
  _base.GatherScatter(s, anchor, sel, hits, out, at);
  if (hits != 0 && _patch->Overlaps(anchor + sel.get_index(0),
                                    anchor + sel.get_index(hits - 1) + 1)) {
    ApplySelected(anchor, sel, hits, out, at);
  }
}

void PatchedColumnReader::GatherDense(ScanState& s, uint64_t anchor,
                                      const duckdb::SelectionVector& sel,
                                      duckdb::idx_t hits, duckdb::idx_t span,
                                      duckdb::Vector& out) const {
  _base.GatherDense(s, anchor, sel, hits, span, out);
  if (hits != 0 && _patch->Overlaps(anchor + sel.get_index(0),
                                    anchor + sel.get_index(hits - 1) + 1)) {
    Own(out, hits);
    ApplySelected(anchor, sel, hits, out, 0);
  }
}

duckdb::idx_t PatchedColumnReader::GatherFilter(
  ScanState& s, uint64_t anchor, duckdb::idx_t span,
  duckdb::SelectionVector& sel, duckdb::idx_t sel_count,
  const duckdb::TableFilter& filter, duckdb::TableFilterState& filter_state,
  NullCheckKind null_check, duckdb::Vector& scratch) const {
  if (!_patch->Overlaps(anchor, anchor + span)) {
    return _base.GatherFilter(s, anchor, span, sel, sel_count, filter,
                              filter_state, null_check, scratch);
  }
  const auto cur = _base.GatherCursor(s);
  SDB_ASSERT(anchor >= cur, "GatherFilter requires ascending rows");
  _base.Skip(s, anchor - cur);
  scratch.Reference(duckdb::Vector{Type(), span});
  _base.ScanCount(s, scratch, span, 0);
  if (scratch.size() < span) {
    duckdb::FlatVector::SetSize(scratch, span);
  }
  ApplyRange(*_patch, anchor, span, scratch, 0);
  duckdb::idx_t approved = sel_count;
  duckdb::ColumnSegment::FilterSelection(sel, scratch, filter_state, span,
                                         approved);
  return approved;
}

bool PatchedColumnReader::FetchPoint(PointReader& reader, uint64_t row,
                                     duckdb::Vector& out,
                                     duckdb::idx_t out_offset) const {
  if (const auto* chunk = _patch->FindChunk(row >> ColumnPatch::kChunkShift)) {
    const auto offset =
      static_cast<uint16_t>(row & (ColumnPatch::kChunkRows - 1));
    const auto it = absl::c_lower_bound(chunk->offsets, offset);
    if (it != chunk->offsets.end() && *it == offset) {
      const auto index =
        static_cast<size_t>(std::distance(chunk->offsets.begin(), it));
      CopyEntries(*chunk, index, out, out_offset, 1);
      return duckdb::FlatVector::Validity(chunk->values).RowIsValid(index);
    }
  }
  return _base.FetchPoint(reader, row, out, out_offset);
}

bool PatchedColumnReader::ValidPoint(PointReader& reader, uint64_t row,
                                     duckdb::Vector& scratch) const {
  if (const auto* chunk = _patch->FindChunk(row >> ColumnPatch::kChunkShift)) {
    const auto offset =
      static_cast<uint16_t>(row & (ColumnPatch::kChunkRows - 1));
    const auto it = absl::c_lower_bound(chunk->offsets, offset);
    if (it != chunk->offsets.end() && *it == offset) {
      return duckdb::FlatVector::Validity(chunk->values)
        .RowIsValid(
          static_cast<size_t>(std::distance(chunk->offsets.begin(), it)));
    }
  }
  return _base.ValidPoint(reader, row, scratch);
}

}  // namespace irs
