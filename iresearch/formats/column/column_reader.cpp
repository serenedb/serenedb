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

#include "iresearch/formats/column/column_reader.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <atomic>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <utility>

#include "iresearch/formats/column/array_column_reader.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/internal/column_reader_scan.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/formats/column/list_column_reader.hpp"
#include "iresearch/formats/column/struct_column_reader.hpp"
#include "iresearch/formats/column/variant_column_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

using column_internal::BorrowsBlockStrings;
using column_internal::FetchSegmentRows;

constexpr duckdb::idx_t kMaxFetchHits = 64;
constexpr uint64_t kTrustedRunBytes = 32 * file_utils::kPage;

struct GatherBands {
  uint32_t flat;
  uint32_t native;
};

GatherBands BandsFor(codecs::CodecType codec,
                     const duckdb::LogicalType& type) noexcept {
  const bool text = type.InternalType() == duckdb::PhysicalType::VARCHAR;
  if (!text && codecs::IsNumeric(codec)) {
    return {10, 200};
  }
  switch (codec) {
    case codecs::CodecType::Constant:
      return {0, 1000};
    case codecs::CodecType::Lz4:
      return {0, 1000};
    default:
      return text ? GatherBands{10, 1000} : GatherBands{10, 0};
  }
}

bool FewerRuns(const duckdb::SelectionVector& sel, duckdb::idx_t hits,
               uint64_t limit) noexcept {
  if (hits < limit) {
    return true;
  }
  uint64_t runs = 1;
  for (duckdb::idx_t i = 1; i < hits; ++i) {
    runs += sel.get_index(i) != sel.get_index(i - 1) + 1;
    if (runs >= limit) {
      return false;
    }
  }
  return runs < limit;
}

bool FewStringRuns(const duckdb::LogicalType& type,
                   const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                   duckdb::idx_t span) noexcept {
  return type.InternalType() == duckdb::PhysicalType::VARCHAR &&
         FewerRuns(sel, hits, (120 * (span - hits) + 999) / 1000);
}

bool ScatterWins(const ColumnBlockMeta& block, const duckdb::LogicalType& type,
                 duckdb::idx_t hits, duckdb::idx_t span) noexcept {
  return hits * 1000 <= BandsFor(block.codec->type, type).flat * span;
}

bool SelectsNatively(const ColumnBlockMeta& block,
                     const duckdb::LogicalType& type, duckdb::idx_t hits,
                     duckdb::idx_t span) noexcept {
  return block.codec->decompression->select &&
         hits * 1000 <= BandsFor(block.codec->type, type).native * span;
}

bool ScansWindows(const duckdb::LogicalType& type) noexcept {
  return type.id() != duckdb::LogicalTypeId::VALIDITY &&
         type.InternalType() != duckdb::PhysicalType::VARCHAR;
}

duckdb::ValidityMask& ResetAllValid(duckdb::Vector& v, duckdb::idx_t count) {
  v.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto& mask = duckdb::FlatVector::ValidityMutable(v);
  if (mask.CanHaveNull()) {
    mask.SetAllValid(count);
  }
  return mask;
}

duckdb::idx_t RowsLeft(const ColumnReader::ScanState& s) noexcept {
  return (s.window.end - s.window.begin) - s.st.offset_in_column;
}

void Advance(ColumnReader::ScanState& s, duckdb::idx_t rows) noexcept {
  s.st.offset_in_column += rows;
  s.st.internal_index = s.st.offset_in_column;
}

bool Opened(const ColumnReader::ScanState& s) noexcept {
  return s.opened_block == s.window.block;
}

duckdb::Vector& Scratch(ColumnReader::ScanState& s,
                        const duckdb::LogicalType& type) {
  if (!s.scanned || s.scanned->vector.GetType() != type) {
    s.scanned = std::make_unique<ColumnReader::VectorScratch>(type);
  }
  return s.scanned->Reset();
}

}  // namespace

ColumnReader::ScanState::ScanState() = default;
ColumnReader::ScanState::ScanState(ScanState&&) = default;
ColumnReader::ScanState& ColumnReader::ScanState::operator=(ScanState&&) =
  default;
ColumnReader::ScanState::~ScanState() = default;

ColumnReader::ColumnReader(field_id id, duckdb::LogicalType type,
                           std::vector<ColumnBlockMeta> segments,
                           std::unique_ptr<ColumnReader> validity,
                           std::vector<std::unique_ptr<ColumnReader>> children)
  : _type{std::move(type)},
    _validity{std::move(validity)},
    _validity_view{_validity.get()},
    _children{std::move(children)},
    _array_size{_type.id() == duckdb::LogicalTypeId::ARRAY
                  ? duckdb::ArrayType::GetSize(_type)
                  : 0},
    _id{id},
    _segments{std::move(segments)} {
  _touched = std::make_unique<std::atomic<bool>[]>(_segments.size());
  auto stats = duckdb::BaseStatistics::CreateEmpty(
    _segments.empty() ? _type : _segments.front().statistics.GetType());
  _offsets.reserve(_segments.size() + 1);
  _offsets.push_back(0);
  for (const auto& m : _segments) {
    _row_count += m.tuple_count;
    _offsets.push_back(_row_count);
    stats.Merge(m.statistics);
  }
  FinishStats(std::move(stats));
}

void ColumnReader::FinishStats(duckdb::BaseStatistics stats) {
  if (_validity) {
    stats.Merge(_validity->MergedStatistics());
  } else if (_row_count > 0 && (!_children.empty() || _segments.empty())) {
    SDB_ASSERT(!stats.CanHaveNull(),
               "column without a validity payload carries null-bearing stats");
    stats.SetHasNoNull();
  }
  _stats = stats.ToUnique();
}

uint64_t ColumnReader::RowGroupEnd(uint64_t row) const noexcept {
  SDB_ASSERT(row < _row_count);
  return Locate(row).end;
}

BlockWindow ColumnReader::Locate(uint64_t row,
                                 BlockWindow hint) const noexcept {
  SDB_ASSERT(!_segments.empty(), "Locate on a column without own blocks");
  if (hint.end != 0) {
    if (row >= hint.begin && row < hint.end) {
      return hint;
    }
    const auto next = hint.block + 1;
    if (next < _segments.size() && row >= _offsets[next] &&
        row < _offsets[next + 1]) {
      return BlockWindow{next, _offsets[next], _offsets[next + 1]};
    }
  }
  SDB_ASSERT(row < _row_count);
  const auto it =
    BranchlessPartitionPoint(_offsets.data(), _offsets.size(),
                             [row](uint64_t offset) { return offset <= row; });
  const auto rg = static_cast<size_t>(it - _offsets.data()) - 1;
  return BlockWindow{rg, _offsets[rg], _offsets[rg + 1]};
}

bool ColumnReader::NextSegment(BlockWindow& w) const noexcept {
  const auto next = w.block + 1;
  if (next >= _segments.size()) {
    return false;
  }
  w.block = next;
  w.begin = _offsets[next];
  w.end = _offsets[next + 1];
  return true;
}

std::unique_ptr<codecs::ColumnSegment> ColumnReader::Open(size_t block,
                                                          ReadContext& ctx,
                                                          ScanState* s) const {
  Readahead(block, ctx, s);
  const auto& m = _segments[block];
  const auto& codec = *m.codec;
  auto segment = std::make_unique<codecs::ColumnSegment>(codecs::ColumnSegment{
    .function = *codec.decompression,
    .type = m.statistics.GetType(),
    .stats = m.statistics,
    .ctx = ctx,
    .count = m.tuple_count,
    .size = m.byte_size,
    .codec = codec.type,
    .shared_offset = m.shared_offset,
    .shared_size = m.shared_size,
  });
  if (m.byte_size == 0) {
    if (codec.type != codecs::CodecType::Constant &&
        codec.type != codecs::CodecType::Empty) [[unlikely]] {
      codecs::ThrowCorrupted(magic_enum::enum_name(codec.type),
                             "segment has no bytes");
    }
    return segment;
  }
  auto& slot = segment->cache_slot;
  slot.file_id = _file_id;
  slot.file_offset = m.file_offset;
  if (auto* cache = ctx.Cache()) {
    slot.touched = &_touched[block];
    slot.cache = cache;
  }
  segment->block = ctx.RegisterColBlock(m.file_offset, m.byte_size);
  return segment;
}

void ColumnReader::Readahead(size_t block, ReadContext& ctx,
                             ScanState* s) const noexcept {
  if (ctx.RandomAccess()) {
    return;
  }
  auto& in = ctx.In();
  const auto& first = _segments[block];
  if (s == nullptr) {
    file_utils::Hint(in, first.file_offset, first.byte_size);
    if (first.shared_size != 0) {
      file_utils::Hint(in, first.shared_offset, first.shared_size);
    }
    return;
  }
  const bool sequential = block != 0 && s->opened_block + 1 == block;
  if (block < s->advised_end || first.byte_size == 0) {
    return;
  }
  if (in.Resident(first.file_offset, first.byte_size)) {
    if (first.byte_size <= kTrustedRunBytes) {
      in.MarkWarm(first.file_offset, first.byte_size);
    }
    auto end = block + 1;
    auto tail = block;
    for (uint64_t budget = kTrustedRunBytes; end < _segments.size(); ++end) {
      const auto& next = _segments[end];
      if (next.byte_size > budget ||
          in.Warm(next.file_offset, next.byte_size) != next.byte_size) {
        break;
      }
      budget -= next.byte_size;
      if (next.byte_size != 0) {
        tail = end;
      }
    }
    if (tail != block &&
        !in.Resident(_segments[tail].file_offset, _segments[tail].byte_size)) {
      file_utils::InvalidateResidency();
      end = block + 1;
    }
    s->advised_end = end;
    return;
  }
  if (in.Warm(first.file_offset, first.byte_size) == first.byte_size) {
    file_utils::InvalidateResidency();
  }
  uint64_t budget = sequential ? file_utils::kMaxReadahead : 0;
  auto shared = std::numeric_limits<uint64_t>::max();
  auto b = block;
  do {
    const auto& m = _segments[b++];
    in.Prefetch(m.file_offset, m.byte_size);
    if (m.shared_size != 0 && m.shared_offset != shared) {
      shared = m.shared_offset;
      in.Prefetch(shared, m.shared_size);
    }
    const auto cost = std::max<uint64_t>(m.byte_size, file_utils::kPage);
    if (budget <= cost) {
      break;
    }
    budget -= cost;
  } while (b < _segments.size());
  s->advised_end = b;
}

ColumnReader::ScanState ColumnReader::InitScan(
  std::shared_ptr<ReadContext> ctx) const {
  ScanState s;
  s.ctx = std::move(ctx);
  if (!_segments.empty()) {
    s.window = BlockWindow{0, _offsets[0], _offsets[1]};
  }
  s.child_states.reserve(_children.size() + 1);
  s.child_states.push_back(_validity ? _validity->InitScan(s.ctx)
                                     : ScanState{});
  for (const auto& child : _children) {
    s.child_states.push_back(child->InitScan(s.ctx));
  }
  return s;
}

namespace {

struct ScannedStrings final : duckdb::AuxiliaryDataHolder {
  ScannedStrings(std::shared_ptr<ReadContext> ctx,
                 duckdb::BufferHandle pin) noexcept
    : ctx{std::move(ctx)}, pin{std::move(pin)} {}

  bool CertifiesImmutablePayloads() const final { return true; }

  std::shared_ptr<ReadContext> ctx;
  duckdb::BufferHandle pin;
};

void CertifyStrings(const ColumnReader::ScanState& s,
                    const codecs::ColumnSegment& segment,
                    duckdb::Vector& result) {
  SDB_ASSERT(BorrowsBlockStrings(segment));
  if (s.ctx.use_count() == 0) {
    return;
  }
  SDB_ASSERT(segment.block);
  SDB_ASSERT(result.GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
  duckdb::StringVector::AddAuxiliaryData(
    result, duckdb::make_uniq<ScannedStrings>(s.ctx, segment.Pin()));
}

}  // namespace

void ColumnReader::OpenScanSegment(ScanState& s) const {
  if (s.st.scan_state) {
    s.st.previous_states.emplace_back(std::move(s.st.scan_state));
  }
  const auto& segment =
    *s.segments.emplace_back(Open(s.window.block, *s.ctx, &s));
  s.opened_block = s.window.block;
  s.st.scan_state = segment.function.init_scan(segment);
  s.st.internal_index = 0;
}

void ColumnReader::BeginScanVector(ScanState& s) const {
  if (RowsLeft(s) == 0 && NextSegment(s.window)) {
    s.st.offset_in_column = 0;
  }
  if (!Opened(s)) {
    OpenScanSegment(s);
  }
  SDB_ASSERT(s.st.internal_index <= s.st.offset_in_column);
  if (s.st.internal_index < s.st.offset_in_column) {
    const auto& segment = *s.segments.back();
    if (segment.function.skip) {
      segment.function.skip(segment, s.st,
                            s.st.offset_in_column - s.st.internal_index);
    }
    s.st.internal_index = s.st.offset_in_column;
  }
}

namespace {

bool HasRetired(const ColumnReader::ScanState& s) noexcept {
  return !s.st.previous_states.empty() || !s.retired_fetches.empty() ||
         !s.retired_states.empty() || s.segments.size() > 1;
}

IRS_NO_INLINE void DropRetired(ColumnReader::ScanState& s) {
  s.st.previous_states.clear();
  s.retired_fetches.clear();
  s.retired_states.clear();
  if (s.segments.size() > 1) {
    s.segments.erase(s.segments.begin(), s.segments.end() - 1);
  }
}

}  // namespace

void ColumnReader::NewOutputVector(ScanState& s) const {
  if (HasRetired(s)) [[unlikely]] {
    DropRetired(s);
  }
  if (_validity) {
    _validity->ColumnReader::NewOutputVector(s.child_states[0]);
  }
  for (size_t i = 0; i < _children.size(); ++i) {
    _children[i]->NewOutputVector(s.child_states[i + 1]);
  }
}

duckdb::ScanVectorType ColumnReader::GetVectorScanType(
  ScanState& s, duckdb::idx_t count, duckdb::Vector& result) const {
  if (result.GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
    return duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR;
  }
  auto rows = RowsLeft(s);
  if (rows == 0 && s.window.block + 1 < _segments.size()) {
    rows = _segments[s.window.block + 1].tuple_count;
  }
  if (rows < count) {
    return duckdb::ScanVectorType::SCAN_FLAT_VECTOR;
  }
  if (!_validity) {
    return duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR;
  }
  return _validity->GetVectorScanType(s.child_states[0], count, result);
}

duckdb::idx_t ColumnReader::ScanVector(ScanState& s, duckdb::Vector& result,
                                       duckdb::idx_t remaining,
                                       duckdb::ScanVectorType scan_type,
                                       duckdb::idx_t base_result_offset) const {
  BeginScanVector(s);
  const auto initial = remaining;
  while (remaining > 0) {
    const auto scan_count = std::min<duckdb::idx_t>(remaining, RowsLeft(s));
    const auto result_offset = base_result_offset + (initial - remaining);
    if (scan_count > 0) {
      const auto& segment = *s.segments.back();
      SDB_ASSERT(scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR
                   ? result_offset == 0
                   : result.GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
      segment.function.scan(segment, s.st, scan_count, result, result_offset,
                            scan_type);
      if (BorrowsBlockStrings(segment)) [[unlikely]] {
        CertifyStrings(s, segment, result);
      }
      s.st.offset_in_column += scan_count;
      remaining -= scan_count;
    }
    if (remaining > 0) {
      if (!NextSegment(s.window)) {
        break;
      }
      s.st.offset_in_column = 0;
      OpenScanSegment(s);
    }
  }
  s.st.internal_index = s.st.offset_in_column;
  return initial - remaining;
}

duckdb::idx_t ColumnReader::ScanValues(ScanState& s, duckdb::Vector& result,
                                       duckdb::idx_t count,
                                       duckdb::ScanVectorType scan_type,
                                       duckdb::idx_t result_offset) const {
  const auto n = ScanVector(s, result, count, scan_type, result_offset);
  if (_validity) {
    SDB_ASSERT(!s.child_states.empty());
    _validity->ScanVector(s.child_states[0], result, count, scan_type,
                          result_offset);
  }
  return n;
}

duckdb::idx_t ColumnReader::Scan(ScanState& s, duckdb::Vector& result,
                                 duckdb::idx_t count) const {
  NewOutputVector(s);
  return ScanValues(s, result, count, GetVectorScanType(s, count, result), 0);
}

duckdb::idx_t ColumnReader::ScanCount(ScanState& s, duckdb::Vector& result,
                                      duckdb::idx_t count,
                                      duckdb::idx_t result_offset) const {
  return ScanValues(s, result, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR,
                    result_offset);
}

void ColumnReader::Skip(ScanState& s, duckdb::idx_t count) const {
  SkipRows(s, count);
  SkipValidity(s, count);
}

void ColumnReader::SkipValidity(ScanState& s, duckdb::idx_t count) const {
  if (_validity) {
    _validity->SkipRows(s.child_states[0], count);
  }
}

void ColumnReader::SkipTo(ScanState& s, uint64_t row) const {
  const uint64_t cur = ColumnReader::GatherCursor(s);
  SDB_ASSERT(row >= cur);
  if (row > cur) {
    ColumnReader::Skip(s, row - cur);
  }
}

void ColumnReader::FetchInto(ScanState& s, std::span<const uint64_t> rows,
                             duckdb::sel_t* positions, duckdb::Vector& out,
                             duckdb::idx_t at) const {
  SDB_ASSERT(rows.size() <= kMaxFetchHits);
  std::array<duckdb::idx_t, kMaxFetchHits> in_block;
  for (duckdb::idx_t i = 0; i < rows.size();) {
    if (!s.fetch.segment || rows[i] < s.fetch_window.begin ||
        rows[i] >= s.fetch_window.end) {
      if (s.fetch.segment) {
        s.retired_fetches.push_back(std::move(s.fetch));
        s.fetch = {};
      }
      s.fetch_window = Locate(rows[i], s.window);
      s.fetch.segment = Open(s.fetch_window.block, *s.ctx);
    }
    auto end = i;
    for (; end < rows.size() && rows[end] < s.fetch_window.end; ++end) {
      in_block[end - i] = rows[end] - s.fetch_window.begin;
    }
    FetchSegmentRows(*s.fetch.segment, s.fetch.state, in_block.data(), end - i,
                     out, duckdb::SelectionVector{positions + i, end - i}, at);
    i = end;
  }
}

bool ColumnReader::FetchSparse(ScanState& s, uint64_t anchor,
                               const duckdb::SelectionVector& sel,
                               duckdb::idx_t hits, duckdb::Vector& out,
                               duckdb::idx_t at) const {
  const uint64_t first = anchor + sel.get_index(0);
  const uint64_t last = anchor + sel.get_index(hits - 1);
  if (hits >= kMaxFetchHits || last >= _row_count) {
    return false;
  }
  const auto window = Locate(first, s.window);
  if (Opened(s) && window.block == s.window.block) {
    return false;
  }
  SkipTo(s, first);
  if (at == 0) {
    out.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  }
  auto& mask = duckdb::FlatVector::ValidityMutable(out);
  std::array<uint64_t, kMaxFetchHits> rows;
  std::array<duckdb::sel_t, kMaxFetchHits> positions;
  for (duckdb::idx_t i = 0; i < hits; ++i) {
    rows[i] = anchor + sel.get_index(i);
    positions[i] = static_cast<duckdb::sel_t>(i);
    mask.SetValid(at + i);
  }
  auto valid = hits;
  if (_validity) {
    _validity->FetchInto(s.child_states[0], {rows.data(), hits},
                         positions.data(), out, at);
    valid = 0;
    for (duckdb::idx_t i = 0; i < hits; ++i) {
      if (mask.RowIsValid(at + i)) {
        rows[valid] = rows[i];
        positions[valid] = static_cast<duckdb::sel_t>(i);
        ++valid;
      }
    }
  }
  FetchInto(s, {rows.data(), valid}, positions.data(), out, at);
  ColumnReader::Skip(s, last + 1 - first);
  return true;
}

void ColumnReader::GatherScatter(ScanState& s, uint64_t anchor,
                                 const duckdb::SelectionVector& sel,
                                 duckdb::idx_t hits, duckdb::Vector& out,
                                 duckdb::idx_t at) const {
  if (at == 0) {
    NewOutputVector(s);
  }
  if (FetchSparse(s, anchor, sel, hits, out, at)) {
    return;
  }
  const auto span = sel.get_index(hits - 1) - sel.get_index(0) + 1;
  const bool windows = ScansWindows(_type);
  for (duckdb::idx_t i = 0; i < hits;) {
    const auto first = sel.get_index(i);
    if (anchor + first >= _row_count) {
      column_internal::ScatterRuns(*this, s, anchor, sel, i, hits, out, at);
      return;
    }
    auto end = i + 1;
    if (!windows) {
      const auto block = Locate(anchor + first, s.window);
      if (!SelectsNatively(_segments[block.block], _type, hits, span)) {
        if (anchor + sel.get_index(hits - 1) < block.end) {
          end = hits;
        }
        while (end < hits && anchor + sel.get_index(end) < block.end) {
          ++end;
        }
        column_internal::ScatterRuns(*this, s, anchor, sel, i, end, out, at);
        i = end;
        continue;
      }
    }
    while (end < hits && sel.get_index(end) - first < STANDARD_VECTOR_SIZE) {
      ++end;
    }
    const auto* window = &sel;
    if (first != 0) {
      auto& local = column_internal::Allocated(s.window_sel);
      for (auto k = i; k < end; ++k) {
        local.set_index(k - i, sel.get_index(k) - first);
      }
      window = &local;
    }
    if ((i == 0 && end == hits) ||
        !FetchSparse(s, anchor + first, *window, end - i, out, at + i)) {
      GatherWindow(s, anchor + first, *window, end - i,
                   sel.get_index(end - 1) - first + 1, out, at + i);
    }
    i = end;
  }
}

void ColumnReader::GatherWindow(ScanState& s, uint64_t anchor,
                                const duckdb::SelectionVector& sel,
                                duckdb::idx_t hits, duckdb::idx_t span,
                                duckdb::Vector& out, duckdb::idx_t at) const {
  SDB_ASSERT(hits > 0 && hits <= span && span <= STANDARD_VECTOR_SIZE);
  if (hits != span && anchor < _row_count &&
      !ScatterWins(_segments[Locate(anchor, s.window).block], _type, hits,
                   span)) {
    SkipTo(s, anchor);
    if (SelectNative(s, sel, hits, span, out, at,
                     duckdb::ScanVectorType::SCAN_FLAT_VECTOR)) {
      return;
    }
    if (ScansWindows(_type)) {
      SelectScanned(s, sel, hits, span, out, at);
      return;
    }
  }
  column_internal::ScatterRuns(*this, s, anchor, sel, 0, hits, out, at);
}

void ColumnReader::SelectScanned(ScanState& s,
                                 const duckdb::SelectionVector& sel,
                                 duckdb::idx_t hits, duckdb::idx_t span,
                                 duckdb::Vector& out, duckdb::idx_t at) const {
  auto& window = Scratch(s, out.GetType());
  ColumnReader::ScanCount(s, window, span, 0);
  out.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::VectorOperations::Copy(window, out, sel, hits, 0, at);
}

void ColumnReader::GatherDense(ScanState& s, uint64_t anchor,
                               const duckdb::SelectionVector& sel,
                               duckdb::idx_t hits, duckdb::idx_t span,
                               duckdb::Vector& out) const {
  SDB_ASSERT(hits > 0 && hits <= span && span <= STANDARD_VECTOR_SIZE);
  NewOutputVector(s);
  if (FetchSparse(s, anchor, sel, hits, out, 0)) {
    return;
  }
  SkipTo(s, anchor);
  if (hits == span) {
    ColumnReader::Scan(s, out, span);
    return;
  }
  if (anchor < _row_count &&
      ScatterWins(_segments[Locate(anchor, s.window).block], _type, hits,
                  span)) {
    column_internal::ScatterRuns(*this, s, anchor, sel, 0, hits, out, 0);
    return;
  }
  if (SelectNative(s, sel, hits, span, out, 0,
                   duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR)) {
    return;
  }
  if (FewStringRuns(_type, sel, hits, span)) {
    column_internal::ScatterRuns(*this, s, anchor, sel, 0, hits, out, 0);
    return;
  }
  ColumnReader::ScanCount(s, out, span, 0);
  out.Slice(sel, hits);
}

bool ColumnReader::SelectsAcross(const ScanState& s,
                                 duckdb::idx_t span) const noexcept {
  const uint64_t end = s.window.begin + s.st.offset_in_column + span;
  if (_type.InternalType() != duckdb::PhysicalType::VARCHAR ||
      end > _offsets.back()) {
    return false;
  }
  for (size_t block = s.window.block; _offsets[block] < end; ++block) {
    const auto type = _segments[block].codec->type;
    if (!codecs::IsText(type) ||
        codecs::ShapeOf(type) != codecs::Shape::Plain) {
      return false;
    }
  }
  return true;
}

void ColumnReader::SelectAcross(ScanState& s,
                                const duckdb::SelectionVector& sel,
                                duckdb::idx_t hits, duckdb::idx_t span,
                                duckdb::Vector& out, duckdb::idx_t at) const {
  out.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto& piece = column_internal::Allocated(s.piece);
  duckdb::idx_t row = 0;
  for (duckdb::idx_t i = 0; i < hits;) {
    SkipRows(s, sel.get_index(i) - row);
    row = sel.get_index(i);
    BeginScanVector(s);
    const auto rows = std::min<duckdb::idx_t>(span - row, RowsLeft(s));
    auto end = i + 1;
    while (end < hits && sel.get_index(end) - row < rows) {
      ++end;
    }
    for (auto k = i; k < end; ++k) {
      piece.set_index(k - i, sel.get_index(k) - row);
    }
    const auto& segment = *s.segments.back();
    segment.function.select(segment, s.st, rows, out, at + i, piece, end - i,
                            duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    const auto taken = sel.get_index(end - 1) + 1 - row;
    Advance(s, taken);
    row += taken;
    i = end;
  }
  SkipRows(s, span - row);
}

bool ColumnReader::SelectNative(ScanState& s,
                                const duckdb::SelectionVector& sel,
                                duckdb::idx_t hits, duckdb::idx_t span,
                                duckdb::Vector& out, duckdb::idx_t at,
                                duckdb::ScanVectorType scan_type) const {
  BeginScanVector(s);
  const bool within = RowsLeft(s) >= span;
  const auto& codec = *_segments[s.window.block].codec;
  const auto& function = *codec.decompression;
  if (hits * 1000 > BandsFor(codec.type, _type).native * span ||
      !function.select || (!within && !SelectsAcross(s, span))) {
    return false;
  }
  const bool self_valid =
    function.validity == codecs::CompressionValidity::NoValidityRequired;
  if (_validity && !self_valid) {
    auto& vs = s.child_states[0];
    _validity->BeginScanVector(vs);
    if (RowsLeft(vs) < span ||
        !_validity->_segments[vs.window.block].codec->decompression->select) {
      return false;
    }
  }
  if (within) {
    const auto& segment = *s.segments.back();
    segment.function.select(segment, s.st, span, out, at, sel, hits, scan_type);
    Advance(s, span);
  } else {
    SelectAcross(s, sel, hits, span, out, at);
  }
  if (self_valid) {
    SkipValidity(s, span);
  } else if (_validity) {
    auto& vs = s.child_states[0];
    const auto& validity = *vs.segments.back();
    validity.function.select(validity, vs.st, span, out, at, sel, hits,
                             scan_type);
    Advance(vs, span);
  }
  return true;
}

duckdb::idx_t ColumnReader::GatherFilter(
  ScanState& s, uint64_t anchor, duckdb::idx_t span,
  duckdb::SelectionVector& sel, duckdb::idx_t sel_count,
  const duckdb::TableFilter& filter, duckdb::TableFilterState& filter_state,
  NullCheckKind null_check, duckdb::Vector& result) const {
  NewOutputVector(s);
  SDB_ASSERT(span <= STANDARD_VECTOR_SIZE);
  SkipTo(s, anchor);
  BeginScanVector(s);
  if (FiltersByBlock(s, anchor, span, null_check)) {
    return FilterBlocks(s, anchor, span, sel, sel_count, filter, filter_state,
                        null_check, result);
  }
  return FilterBlock(s, anchor, span, sel, sel_count, filter, filter_state,
                     null_check, result);
}

bool ColumnReader::FiltersByBlock(const ScanState& s, uint64_t anchor,
                                  duckdb::idx_t span,
                                  NullCheckKind null_check) const noexcept {
  const uint64_t end = anchor + span;
  if (end <= s.window.end || end > _offsets.back()) {
    return false;
  }
  if (null_check != NullCheckKind::None) {
    return true;
  }
  for (size_t block = s.window.block; _offsets[block] < end; ++block) {
    if (_segments[block].codec->decompression->filter) {
      return true;
    }
  }
  return false;
}

IRS_NO_INLINE duckdb::idx_t ColumnReader::FilterBlocks(
  ScanState& s, uint64_t anchor, duckdb::idx_t span,
  duckdb::SelectionVector& sel, duckdb::idx_t sel_count,
  const duckdb::TableFilter& filter, duckdb::TableFilterState& filter_state,
  NullCheckKind null_check, duckdb::Vector& result) const {
  auto& piece = column_internal::Allocated(s.piece);
  auto& rows = column_internal::Allocated(s.window_sel);
  for (duckdb::idx_t i = 0; i < sel_count; ++i) {
    rows.set_index(i, sel.get_index(i));
  }
  ResetAllValid(result, span);
  const uint64_t end = anchor + span;
  duckdb::idx_t kept = 0;
  duckdb::idx_t next = 0;
  size_t block = s.window.block;
  for (uint64_t pos = anchor; pos < end; ++block) {
    const auto count =
      static_cast<duckdb::idx_t>(std::min(end, _offsets[block + 1]) - pos);
    const auto base = static_cast<duckdb::idx_t>(pos - anchor);
    pos += count;
    duckdb::idx_t offered = 0;
    for (; next < sel_count && rows.get_index(next) < base + count; ++next) {
      piece.set_index(offered++, rows.get_index(next) - base);
    }
    if (offered == 0) {
      ColumnReader::Skip(s, count);
      continue;
    }
    BeginScanVector(s);
    auto& part = Scratch(s, result.GetType());
    duckdb::SelectionVector part_rows{piece};
    const auto approved =
      FilterBlock(s, anchor + base, count, part_rows, offered, filter,
                  filter_state, null_check, part);
    for (duckdb::idx_t i = 0; i < approved;) {
      const auto first = part_rows.get_index(i);
      auto run = i + 1;
      while (run < approved && part_rows.get_index(run) == first + (run - i)) {
        ++run;
      }
      if (null_check == NullCheckKind::None) {
        duckdb::ImmutableStrings::Copy(part, result, first + (run - i), first,
                                       base + first);
      }
      for (; i < run; ++i) {
        rows.set_index(kept++, base + part_rows.get_index(i));
      }
    }
  }
  for (duckdb::idx_t i = 0; i < kept; ++i) {
    sel.set_index(i, rows.get_index(i));
  }
  return kept;
}

duckdb::idx_t ColumnReader::FilterBlock(
  ScanState& s, uint64_t anchor, duckdb::idx_t span,
  duckdb::SelectionVector& sel, duckdb::idx_t sel_count,
  const duckdb::TableFilter& filter, duckdb::TableFilterState& filter_state,
  NullCheckKind null_check, duckdb::Vector& result) const {
  const auto& seg = *s.segments.back();
  const auto& function = seg.function;
  duckdb::idx_t approved = sel_count;
  const bool within_segment = RowsLeft(s) >= span;
  const bool self_valid =
    function.validity == codecs::CompressionValidity::NoValidityRequired;
  const bool codec_filter =
    within_segment && function.filter && !filter_state.can_throw &&
    (self_valid || (null_check == NullCheckKind::None &&
                    ValiditySpanAllValid(s, anchor, span)));
  if (codec_filter) {
    if (!self_valid) {
      ResetAllValid(result, span);
    }
    function.filter(seg, s.st, span, result, sel, approved, filter,
                    filter_state);
    Advance(s, span);
    SkipValidity(s, span);
  } else if (within_segment && null_check != NullCheckKind::None &&
             !self_valid) {
    SkipRows(s, span);
    auto& mask = ResetAllValid(result, span);
    if (_validity) {
      SDB_ASSERT(!s.child_states.empty());
      _validity->ScanVector(s.child_states[0], result, span,
                            duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    }
    const bool want_valid = null_check == NullCheckKind::IsNotNull;
    duckdb::idx_t kept = 0;
    for (duckdb::idx_t k = 0; k < sel_count; ++k) {
      const auto idx = sel.get_index(k);
      if (mask.RowIsValid(idx) == want_valid) {
        sel.set_index(kept++, idx);
      }
    }
    approved = kept;
  } else {
    ResetAllValid(result, span);
    ColumnReader::ScanCount(s, result, span, 0);
    duckdb::ColumnSegment::FilterSelection(sel, result, filter_state, span,
                                           approved);
  }
  return approved;
}

bool ColumnReader::ValiditySpanAllValid(ScanState& s, uint64_t anchor,
                                        duckdb::idx_t span) const {
  if (!_validity) {
    return true;
  }
  SDB_ASSERT(!s.child_states.empty());
  const auto w = _validity->Locate(anchor, s.child_states[0].window);
  return anchor + span <= w.end &&
         _validity->_segments[w.block].codec->type == codecs::CodecType::Empty;
}

void ColumnReader::SkipRows(ScanState& s, duckdb::idx_t count) const {
  if (count < RowsLeft(s)) {
    s.st.offset_in_column += count;
    return;
  }
  if (count == 0 || _segments.empty()) {
    return;
  }
  const uint64_t end = _offsets.back();
  const uint64_t target =
    std::min<uint64_t>(s.window.begin + s.st.offset_in_column + count, end);
  const auto window =
    target < end
      ? Locate(target, s.window)
      : BlockWindow{_segments.size() - 1, _offsets[_segments.size() - 1], end};
  s.window = window;
  s.st.offset_in_column = target - s.window.begin;
}

std::unique_ptr<ColumnReader> ColumnReader::Make(ColumnMeta&& meta,
                                                 uint64_t file_id) {
  const auto id = meta.type.id();
  const bool fieldless =
    meta.children.empty() &&
    (id == duckdb::LogicalTypeId::STRUCT ||
     id == duckdb::LogicalTypeId::TUPLE || id == duckdb::LogicalTypeId::UNION);
  std::unique_ptr<ColumnReader> validity;
  if ((fieldless && !meta.validity.empty()) ||
      absl::c_any_of(meta.validity, [](const ColumnBlockMeta& m) {
        return m.codec->type != codecs::CodecType::Empty;
      })) {
    validity.reset(new ColumnReader{
      meta.id,
      duckdb::LogicalTypeId::VALIDITY,
      std::move(meta.validity),
      nullptr,
      {},
    });
    validity->_file_id = file_id;
  }

  std::vector<std::unique_ptr<ColumnReader>> children;
  children.reserve(meta.children.size());
  for (auto& c : meta.children) {
    children.push_back(Make(std::move(c), file_id));
  }

  std::unique_ptr<ColumnReader> col;
  switch (meta.type.id()) {
    case duckdb::LogicalTypeId::VARIANT:
      SDB_ASSERT(children.empty());
      col = std::make_unique<VariantColumnReader>(
        meta.id, std::move(meta.type), std::move(validity),
        std::move(meta.variant_rgs), file_id);
      break;
    case duckdb::LogicalTypeId::UNION:
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::TUPLE:
      col = std::make_unique<StructColumnReader>(meta.id, std::move(meta.type),
                                                 std::move(validity),
                                                 std::move(children));
      break;
    case duckdb::LogicalTypeId::ARRAY:
      col = std::make_unique<ArrayColumnReader>(meta.id, std::move(meta.type),
                                                std::move(validity),
                                                std::move(children));
      break;
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::MAP:
      col = std::make_unique<ListColumnReader>(
        meta.id, std::move(meta.type), std::move(meta.data),
        std::move(validity), std::move(children));
      break;
    default:
      col.reset(new ColumnReader{
        meta.id,
        std::move(meta.type),
        std::move(meta.data),
        std::move(validity),
        std::move(children),
      });
      break;
  }
  col->_distinct = std::move(meta.distinct);
  col->_compression_level = meta.compression_level;
  col->_file_id = file_id;
  return col;
}

}  // namespace irs
