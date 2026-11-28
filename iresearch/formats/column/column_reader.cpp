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
#include <bit>
#include <duckdb/common/page_residency.hpp>
#include <duckdb/common/type_visitor.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <duckdb/storage/statistics/array_stats.hpp>
#include <duckdb/storage/statistics/list_stats.hpp>
#include <duckdb/storage/statistics/struct_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <utility>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/internal/column_reader_scan.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/formats/column/variant_column_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/bitset.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

using column_internal::BorrowsBlockStrings;
using column_internal::FetchSegmentRows;

constexpr uint64_t kTrustedRunBytes = 256 * duckdb::PageResidency::PAGE_SIZE;
constexpr size_t kBlocksAhead = 32;

bool BytesInMemory(const duckdb::InputFile& file, uint64_t offset,
                   uint64_t size) noexcept {
  return file.IsPaged()
           ? duckdb::PageResidency::IsResident(file.Data() + offset, size)
           : file.IsResident(offset, size);
}

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
  return block.codec->decompression.select &&
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
    s.scanned =
      std::make_unique<ColumnReader::VectorScratch>(type, s.ctx->Allocator());
  }
  return s.scanned->Reset();
}

}  // namespace

struct ListRun {
  uint64_t begin = 0;
  uint64_t end = 0;
};

struct ListDictionary {
  explicit ListDictionary(TrackedAllocator<> alloc) : ends{alloc} {}

  uint64_t begin = 0;
  uint64_t end = 0;
  uint64_t missed_block = 0;
  uint64_t oversized_block = 0;
  uint64_t ends_block = 0;
  uint64_t ends_base = 0;
  uint64_t ends_begin = 0;
  uint64_t ends_end = 0;
  duckdb::buffer_ptr<duckdb::DictionaryEntry> lists;
  bitset decoded;
  std::vector<uint32_t> ranks;
  std::vector<ListRun> runs;
  TrackedScratchVector<uint64_t> ends;
};

struct ListScanState {
  explicit ListScanState(TrackedAllocator<> alloc) : dictionary{alloc} {}

  std::unique_ptr<ColumnReader::VectorScratch> codes;
  std::unique_ptr<ColumnReader::VectorScratch> ends;
  ListDictionary dictionary;
  std::vector<duckdb::sel_t> picked;
  duckdb::SelectionVector sel;
};

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

void ColumnReader::CollectBlocks(uint64_t first, uint64_t last,
                                 std::vector<duckdb::FileRange>& out) const {
  if (!_segments.empty() && first < _row_count) {
    const auto from = Locate(first).block;
    const auto to = Locate(std::min(last, _row_count - 1)).block;
    auto shared = std::numeric_limits<uint64_t>::max();
    for (auto block = from; block <= to; ++block) {
      const auto& m = _segments[block];
      if (m.shared_size != 0 && m.shared_offset != shared) {
        shared = m.shared_offset;
        out.push_back({.offset = shared, .size = m.shared_size});
      }
      if (m.byte_size != 0) {
        out.push_back({.offset = m.file_offset, .size = m.byte_size});
      }
    }
  }
  if (_validity) {
    _validity->CollectBlocks(first, last, out);
  }
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
    .function = codec.decompression,
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
  slot.touched = &_touched[block];
  slot.cache = ctx.Cache();
  segment->block = ctx.RegisterColBlock(m.file_offset, m.byte_size);
  return segment;
}

void ColumnReader::Readahead(size_t block, ReadContext& ctx,
                             ScanState* s) const noexcept {
  const auto* file = ctx.File();
  if (ctx.RandomAccess() || file == nullptr) {
    return;
  }
  const auto& first = _segments[block];
  if (s == nullptr) {
    file->Hint(first.file_offset, first.byte_size);
    if (first.shared_size != 0) {
      file->Hint(first.shared_offset, first.shared_size);
    }
    return;
  }
  const bool sequential = block != 0 && s->opened_block + 1 == block;
  if (block < s->advised_end || first.byte_size == 0) {
    return;
  }
  if (first.byte_size <= kTrustedRunBytes &&
      file->Warm(first.file_offset, first.byte_size) == first.byte_size) {
    auto end = block + 1;
    auto tail = block;
    for (auto budget = kTrustedRunBytes - first.byte_size;
         end < _segments.size(); ++end) {
      const auto& next = _segments[end];
      if (next.byte_size > budget ||
          file->Warm(next.file_offset, next.byte_size) != next.byte_size) {
        break;
      }
      budget -= next.byte_size;
      if (next.byte_size != 0) {
        tail = end;
      }
    }
    const auto& last = _segments[tail];
    if (BytesInMemory(*file, first.file_offset, 1) &&
        BytesInMemory(*file, last.file_offset + last.byte_size - 1, 1)) {
      s->advised_end = end;
      return;
    }
    if (file->IsPaged()) {
      duckdb::PageResidency::Invalidate();
    }
  }
  if (BytesInMemory(*file, first.file_offset, first.byte_size)) {
    file->MarkWarm(first.file_offset, first.byte_size);
    s->advised_end = block + 1;
    return;
  }
  if (file->IsPaged() &&
      file->Warm(first.file_offset, first.byte_size) == first.byte_size) {
    duckdb::PageResidency::Invalidate();
  }
  if (!sequential) {
    file->Prefetch(first.file_offset, first.byte_size);
    if (first.shared_size != 0) {
      file->Prefetch(first.shared_offset, first.shared_size);
    }
    s->advised_end = block + 1;
    return;
  }
  containers::SmallVector<duckdb::FileRange, kBlocksAhead> ahead;
  uint64_t budget = duckdb::InputFile::MAX_HINT;
  auto shared = std::numeric_limits<uint64_t>::max();
  auto b = block;
  do {
    const auto& m = _segments[b++];
    if (m.shared_size != 0 && m.shared_offset != shared) {
      shared = m.shared_offset;
      ahead.push_back({.offset = shared, .size = m.shared_size});
    }
    if (m.byte_size != 0) {
      ahead.push_back({.offset = m.file_offset, .size = m.byte_size});
    }
    const auto cost =
      std::max<uint64_t>(m.byte_size, duckdb::PageResidency::PAGE_SIZE);
    if (budget <= cost) {
      break;
    }
    budget -= cost;
  } while (b < _segments.size());
  if (file->IsMapped()) {
    for (const auto& range : ahead) {
      file->Prefetch(range.offset, range.size);
    }
  } else {
    file->Hint(ahead);
  }
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
  SDB_ASSERT(rows.size() <= codecs::kSparseRows);
  std::array<duckdb::idx_t, codecs::kSparseRows> in_block;
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
  if (hits >= codecs::kSparseRows || last >= _row_count) {
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
  std::array<uint64_t, codecs::kSparseRows> rows;
  std::array<duckdb::sel_t, codecs::kSparseRows> positions;
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
  SDB_ASSERT(hits > 0 && hits <= span && span <= STANDARD_VECTOR_SIZE &&
             anchor < _row_count);
  if (hits != span && !ScatterWins(_segments[Locate(anchor, s.window).block],
                                   _type, hits, span)) {
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
  const auto& function = codec.decompression;
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
        !_validity->_segments[vs.window.block].codec->decompression.select) {
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
    if (_segments[block].codec->decompression.filter) {
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

void KeepReadahead(const ColumnReader::ScanState& from,
                   ColumnReader::ScanState& to) noexcept {
  SDB_ASSERT(from.child_states.size() == to.child_states.size());
  to.advised_end = from.advised_end;
  for (size_t i = 0; i != to.child_states.size(); ++i) {
    KeepReadahead(from.child_states[i], to.child_states[i]);
  }
}

class ListColumnReader final : public ColumnReader {
 public:
  ListColumnReader(field_id id, duckdb::LogicalType type,
                   std::vector<ColumnBlockMeta> segments,
                   std::unique_ptr<ColumnReader> validity,
                   std::vector<std::unique_ptr<ColumnReader>> children)
    : ColumnReader{id, std::move(type), std::move(segments),
                   std::move(validity), std::move(children)} {
    SDB_ENSURE(_children.size() == 2, "list column: corrupted list ends");
    _element_strings = duckdb::TypeVisitor::Contains(
      _children.front()->Type(), [](const duckdb::LogicalType& type) {
        return type.InternalType() == duckdb::PhysicalType::VARCHAR;
      });
    auto stats = duckdb::ListStats::CreateEmpty(_type);
    duckdb::ListStats::SetChildStats(
      stats, _children.front()->MergedStatistics().ToUnique());
    FinishStats(std::move(stats));
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final {
    NewOutputVector(s);
    return ScanCodes<true>(s, result, count, 0);
  }

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final {
    return ScanCodes<false>(s, result, count, result_offset);
  }

  void GatherScatter(ScanState& s, uint64_t anchor,
                     const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                     duckdb::Vector& out, duckdb::idx_t at) const final {
    if (at == 0) {
      NewOutputVector(s);
    }
    auto& codes_vec = ResetScratch(List(s).codes, duckdb::LogicalType::UBIGINT,
                                   s.ctx->Allocator(), hits);
    uint64_t pos = GatherCursor(s);
    for (duckdb::idx_t i = 0; i < hits;) {
      const uint64_t target = anchor + sel.get_index(i);
      SDB_ASSERT(target >= pos);
      const auto run = column_internal::ConsecutiveRows(sel, i, hits);
      if (target > pos) {
        ColumnReader::Skip(s, static_cast<duckdb::idx_t>(target - pos));
      }
      ScanVector(s, codes_vec, run, duckdb::ScanVectorType::SCAN_FLAT_VECTOR,
                 i);
      if (_validity) {
        _validity->ColumnReader::ScanCount(s.child_states[0], out, run, at + i);
      }
      pos = target + run;
      i += run;
    }
    ResolveCodes<false>(
      s, out, {duckdb::FlatVector::GetData<uint64_t>(codes_vec), hits}, at);
  }

  IRS_COLUMN_READER_GATHER_DENSE

 private:
  static constexpr duckdb::idx_t kPointRows = 16;
  static constexpr uint64_t kReadAheadLists = 2048;
  static constexpr uint64_t kWholeBlockElems = 1 << 19;
  static constexpr uint64_t kWholeBlockLists = 8192;
  static constexpr uint64_t kRunGapLists = 16;
  static constexpr uint64_t kEndsPerSeek = 1024;
  static constexpr uint64_t kAnyElems = std::numeric_limits<uint64_t>::max();

  bool Reusable(const ScanState& s) const noexcept {
    return !_element_strings || s.ctx.use_count() != 0;
  }

  IRS_NO_INLINE static void NewListState(ScanState& s) {
    s.list = std::make_unique<ListScanState>(s.ctx->Allocator());
  }

  static ListScanState& List(ScanState& s) {
    if (!s.list) [[unlikely]] {
      NewListState(s);
    }
    return *s.list;
  }

  void NewOutputVector(ScanState& s) const final {
    ColumnReader::NewOutputVector(s);
    if (s.list && !Reusable(s)) {
      s.list->dictionary.lists.reset();
    }
  }

  duckdb::buffer_ptr<duckdb::DictionaryEntry> NewLists(uint64_t count) const {
    const auto n = static_cast<duckdb::idx_t>(count + 1);
    auto dictionary =
      duckdb::make_buffer<duckdb::DictionaryEntry>(duckdb::Vector{_type, n});
    duckdb::FlatVector::ValidityMutable(dictionary->data).SetInvalid(0);
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(
      dictionary->data)[0] = duckdb::list_entry_t{0, 0};
    return dictionary;
  }

  void SeekChild(ScanState& s, size_t child, uint64_t target) const {
    const auto& reader = *_children[child];
    auto& state = s.child_states[child + 1];
    uint64_t pos = reader.GatherCursor(state);
    if (target < pos) {
      auto fresh = reader.InitScan(s.ctx);
      KeepReadahead(state, fresh);
      s.retired_states.push_back(std::move(state));
      state = std::move(fresh);
      pos = 0;
    }
    if (target > pos) {
      reader.Skip(state, static_cast<duckdb::idx_t>(target - pos));
    }
  }

  struct ListEnds {
    uint64_t first_elem = 0;
    const uint64_t* ends = nullptr;
  };

  static uint64_t DecodedStart(const ListDictionary& d,
                               uint64_t list) noexcept {
    return list == 0 ? 0 : d.ends[list - 1 - d.ends_base];
  }

  static bool EndsDecoded(const ListDictionary& d, uint64_t begin,
                          uint64_t end) noexcept {
    return d.ends_block != 0 && begin >= d.ends_begin && end <= d.ends_end;
  }

  ListEnds ReadEnds(ScanState& s, uint64_t first, uint64_t count) const {
    auto& list = List(s);
    if (const auto& d = list.dictionary; EndsDecoded(d, first, first + count)) {
      return {DecodedStart(d, first), d.ends.data() + (first - d.ends_base)};
    }
    const bool chained =
      first == 0 || first == _children[1]->GatherCursor(s.child_states[2]);
    const uint64_t from = chained ? first : first - 1;
    SeekChild(s, 1, from);
    const uint64_t end_count = first + count - from;
    auto& ends_vec = ResetScratch(list.ends, duckdb::LogicalType::UBIGINT,
                                  s.ctx->Allocator(), end_count);
    _children[1]->ScanCount(s.child_states[2], ends_vec,
                            static_cast<duckdb::idx_t>(end_count), 0);
    const auto* ends = duckdb::FlatVector::GetData<uint64_t>(ends_vec);
    const auto* list_ends = ends + (end_count - count);
    const uint64_t first_elem =
      first == 0 ? 0 : (chained ? s.st.last_offset : ends[0]);
    s.st.last_offset = list_ends[count - 1];
    return {first_elem, list_ends};
  }

  IRS_NO_INLINE void DecodeEnds(ScanState& s, ListDictionary& d, uint64_t lo,
                                uint64_t hi, uint64_t seeks) const {
    const auto w = _children[1]->Locate(lo);
    const uint64_t key = w.block + 1;
    if (hi >= w.end || d.ends_block == key ||
        seeks * kEndsPerSeek < w.end - w.begin) {
      return;
    }
    const uint64_t base = w.begin == 0 ? 0 : w.begin - 1;
    d.ends.resize(w.end - base);
    SeekChild(s, 1, base);
    auto& scratch = List(s).ends;
    for (uint64_t pos = 0; pos < d.ends.size();) {
      const auto take =
        std::min<uint64_t>(d.ends.size() - pos, STANDARD_VECTOR_SIZE);
      auto& chunk = ResetScratch(scratch, duckdb::LogicalType::UBIGINT,
                                 s.ctx->Allocator());
      _children[1]->ScanCount(s.child_states[2], chunk,
                              static_cast<duckdb::idx_t>(take), 0);
      std::copy_n(duckdb::FlatVector::GetData<uint64_t>(chunk), take,
                  d.ends.data() + pos);
      pos += take;
    }
    s.st.last_offset = d.ends.back();
    d.ends_block = key;
    d.ends_base = base;
    d.ends_begin = w.begin;
    d.ends_end = w.end;
  }

  bool ReadLists(ScanState& s, uint64_t first, uint64_t count,
                 duckdb::Vector& out, duckdb::list_entry_t* entries,
                 uint64_t child_base, uint64_t max_elems) const {
    SDB_ENSURE(first + count <= _children[1]->RowCount(),
               "list column: corrupted list codes");
    const auto [first_elem, list_ends] = ReadEnds(s, first, count);
    const uint64_t last_elem = list_ends[count - 1];
    SDB_ENSURE(first_elem <= last_elem && last_elem <= _children[0]->RowCount(),
               "list column: corrupted list ends");
    const uint64_t elem_count = last_elem - first_elem;
    if (elem_count > max_elems) {
      return false;
    }
    uint64_t prev = first_elem;
    bool ordered = true;
    for (uint64_t k = 0; k < count; ++k) {
      ordered &= list_ends[k] >= prev;
      entries[k] = duckdb::list_entry_t{child_base + (prev - first_elem),
                                        list_ends[k] - prev};
      prev = list_ends[k];
    }
    SDB_ENSURE(ordered, "list column: corrupted list ends");
    duckdb::ListVector::Reserve(
      out, static_cast<duckdb::idx_t>(child_base + elem_count));
    if (elem_count > 0) {
      SeekChild(s, 0, first_elem);
      _children[0]->ScanCount(s.child_states[1],
                              duckdb::ListVector::GetChildMutable(out),
                              static_cast<duckdb::idx_t>(elem_count),
                              static_cast<duckdb::idx_t>(child_base));
    }
    duckdb::ListVector::SetListSize(
      out, static_cast<duckdb::idx_t>(child_base + elem_count));
    return true;
  }

  IRS_NO_INLINE bool DirectRun(const ScanState& s, uint64_t first,
                               duckdb::idx_t count) const noexcept {
    if (first < _children[1]->GatherCursor(s.child_states[2])) {
      return false;
    }
    SDB_ASSERT(s.list);
    const auto& d = s.list->dictionary;
    return !(d.lists && first >= d.begin && first + count - 1 < d.end);
  }

  IRS_NO_INLINE bool WidenToBlock(ListDictionary& d, uint64_t lo, uint64_t hi,
                                  uint64_t& first, uint64_t& last) const {
    const auto w = _children[1]->Locate(lo);
    const uint64_t key = w.block + 1;
    if (hi >= w.end || w.end - w.begin > kWholeBlockLists ||
        d.oversized_block == key) {
      return false;
    }
    if (d.missed_block != key) {
      d.missed_block = key;
      return false;
    }
    first = w.begin;
    last = w.end - 1;
    return true;
  }

  static bool Covers(const ListDictionary& d, std::span<const uint64_t> codes,
                     uint64_t lo, uint64_t hi) noexcept {
    if (!d.lists || lo < d.begin || hi >= d.end) {
      return false;
    }
    return d.decoded.size() == 0 || absl::c_all_of(codes, [&](uint64_t code) {
             return d.decoded.test(code - d.begin);
           });
  }

  static uint64_t NextBit(const bitset& bits, uint64_t pos, bool set) noexcept {
    const uint64_t limit = bits.words() * 64;
    if (pos >= limit) {
      return limit;
    }
    const uint64_t flip = set ? 0 : ~uint64_t{0};
    uint64_t w = pos / 64;
    uint64_t word = (bits[w] ^ flip) & (~uint64_t{0} << (pos % 64));
    while (word == 0) {
      if (++w == bits.words()) {
        return limit;
      }
      word = bits[w] ^ flip;
    }
    return w * 64 + static_cast<uint64_t>(std::countr_zero(word));
  }

  bool ReadRuns(ScanState& s, ListDictionary& d, uint64_t base, uint64_t total,
                uint64_t max_elems) const {
    auto dictionary = NewLists(total);
    auto& lists = dictionary->data;
    const uint64_t end = base + d.runs.back().end;
    if (EndsDecoded(d, base, end)) {
      uint64_t elems = 0;
      for (const auto& run : d.runs) {
        elems +=
          DecodedStart(d, base + run.end) - DecodedStart(d, base + run.begin);
      }
      if (elems > max_elems) {
        return false;
      }
      duckdb::ListVector::Reserve(lists, static_cast<duckdb::idx_t>(elems));
    }
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(lists) + 1;
    for (const auto& run : d.runs) {
      if (!ReadLists(s, base + run.begin, run.end - run.begin, lists, entries,
                     duckdb::ListVector::GetListSize(lists), max_elems)) {
        return false;
      }
      entries += run.end - run.begin;
    }
    d.lists = std::move(dictionary);
    d.begin = base;
    d.end = end;
    return true;
  }

  void ReferencedLists(ScanState& s, ListDictionary& d,
                       std::span<const uint64_t> codes, uint64_t lo,
                       uint64_t hi) const {
    auto& bits = d.decoded;
    bits.reset(hi - lo + 1);
    for (const auto code : codes) {
      bits.set(code - lo);
    }
    auto& runs = d.runs;
    runs.clear();
    const uint64_t limit = bits.words() * 64;
    for (uint64_t from = NextBit(bits, 0, true); from < limit;) {
      const uint64_t to = NextBit(bits, from, false);
      if (!runs.empty() && from - runs.back().end <= kRunGapLists) {
        for (auto gap = runs.back().end; gap < from; ++gap) {
          bits.set(gap);
        }
        runs.back().end = to;
      } else {
        runs.push_back({.begin = from, .end = to});
      }
      from = NextBit(bits, to, true);
    }
    DecodeEnds(s, d, lo, hi, runs.size() + 1);
    d.ranks.resize(bits.words());
    uint64_t total = 0;
    for (size_t w = 0; w != bits.words(); ++w) {
      d.ranks[w] = static_cast<uint32_t>(total);
      total += static_cast<uint64_t>(std::popcount(bits[w]));
    }
    ReadRuns(s, d, lo, total, kAnyElems);
  }

  void Lists(ScanState& s, ListDictionary& d, uint64_t lo, uint64_t hi,
             duckdb::idx_t rows) const {
    const uint64_t distinct = _children[1]->RowCount();
    SDB_ASSERT(hi < distinct);
    d.decoded.reset(0);
    const auto read = [&](uint64_t first, uint64_t last, uint64_t max_elems) {
      d.runs.assign(1, {.end = last - first + 1});
      return ReadRuns(s, d, first, last - first + 1, max_elems);
    };
    uint64_t first = lo;
    uint64_t last = hi;
    if (rows > kPointRows) {
      last = std::min(distinct - 1, std::max(hi, lo + kReadAheadLists - 1));
    } else if (WidenToBlock(d, lo, hi, first, last)) {
      if (read(first, last, kWholeBlockElems)) {
        return;
      }
      d.oversized_block = d.missed_block;
      d.missed_block = 0;
      first = lo;
      last = hi;
    }
    read(first, last, kAnyElems);
  }

  template<bool kDictionary>
  duckdb::idx_t ScanCodes(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const {
    if (count == 0) {
      return 0;
    }
    auto& codes_vec = ResetScratch(List(s).codes, duckdb::LogicalType::UBIGINT,
                                   s.ctx->Allocator(), count);
    const auto scan_count =
      ScanVector(s, codes_vec, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    if (_validity) {
      _validity->ColumnReader::ScanCount(s.child_states[0], result, count,
                                         result_offset);
    }
    ResolveCodes<kDictionary>(
      s, result, {duckdb::FlatVector::GetData<uint64_t>(codes_vec), scan_count},
      result_offset);
    return scan_count;
  }

  template<bool kDictionary>
  void ResolveCodes(ScanState& s, duckdb::Vector& result,
                    std::span<const uint64_t> codes,
                    duckdb::idx_t result_offset) const {
    const uint64_t child_base =
      result_offset != 0 ? duckdb::ListVector::GetListSize(result) : 0;
    if (ConsecutiveRunLength(codes, 0) == codes.size() &&
        DirectRun(s, codes[0], codes.size())) {
      ReadLists(
        s, codes[0], codes.size(), result,
        duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result) +
          result_offset,
        child_base, kAnyElems);
      return;
    }
    FromDictionary<kDictionary>(s, result, codes, result_offset, child_base);
  }

  template<bool kDictionary>
  IRS_NO_INLINE void FromDictionary(ScanState& s, duckdb::Vector& result,
                                    std::span<const uint64_t> codes,
                                    duckdb::idx_t result_offset,
                                    uint64_t child_base) const {
    const duckdb::idx_t scan_count = codes.size();
    const auto [lo, hi] = std::ranges::minmax(codes);
    SDB_ENSURE(hi < _children[1]->RowCount(),
               "list column: corrupted list codes");
    auto& state = List(s);
    auto& d = state.dictionary;
    if (!Covers(d, codes, lo, hi)) {
      if (Reusable(s) && hi - lo < 2 * kReadAheadLists) {
        Lists(s, d, lo, hi, scan_count);
      } else {
        ReferencedLists(s, d, codes, lo, hi);
      }
    }
    auto& sel = state.sel;
    if (sel.Capacity() < scan_count || sel.sel_data().use_count() != 1) {
      sel.Initialize(std::max<duckdb::idx_t>(scan_count, STANDARD_VECTOR_SIZE));
    }
    const auto& validity = duckdb::FlatVector::Validity(result);
    const duckdb::idx_t first = kDictionary ? 0 : result_offset;
    const auto pick = [&](auto index) {
      for (duckdb::idx_t i = 0; i < scan_count; ++i) {
        sel.set_index(i, validity.RowIsValid(first + i)
                           ? static_cast<duckdb::idx_t>(index(codes[i]))
                           : 0);
      }
    };
    if (d.decoded.size() == 0) {
      pick([begin = d.begin](uint64_t code) { return code - begin + 1; });
    } else {
      pick([begin = d.begin, words = d.decoded.data(),
            ranks = d.ranks.data()](uint64_t code) {
        const uint64_t off = code - begin;
        const auto below = words[off / 64] & ((uint64_t{1} << (off % 64)) - 1);
        return 1 + ranks[off / 64] +
               static_cast<uint64_t>(std::popcount(below));
      });
    }
    if constexpr (kDictionary) {
      result.Dictionary(d.lists, sel, scan_count);
    } else {
      CopyLists(state, result, scan_count, result_offset, child_base);
    }
  }

  static void CopyLists(ListScanState& state, duckdb::Vector& result,
                        duckdb::idx_t scan_count, duckdb::idx_t result_offset,
                        uint64_t child_base) {
    const auto& d = state.dictionary;
    const auto* picks = state.sel.data();
    const auto* lists =
      duckdb::FlatVector::GetData<duckdb::list_entry_t>(d.lists->data);
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result) +
      result_offset;
    uint64_t total = 0;
    for (duckdb::idx_t i = 0; i < scan_count; ++i) {
      total += lists[picks[i]].length;
    }
    duckdb::ListVector::Reserve(result,
                                static_cast<duckdb::idx_t>(child_base + total));
    auto& picked = state.picked;
    if (picked.size() < total) {
      picked.resize(total);
    }
    uint64_t pos = 0;
    for (duckdb::idx_t i = 0; i < scan_count; ++i) {
      const auto& list = lists[picks[i]];
      entries[i] = duckdb::list_entry_t{child_base + pos, list.length};
      for (uint64_t k = 0; k < list.length; ++k) {
        picked[pos++] = static_cast<duckdb::sel_t>(list.offset + k);
      }
    }
    if (total != 0) {
      duckdb::ImmutableStrings::Copy(
        duckdb::ListVector::GetChild(d.lists->data),
        duckdb::ListVector::GetChildMutable(result),
        duckdb::SelectionVector{picked.data(),
                                static_cast<duckdb::idx_t>(total)},
        static_cast<duckdb::idx_t>(total), 0,
        static_cast<duckdb::idx_t>(child_base));
    }
    duckdb::ListVector::SetListSize(
      result, static_cast<duckdb::idx_t>(child_base + total));
  }

  bool _element_strings = false;
};

class StructColumnReader final : public ColumnReader {
 public:
  StructColumnReader(field_id id, duckdb::LogicalType type,
                     std::unique_ptr<ColumnReader> validity,
                     std::vector<std::unique_ptr<ColumnReader>> children)
    : ColumnReader{
        id, std::move(type), {}, std::move(validity), std::move(children)} {
    _row_count = !_children.empty() ? _children.front()->RowCount()
                                    : (_validity ? _validity->RowCount() : 0);
    auto stats = duckdb::StructStats::CreateEmpty(_type);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      duckdb::StructStats::SetChildStats(
        stats, fi, _children[fi]->MergedStatistics().ToUnique());
    }
    FinishStats(std::move(stats));
  }

  uint64_t RowGroupEnd(uint64_t row) const noexcept final {
    return _children.empty() ? _validity->RowGroupEnd(row)
                             : _children.front()->RowGroupEnd(row);
  }

  uint64_t GatherCursor(const ScanState& s) const noexcept final {
    SDB_ASSERT(_validity || (!_children.empty() && s.child_states.size() > 1));
    const uint64_t row =
      _validity ? _validity->ColumnReader::GatherCursor(s.child_states[0])
                : _children.front()->GatherCursor(s.child_states[1]);
    SDB_ASSERT(InStep(s, row));
    return row;
  }

  void CollectBlocks(uint64_t first, uint64_t last,
                     std::vector<duckdb::FileRange>& out) const final {
    ColumnReader::CollectBlocks(first, last, out);
    for (const auto& child : _children) {
      child->CollectBlocks(first, last, out);
    }
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final {
    NewOutputVector(s);
    duckdb::idx_t scan_count = count;
    if (_validity) {
      SDB_ASSERT(!s.child_states.empty());
      scan_count =
        _validity->ColumnReader::Scan(s.child_states[0], result, count);
      if (result.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
        SDB_ASSERT(duckdb::ConstantVector::IsNull(result));
        for (size_t fi = 0; fi < _children.size(); ++fi) {
          _children[fi]->Skip(s.child_states[fi + 1], scan_count);
        }
        return scan_count;
      }
    }
    auto& entries = duckdb::StructVector::GetEntries(result);
    SDB_ASSERT(entries.size() == _children.size());
    SDB_ASSERT(s.child_states.size() == _children.size() + 1);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->Scan(s.child_states[fi + 1], entries[fi], count);
    }
    duckdb::FlatVector::SetSize(result, scan_count);
    return scan_count;
  }

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final {
    duckdb::idx_t scan_count = count;
    if (_validity) {
      scan_count = _validity->ColumnReader::ScanCount(s.child_states[0], result,
                                                      count, result_offset);
    }
    auto& entries = duckdb::StructVector::GetEntries(result);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->ScanCount(s.child_states[fi + 1], entries[fi], count,
                               result_offset);
    }
    return scan_count;
  }

  void Skip(ScanState& s, duckdb::idx_t count) const final {
    if (_validity) {
      _validity->ColumnReader::Skip(s.child_states[0], count);
    }
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->Skip(s.child_states[fi + 1], count);
    }
  }

  void GatherScatter(ScanState& s, uint64_t anchor,
                     const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                     duckdb::Vector& out, duckdb::idx_t at) const final {
    if (at == 0) {
      NewOutputVector(s);
    }
    if (_validity) {
      _validity->ColumnReader::GatherScatter(s.child_states[0], anchor, sel,
                                             hits, out, at);
    }
    auto& entries = duckdb::StructVector::GetEntries(out);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->GatherScatter(s.child_states[fi + 1], anchor, sel, hits,
                                   entries[fi], at);
    }
  }

  void GatherDense(ScanState& s, uint64_t anchor,
                   const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                   duckdb::idx_t span, duckdb::Vector& out) const final {
    SDB_ASSERT(hits > 0 && hits <= span && span <= STANDARD_VECTOR_SIZE);
    NewOutputVector(s);
    const uint64_t cur = GatherCursor(s);
    SDB_ASSERT(anchor >= cur, "GatherDense requires ascending rows");
    if (hits == span) {
      if (anchor > cur) {
        Skip(s, anchor - cur);
      }
      Scan(s, out, span);
      return;
    }
    if (_validity) {
      _validity->ColumnReader::GatherScatter(s.child_states[0], anchor, sel,
                                             hits, out, 0);
    }
    auto& entries = duckdb::StructVector::GetEntries(out);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->GatherDense(s.child_states[fi + 1], anchor, sel, hits,
                                 span, entries[fi]);
    }
    SkipTo(s, anchor + span);
  }

 private:
  bool InStep(const ScanState& s, uint64_t row) const noexcept {
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      if (_children[fi]->GatherCursor(s.child_states[fi + 1]) != row) {
        return false;
      }
    }
    return true;
  }

  void SkipTo(ScanState& s, uint64_t row) const {
    if (_validity) {
      auto& vs = s.child_states[0];
      if (const auto at = _validity->ColumnReader::GatherCursor(vs); at < row) {
        _validity->ColumnReader::Skip(vs, row - at);
      }
    }
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      auto& fs = s.child_states[fi + 1];
      if (const auto at = _children[fi]->GatherCursor(fs); at < row) {
        _children[fi]->Skip(fs, row - at);
      }
    }
  }
};

class ArrayColumnReader final : public ColumnReader {
 public:
  ArrayColumnReader(field_id id, duckdb::LogicalType type,
                    std::unique_ptr<ColumnReader> validity,
                    std::vector<std::unique_ptr<ColumnReader>> children)
    : ColumnReader{
        id, std::move(type), {}, std::move(validity), std::move(children)} {
    SDB_ASSERT(_children.size() == 1 && _array_size != 0);
    const auto& child = *_children.front();
    _row_count = child.RowCount() / _array_size;
    auto stats = duckdb::ArrayStats::CreateEmpty(_type);
    duckdb::ArrayStats::SetChildStats(stats,
                                      child.MergedStatistics().ToUnique());
    FinishStats(std::move(stats));
  }

  uint64_t GatherCursor(const ScanState& s) const noexcept final {
    if (_validity) {
      return _validity->ColumnReader::GatherCursor(s.child_states[0]);
    }
    SDB_ASSERT(!_children.empty() && s.child_states.size() > 1);
    return _children.front()->GatherCursor(s.child_states[1]) / _array_size;
  }

  uint64_t RowGroupEnd(uint64_t row) const noexcept final {
    SDB_ASSERT(_array_size != 0 && !_children.empty());
    const auto end = _children.front()->RowGroupEnd(row * _array_size);
    return (end + _array_size - 1) / _array_size;
  }

  void CollectBlocks(uint64_t first, uint64_t last,
                     std::vector<duckdb::FileRange>& out) const final {
    ColumnReader::CollectBlocks(first, last, out);
    _children.front()->CollectBlocks(first * _array_size,
                                     (last + 1) * _array_size - 1, out);
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final {
    NewOutputVector(s);
    return ScanCount(s, result, count, /*result_offset=*/0);
  }

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final {
    duckdb::idx_t scan_count = count;
    if (_validity) {
      scan_count = _validity->ColumnReader::ScanCount(s.child_states[0], result,
                                                      count, result_offset);
    }
    auto& child = duckdb::ArrayVector::GetChildMutable(result);
    _children[0]->ScanCount(s.child_states[1], child, count * _array_size,
                            result_offset * _array_size);
    return scan_count;
  }

  void Skip(ScanState& s, duckdb::idx_t count) const final {
    if (_validity) {
      _validity->ColumnReader::Skip(s.child_states[0], count);
    }
    _children[0]->Skip(s.child_states[1], count * _array_size);
  }

  IRS_COLUMN_READER_GATHER_SCATTER
  IRS_COLUMN_READER_GATHER_DENSE
};

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
