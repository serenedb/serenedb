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

#include <absl/strings/str_cat.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/planner/filter/zonemap_checker.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/utils/enum_name.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::numeric {

using duckdb::const_data_ptr_t;
using duckdb::idx_t;

inline FrameHeader SegmentFrames(const_data_ptr_t base, const ColumnSegment& segment) {
  const auto frames = ParseFrames(base, segment.size, segment.codec);
  SDB_ENSURE(frames.row_count == segment.count,
             "numeric codec: corrupted row count");
  return frames;
}

template<typename R>
struct ScanState final : SegmentScanState {
  ScanState(const ColumnSegment& segment, duckdb::BufferHandle handle_p)
    : handle{std::move(handle_p)},
      frames{handle.Ptr() + segment.offset,
             SegmentFrames(handle.Ptr() + segment.offset, segment),
             segment.codec, segment.cache_slot} {}

  duckdb::BufferHandle handle;
  R frames;
  duckdb::unique_ptr<duckdb::BaseStatistics> filter_stats;
};

template<typename R>
std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment& segment) {
  return std::make_unique<ScanState<R>>(segment, segment.Pin());
}

template<typename R>
void Scan(const ColumnSegment&, ColumnScanState& state, idx_t scan_count,
          duckdb::Vector& result, idx_t result_offset, duckdb::ScanVectorType) {
  state.scan_state->Cast<ScanState<R>>().frames.Read(
    state.GetPositionInSegment(), scan_count,
    duckdb::FlatVector::GetDataMutable<typename R::Value>(result) +
      result_offset);
}

template<typename R>
void Select(const ColumnSegment&, ColumnScanState& state, idx_t,
            duckdb::Vector& result, idx_t result_offset,
            const duckdb::SelectionVector& sel, idx_t sel_count,
            duckdb::ScanVectorType) {
  auto& frames = state.scan_state->Cast<ScanState<R>>().frames;
  auto* out = duckdb::FlatVector::GetDataMutable<typename R::Value>(result) +
              result_offset;
  const uint64_t start = state.GetPositionInSegment();
  for (idx_t i = 0; i < sel_count; ++i) {
    out[i] = frames.Get(start + sel.get_index(i));
  }
}

template<typename R>
duckdb::FilterPropagateResult FramesVerdict(
  ScanState<R>& scan, const duckdb::LogicalType& type, uint64_t begin,
  uint64_t end, const duckdb::ZonemapChecker& checker) {
  using T = typename R::Value;
  if (!scan.filter_stats) {
    scan.filter_stats = duckdb::NumericStats::CreateEmpty(type).ToUnique();
    scan.filter_stats->SetHasNoNullFast();
  }
  auto& stats = *scan.filter_stats;
  const auto check = [&](uint32_t f) {
    duckdb::NumericStats::SetMin<T>(stats, scan.frames.FrameMin(f));
    duckdb::NumericStats::SetMax<T>(stats, scan.frames.FrameMax(f));
    return checker.Check(stats, nullptr);
  };
  const auto first = scan.frames.FrameOf(begin);
  const auto last = scan.frames.FrameOf(end - 1);
  const auto verdict = check(first);
  for (auto f = first + 1;
       f <= last &&
       verdict != duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
       ++f) {
    if (check(f) != verdict) {
      return duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
    }
  }
  return verdict;
}

template<typename R>
void Filter(const ColumnSegment& segment, ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            duckdb::SelectionVector& sel, idx_t& sel_count,
            const duckdb::TableFilter& /*filter*/,
            duckdb::TableFilterState& filter_state) {
  auto& scan = state.scan_state->Cast<ScanState<R>>();
  const uint64_t start = state.GetPositionInSegment();
  auto verdict = duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
  if constexpr (std::is_integral_v<typename R::Value>) {
    const auto& checker =
      *filter_state.Cast<duckdb::ExpressionFilterState>().zonemap_checker;
    if (checker.IsFullyCompiled()) {
      verdict = FramesVerdict(scan, segment.stats.GetType(), start,
                              start + vector_count, checker);
    }
  }
  if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
      verdict == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL) {
    sel_count = 0;
    return;
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  Scan<R>(segment, state, vector_count, result, 0,
          duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
  if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE) {
    return;
  }
  duckdb::ColumnSegment::FilterSelection(sel, result, filter_state,
                                         vector_count, sel_count);
}

template<typename R>
struct FetchCache final : SegmentScanState {
  std::optional<R> frames;
};

template<typename R>
void FetchRows(const ColumnSegment& segment, ColumnFetchState& state,
               const idx_t* rows, idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  auto& handle = state.GetOrInsertHandle(segment);
  auto& cache = state.GetOrCreateSegmentState<FetchCache<R>>(
    [] { return std::make_unique<FetchCache<R>>(); });
  if (!cache.frames) {
    const auto* base = handle.Ptr() + segment.offset;
    cache.frames.emplace(base, SegmentFrames(base, segment), segment.codec,
                         segment.cache_slot);
  }
  auto& frames = *cache.frames;
  auto* data = duckdb::FlatVector::GetDataMutable<typename R::Value>(result);
  constexpr auto kNone = std::numeric_limits<uint64_t>::max();
  for (idx_t i = 0; i < count; ++i) {
    const auto row = static_cast<uint64_t>(rows[i]);
    SDB_ENSURE(row < segment.count, "numeric codec: row out of range");
    const uint64_t next = i + 1 < count ? rows[i + 1] : kNone;
    data[result_offset + result_sel.get_index(i)] = frames.Fetch(row, next);
  }
}

inline duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  const ColumnSegment& segment) {
  auto handle = segment.Pin();
  const auto* base = handle.Ptr() + segment.offset;
  const auto h = ParseFrames(base, segment.size, segment.codec);
  const auto shape = ShapeOf(segment.codec);
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["transform"] = TransformName(shape, h);
  info["leaf"] = SnakeCaseName(LeafOf(segment.codec));
  info["level"] = absl::StrCat(static_cast<uint32_t>(h.level));
  info["stored"] = absl::StrCat(static_cast<uint32_t>(h.stored));
  info["shuffled"] = h.flags & kNumericShuffled ? "true" : "false";
  info["frames"] = absl::StrCat(h.frame_count);
  info["entries"] = absl::StrCat(
    shape == Shape::Dictionary
      ? duckdb::Load<DictionaryHeader>(base + sizeof(FrameHeader)).count
      : 0);
  info["raw_bytes"] = absl::StrCat(h.raw_bytes);
  info["data_bytes"] = absl::StrCat(h.data_size);
  info["scale"] = absl::StrCat(h.scale > 1 ? h.scale : 1);
  return info;
}

template<typename R>
inline constexpr DecompressionFunction kDecompression{
  .init_scan = InitScan<R>,
  .scan = Scan<R>,
  .select = Select<R>,
  .filter = Filter<R>,
  .fetch_rows = FetchRows<R>,
  .get_segment_info = SegmentInfo,
};

}  // namespace irs::codecs::numeric
