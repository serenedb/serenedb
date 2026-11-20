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

#include <algorithm>
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
#include <span>
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

inline FrameHeader SegmentFrames(const_data_ptr_t base,
                                 const ColumnSegment& segment) {
  return ParseFrames(base, segment.size, segment.count, segment.codec);
}

inline constexpr uint64_t kWalkedRunRows = 4;
inline constexpr idx_t kRunBurst = 32;
inline constexpr uint32_t kEntryBatch = 512;

static_assert(STANDARD_VECTOR_SIZE / kWalkedRunRows <= kEntryBatch);

template<typename T>
struct EntryBatch {
  explicit EntryBatch(const duckdb::LogicalType& type)
    : vector{type, reinterpret_cast<duckdb::data_ptr_t>(values),
             std::size(values)} {}

  T values[kEntryBatch];
  duckdb::Vector vector;
};

template<typename R>
R OpenReader(const_data_ptr_t base, const ColumnSegment& segment) {
  if constexpr (std::is_constructible_v<R, const_data_ptr_t,
                                        const ColumnSegment&>) {
    return R{base, segment};
  } else {
    return R{base, SegmentFrames(base, segment), segment.codec,
             segment.cache_slot};
  }
}

template<typename T>
struct RangeVerdict {
  const duckdb::ZonemapChecker* checker = nullptr;
  T min{};
  T max{};
  duckdb::FilterPropagateResult result =
    duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
};

template<typename R>
struct ScanState final : SegmentScanState {
  ScanState(const ColumnSegment& segment, duckdb::BufferHandle handle_p)
    : handle{std::move(handle_p)},
      frames{OpenReader<R>(handle.Ptr(), segment)} {}

  EntryBatch<typename R::Value>& Batch(const duckdb::LogicalType& type) {
    if (!batch) [[unlikely]] {
      batch = std::make_unique<EntryBatch<typename R::Value>>(type);
    }
    return *batch;
  }

  duckdb::BufferHandle handle;
  R frames;
  duckdb::unique_ptr<duckdb::BaseStatistics> filter_stats;
  RangeVerdict<typename R::Value> frame_verdict;
  RangeVerdict<typename R::Value> value_verdict;
  std::unique_ptr<EntryBatch<typename R::Value>> batch;
  [[no_unique_address]] typename R::Matches matches;
};

template<typename R>
std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment& segment) {
  return std::make_unique<ScanState<R>>(segment, segment.Pin());
}

template<typename R>
IRS_FORCE_INLINE bool ScanConstant(R& reader, uint64_t start,
                                   idx_t vector_count,
                                   duckdb::ScanVectorType scan_type,
                                   duckdb::Vector& result) {
  if (scan_type != duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR ||
      vector_count != STANDARD_VECTOR_SIZE) {
    return false;
  }
  typename R::Value value{};
  if constexpr (R::kHasRuns) {
    if (!reader.RunHolds(start, vector_count, value)) {
      return false;
    }
  } else if constexpr (requires { reader.ConstantRows(start, start, value); }) {
    if (!reader.ConstantRows(start, vector_count, value)) {
      return false;
    }
  } else {
    return false;
  }
  duckdb::FlatVector::GetDataMutable<typename R::Value>(result)[0] = value;
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::FlatVector::SetSize(result, duckdb::count_t(vector_count));
  return true;
}

template<typename R>
void Scan(const ColumnSegment&, ColumnScanState& state, idx_t scan_count,
          duckdb::Vector& result, idx_t result_offset,
          duckdb::ScanVectorType scan_type) {
  auto& frames = state.scan_state->Cast<ScanState<R>>().frames;
  const uint64_t start = state.GetPositionInSegment();
  if (ScanConstant(frames, start, scan_count, scan_type, result)) {
    return;
  }
  frames.Read(start, scan_count,
              duckdb::FlatVector::GetDataMutable<typename R::Value>(result) +
                result_offset);
}

template<typename R>
void Select(const ColumnSegment&, ColumnScanState& state, idx_t vector_count,
            duckdb::Vector& result, idx_t result_offset,
            const duckdb::SelectionVector& sel, idx_t sel_count,
            duckdb::ScanVectorType scan_type) {
  auto& frames = state.scan_state->Cast<ScanState<R>>().frames;
  const uint64_t start = state.GetPositionInSegment();
  if (ScanConstant(frames, start, vector_count, scan_type, result)) {
    return;
  }
  auto* out = duckdb::FlatVector::GetDataMutable<typename R::Value>(result) +
              result_offset;
  for (idx_t i = 0; i < sel_count; ++i) {
    out[i] = frames.Get(start + sel.get_index(i));
  }
}

template<typename R>
duckdb::FilterPropagateResult VerdictOf(ScanState<R>& scan,
                                        RangeVerdict<typename R::Value>& kept,
                                        const duckdb::LogicalType& type,
                                        typename R::Value min,
                                        typename R::Value max,
                                        const duckdb::ZonemapChecker& checker) {
  using T = typename R::Value;
  if (kept.checker != &checker || kept.min != min || kept.max != max) {
    if (!scan.filter_stats) {
      scan.filter_stats = duckdb::NumericStats::CreateEmpty(type).ToUnique();
      scan.filter_stats->SetHasNoNullFast();
    }
    duckdb::NumericStats::SetMin<T>(*scan.filter_stats, min);
    duckdb::NumericStats::SetMax<T>(*scan.filter_stats, max);
    kept = {
      .checker = &checker,
      .min = min,
      .max = max,
      .result = checker.Check(*scan.filter_stats, nullptr),
    };
  }
  return kept.result;
}

template<typename R>
duckdb::FilterPropagateResult FramesVerdict(
  ScanState<R>& scan, const duckdb::LogicalType& type, uint64_t begin,
  uint64_t end, const duckdb::ZonemapChecker& checker) {
  const auto check = [&](uint32_t f) {
    return VerdictOf(scan, scan.frame_verdict, type, scan.frames.FrameMin(f),
                     scan.frames.FrameMax(f), checker);
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
bool FilterRuns(ScanState<R>& scan, uint64_t start, idx_t count,
                duckdb::Vector& result, duckdb::SelectionVector& sel,
                idx_t& sel_count, duckdb::TableFilterState& filter_state) {
  using T = typename R::Value;
  auto& frames = scan.frames;
  frames.Seek(start);
  if (start + count > frames.FrameEnd()) {
    return false;
  }
  auto& batch = scan.Batch(result.GetType());
  auto& values = batch.values;
  const auto runs =
    frames.ReadRuns(start, count,
                    std::span{values}.first(std::min<size_t>(
                      count / kWalkedRunRows, std::size(values))));
  if (runs.count == 0) {
    return false;
  }
  duckdb::SelectionVector accepted;
  idx_t accepted_count = runs.count;
  duckdb::ColumnSegment::FilterSelection(accepted, batch.vector, filter_state,
                                         runs.count, accepted_count);
  auto* out = duckdb::FlatVector::GetDataMutable<T>(result);
  auto* picked = sel.data();
  SDB_ASSERT(picked != accepted.data());
  const bool every_row = sel_count == count;
  const idx_t burst_end = count >= kRunBurst ? count - kRunBurst + 1 : 0;
  idx_t kept = 0;
  idx_t next = 0;
  for (idx_t i = 0; i < accepted_count; ++i) {
    const auto run = static_cast<uint32_t>(accepted.get_index(i));
    const auto rows = frames.RunRows(runs.first + run, start, count);
    const auto value = values[run];
    const idx_t length = rows.end - rows.begin;
    idx_t filled = 0;
    if (rows.begin < burst_end) {
      for (; filled < kRunBurst; ++filled) {
        out[rows.begin + filled] = value;
      }
    }
    if (filled < length) {
      std::fill(out + rows.begin + filled, out + rows.end, value);
    }
    if (every_row) {
      idx_t listed = 0;
      if (rows.begin < burst_end) {
        for (; listed < kRunBurst; ++listed) {
          picked[kept + listed] =
            static_cast<duckdb::sel_t>(rows.begin + listed);
        }
      }
      for (; listed < length; ++listed) {
        picked[kept + listed] = static_cast<duckdb::sel_t>(rows.begin + listed);
      }
      kept += length;
      continue;
    }
    while (next < sel_count && sel.get_index(next) < rows.begin) {
      ++next;
    }
    for (; next < sel_count && sel.get_index(next) < rows.end; ++next) {
      sel.set_index(kept++, sel.get_index(next));
    }
  }
  sel_count = kept;
  return true;
}

template<typename R>
IRS_NO_INLINE void CountMatchingEntries(
  ScanState<R>& scan, const duckdb::LogicalType& type,
  duckdb::TableFilterState& filter_state) {
  auto& matches = scan.matches;
  matches.counted = true;
  const auto entries = scan.frames.EntryCount();
  auto& batch = scan.Batch(type);
  for (uint32_t first = 0; first < entries; first += kEntryBatch) {
    const auto batch_count = std::min<uint32_t>(entries - first, kEntryBatch);
    scan.frames.Entries(first, batch_count, batch.values);
    duckdb::SelectionVector accepted;
    idx_t accepted_count = batch_count;
    duckdb::ColumnSegment::FilterSelection(accepted, batch.vector, filter_state,
                                           batch_count, accepted_count);
    matches.count += static_cast<uint32_t>(accepted_count);
    if (matches.count != 0 && matches.count != first + batch_count) {
      break;
    }
  }
}

template<typename R>
bool FilterEntries(const ColumnSegment& segment, ColumnScanState& state,
                   ScanState<R>& scan, idx_t count, duckdb::Vector& result,
                   idx_t& sel_count, duckdb::TableFilterState& filter_state) {
  const auto& matches = scan.matches;
  if (!matches.counted) [[unlikely]] {
    CountMatchingEntries(scan, result.GetType(), filter_state);
  }
  if (matches.count == 0) {
    sel_count = 0;
    return true;
  }
  if (matches.count != scan.frames.EntryCount()) {
    return false;
  }
  Scan<R>(segment, state, count, result, 0,
          duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
  return true;
}

template<typename R>
void Filter(const ColumnSegment& segment, ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            duckdb::SelectionVector& sel, idx_t& sel_count,
            const duckdb::TableFilter& /*filter*/,
            duckdb::TableFilterState& filter_state) {
  auto& scan = state.scan_state->Cast<ScanState<R>>();
  if (state.dynamic_filter) {
    scan.frame_verdict = {};
    scan.value_verdict = {};
  }
  const uint64_t start = state.GetPositionInSegment();
  auto verdict = duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
  if constexpr (std::is_integral_v<typename R::Value>) {
    const auto& checker =
      *filter_state.Cast<duckdb::ExpressionFilterState>().zonemap_checker;
    if (checker.IsFullyCompiled()) {
      verdict = FramesVerdict(scan, segment.stats.GetType(), start,
                              start + vector_count, checker);
      if constexpr (requires(typename R::Value value) {
                      scan.frames.NarrowBounds(start, start, value, value);
                    }) {
        typename R::Value min;
        typename R::Value max;
        if (verdict == duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE &&
            scan.frames.NarrowBounds(start, vector_count, min, max)) {
          verdict = VerdictOf(scan, scan.value_verdict, segment.stats.GetType(),
                              min, max, checker);
        }
      }
    }
  }
  if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
      verdict == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL) {
    sel_count = 0;
    return;
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  const bool every_row =
    verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE;
  if constexpr (R::kFiltersRuns) {
    if (!every_row && FilterRuns(scan, start, vector_count, result, sel,
                                 sel_count, filter_state)) {
      return;
    }
  } else if constexpr (R::kFiltersEntries) {
    if (!every_row && !state.dynamic_filter &&
        FilterEntries(segment, state, scan, vector_count, result, sel_count,
                      filter_state)) {
      return;
    }
  }
  Scan<R>(segment, state, vector_count, result, 0,
          duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
  if (every_row) {
    return;
  }
  duckdb::ColumnSegment::FilterSelection(sel, result, filter_state,
                                         vector_count, sel_count);
}

template<typename R>
struct FetchCache final : SegmentScanState {
  FetchCache(const_data_ptr_t base, const ColumnSegment& segment)
    : frames{OpenReader<R>(base, segment)} {}

  R frames;
};

template<typename R>
void FetchRows(const ColumnSegment& segment, ColumnFetchState& state,
               const idx_t* rows, idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  auto& frames = state
                   .GetOrCreateSegmentState<FetchCache<R>>([&] {
                     return std::make_unique<FetchCache<R>>(
                       state.GetOrInsertHandle(segment).Ptr(), segment);
                   })
                   .frames;
  auto* data = duckdb::FlatVector::GetDataMutable<typename R::Value>(result);
  if constexpr (requires {
                  frames.FetchRows(rows, count, data, result_sel,
                                   result_offset);
                }) {
    frames.FetchRows(rows, count, data, result_sel, result_offset);
  } else {
    for (idx_t i = 0; i < count; ++i) {
      const auto row = static_cast<uint64_t>(rows[i]);
      SDB_ASSERT(row < segment.count);
      const uint64_t next = i + 1 < count ? rows[i + 1] : kNoRow;
      data[result_offset + result_sel.get_index(i)] = frames.Fetch(row, next);
    }
  }
}

inline uint64_t RawBytes(const_data_ptr_t base, const FrameHeader& h,
                         Shape shape) {
  const bool packed = h.flags & kNumericPacked;
  if (packed && shape != Shape::Runs) {
    return uint64_t{h.row_count} * h.width;
  }
  const auto meta = FrameMetaSize(shape, packed, h.width);
  uint64_t raw = 0;
  for (uint32_t f = 0; f < h.frame_count; ++f) {
    const auto* at = base + h.off_frames + size_t{f} * meta;
    raw +=
      packed
        ? uint64_t{duckdb::Load<uint16_t>(at + 2 * sizeof(uint32_t))} * h.width
        : duckdb::Load<NumericFrameMeta>(at).frame.raw_len;
  }
  return raw;
}

inline duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  const ColumnSegment& segment) {
  auto handle = segment.Pin();
  const auto* base = handle.Ptr();
  const auto h = SegmentFrames(base, segment);
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
      ? duckdb::Load<DictionaryHeader>(base + OwnHeaderOffset(h.flags)).count
      : 0);
  info["raw_bytes"] = absl::StrCat(RawBytes(base, h, shape));
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
