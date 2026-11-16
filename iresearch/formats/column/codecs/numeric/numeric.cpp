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

#include "iresearch/formats/column/codecs/numeric/numeric.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/filter/zonemap_checker.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <functional>
#include <limits>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/numeric/delta.hpp"
#include "iresearch/formats/column/codecs/numeric/dictionary.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/plain.hpp"
#include "iresearch/formats/column/codecs/numeric/runs.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/enum_name.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

/*
Data layout per segment, one block of one column holding a whole row group:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   FrameHeader, 48 bytes                      |   |
|   +----------------------------------------------+   |
|   value width, stored width, shape, leaf, flags,     |
|   counts, section offsets, frame size, common        |
|   divisor (scale)                                    |
|   +----------------------------------------------+   |
|   |   the function's own header, 8 or 16 bytes   |   |
|   +----------------------------------------------+   |
|   plain and delta: base; runs: base and run          |
|   width; dictionary: entry count and offset          |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   NumericFrameMeta frames[frame_count]       |   |
|   +----------------------------------------------+   |
|   first row, decoded size and stored range of        |
|   each frame in Data, its min and max, and a         |
|   base: delta's previous value, rle_ffor's           |
|   run count                                          |
+------------------------------------------------------+
|                      Dictionary                      |
|   +----------------------------------------------+   |
|   |   value dict[dict_count]                     |   |
|   +----------------------------------------------+   |
|   dict and dict_ffor only                            |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   frames                                     |   |
|   +----------------------------------------------+   |
|   raw, for, delta, dict: one item per row, the       |
|   value, value - base, the difference to the         |
|   previous row minus base, or the dictionary         |
|   index, in the stored width; shuffled into          |
|   byte planes when flagged                           |
|   rle: the frame's run values (value - base),        |
|   then their run lengths                             |
|   these frames are lz4 or zstd compressed when       |
|   the leaf makes them smaller                        |
|   ffor, dict_ffor: 16384 rows per frame, in          |
|   blocks of 1024 values (dictionary indexes for      |
|   dict_ffor): a 16-byte meta (base, bit width)       |
|   per block, then each block's values minus its      |
|   base, bitpacked                                    |
|   rle_ffor: 1024 runs per frame, run values and      |
|   run lengths each packed like ffor                  |
+------------------------------------------------------+
The header holds the section offsets; bit-packed data starts at an
8-byte boundary.
*/

namespace irs::codecs {
namespace {

using duckdb::const_data_ptr_t;
using duckdb::idx_t;

FrameHeader SegmentFrames(const_data_ptr_t base, const ColumnSegment& segment) {
  const auto frames = ParseFrames(base, segment.size);
  SDB_ENSURE(frames.row_count == segment.count,
             "numeric codec: row count mismatch");
  SDB_ENSURE(NumericCodec(frames.shape, frames.leaf) == segment.codec,
             "numeric codec: the segment is not of its block's codec");
  return frames;
}

template<typename R>
struct ScanState final : SegmentScanState {
  ScanState(const ColumnSegment& segment, duckdb::BufferHandle handle_p)
    : handle{std::move(handle_p)},
      frames{handle.Ptr(), SegmentFrames(handle.Ptr(), segment),
             segment.cache_slot} {}

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
    const auto* base = handle.Ptr();
    cache.frames.emplace(base, SegmentFrames(base, segment),
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

duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  const ColumnSegment& segment) {
  auto handle = segment.Pin();
  const auto* base = handle.Ptr();
  const auto h = ParseFrames(base, segment.size);
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["transform"] = SnakeCaseName(TransformOf(h));
  info["leaf"] = SnakeCaseName(h.leaf);
  info["level"] = absl::StrCat(static_cast<uint32_t>(h.level));
  info["stored"] = absl::StrCat(static_cast<uint32_t>(h.stored));
  info["shuffled"] = h.flags & kNumericShuffled ? "true" : "false";
  info["frames"] = absl::StrCat(h.frame_count);
  info["entries"] = absl::StrCat(
    h.shape == NumericShape::Dictionary
      ? duckdb::Load<DictionaryHeader>(base + sizeof(FrameHeader)).count
      : 0);
  info["raw_bytes"] = absl::StrCat(h.raw_bytes);
  info["data_bytes"] = absl::StrCat(h.data_size);
  info["scale"] = absl::StrCat(h.scale > 1 ? h.scale : 1);
  return info;
}

template<typename F>
std::invoke_result_t<F&, std::type_identity<int8_t>> WithNumericType(
  duckdb::PhysicalType physical, F&& f) {
  switch (physical) {
    case duckdb::PhysicalType::INT8:
      return f(std::type_identity<int8_t>{});
    case duckdb::PhysicalType::INT16:
      return f(std::type_identity<int16_t>{});
    case duckdb::PhysicalType::INT32:
      return f(std::type_identity<int32_t>{});
    case duckdb::PhysicalType::INT64:
      return f(std::type_identity<int64_t>{});
    case duckdb::PhysicalType::UINT8:
      return f(std::type_identity<uint8_t>{});
    case duckdb::PhysicalType::UINT16:
      return f(std::type_identity<uint16_t>{});
    case duckdb::PhysicalType::UINT32:
      return f(std::type_identity<uint32_t>{});
    case duckdb::PhysicalType::UINT64:
      return f(std::type_identity<uint64_t>{});
    case duckdb::PhysicalType::FLOAT:
      return f(std::type_identity<float>{});
    case duckdb::PhysicalType::DOUBLE:
      return f(std::type_identity<double>{});
    default:
      return {};
  }
}

uint64_t CodesBound(uint64_t rows, uint64_t entries, uint64_t runs) noexcept {
  return std::min(Packed(rows, entries),
                  Packed(runs, entries) + Packed(runs, rows));
}

template<typename T>
consteval const CompressionFunction* DictionaryFunction() {
  if constexpr (sizeof(T) > 1) {
    return &kShapeFunction<DictionaryFrames<T>>;
  } else {
    return nullptr;
  }
}

template<typename T>
inline constexpr const CompressionFunction* kFunctions[] = {
  &kShapeFunction<PlainFrames<T>>,
  &kShapeFunction<DeltaFrames<T>>,
  &kShapeFunction<RunFrames<T>>,
  DictionaryFunction<T>(),
};

template<typename R>
constexpr DecompressionFunction kDecompression{
  .init_scan = InitScan<R>,
  .scan = Scan<R>,
  .select = Select<R>,
  .filter = Filter<R>,
  .fetch_rows = FetchRows<R>,
  .get_segment_info = SegmentInfo,
};

template<typename T>
constexpr auto kCodecs = []() consteval {
  constexpr auto kLeaves = std::size(kNumericCodecs[0]);
  constexpr auto kShapes = std::size(kNumericCodecs) - (sizeof(T) == 1);
  std::array<Codec, kShapes * kLeaves> codecs;
  for (size_t leaf = 0; leaf < kLeaves; ++leaf) {
    const auto add = [&]<typename R>(NumericShape shape) consteval {
      const auto s = static_cast<size_t>(shape);
      codecs[s * kLeaves + leaf] = {
        .type = kNumericCodecs[s][leaf],
        .compression = kFunctions<T>[s],
        .decompression = &kDecompression<R>,
      };
    };
    add.template operator()<PlainReader<T>>(NumericShape::Plain);
    add.template operator()<DeltaReader<T>>(NumericShape::Delta);
    add.template operator()<RunReader<T>>(NumericShape::Runs);
    if constexpr (sizeof(T) > 1) {
      add.template operator()<DictionaryReader<T>>(NumericShape::Dictionary);
    }
  }
  return codecs;
}();

}  // namespace

uint64_t Packed(uint64_t count, uint64_t max_value) noexcept {
  return duckdb::BitpackingPrimitives::GetRequiredSize(
    count, duckdb::BitpackingPrimitives::MinimumBitWidth<uint32_t>(
             static_cast<uint32_t>(max_value)));
}

uint64_t EstimateCodes(const NumericTuning& tuning, uint64_t rows,
                       uint64_t entries, uint64_t runs) noexcept {
  return static_cast<uint64_t>(
    static_cast<double>(CodesBound(rows, entries, runs)) * tuning.codes_ratio);
}

std::string EncodeCodes(std::span<const uint32_t> codes, uint64_t entries,
                        uint64_t runs, NumericTuning& tuning) {
  static constexpr const CompressionFunction* kCodeFunctions[] = {
    &kShapeFunction<PlainFrames<uint32_t>>,
    &kShapeFunction<DeltaFrames<uint32_t>>,
    &kShapeFunction<RunFrames<uint32_t>>,
  };
  std::string bytes;
  if (codes.empty()) {
    return bytes;
  }
  const duckdb::LogicalType type{duckdb::LogicalTypeId::UINTEGER};
  ColumnTuning column{.reuse = tuning.reuse, .numeric = tuning.choice};
  const auto flush = [&](const Codec&, duckdb::BaseStatistics, uint64_t,
                         std::string_view segment) { bytes = segment; };
  const auto seal = [&] {
    ColumnDataCheckpointData data{
      .type = type,
      .flush = flush,
      .tuning = &column,
      .rows = codes.size(),
      .input = codes.size(),
    };
    data.Emplace<Values<uint32_t>>(codes);
    Compress(kCodeFunctions, data);
    return column.reuse.clock.since == 0;
  };
  const auto bound =
    static_cast<double>(CodesBound(codes.size(), entries, runs));
  bool contested = seal();
  if (!contested &&
      static_cast<double>(bytes.size()) > bound * tuning.codes_ratio * kDrift) {
    column.reuse.clock.since = column.reuse.clock.gap - 1;
    contested = seal();
  }
  tuning.reuse = column.reuse;
  tuning.choice = column.numeric;
  if (contested && bound != 0) {
    tuning.codes_ratio = static_cast<double>(bytes.size()) / bound;
  }
  return bytes;
}

bool UpdateConstant(const duckdb::Vector& input,
                    duckdb::BaseStatistics& stats) {
  if (stats.GetStatsType() != duckdb::StatisticsType::NUMERIC_STATS) {
    return false;
  }
  return WithNumericType(stats.GetType().InternalType(),
                         [&]<typename T>(std::type_identity<T>) {
                           for (const auto value : input.Values<T>()) {
                             if (!value.IsValid()) {
                               stats.SetHasNullFast();
                               continue;
                             }
                             stats.SetHasNoNullFast();
                             stats.UpdateNumericStats<T>(value.GetValue());
                           }
                           return duckdb::NumericStats::IsConstant(stats);
                         });
}

const Codec* NumericCodecOf(CodecType type, duckdb::PhysicalType physical) {
  return WithNumericType(
    physical, [&]<typename T>(std::type_identity<T>) -> const Codec* {
      const auto it = absl::c_find_if(
        kCodecs<T>, [&](const Codec& codec) { return codec.type == type; });
      return it == kCodecs<T>.end() ? nullptr : &*it;
    });
}

}  // namespace irs::codecs
