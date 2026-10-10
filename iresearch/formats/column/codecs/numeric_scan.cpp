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

#include "iresearch/formats/column/codecs/numeric_scan.hpp"

#include <absl/strings/str_cat.h>

#include <array>
#include <cstring>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <optional>
#include <string>
#include <type_traits>

#include "iresearch/formats/column/codecs/numeric_decoder.hpp"
#include "iresearch/formats/column/codecs/numeric_layout.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::const_data_ptr_t;
using duckdb::idx_t;

NumericHeader SegmentHeader(const_data_ptr_t base,
                            const duckdb::ColumnSegment& segment) {
  const auto h = NumericHeader::Parse(base, segment.SegmentSize());
  SDB_ENSURE(h.row_count == segment.count, "numeric codec: row count mismatch");
  return h;
}

FrameCache FrameCacheOf(duckdb::ColumnSegment& segment) {
  const auto& block = segment.GetBlockHandle();
  const auto& slot = static_cast<const ReadContext&>(block->GetBlockManager())
                       .CacheSlotOf(block->BlockId());
  if (slot.key.empty()) {
    return {};
  }
  return {&segment.GetDatabase().GetObjectCache(), slot.key, slot.touched};
}

template<typename T>
struct ScanState final : duckdb::SegmentScanState {
  ScanState(duckdb::ColumnSegment& segment, duckdb::BufferHandle handle_p)
    : handle{std::move(handle_p)},
      base{handle.Ptr() + segment.GetBlockOffset()},
      frames{base, SegmentHeader(base, segment), FrameCacheOf(segment)} {}

  duckdb::BufferHandle handle;
  const_data_ptr_t base;
  FrameDecoder<T> frames;
};

template<typename T>
duckdb::unique_ptr<duckdb::SegmentScanState> InitScan(
  const duckdb::QueryContext&, duckdb::ColumnSegment& segment) {
  auto& buffer_manager =
    duckdb::BufferManager::GetBufferManager(segment.GetDatabase());
  return duckdb::make_uniq<ScanState<T>>(
    segment, buffer_manager.Pin(segment.GetBlockHandle()));
}

template<typename T>
void ScanPartial(duckdb::ColumnSegment&, duckdb::ColumnScanState& state,
                 idx_t scan_count, duckdb::Vector& result,
                 idx_t result_offset) {
  state.scan_state->Cast<ScanState<T>>().frames.Read(
    state.GetPositionInSegment(), scan_count,
    duckdb::FlatVector::GetDataMutable<T>(result) + result_offset);
}

template<typename T>
void ScanVector(duckdb::ColumnSegment& segment, duckdb::ColumnScanState& state,
                idx_t scan_count, duckdb::Vector& result) {
  ScanPartial<T>(segment, state, scan_count, result, 0);
}

template<typename T>
void Select(duckdb::ColumnSegment&, duckdb::ColumnScanState& state, idx_t,
            duckdb::Vector& result, const duckdb::SelectionVector& sel,
            idx_t sel_count) {
  auto& frames = state.scan_state->Cast<ScanState<T>>().frames;
  auto* out = duckdb::FlatVector::GetDataMutable<T>(result);
  const uint64_t start = state.GetPositionInSegment();
  for (idx_t i = 0; i < sel_count; ++i) {
    const uint64_t row = start + sel.get_index(i);
    frames.Seek(row);
    out[i] = frames.At(row);
  }
}

template<typename T>
bool MayMatch(const FrameDecoder<T>& frames, const duckdb::LogicalType& type,
              uint64_t begin, uint64_t end,
              const duckdb::ExpressionFilter& filter,
              duckdb::TableFilterState& filter_state) {
  auto stats = duckdb::BaseStatistics::CreateEmpty(type);
  stats.SetHasNoNull();
  const auto last = frames.FrameOf(end - 1);
  for (auto f = frames.FrameOf(begin); f <= last; ++f) {
    duckdb::NumericStats::SetMin<T>(stats, frames.FrameMin(f));
    duckdb::NumericStats::SetMax<T>(stats, frames.FrameMax(f));
    if (filter.CheckStatistics(stats, filter_state) !=
        duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE) {
      return true;
    }
  }
  return false;
}

template<typename T>
void Filter(duckdb::ColumnSegment& segment, duckdb::ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            duckdb::SelectionVector& sel, idx_t& sel_count,
            const duckdb::TableFilter& filter,
            duckdb::TableFilterState& filter_state) {
  auto& scan = state.scan_state->Cast<ScanState<T>>();
  const uint64_t start = state.GetPositionInSegment();
  if constexpr (std::is_integral_v<T>) {
    if (filter.filter_type == duckdb::TableFilterType::EXPRESSION_FILTER &&
        !MayMatch(scan.frames, segment.GetType(), start, start + vector_count,
                  filter.Cast<duckdb::ExpressionFilter>(), filter_state)) {
      sel_count = 0;
      return;
    }
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  ScanPartial<T>(segment, state, vector_count, result, 0);
  duckdb::ColumnSegment::FilterSelection(sel, result, filter_state,
                                         vector_count, sel_count);
}

template<typename T>
struct FetchCache final : duckdb::SegmentScanState {
  duckdb::block_id_t block = INVALID_BLOCK;
  std::optional<FrameDecoder<T>> frames;
};

template<typename T>
void FetchRow(duckdb::ColumnSegment& segment, duckdb::ColumnFetchState& state,
              duckdb::row_t row_id, duckdb::Vector& result, idx_t result_idx) {
  auto& handle = state.GetOrInsertHandle(segment);
  auto* cached = dynamic_cast<FetchCache<T>*>(state.codec_state.get());
  if (!cached) {
    auto fresh = duckdb::make_uniq<FetchCache<T>>();
    cached = fresh.get();
    state.codec_state = std::move(fresh);
  }
  auto& cache = *cached;
  if (const auto block = segment.GetBlockHandle()->BlockId();
      cache.block != block) {
    const auto* base = handle.Ptr() + segment.GetBlockOffset();
    cache.frames.emplace(base, SegmentHeader(base, segment),
                         FrameCacheOf(segment));
    cache.block = block;
  }
  auto& frames = *cache.frames;
  const auto row = static_cast<uint64_t>(row_id);
  SDB_ENSURE(row < segment.count, "numeric codec: row out of range");
  frames.Seek(row);
  duckdb::FlatVector::GetDataMutable<T>(result)[result_idx] = frames.At(row);
}

duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  duckdb::QueryContext, duckdb::ColumnSegment& segment) {
  auto& buffer_manager =
    duckdb::BufferManager::GetBufferManager(segment.GetDatabase());
  auto handle = buffer_manager.Pin(segment.GetBlockHandle());
  const auto h = NumericHeader::Parse(handle.Ptr() + segment.GetBlockOffset(),
                                      segment.SegmentSize());
  constexpr std::array<std::string_view, 3> kLeaves{"none", "lz4", "zstd"};
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["transform"] = std::string{NumericTransformName(h.transform)};
  info["leaf"] = std::string{kLeaves[static_cast<uint8_t>(h.leaf)]};
  info["level"] = absl::StrCat(static_cast<uint32_t>(h.level));
  info["stored"] = absl::StrCat(static_cast<uint32_t>(h.stored));
  info["shuffled"] = h.Shuffled() ? "true" : "false";
  info["frames"] = absl::StrCat(h.frame_count);
  info["entries"] = absl::StrCat(h.dict_count);
  info["raw_bytes"] = absl::StrCat(h.raw_bytes);
  info["data_bytes"] = absl::StrCat(h.data_size);
  return info;
}

template<typename T>
duckdb::CompressionFunction MakeFunction(duckdb::PhysicalType physical) {
  duckdb::CompressionFunction f{
    duckdb::CompressionType::COMPRESSION_COL_NUMERIC,
    physical,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    InitScan<T>,
    ScanVector<T>,
    ScanPartial<T>,
    FetchRow<T>,
    duckdb::UncompressedFunctions::EmptySkip,
  };
  f.select = Select<T>;
  f.filter = Filter<T>;
  f.get_segment_info = SegmentInfo;
  return f;
}

}  // namespace

const duckdb::CompressionFunction* NumericScanFunction(
  duckdb::PhysicalType physical) {
  using duckdb::PhysicalType;
  static const auto kInt8 = MakeFunction<int8_t>(PhysicalType::INT8);
  static const auto kInt16 = MakeFunction<int16_t>(PhysicalType::INT16);
  static const auto kInt32 = MakeFunction<int32_t>(PhysicalType::INT32);
  static const auto kInt64 = MakeFunction<int64_t>(PhysicalType::INT64);
  static const auto kUint8 = MakeFunction<uint8_t>(PhysicalType::UINT8);
  static const auto kUint16 = MakeFunction<uint16_t>(PhysicalType::UINT16);
  static const auto kUint32 = MakeFunction<uint32_t>(PhysicalType::UINT32);
  static const auto kUint64 = MakeFunction<uint64_t>(PhysicalType::UINT64);
  static const auto kFloat = MakeFunction<float>(PhysicalType::FLOAT);
  static const auto kDouble = MakeFunction<double>(PhysicalType::DOUBLE);
  switch (physical) {
    case PhysicalType::INT8:
      return &kInt8;
    case PhysicalType::INT16:
      return &kInt16;
    case PhysicalType::INT32:
      return &kInt32;
    case PhysicalType::INT64:
      return &kInt64;
    case PhysicalType::UINT8:
      return &kUint8;
    case PhysicalType::UINT16:
      return &kUint16;
    case PhysicalType::UINT32:
      return &kUint32;
    case PhysicalType::UINT64:
      return &kUint64;
    case PhysicalType::FLOAT:
      return &kFloat;
    case PhysicalType::DOUBLE:
      return &kDouble;
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
