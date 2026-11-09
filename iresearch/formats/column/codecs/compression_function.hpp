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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/read_context.hpp"

namespace irs::codecs {

struct CompressionFunction;

enum class CompressionValidity : uint8_t {
  RequiresValidity,
  NoValidityRequired,
};

struct ColumnSegment {
  duckdb::BufferHandle Pin() const;

  const CompressionFunction& function;
  const duckdb::LogicalType& type;
  const duckdb::BaseStatistics& stats;
  ReadContext& ctx;
  duckdb::shared_ptr<duckdb::BlockHandle> block;
  ReadContext::CacheSlot cache_slot;
  uint64_t count = 0;
  uint64_t size = 0;
};

struct SegmentScanState {
  virtual ~SegmentScanState() = default;

  template<typename T>
  T& Cast() noexcept {
    return static_cast<T&>(*this);
  }
};

struct ColumnScanState {
  uint64_t GetPositionInSegment() const noexcept { return offset_in_column; }

  std::unique_ptr<SegmentScanState> scan_state;
  std::vector<std::unique_ptr<SegmentScanState>> previous_states;
  uint64_t offset_in_column = 0;
  uint64_t internal_index = 0;
  uint64_t last_offset = 0;
};

struct ColumnFetchState {
  duckdb::BufferHandle& GetOrInsertHandle(const ColumnSegment& segment);

  template<typename T, typename Create>
  T& GetOrCreateSegmentState(Create&& create) {
    if (!segment_state) {
      segment_state = create();
    }
    return segment_state->template Cast<T>();
  }

  duckdb::BufferHandle handle;
  std::unique_ptr<SegmentScanState> segment_state;
};

struct AnalyzeState {
  virtual ~AnalyzeState() = default;

  template<typename T>
  T& Cast() noexcept {
    return static_cast<T&>(*this);
  }
};

struct CompressionAnalyzeContext {
  WriteContext& ctx;
};

using FlushSegmentFn = absl::FunctionRef<void(
  duckdb::BaseStatistics stats, uint64_t rows, std::string_view bytes)>;

struct ColumnDataCheckpointData {
  const duckdb::LogicalType& type;
  WriteContext& ctx;
  FlushSegmentFn flush;
};

struct CompressionState {
  virtual ~CompressionState() = default;

  template<typename T>
  T& Cast() noexcept {
    return static_cast<T&>(*this);
  }
};

inline constexpr uint64_t kBlockSize = duckdb::Storage::DEFAULT_BLOCK_SIZE;

class CompressedSegment {
 public:
  explicit CompressedSegment(const duckdb::LogicalType& type);

  void Reset();

  duckdb::data_ptr_t Data() noexcept { return _buffer.get(); }

  void Flush(ColumnDataCheckpointData& checkpoint_data, uint64_t size);

  uint64_t count = 0;
  duckdb::BaseStatistics stats;

 private:
  duckdb::LogicalType _type;
  std::unique_ptr<duckdb::data_t[]> _buffer;
};

struct CompressionFunction {
  duckdb::CompressionType type = duckdb::CompressionType::COMPRESSION_AUTO;
  duckdb::PhysicalType data_type = duckdb::PhysicalType::INVALID;
  std::unique_ptr<AnalyzeState> (*init_analyze)(
    CompressionAnalyzeContext& ctx, duckdb::PhysicalType type) = nullptr;
  bool (*analyze)(AnalyzeState& state, const duckdb::Vector& input) = nullptr;
  duckdb::idx_t (*final_analyze)(AnalyzeState& state) = nullptr;
  std::unique_ptr<CompressionState> (*init_compression)(
    ColumnDataCheckpointData& checkpoint_data,
    std::unique_ptr<AnalyzeState> state) = nullptr;
  void (*compress)(CompressionState& state,
                   const duckdb::Vector& input) = nullptr;
  void (*compress_finalize)(CompressionState& state) = nullptr;
  std::unique_ptr<SegmentScanState> (*init_scan)(const ColumnSegment& segment) =
    nullptr;
  void (*scan_vector)(const ColumnSegment& segment, ColumnScanState& state,
                      duckdb::idx_t scan_count,
                      duckdb::Vector& result) = nullptr;
  void (*scan_partial)(const ColumnSegment& segment, ColumnScanState& state,
                       duckdb::idx_t scan_count, duckdb::Vector& result,
                       duckdb::idx_t result_offset) = nullptr;
  void (*select)(const ColumnSegment& segment, ColumnScanState& state,
                 duckdb::idx_t vector_count, duckdb::Vector& result,
                 const duckdb::SelectionVector& sel,
                 duckdb::idx_t sel_count) = nullptr;
  void (*filter)(const ColumnSegment& segment, ColumnScanState& state,
                 duckdb::idx_t vector_count, duckdb::Vector& result,
                 duckdb::SelectionVector& sel, duckdb::idx_t& sel_count,
                 const duckdb::TableFilter& filter,
                 duckdb::TableFilterState& filter_state) = nullptr;
  void (*fetch_rows)(const ColumnSegment& segment, ColumnFetchState& state,
                     const duckdb::idx_t* rows, duckdb::idx_t count,
                     duckdb::Vector& result,
                     const duckdb::SelectionVector& result_sel,
                     duckdb::idx_t result_offset) = nullptr;
  void (*skip)(const ColumnSegment& segment, ColumnScanState& state,
               duckdb::idx_t skip_count) = nullptr;
  duckdb::InsertionOrderPreservingMap<std::string> (*get_segment_info)(
    const ColumnSegment& segment) = nullptr;
  CompressionValidity validity = CompressionValidity::RequiresValidity;
};

void EmptySkip(const ColumnSegment& segment, ColumnScanState& state,
               duckdb::idx_t skip_count);

[[noreturn]] void ThrowCorrupted(std::string_view codec, std::string_view what);

}  // namespace irs::codecs
