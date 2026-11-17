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

#include <algorithm>
#include <cstdint>
#include <duckdb/common/enums/scan_vector_type.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/codec_type.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/formats/column/internal/write_chunk.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs::codecs {

struct Codec;
struct CompressionFunction;
struct DecompressionFunction;

enum class CompressionValidity : uint8_t {
  RequiresValidity,
  NoValidityRequired,
};

struct ColumnSegment {
  duckdb::BufferHandle Pin() const;

  const DecompressionFunction& function;
  const duckdb::LogicalType& type;
  const duckdb::BaseStatistics& stats;
  ReadContext& ctx;
  duckdb::shared_ptr<duckdb::BlockHandle> block;
  ReadContext::CacheSlot cache_slot;
  uint64_t count = 0;
  uint64_t size = 0;
  CodecType codec = CodecType::Uncompressed;
};

struct SegmentScanState {
  virtual ~SegmentScanState() = default;

  template<typename T>
  T& Cast() noexcept {
    return utils::downCast<T>(*this);
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

using FlushSegmentFn =
  absl::FunctionRef<void(const Codec& codec, duckdb::BaseStatistics stats,
                         uint64_t rows, std::string_view bytes)>;

struct State {
  virtual ~State() = default;
};

struct ColumnDataCheckpointData {
  void FlushSegment(CodecType codec, duckdb::BaseStatistics stats,
                    uint64_t rows, std::string_view bytes);

  template<typename S>
  S& Get() {
    for (auto& slot : states) {
      if (slot.tag == &kTag<S>) {
        return static_cast<S&>(*slot.state);
      }
    }
    return static_cast<S&>(
      *states.emplace_back(&kTag<S>, std::make_unique<S>(*this)).state);
  }

  struct Slot {
    const void* tag;
    std::unique_ptr<State> state;
  };

  template<typename S>
  static inline char kTag = 0;

  const duckdb::LogicalType& type;
  std::span<const WriteChunk> chunks;
  WriteContext* ctx = nullptr;
  FlushSegmentFn flush;
  ColCodecParams params;
  ColumnTuning& tuning;
  CodecSet disabled;
  uint32_t min_packed_run = 1;
  CodecType method = CodecType::Auto;
  uint64_t rows = 0;
  uint64_t written = 0;
  std::vector<Slot> states;
};

struct Estimate {
  CodecType codec = CodecType::Auto;
  double score = std::numeric_limits<double>::infinity();
};

struct CompressionFunction {
  Estimate (*analyze)(ColumnDataCheckpointData& data, double bound) = nullptr;
  bool (*compress)(ColumnDataCheckpointData& data) = nullptr;
};

struct Contender {
  const CompressionFunction* function;
  CodecSet codecs;
};

inline constexpr uint64_t kBlockSize = duckdb::Storage::DEFAULT_BLOCK_SIZE;

class CompressedSegment {
 public:
  CompressedSegment(CodecType codec, const duckdb::LogicalType& type);

  void Reset();

  duckdb::data_ptr_t Data() noexcept { return _buffer.get(); }

  void Flush(ColumnDataCheckpointData& checkpoint_data, uint64_t size);

  uint64_t count = 0;
  duckdb::BaseStatistics stats;

 private:
  CodecType _codec;
  duckdb::LogicalType _type;
  std::unique_ptr<duckdb::data_t[]> _buffer;
};

struct DecompressionFunction {
  std::unique_ptr<SegmentScanState> (*init_scan)(const ColumnSegment& segment) =
    nullptr;
  void (*scan)(const ColumnSegment& segment, ColumnScanState& state,
               duckdb::idx_t scan_count, duckdb::Vector& result,
               duckdb::idx_t result_offset,
               duckdb::ScanVectorType scan_type) = nullptr;
  void (*select)(const ColumnSegment& segment, ColumnScanState& state,
                 duckdb::idx_t vector_count, duckdb::Vector& result,
                 duckdb::idx_t result_offset,
                 const duckdb::SelectionVector& sel, duckdb::idx_t sel_count,
                 duckdb::ScanVectorType scan_type) = nullptr;
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

struct Codec {
  CodecType type = CodecType::Uncompressed;
  const CompressionFunction* compression = nullptr;
  const DecompressionFunction* decompression = nullptr;
};

duckdb::idx_t CountRows(std::span<const WriteChunk> chunks) noexcept;

uint64_t RawInput(const ColumnDataCheckpointData& data);

bool HasRows(std::span<const WriteChunk> chunks) noexcept;

const Codec* GetCodec(CodecType type, duckdb::PhysicalType physical);

template<CodecType kType, auto kAnalyze, auto kCompress, auto kAccepts>
struct Streamed {
  static Estimate Analyze([[maybe_unused]] ColumnDataCheckpointData& data,
                          double) {
    if constexpr (kAnalyze == nullptr) {
      return {};
    } else {
      return {
        .codec = kType,
        .score = static_cast<double>(kAnalyze(data.chunks)) /
                 static_cast<double>(std::max<uint64_t>(data.rows, 1)),
      };
    }
  }

  static bool Compress(ColumnDataCheckpointData& data) {
    if constexpr (kAccepts != nullptr) {
      if (!kAccepts(data.chunks)) {
        return false;
      }
    }
    kCompress(data, data.chunks);
    return true;
  }
};

template<CodecType kType, auto kAnalyze, auto kCompress,
         auto kAccepts = nullptr>
inline constexpr CompressionFunction kStreamed{
  .analyze = Streamed<kType, kAnalyze, kCompress, kAccepts>::Analyze,
  .compress = Streamed<kType, kAnalyze, kCompress, kAccepts>::Compress,
};

void Contest(std::span<const Contender> contenders,
             ColumnDataCheckpointData& data);

const CompressionFunction& UncompressedCompression(
  duckdb::PhysicalType physical);

void Compress(CodecType method, ColumnDataCheckpointData& data);

[[noreturn]] void ThrowCorrupted(std::string_view codec, std::string_view what);

}  // namespace irs::codecs
