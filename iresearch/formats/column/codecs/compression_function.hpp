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
#include <array>
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
#include "iresearch/utils/shared.hpp"

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
  uint64_t shared_offset = 0;
  uint64_t shared_size = 0;
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
  bool dynamic_filter = false;
};

struct ColumnFetchState {
  duckdb::BufferHandle& GetOrInsertHandle(const ColumnSegment& segment);

  template<typename T, typename Create>
  IRS_FORCE_INLINE T& GetOrCreateSegmentState(Create&& create) {
    if (!segment_state) {
      segment_state = create();
    }
    return segment_state->template Cast<T>();
  }

  duckdb::BufferHandle handle;
  std::unique_ptr<SegmentScanState> segment_state;
};

using FlushSegmentFn = absl::FunctionRef<void(
  const Codec& codec, duckdb::BaseStatistics stats, uint64_t rows,
  std::string_view bytes, std::string_view shared)>;

enum class StateId : uint8_t {
  NumericValues,
  NumericLz4,
  NumericZstd,
  NumericScratch,
  WideValues,
  Raw,
  For,
  Bitpacking,
  Steps,
  Delta,
  DeltaBitpacking,
  Runs,
  Rle,
  RleBitpacking,
  Entries,
  Dict,
  DictBitpacking,
  RoaringContainers,
  TextValues,
  TextWindow,
  TextLz4,
  TextZstd,
  TextFsst,
  TextZxc,
  TextFsstPlus,
  TextOnPair,
  TextFsst12,
  TextDictLz4,
  TextDictZstd,
  TextDictFsst,
  TextDictZxc,
  TextDictFsstPlus,
  TextDictOnPair,
  TextDictFsst12,
  FsstEncoder,
  FsstPlusEncoder,
  Fsst12Encoder,
  TextLz4Sizes,
  TextZstdSizes,
  TextZxcSizes,
  AlpSample,
  AlprdSample,
};

consteval StateId TextState(Shape shape, ByteCodec leaf) noexcept {
  return static_cast<StateId>(
    std::to_underlying(shape == Shape::Dictionary ? StateId::TextDictLz4
                                                  : StateId::TextLz4) +
    std::to_underlying(leaf) - std::to_underlying(ByteCodec::Lz4));
}

static_assert(TextState(Shape::Plain, ByteCodec::Fsst12) ==
              StateId::TextFsst12);
static_assert(TextState(Shape::Dictionary, ByteCodec::Fsst12) ==
              StateId::TextDictFsst12);

consteval StateId FsstEncoderState(ByteCodec leaf) noexcept {
  switch (leaf) {
    case ByteCodec::FsstPlus:
      return StateId::FsstPlusEncoder;
    case ByteCodec::Fsst12:
      return StateId::Fsst12Encoder;
    default:
      return StateId::FsstEncoder;
  }
}

struct State {
  virtual ~State() = default;
};

struct ColumnDataCheckpointData {
  template<typename S, typename... Args>
  S& Get(Args&&... args) {
    auto& state = states[std::to_underlying(S::kId)];
    if (!state) {
      state = std::make_unique<S>(*this, std::forward<Args>(args)...);
    }
    return static_cast<S&>(*state);
  }

  template<typename S>
  S* Find() noexcept {
    return static_cast<S*>(states[std::to_underlying(S::kId)].get());
  }

  const duckdb::LogicalType& type;
  std::span<const WriteChunk> chunks;
  WriteContext* ctx = nullptr;
  FlushSegmentFn flush;
  ColCodecParams params;
  ColumnTuning& tuning;
  CodecSet disabled;
  bool contested = false;
  double price = 1.0;
  uint8_t option = 0;
  uint32_t min_packed_run = 1;
  double run_price = 1.0;
  uint64_t rows = 0;
  std::array<std::unique_ptr<State>, magic_enum::enum_count<StateId>()> states;
};

struct CompressionFunction {
  double (*analyze)(ColumnDataCheckpointData& data, double bound) = nullptr;
  bool (*compress)(const Codec& codec,
                   ColumnDataCheckpointData& data) = nullptr;
  double (*estimate)(ColumnDataCheckpointData& data) = nullptr;
};

using CompressionFunctions = std::span<const CompressionFunction>;

inline constexpr uint64_t kBlockSize = duckdb::Storage::DEFAULT_BLOCK_SIZE;

class CompressedSegment {
 public:
  CompressedSegment(const Codec& codec, const duckdb::LogicalType& type);

  void Reset();

  duckdb::data_ptr_t Data() noexcept { return _buffer.get(); }

  void Flush(ColumnDataCheckpointData& checkpoint_data, uint64_t size);

  uint64_t count = 0;
  uint64_t stored = 0;
  duckdb::BaseStatistics stats;

 private:
  const Codec& _codec;
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
  CompressionFunctions compression;
  const DecompressionFunction& decompression;
};

duckdb::idx_t CountRows(std::span<const WriteChunk> chunks) noexcept;

bool HasRows(std::span<const WriteChunk> chunks) noexcept;

const Codec* GetCodec(CodecType type, duckdb::PhysicalType physical);

template<CodecType kType, auto kAnalyze, auto kCompress, auto kAccepts>
struct Streamed {
  static double Analyze([[maybe_unused]] ColumnDataCheckpointData& data,
                        double) {
    if constexpr (kAnalyze == nullptr) {
      return kNoScore;
    } else {
      return static_cast<double>(kAnalyze(data.chunks)) /
             static_cast<double>(std::max<uint64_t>(data.rows, 1)) * data.price;
    }
  }

  static bool Compress(const Codec& codec, ColumnDataCheckpointData& data) {
    if constexpr (kAccepts != nullptr) {
      if (!kAccepts(data.chunks)) {
        return false;
      }
    }
    kCompress(codec, data, data.chunks);
    return true;
  }
};

template<CodecType kType, auto kAnalyze, auto kCompress,
         auto kAccepts = nullptr>
inline constexpr CompressionFunction kStreamed{
  .analyze = Streamed<kType, kAnalyze, kCompress, kAccepts>::Analyze,
  .compress = Streamed<kType, kAnalyze, kCompress, kAccepts>::Compress,
};

bool CompressAuto(ColumnDataCheckpointData& data);

bool CompressAs(const Codec& codec, ColumnDataCheckpointData& data);

void CompressUncompressed(ColumnDataCheckpointData& data);

void Compress(CodecType method, ColumnDataCheckpointData& data);

[[noreturn]] void ThrowCorrupted(std::string_view codec, std::string_view what);

}  // namespace irs::codecs
