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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/limits.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/operator/subtract.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/null_value.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/types/validity_mask.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/segment_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs {
namespace {

using duckdb::bitpacking_width_t;
using duckdb::const_data_ptr_t;
using duckdb::data_ptr_t;
using duckdb::idx_t;

using RleCount = uint16_t;

constexpr idx_t kGroupSize =
  duckdb::BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
constexpr uint64_t kPackedMarker = uint64_t{1} << 63;
constexpr RleCount kMaxRunCount = std::numeric_limits<RleCount>::max();

[[noreturn]] void ThrowCountOffsetInvalid() {
  ThrowCorrupted("RLE", "rle_count_offset is corrupted");
}

[[noreturn]] void ThrowWidthInvalid() {
  ThrowCorrupted("RLE", "bitpacking width exceeds the value width");
}

[[noreturn]] void ThrowEntryCountInvalid() {
  ThrowCorrupted("RLE", "entry count exceeds the segment capacity");
}

[[noreturn]] void ThrowRunCountArrayExhausted() {
  ThrowCorrupted("RLE", "run counts do not cover the segment row count");
}

[[noreturn]] void ThrowRunCountExceedsRemaining(RleCount run_count,
                                                idx_t remaining_count) {
  ThrowCorrupted(
    "RLE", absl::StrCat("run count ", run_count,
                        " exceeds the remaining row count ", remaining_count));
}

template<typename T>
bool ValueEqual(const T& lhs, const T& rhs) {
  if constexpr (std::is_same_v<T, float>) {
    return std::bit_cast<uint32_t>(lhs) == std::bit_cast<uint32_t>(rhs);
  } else if constexpr (std::is_same_v<T, double>) {
    return std::bit_cast<uint64_t>(lhs) == std::bit_cast<uint64_t>(rhs);
  } else {
    return lhs == rhs;
  }
}

template<typename T>
struct RleHeader {
  uint64_t counts_offset;
  uint32_t entry_count;
  RleCount count_frame;
  bitpacking_width_t value_width;
  bitpacking_width_t count_width;
  T value_frame;
};

template<typename T>
constexpr idx_t kHeaderSize = sizeof(RleHeader<T>);

template<typename T>
constexpr idx_t kMaxEntryCount =
  duckdb::AlignValueFloor<idx_t>((kBlockSize - kHeaderSize<T>) /
                                 (sizeof(T) + sizeof(RleCount))) -
  2 * kGroupSize;

template<typename T>
constexpr bool kIntegral = duckdb::NumericLimits<T>::IsIntegral();

template<typename T>
constexpr auto kFullWidth = static_cast<bitpacking_width_t>(sizeof(T) * 8);

template<typename T>
bitpacking_width_t WidthForRange(T minimum, T maximum, T& frame) {
  if constexpr (kIntegral<T>) {
    T min_max_diff{};
    if (!duckdb::TrySubtractOperator::Operation(maximum, minimum,
                                                min_max_diff)) {
      frame = 0;
      return kFullWidth<T>;
    }
    frame = minimum;
    return duckdb::BitpackingPrimitives::MinimumBitWidth<T, false>(
      min_max_diff);
  } else {
    frame = 0;
    return kFullWidth<T>;
  }
}

template<typename T>
bitpacking_width_t AnalyzeWidth(const T* values, idx_t count, T& frame) {
  if constexpr (kIntegral<T>) {
    SDB_ASSERT(count != 0);
    T minimum = values[0];
    T maximum = values[0];
    for (idx_t i = 1; i < count; ++i) {
      minimum = duckdb::MinValue<T>(minimum, values[i]);
      maximum = duckdb::MaxValue<T>(maximum, values[i]);
    }
    return WidthForRange(minimum, maximum, frame);
  } else {
    frame = 0;
    return kFullWidth<T>;
  }
}

template<typename T>
idx_t PackedSize(idx_t count, bitpacking_width_t width) {
  if constexpr (kIntegral<T>) {
    return duckdb::BitpackingPrimitives::GetRequiredSize(count, width);
  } else {
    return duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(count) *
           sizeof(T);
  }
}

template<typename T>
void Pack(data_ptr_t dst, T* values, idx_t count, T frame,
          bitpacking_width_t width) {
  if constexpr (kIntegral<T>) {
    using U = typename duckdb::MakeUnsigned<T>::type;
    if (frame) {
      for (idx_t i = 0; i < count; ++i) {
        reinterpret_cast<U*>(values)[i] -= static_cast<U>(frame);
      }
    }
    duckdb::BitpackingPrimitives::PackBuffer<T>(dst, values, count, width);
  } else {
    std::memcpy(dst, values, count * sizeof(T));
    std::memset(dst + count * sizeof(T), 0,
                PackedSize<T>(count, width) - count * sizeof(T));
  }
}

template<typename T>
void Unpack(T* dst, const_data_ptr_t src, idx_t count, T frame,
            bitpacking_width_t width) {
  if constexpr (kIntegral<T>) {
    using U = typename duckdb::MakeUnsigned<T>::type;
    duckdb::BitpackingPrimitives::UnPackBuffer<T>(
      reinterpret_cast<data_ptr_t>(dst), src, count, width, true);
    if (frame) {
      for (idx_t i = 0; i < count; ++i) {
        reinterpret_cast<U*>(dst)[i] += static_cast<U>(frame);
      }
    }
  } else {
    std::memcpy(dst, src, count * sizeof(T));
  }
}

template<typename T>
T UnpackValue(const_data_ptr_t src, idx_t index, T frame,
              bitpacking_width_t width) {
  if constexpr (kIntegral<T>) {
    using U = typename duckdb::MakeUnsigned<T>::type;
    T value = duckdb::BitpackingPrimitives::UnPackValue<T>(src, index, width);
    reinterpret_cast<U&>(value) += static_cast<U>(frame);
    return value;
  } else {
    return duckdb::Load<T>(src + index * sizeof(T));
  }
}

template<typename T>
struct RleState {
  T last_value = duckdb::NullValue<T>();
  RleCount last_seen_count = 0;
  bool all_null = true;
};

template<typename T, typename Sink>
void FlushRun(const RleState<T>& state, Sink&& sink) {
  sink(state.last_value, state.last_seen_count, state.all_null);
}

template<typename T, typename Sink>
void UpdateFlatValid(RleState<T>& state, const T* data, idx_t count,
                     Sink&& sink) {
  SDB_ASSERT(count != 0);
  if (state.all_null) [[unlikely]] {
    state.last_value = data[0];
    state.all_null = false;
  }
  idx_t i = 0;
  while (i < count) {
    idx_t j = i;
    while (j < count && ValueEqual(data[j], state.last_value)) {
      ++j;
    }
    if (j == i) {
      if (state.last_seen_count > 0) {
        FlushRun(state, sink);
      }
      state.last_value = data[i];
      state.last_seen_count = 1;
      ++i;
      continue;
    }
    idx_t run = j - i;
    while (run > 0) {
      const idx_t take =
        std::min<idx_t>(kMaxRunCount - state.last_seen_count, run);
      state.last_seen_count =
        static_cast<RleCount>(state.last_seen_count + take);
      run -= take;
      if (state.last_seen_count == kMaxRunCount) {
        FlushRun(state, sink);
        state.last_seen_count = 0;
      }
    }
    i = j;
  }
}

template<typename T, typename Sink>
void Update(RleState<T>& state, const T* data,
            const duckdb::ValidityMask& validity, idx_t idx, Sink&& sink) {
  if (validity.RowIsValid(idx)) {
    if (state.all_null) {
      state.last_value = data[idx];
      ++state.last_seen_count;
      state.all_null = false;
    } else if (ValueEqual(state.last_value, data[idx])) {
      ++state.last_seen_count;
    } else {
      if (state.last_seen_count > 0) {
        FlushRun(state, sink);
      }
      state.last_value = data[idx];
      state.last_seen_count = 1;
    }
  } else {
    ++state.last_seen_count;
  }
  if (state.last_seen_count == kMaxRunCount) {
    FlushRun(state, sink);
    state.last_seen_count = 0;
  }
}

template<typename T>
struct RleRuns {
  idx_t run_count = 0;
  T min_value{};
  T max_value{};
  RleCount min_count = 0;
  RleCount max_count = 0;
};

template<typename T>
struct RleAnalyzeState {
  RleState<T> state;
  RleRuns<T> runs;
  idx_t total_count = 0;
};

template<typename T>
void Observe(RleRuns<T>& runs, T value, RleCount count) {
  if (!runs.run_count) {
    runs.min_value = runs.max_value = value;
    runs.min_count = runs.max_count = count;
  } else {
    runs.min_value = duckdb::MinValue<T>(runs.min_value, value);
    runs.max_value = duckdb::MaxValue<T>(runs.max_value, value);
    runs.min_count = duckdb::MinValue<RleCount>(runs.min_count, count);
    runs.max_count = duckdb::MaxValue<RleCount>(runs.max_count, count);
  }
  ++runs.run_count;
}

template<typename T>
auto AnalyzeSink(RleRuns<T>& runs) {
  return
    [&runs](T value, RleCount count, bool) { Observe(runs, value, count); };
}

template<typename T>
IRS_NO_INLINE void AnalyzeVector(RleAnalyzeState<T>& analyze,
                                 const duckdb::Vector& input) {
  duckdb::UnifiedVectorFormat vdata;
  input.ToUnifiedFormat(vdata);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<T>(vdata);
  const idx_t count = input.size();
  auto run_state = analyze.state;
  auto runs = analyze.runs;
  const auto sink = AnalyzeSink(runs);
  if (!vdata.sel->IsSet() && vdata.validity.CannotHaveNull()) {
    UpdateFlatValid(run_state, data, count, sink);
  } else {
    for (idx_t i = 0; i < count; ++i) {
      Update(run_state, data, vdata.validity, vdata.sel->get_index(i), sink);
    }
  }
  analyze.state = run_state;
  analyze.runs = runs;
  analyze.total_count += count;
}

template<typename T>
idx_t AnalyzeChunks(std::span<const WriteChunk> chunks) {
  RleAnalyzeState<T> analyze;
  for (const auto& chunk : chunks) {
    AnalyzeVector(analyze, chunk.data);
  }
  auto& runs = analyze.runs;
  if (analyze.state.last_seen_count > 0) {
    FlushRun(analyze.state, AnalyzeSink(runs));
  }
  const auto count = runs.run_count;
  if (!count) {
    return 0;
  }
  if (count >= analyze.total_count) {
    return duckdb::DConstants::INVALID_INDEX;
  }
  T value_frame{};
  RleCount count_frame = 0;
  const auto value_width =
    WidthForRange(runs.min_value, runs.max_value, value_frame);
  const auto count_width =
    WidthForRange(runs.min_count, runs.max_count, count_frame);
  constexpr auto kEntries = kMaxEntryCount<T>;
  const auto full_size = PackedSize<T>(kEntries, value_width) +
                         PackedSize<RleCount>(kEntries, count_width);
  const auto rest = count % kEntries;
  return (count / kEntries) * full_size + PackedSize<T>(rest, value_width) +
         PackedSize<RleCount>(rest, count_width);
}

template<typename T, bool kWriteStatistics>
class RleCompressState {
 public:
  explicit RleCompressState(ColumnDataCheckpointData& checkpoint_data)
    : _checkpoint_data{&checkpoint_data},
      _segment{checkpoint_data.type},
      _pack_buffer{std::make_unique<duckdb::data_t[]>(kBlockSize)} {}

  void Append(const duckdb::UnifiedVectorFormat& vdata, idx_t count) {
    const auto* data = duckdb::UnifiedVectorFormat::GetData<T>(vdata);
    const auto writer = [this](T value, RleCount run, bool is_null) {
      WriteValue(value, run, is_null);
    };
    if (!vdata.sel->IsSet() && vdata.validity.CannotHaveNull()) {
      UpdateFlatValid(_state, data, count, writer);
      return;
    }
    for (idx_t i = 0; i < count; ++i) {
      const auto idx = vdata.sel->get_index(i);
      if constexpr (kWriteStatistics) {
        if (!vdata.validity.RowIsValid(idx)) {
          _stats.SetHasNull();
        }
      }
      Update(_state, data, vdata.validity, idx, writer);
    }
  }

  void Finalize() {
    FlushRun(_state, [this](T value, RleCount run, bool is_null) {
      WriteValue(value, run, is_null);
    });
    FlushSegment();
  }

 private:
  T* StagedValues() {
    return reinterpret_cast<T*>(_segment.Data() + kHeaderSize<T>);
  }

  RleCount* StagedCounts() {
    return reinterpret_cast<RleCount*>(_segment.Data() + kHeaderSize<T> +
                                       kMaxEntryCount<T> * sizeof(T));
  }

  void WriteValue(T value, RleCount count, bool is_null) {
    StagedValues()[_entry_count] = value;
    StagedCounts()[_entry_count] = count;
    ++_entry_count;
    if constexpr (kWriteStatistics) {
      if (!is_null) {
        _stats.Update(value);
      } else {
        _stats.SetHasNull();
      }
    }
    _segment.count += count;
    if (_entry_count == kMaxEntryCount<T>) {
      FlushSegment();
      _entry_count = 0;
    }
  }

  void FlushSegment() {
    auto* values = StagedValues();
    auto* counts = StagedCounts();
    RleHeader<T> header;
    std::memset(&header, 0, sizeof(header));
    header.entry_count = static_cast<uint32_t>(_entry_count);
    header.value_width = AnalyzeWidth(values, _entry_count, header.value_frame);
    header.count_width = AnalyzeWidth(counts, _entry_count, header.count_frame);
    const auto values_size = PackedSize<T>(_entry_count, header.value_width);
    const auto counts_size =
      PackedSize<RleCount>(_entry_count, header.count_width);
    header.counts_offset = kPackedMarker | (kHeaderSize<T> + values_size);
    Pack(_pack_buffer.get(), values, _entry_count, header.value_frame,
         header.value_width);
    Pack(_pack_buffer.get() + values_size, counts, _entry_count,
         header.count_frame, header.count_width);
    auto* data = _segment.Data();
    std::memcpy(data, &header, sizeof(header));
    std::memcpy(data + kHeaderSize<T>, _pack_buffer.get(),
                values_size + counts_size);
    _stats.Merge(_segment.stats);
    _segment.Flush(*_checkpoint_data,
                   kHeaderSize<T> + values_size + counts_size);
    _stats.Clear();
  }

  ColumnDataCheckpointData* _checkpoint_data;
  CompressedSegment _segment;
  RleState<T> _state;
  duckdb::StatsWriter<T> _stats;
  std::unique_ptr<duckdb::data_t[]> _pack_buffer;
  idx_t _entry_count = 0;
};

template<typename T, bool kWriteStatistics>
void CompressChunks(ColumnDataCheckpointData& checkpoint_data,
                    std::span<const WriteChunk> chunks) {
  const auto state =
    std::make_unique<RleCompressState<T, kWriteStatistics>>(checkpoint_data);
  for (const auto& chunk : chunks) {
    duckdb::UnifiedVectorFormat vdata;
    chunk.data.ToUnifiedFormat(vdata);
    state->Append(vdata, chunk.data.size());
  }
  state->Finalize();
}

template<typename T>
bool HasRuns(std::span<const WriteChunk> chunks) {
  if (const auto rows = CountRows(chunks); rows < 2) {
    return rows == 0;
  }
  bool first = true;
  T previous{};
  for (const auto& chunk : chunks) {
    duckdb::UnifiedVectorFormat vdata;
    chunk.data.ToUnifiedFormat(vdata);
    const auto* data = duckdb::UnifiedVectorFormat::GetData<T>(vdata);
    const idx_t count = chunk.data.size();
    for (idx_t i = 0; i < count; ++i) {
      const auto idx = vdata.sel->get_index(i);
      if (!vdata.validity.RowIsValid(idx)) {
        return true;
      }
      if (!first && ValueEqual(data[idx], previous)) {
        return true;
      }
      previous = data[idx];
      first = false;
    }
  }
  return false;
}

template<typename T>
struct RleLayout {
  idx_t entry_capacity = 0;
  const_data_ptr_t values = nullptr;
  const_data_ptr_t counts = nullptr;
  T value_frame{};
  RleCount count_frame = 0;
  bitpacking_width_t value_width = 0;
  bitpacking_width_t count_width = 0;
};

template<typename T>
RleLayout<T> ParseLayout(const duckdb::BufferHandle& handle,
                         const ColumnSegment& segment) {
  SegmentReader reader{handle.Ptr(), segment.size, "RLE segment"};
  const auto marked = reader.Read<uint64_t>();
  const auto counts_offset = marked & ~kPackedMarker;
  if (!(marked & kPackedMarker) || counts_offset < kHeaderSize<T> ||
      counts_offset > reader.Size() || counts_offset % alignof(RleCount)) {
    ThrowCountOffsetInvalid();
  }
  const auto header = reader.Get<RleHeader<T>>(0);
  if (header.value_width > kFullWidth<T> ||
      header.count_width > kFullWidth<RleCount>) {
    ThrowWidthInvalid();
  }
  if (header.entry_count > kMaxEntryCount<T>) {
    ThrowEntryCountInvalid();
  }
  const idx_t entry_capacity = header.entry_count;
  const auto values_size = PackedSize<T>(entry_capacity, header.value_width);
  const auto counts_size =
    PackedSize<RleCount>(entry_capacity, header.count_width);
  if (counts_offset - kHeaderSize<T> < values_size ||
      reader.Size() - counts_offset < counts_size) {
    ThrowCountOffsetInvalid();
  }
  return {
    .entry_capacity = entry_capacity,
    .values = reader.GetBytes(kHeaderSize<T>, values_size).data(),
    .counts = reader.GetBytes(counts_offset, counts_size).data(),
    .value_frame = header.value_frame,
    .count_frame = header.count_frame,
    .value_width = header.value_width,
    .count_width = header.count_width,
  };
}

template<typename T>
struct RleScanState;

template<typename T>
void LoadGroup(RleScanState<T>& scan);

template<typename T>
struct RleScanState final : SegmentScanState {
  RleScanState(duckdb::BufferHandle handle_p, const ColumnSegment& segment)
    : handle{std::move(handle_p)},
      layout{ParseLayout<T>(handle, segment)},
      max_entry_pos{layout.entry_capacity},
      unvalidated_row_count{segment.count} {
    if (max_entry_pos) {
      LoadGroup(*this);
    }
  }

  duckdb::BufferHandle handle;
  const RleLayout<T> layout;
  const idx_t max_entry_pos;
  idx_t entry_pos = 0;
  idx_t position_in_entry = 0;
  idx_t validated_entry_count = 0;
  idx_t unvalidated_row_count;
  std::unique_ptr<bool[]> matching_runs;
  idx_t matching_run_count = 0;
  std::array<T, kGroupSize> value_window{};
  std::array<RleCount, kGroupSize> count_window{};
  bool values_loaded = false;
};

template<typename T>
void LoadValues(RleScanState<T>& scan) {
  const idx_t group_start = scan.entry_pos & ~(kGroupSize - 1);
  Unpack(scan.value_window.data(),
         scan.layout.values + group_start * scan.layout.value_width / 8,
         kGroupSize, scan.layout.value_frame, scan.layout.value_width);
  scan.values_loaded = true;
}

template<typename T>
void LoadCounts(RleScanState<T>& scan) {
  const idx_t group_start = scan.entry_pos & ~(kGroupSize - 1);
  Unpack(scan.count_window.data(),
         scan.layout.counts + group_start * scan.layout.count_width / 8,
         kGroupSize, scan.layout.count_frame, scan.layout.count_width);
}

template<typename T>
void LoadGroup(RleScanState<T>& scan) {
  LoadValues(scan);
  LoadCounts(scan);
}

template<typename T>
T PackedValue(const RleScanState<T>& scan) {
  if (scan.values_loaded) {
    return scan.value_window[scan.entry_pos & (kGroupSize - 1)];
  }
  const idx_t group_start = scan.entry_pos & ~(kGroupSize - 1);
  return UnpackValue(
    scan.layout.values + group_start * scan.layout.value_width / 8,
    scan.entry_pos & (kGroupSize - 1), scan.layout.value_frame,
    scan.layout.value_width);
}

template<typename T>
RleCount RawCount(const RleScanState<T>& scan, idx_t entry_index) {
  SDB_ASSERT((entry_index & ~(kGroupSize - 1)) ==
             (scan.entry_pos & ~(kGroupSize - 1)));
  return scan.count_window[entry_index & (kGroupSize - 1)];
}

template<typename T>
T RunValue(const RleScanState<T>& scan) {
  return scan.value_window[scan.entry_pos & (kGroupSize - 1)];
}

template<typename T>
void ValidateRun(RleScanState<T>& scan, RleCount run_count) {
  if (run_count > scan.unvalidated_row_count) {
    ThrowRunCountExceedsRemaining(run_count, scan.unvalidated_row_count);
  }
  scan.unvalidated_row_count -= run_count;
  ++scan.validated_entry_count;
}

template<typename T>
void ValidateThrough(RleScanState<T>& scan, idx_t entry_index) {
  while (scan.validated_entry_count <= entry_index) {
    if (scan.validated_entry_count >= scan.max_entry_pos) {
      ThrowRunCountArrayExhausted();
    }
    ValidateRun(scan, RawCount(scan, scan.validated_entry_count));
  }
}

template<typename T>
void ForwardToNextRun(RleScanState<T>& scan) {
  ++scan.entry_pos;
  if (scan.entry_pos > scan.max_entry_pos) {
    ThrowRunCountArrayExhausted();
  }
  if (!(scan.entry_pos & (kGroupSize - 1)) &&
      scan.entry_pos < scan.max_entry_pos) {
    LoadGroup(scan);
  }
  scan.position_in_entry = 0;
}

template<typename T>
RleCount RunCount(RleScanState<T>& scan) {
  if (scan.entry_pos >= scan.validated_entry_count) {
    ValidateThrough(scan, scan.entry_pos);
  }
  return RawCount(scan, scan.entry_pos);
}

template<typename T>
RleCount CurrentRunLength(RleScanState<T>& scan) {
  auto run_length = RunCount(scan);
  while (run_length == 0) {
    ForwardToNextRun(scan);
    run_length = RunCount(scan);
  }
  return run_length;
}

template<typename T>
T CurrentValue(RleScanState<T>& scan) {
  CurrentRunLength(scan);
  return RunValue(scan);
}

template<typename T>
idx_t ValidateAllRuns(RleScanState<T>& scan) {
  const auto padded_count =
    duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(
      scan.max_entry_pos);
  auto counts = std::make_unique_for_overwrite<RleCount[]>(padded_count);
  Unpack(counts.get(), scan.layout.counts, padded_count,
         scan.layout.count_frame, scan.layout.count_width);
  while (scan.unvalidated_row_count > 0) {
    if (scan.validated_entry_count >= scan.max_entry_pos) {
      ThrowRunCountArrayExhausted();
    }
    ValidateRun(scan, counts[scan.validated_entry_count]);
  }
  return scan.validated_entry_count;
}

template<typename T>
bool ExhaustedRun(const RleScanState<T>& scan, RleCount run_length) {
  return scan.position_in_entry >= run_length;
}

template<typename T>
void SkipInternal(RleScanState<T>& scan, idx_t skip_count) {
  while (skip_count > 0) {
    const auto run_length = CurrentRunLength(scan);
    SDB_ASSERT(scan.position_in_entry < run_length);
    const idx_t skip_amount =
      std::min<idx_t>(skip_count, run_length - scan.position_in_entry);
    skip_count -= skip_amount;
    scan.position_in_entry += skip_amount;
    if (ExhaustedRun(scan, run_length)) {
      ForwardToNextRun(scan);
    }
  }
}

template<typename T>
constexpr idx_t kShortBurst = 32 / sizeof(T);

template<typename T>
constexpr idx_t kLongBurst = 128 / sizeof(T);

template<idx_t kBurst, typename T>
void FillRun(T* result, idx_t written, idx_t run_count, idx_t end,
             const T element) {
  if (written + kBurst <= end) {
    for (idx_t i = 0; i < kBurst; ++i) {
      result[written + i] = element;
    }
    for (idx_t i = kBurst; i < run_count; ++i) {
      result[written + i] = element;
    }
    return;
  }
  for (idx_t i = 0; i < run_count; ++i) {
    result[written + i] = element;
  }
}

template<idx_t kBurst, typename T>
void FillGroup(const RleScanState<T>& scan, T* result, idx_t& written,
               idx_t lane, idx_t runs, idx_t end) {
  for (idx_t r = 0; r < runs; ++r) {
    const idx_t run_count = scan.count_window[lane + r];
    FillRun<kBurst>(result, written, run_count, end,
                    scan.value_window[lane + r]);
    written += run_count;
  }
}

template<typename T>
void FillRuns(RleScanState<T>& scan, T* result, idx_t scan_count) {
  idx_t written = 0;
  while (written < scan_count) {
    if (scan.position_in_entry == 0) {
      const idx_t lane = scan.entry_pos & (kGroupSize - 1);
      const idx_t runs =
        std::min<idx_t>(kGroupSize - lane, scan.max_entry_pos - scan.entry_pos);
      idx_t total = 0;
      for (idx_t r = 0; r < runs; ++r) {
        total += scan.count_window[lane + r];
      }
      if (total && total <= scan_count - written) {
        ValidateThrough(scan, scan.entry_pos + runs - 1);
        if (total >= runs * kShortBurst<T>) {
          FillGroup<kLongBurst<T>>(scan, result, written, lane, runs,
                                   scan_count);
        } else {
          FillGroup<kShortBurst<T>>(scan, result, written, lane, runs,
                                    scan_count);
        }
        scan.entry_pos += runs;
        if (scan.entry_pos < scan.max_entry_pos) {
          LoadGroup(scan);
        }
        continue;
      }
    }
    const RleCount run_end = RunCount(scan);
    const T element = RunValue(scan);
    const idx_t run_count = run_end - scan.position_in_entry;
    const idx_t remaining = scan_count - written;
    if (run_count > remaining) [[unlikely]] {
      for (idx_t i = 0; i < remaining; ++i) {
        result[written + i] = element;
      }
      scan.position_in_entry += remaining;
      return;
    }
    FillRun<kShortBurst<T>>(result, written, run_count, scan_count, element);
    written += run_count;
    ForwardToNextRun(scan);
  }
}

template<typename T>
std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment& segment) {
  return std::make_unique<RleScanState<T>>(segment.Pin(), segment);
}

template<typename T>
void Skip(const ColumnSegment& segment, ColumnScanState& state,
          idx_t skip_count) {
  SDB_ASSERT(state.GetPositionInSegment() <= segment.count);
  SDB_ASSERT(skip_count <= state.GetPositionInSegment());
  SkipInternal(state.scan_state->Cast<RleScanState<T>>(), skip_count);
}

template<bool kEntireVector>
bool CanEmitConstantVector(idx_t position, idx_t run_length, idx_t scan_count) {
  if constexpr (!kEntireVector) {
    return false;
  } else {
    if (scan_count != STANDARD_VECTOR_SIZE) {
      return false;
    }
    SDB_ASSERT(position < run_length);
    return run_length - position >= scan_count;
  }
}

template<typename T>
void ScanConstant(RleScanState<T>& scan, RleCount run_length, idx_t scan_count,
                  duckdb::Vector& result) {
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::FlatVector::SetSize(result, duckdb::count_t{scan_count});
  duckdb::ConstantVector::GetData<T>(result)[0] = RunValue(scan);
  scan.position_in_entry += scan_count;
  if (ExhaustedRun(scan, run_length)) {
    ForwardToNextRun(scan);
  }
}

template<typename T, bool kEntireVector>
void ScanPartialInternal(const ColumnSegment& segment, ColumnScanState& state,
                         idx_t scan_count, duckdb::Vector& result,
                         idx_t result_offset) {
  SDB_ASSERT(state.GetPositionInSegment() <= segment.count &&
             scan_count <= segment.count - state.GetPositionInSegment());
  auto& scan = state.scan_state->Cast<RleScanState<T>>();
  const auto run_length = CurrentRunLength(scan);
  if (scan.position_in_entry < run_length &&
      CanEmitConstantVector<kEntireVector>(scan.position_in_entry, run_length,
                                           scan_count)) {
    ScanConstant(scan, run_length, scan_count, result);
    return;
  }
  FillRuns(scan, duckdb::FlatVector::GetDataMutable<T>(result) + result_offset,
           scan_count);
}

template<typename T>
void ScanPartial(const ColumnSegment& segment, ColumnScanState& state,
                 idx_t scan_count, duckdb::Vector& result,
                 idx_t result_offset) {
  ScanPartialInternal<T, false>(segment, state, scan_count, result,
                                result_offset);
}

template<typename T>
void Scan(const ColumnSegment& segment, ColumnScanState& state,
          idx_t scan_count, duckdb::Vector& result) {
  ScanPartialInternal<T, true>(segment, state, scan_count, result, 0);
}

template<typename T>
void Select(const ColumnSegment&, ColumnScanState& state, idx_t vector_count,
            duckdb::Vector& result, idx_t result_offset,
            const duckdb::SelectionVector& sel, idx_t sel_count,
            duckdb::ScanVectorType scan_type) {
  auto& scan = state.scan_state->Cast<RleScanState<T>>();
  const auto run_length = CurrentRunLength(scan);
  if (scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR &&
      scan.position_in_entry < run_length &&
      CanEmitConstantVector<true>(scan.position_in_entry, run_length,
                                  vector_count)) {
    ScanConstant(scan, run_length, vector_count, result);
    return;
  }
  auto writer = duckdb::FlatVector::Writer<T>(result, sel_count, result_offset);
  idx_t prev_idx = 0;
  for (idx_t i = 0; i < sel_count; ++i) {
    const auto next_idx = sel.get_index(i);
    SDB_ASSERT(next_idx >= prev_idx);
    SDB_ASSERT(next_idx < vector_count);
    SkipInternal(scan, next_idx - prev_idx);
    writer.WriteValue(CurrentValue(scan));
    prev_idx = next_idx;
  }
  SkipInternal(scan, vector_count - prev_idx);
}

template<typename T>
void Filter(const ColumnSegment&, ColumnScanState& state, idx_t vector_count,
            duckdb::Vector& result, duckdb::SelectionVector& sel,
            idx_t& sel_count, const duckdb::TableFilter&,
            duckdb::TableFilterState& filter_state) {
  auto& scan = state.scan_state->Cast<RleScanState<T>>();
  if (!scan.matching_runs) {
    const auto total_run_count = ValidateAllRuns(scan);
    scan.matching_runs = std::make_unique<bool[]>(total_run_count);
    const auto padded_run_count =
      duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(
        total_run_count);
    auto run_values = std::make_unique_for_overwrite<T[]>(padded_run_count);
    Unpack(run_values.get(), scan.layout.values, padded_run_count,
           scan.layout.value_frame, scan.layout.value_width);
    duckdb::Vector run_vector{result.GetType(),
                              reinterpret_cast<data_ptr_t>(run_values.get()),
                              total_run_count};
    duckdb::SelectionVector run_matches;
    scan.matching_run_count = total_run_count;
    duckdb::ColumnSegment::FilterSelection(run_matches, run_vector,
                                           filter_state, total_run_count,
                                           scan.matching_run_count);
    for (idx_t i = 0; i < scan.matching_run_count; ++i) {
      scan.matching_runs[run_matches.get_index(i)] = true;
    }
  }
  if (scan.matching_run_count == 0) {
    sel_count = 0;
    return;
  }
  auto* result_data = duckdb::FlatVector::GetDataMutable<T>(result);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

  duckdb::SelectionVector matching_sel;
  duckdb::sel_t* out_sel = nullptr;
  idx_t matching_count = 0;
  idx_t pos = 0;
  idx_t sel_idx = 0;
  idx_t prev_row = 0;
  while (pos < vector_count) {
    const auto run_length = CurrentRunLength(scan);
    const idx_t run_count = run_length - scan.position_in_entry;
    const idx_t take = std::min<idx_t>(vector_count - pos, run_count);
    const bool match = scan.matching_runs[scan.entry_pos];
    if (match) {
      std::fill_n(result_data + pos, take, RunValue(scan));
    }
    const idx_t run_begin_sel = sel_idx;
    while (sel_idx < sel_count && sel.get_index(sel_idx) < pos + take) {
      const auto row = sel.get_index(sel_idx);
      SDB_ASSERT(row >= prev_row);
      prev_row = row;
      ++sel_idx;
    }
    if (match) {
      if (out_sel) {
        for (idx_t i = run_begin_sel; i < sel_idx; ++i) {
          out_sel[matching_count++] =
            duckdb::UnsafeNumericCast<duckdb::sel_t>(sel.get_index(i));
        }
      } else {
        matching_count = sel_idx;
      }
    } else if (sel_idx != run_begin_sel && !out_sel) {
      matching_sel.Initialize(sel_count);
      out_sel = matching_sel.data();
      for (idx_t i = 0; i < run_begin_sel; ++i) {
        out_sel[i] = duckdb::UnsafeNumericCast<duckdb::sel_t>(sel.get_index(i));
      }
      matching_count = run_begin_sel;
    }
    scan.position_in_entry += take;
    if (ExhaustedRun(scan, run_length)) {
      ForwardToNextRun(scan);
    }
    pos += take;
  }

  if (matching_count != sel_count) {
    sel.Initialize(matching_sel);
    sel_count = matching_count;
  }
}

template<typename T>
struct RleFetchState final : SegmentScanState {
  RleFetchState(duckdb::BufferHandle handle, const ColumnSegment& segment)
    : scan{std::move(handle), segment} {}

  RleScanState<T> scan;
  idx_t row = 0;
  idx_t previous_group = duckdb::DConstants::INVALID_INDEX;
  std::vector<idx_t> group_rows = {0};
};

template<typename T>
void WalkInGroup(RleFetchState<T>& fetch, idx_t target) {
  auto& scan = fetch.scan;
  const idx_t group_end = std::min<idx_t>(
    (scan.entry_pos / kGroupSize + 1) * kGroupSize, scan.max_entry_pos);
  ValidateThrough(scan, group_end - 1);
  idx_t entry = scan.entry_pos;
  idx_t remaining = scan.position_in_entry + (target - fetch.row);
  while (true) {
    if (entry >= group_end) {
      ThrowRunCountArrayExhausted();
    }
    const idx_t run_length = RawCount(scan, entry);
    if (remaining < run_length) {
      break;
    }
    remaining -= run_length;
    ++entry;
  }
  scan.entry_pos = entry;
  scan.position_in_entry = remaining;
  fetch.row = target;
}

template<typename T>
void WalkTo(RleFetchState<T>& fetch, idx_t target) {
  auto& scan = fetch.scan;
  const auto group = scan.entry_pos / kGroupSize;
  if (group + 1 < fetch.group_rows.size()) {
    SDB_ASSERT(fetch.group_rows[group + 1] > target);
    WalkInGroup(fetch, target);
    return;
  }
  while (true) {
    while (fetch.group_rows.size() <= scan.entry_pos / kGroupSize) {
      fetch.group_rows.emplace_back(fetch.row);
    }
    if (scan.position_in_entry == 0) {
      const idx_t lane = scan.entry_pos & (kGroupSize - 1);
      const idx_t runs =
        std::min<idx_t>(kGroupSize - lane, scan.max_entry_pos - scan.entry_pos);
      idx_t total = 0;
      for (idx_t r = 0; r < runs; ++r) {
        total += RawCount(scan, scan.entry_pos + r);
      }
      if (runs > 0 && total <= target - fetch.row) {
        ValidateThrough(scan, scan.entry_pos + runs - 1);
        scan.entry_pos += runs;
        fetch.row += total;
        if (scan.entry_pos < scan.max_entry_pos) {
          LoadCounts(scan);
          scan.values_loaded = false;
        }
        continue;
      }
      WalkInGroup(fetch, target);
      return;
    }
    const auto run_length = CurrentRunLength(scan);
    if (fetch.row == target) {
      return;
    }
    const auto skip_amount =
      std::min<idx_t>(target - fetch.row, run_length - scan.position_in_entry);
    scan.position_in_entry += skip_amount;
    fetch.row += skip_amount;
    if (ExhaustedRun(scan, run_length)) {
      ForwardToNextRun(scan);
    }
  }
}

template<typename T>
void Seek(RleFetchState<T>& fetch, idx_t target) {
  auto& scan = fetch.scan;
  const auto current_group = scan.entry_pos / kGroupSize;
  if (target < fetch.row || (current_group + 1 < fetch.group_rows.size() &&
                             fetch.group_rows[current_group + 1] <= target)) {
    const auto group_end = absl::c_upper_bound(fetch.group_rows, target);
    const auto group =
      static_cast<idx_t>(group_end - fetch.group_rows.begin()) - 1;
    scan.entry_pos = group * kGroupSize;
    scan.position_in_entry = 0;
    if (group != current_group) {
      LoadCounts(scan);
      scan.values_loaded = false;
    }
    fetch.row = fetch.group_rows[group];
  }
  WalkTo(fetch, target);
}

template<typename T>
void FetchRows(const ColumnSegment& segment, ColumnFetchState& state,
               const idx_t* rows, idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  auto& fetch = state.GetOrCreateSegmentState<RleFetchState<T>>(
    [&] { return std::make_unique<RleFetchState<T>>(segment.Pin(), segment); });
  auto& scan = fetch.scan;
  auto* result_data = duckdb::FlatVector::GetDataMutable<T>(result);
  for (idx_t i = 0; i < count; ++i) {
    Seek(fetch, rows[i]);
    CurrentRunLength(scan);
    if (!scan.values_loaded) {
      const auto group = scan.entry_pos / kGroupSize;
      if (group == fetch.previous_group) {
        LoadValues(scan);
      }
      fetch.previous_group = group;
    }
    result_data[result_offset + result_sel.get_index(i)] = PackedValue(scan);
  }
}

template<typename T, duckdb::PhysicalType kPhysical,
         bool kWriteStatistics = true>
constexpr CompressionFunction kFunction{
  .type = CodecType::Rle,
  .analyze = AnalyzeChunks<T>,
  .compress = CompressChunks<T, kWriteStatistics>,
  .accepts = HasRuns<T>,
  .init_scan = InitScan<T>,
  .scan_vector = Scan<T>,
  .scan_partial = ScanPartial<T>,
  .select = Select<T>,
  .filter = kPhysical == duckdb::PhysicalType::BOOL ? nullptr : Filter<T>,
  .fetch_rows = FetchRows<T>,
  .skip = Skip<T>,
};

}  // namespace

const CompressionFunction* RleFunction(duckdb::PhysicalType physical) {
  using enum duckdb::PhysicalType;
  switch (physical) {
    case BOOL:
      return &kFunction<int8_t, BOOL>;
    case INT8:
      return &kFunction<int8_t, INT8>;
    case INT16:
      return &kFunction<int16_t, INT16>;
    case INT32:
      return &kFunction<int32_t, INT32>;
    case INT64:
      return &kFunction<int64_t, INT64>;
    case INT128:
      return &kFunction<duckdb::hugeint_t, INT128>;
    case UINT128:
      return &kFunction<duckdb::uhugeint_t, UINT128>;
    case UINT8:
      return &kFunction<uint8_t, UINT8>;
    case UINT16:
      return &kFunction<uint16_t, UINT16>;
    case UINT32:
      return &kFunction<uint32_t, UINT32>;
    case UINT64:
      return &kFunction<uint64_t, UINT64>;
    case FLOAT:
      return &kFunction<float, FLOAT>;
    case DOUBLE:
      return &kFunction<double, DOUBLE>;
    case LIST:
      return &kFunction<uint64_t, LIST, false>;
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
