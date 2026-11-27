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

#include <algorithm>
#include <cstring>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/enums/scan_vector_type.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <utility>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/alp_primitives.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::alp {

inline constexpr duckdb::idx_t kDecodeDistance = kVectorSize / 64;
inline constexpr uint32_t kDecodeRun = 8;

struct SegmentLayout {
  duckdb::const_data_ptr_t base = nullptr;
  uint32_t data_start = 0;
  uint32_t data_end = 0;
  uint32_t vector_count = 0;
};

inline uint32_t MetadataEnd(duckdb::const_data_ptr_t base, duckdb::idx_t size,
                            uint32_t header_size, std::string_view codec) {
  const uint32_t metadata_end =
    size < kMetadataPointerSize ? 0 : duckdb::Load<uint32_t>(base);
  if (metadata_end < header_size) [[unlikely]] {
    ThrowCorrupted(codec, "metadata ends before the segment header");
  }
  if (metadata_end > size) [[unlikely]] {
    ThrowCorrupted(codec, "metadata offset table exceeds the segment");
  }
  return metadata_end;
}

inline SegmentLayout ReadLayout(duckdb::const_data_ptr_t base,
                                uint32_t data_start, uint32_t metadata_end,
                                duckdb::idx_t count, std::string_view codec) {
  const duckdb::idx_t vector_count =
    count / kVectorSize + (count % kVectorSize != 0);
  if (data_start > metadata_end ||
      vector_count > (metadata_end - data_start) / kMetadataPointerSize)
    [[unlikely]] {
    ThrowCorrupted(codec, "metadata offset table exceeds the segment");
  }
  return {
    .base = base,
    .data_start = data_start,
    .data_end =
      static_cast<uint32_t>(metadata_end - vector_count * kMetadataPointerSize),
    .vector_count = static_cast<uint32_t>(vector_count),
  };
}

IRS_FORCE_INLINE inline std::span<const uint8_t> VectorBytes(
  const SegmentLayout& layout, duckdb::idx_t vector_index, uint32_t min_size,
  std::string_view codec) {
  SDB_ASSERT(vector_index < layout.vector_count);
  const auto* entry =
    layout.base + layout.data_end +
    (layout.vector_count - 1 - vector_index) * kMetadataPointerSize;
  const auto start = duckdb::Load<uint32_t>(entry);
  const auto end = vector_index + 1 < layout.vector_count
                     ? duckdb::Load<uint32_t>(entry - kMetadataPointerSize)
                     : layout.data_end;
  if (!(layout.data_start <= start && start <= end && end <= layout.data_end &&
        min_size <= end - start)) [[unlikely]] {
    ThrowCorrupted(codec, "vector offsets do not describe a data range");
  }
  return {layout.base + start, end - start};
}

template<typename State>
duckdb::idx_t LeftInVector(const State& state) {
  return kVectorSize - (state.total_value_count % kVectorSize);
}

template<typename State>
bool VectorFinished(const State& state) {
  return !(state.total_value_count % kVectorSize);
}

template<typename State, bool kSkip>
void ScanVector(State& state, typename State::Value* values,
                duckdb::idx_t vector_size) {
  SDB_ASSERT(vector_size <= kVectorSize);
  SDB_ASSERT(vector_size <= LeftInVector(state));
  if (VectorFinished(state)) {
    SDB_ASSERT(state.total_value_count < state.count);
    if constexpr (!kSkip) {
      if (vector_size == kVectorSize) {
        LoadVector(state, values);
        state.total_value_count += vector_size;
        return;
      }
    }
    LoadVector(state, state.vector_state->decoded_values);
  }
  if constexpr (!kSkip) {
    std::memcpy(values,
                state.vector_state->decoded_values + state.vector_state->index,
                sizeof(typename State::Value) * vector_size);
  }
  state.vector_state->index += vector_size;
  state.total_value_count += vector_size;
}

template<typename State>
void SkipVector(State& state) {
  SDB_ASSERT(state.total_value_count < state.count);
  state.total_value_count +=
    std::min<duckdb::idx_t>(kVectorSize, state.count - state.total_value_count);
}

template<typename State>
void SkipValues(State& state, duckdb::idx_t skip_count) {
  SDB_ASSERT(state.total_value_count <= state.count);
  SDB_ASSERT(skip_count <= state.count - state.total_value_count);
  if (state.total_value_count && !VectorFinished(state)) {
    const duckdb::idx_t to_skip =
      std::min<duckdb::idx_t>(skip_count, LeftInVector(state));
    ScanVector<State, true>(state, nullptr, to_skip);
    skip_count -= to_skip;
  }
  const duckdb::idx_t vectors_to_skip = skip_count / kVectorSize;
  for (duckdb::idx_t i = 0; i < vectors_to_skip; ++i) {
    SkipVector(state);
  }
  skip_count -= kVectorSize * vectors_to_skip;
  if (!skip_count) {
    return;
  }
  ScanVector<State, true>(state, nullptr, skip_count);
}

template<typename State>
SegmentScanStatePtr InitScan(const ColumnSegment& segment) {
  const auto alloc = segment.ctx.Allocator();
  return MakeScanState<State>(
    alloc, segment.Pin(), segment,
    MakeTrackedUniqueForOverwrite<typename State::VectorState>(alloc));
}

template<typename State>
void Scan(const ColumnSegment&, ColumnScanState& state,
          duckdb::idx_t scan_count, duckdb::Vector& result,
          duckdb::idx_t result_offset, duckdb::ScanVectorType) {
  auto& scan_state = state.scan_state->template Cast<State>();
  SDB_ASSERT(scan_state.total_value_count <= scan_state.count);
  SDB_ASSERT(scan_count <= scan_state.count - scan_state.total_value_count);

  auto* current_result_ptr = ResultValues(scan_state, result);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  current_result_ptr += result_offset;

  duckdb::idx_t scanned = 0;
  while (scanned < scan_count) {
    const auto remaining = scan_count - scanned;
    const duckdb::idx_t to_scan = std::min(remaining, LeftInVector(scan_state));
    ScanVector<State, false>(scan_state, current_result_ptr + scanned, to_scan);
    scanned += to_scan;
  }
}

template<typename State>
void Skip(const ColumnSegment&, ColumnScanState& state,
          duckdb::idx_t skip_count) {
  SkipValues(state.scan_state->template Cast<State>(), skip_count);
}

template<typename State>
struct FetchState final : SegmentScanState {
  FetchState(duckdb::BufferHandle handle, const ColumnSegment& segment)
    : scan_state{std::move(handle), segment} {}

  State scan_state;
  typename State::Vector opened;
  duckdb::idx_t opened_vector = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t decoded_vector = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t previous_row = duckdb::DConstants::INVALID_INDEX;
  uint32_t opened_rows = 0;
  uint32_t near_rows = 0;
};

template<typename State>
IRS_NO_INLINE auto OpenAndFetchValue(FetchState<State>& fetch_state,
                                     duckdb::idx_t row) {
  const auto vector_index = row / kVectorSize;
  const auto vector = OpenVector(fetch_state.scan_state, vector_index);
  fetch_state.opened = vector;
  fetch_state.opened_vector = vector_index;
  fetch_state.previous_row = row;
  fetch_state.opened_rows = 1;
  fetch_state.near_rows = 0;
  return ValueAt(fetch_state.scan_state, vector, row % kVectorSize);
}

template<typename State>
IRS_NO_INLINE auto FetchValue(FetchState<State>& fetch_state,
                              duckdb::idx_t row) {
  if (row / kVectorSize != fetch_state.opened_vector) {
    return OpenAndFetchValue(fetch_state, row);
  }
  return ValueAt(fetch_state.scan_state, fetch_state.opened, row % kVectorSize);
}

template<typename State>
IRS_FORCE_INLINE bool Dense(FetchState<State>& fetch_state,
                            const duckdb::idx_t* rows, duckdb::idx_t i,
                            duckdb::idx_t count, duckdb::idx_t last_vector) {
  const auto row = rows[i];
  const auto vector_index = row / kVectorSize;
  if (i + kSparseRows <= count &&
      rows[i + kSparseRows - 1] / kVectorSize == vector_index) {
    return true;
  }
  if (vector_index != fetch_state.opened_vector) {
    return false;
  }
  fetch_state.near_rows = row - fetch_state.previous_row <= kDecodeDistance
                            ? fetch_state.near_rows + 1
                            : 0;
  fetch_state.previous_row = row;
  return ++fetch_state.opened_rows >= kSparseRows ||
         (fetch_state.near_rows >= kDecodeRun && vector_index == last_vector);
}

template<typename State>
IRS_NO_INLINE void DecodeVector(FetchState<State>& fetch_state,
                                duckdb::idx_t vector_index) {
  auto& state = fetch_state.scan_state;
  if (!state.vector_state) {
    state.vector_state =
      MakeTrackedUniqueForOverwrite<typename State::VectorState>(state.alloc);
  }
  state.total_value_count = vector_index * kVectorSize;
  LoadVector(state, state.vector_state->decoded_values);
  fetch_state.decoded_vector = vector_index;
}

template<typename State>
IRS_FORCE_INLINE bool CopyOneDecoded(const FetchState<State>& fetch_state,
                                     const duckdb::idx_t* rows,
                                     duckdb::idx_t count,
                                     typename State::Value* result,
                                     const duckdb::SelectionVector& sel) {
  if (count != 1 || rows[0] / kVectorSize != fetch_state.decoded_vector) {
    return false;
  }
  result[sel.get_index(0)] =
    fetch_state.scan_state.vector_state->decoded_values[rows[0] % kVectorSize];
  return true;
}

template<typename State>
IRS_NO_INLINE duckdb::idx_t CopyDecoded(FetchState<State>& fetch_state,
                                        const duckdb::idx_t* rows,
                                        duckdb::idx_t i, duckdb::idx_t count,
                                        typename State::Value* result,
                                        const duckdb::SelectionVector& sel) {
  const auto vector_index = rows[i] / kVectorSize;
  if (vector_index != fetch_state.decoded_vector) {
    DecodeVector(fetch_state, vector_index);
  }
  const auto* decoded = fetch_state.scan_state.vector_state->decoded_values;
  do {
    result[sel.get_index(i)] = decoded[rows[i] % kVectorSize];
    ++i;
  } while (i < count && rows[i] / kVectorSize == vector_index);
  return i;
}

}  // namespace irs::codecs::alp
