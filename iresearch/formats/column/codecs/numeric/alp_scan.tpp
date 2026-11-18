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
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <memory>
#include <utility>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/alp_primitives.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::alp {

inline constexpr duckdb::idx_t kDecodeDistance = kVectorSize / 64;

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
    LoadVector(state, state.vector_state.decoded_values);
  }
  if constexpr (!kSkip) {
    std::memcpy(values,
                state.vector_state.decoded_values + state.vector_state.index,
                sizeof(typename State::Value) * vector_size);
  }
  state.vector_state.index += vector_size;
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
std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment& segment) {
  return std::make_unique<State>(segment.Pin(), segment);
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
  duckdb::idx_t decoded_vector = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t previous_row = duckdb::DConstants::INVALID_INDEX;
};

inline bool Near(duckdb::idx_t row, duckdb::idx_t other) {
  return other != duckdb::DConstants::INVALID_INDEX &&
         row / kVectorSize == other / kVectorSize &&
         (row > other ? row - other : other - row) <= kDecodeDistance;
}

template<typename State>
bool Alone(FetchState<State>& fetch_state, const duckdb::idx_t* rows,
           duckdb::idx_t i, duckdb::idx_t count) {
  const auto previous = fetch_state.previous_row;
  fetch_state.previous_row = rows[i];
  return !Near(rows[i], previous) &&
         (i + 1 == count || !Near(rows[i], rows[i + 1]));
}

}  // namespace irs::codecs::alp
