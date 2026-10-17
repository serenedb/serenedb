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

#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/types.hpp>
#include <limits>
#include <optional>
#include <string_view>

namespace sdb::connector {

inline constexpr uint32_t kRowPositionDocBits = 32;
inline constexpr uint64_t kRowPositionDocMask =
  (uint64_t{1} << kRowPositionDocBits) - 1;
inline constexpr uint64_t kNoRowPosition = std::numeric_limits<uint64_t>::max();

inline std::optional<uint64_t> SegmentNumber(std::string_view name) noexcept {
  const auto number = irs::SegmentNumber(name);
  if (!number || *number >= kRowPositionDocMask) {
    return std::nullopt;
  }
  return number;
}

inline uint64_t MakeRowPosition(uint64_t segment, irs::doc_id_t doc) noexcept {
  return segment << kRowPositionDocBits | doc;
}

inline uint64_t RowPositionSegment(uint64_t position) noexcept {
  return position >> kRowPositionDocBits;
}

inline irs::doc_id_t RowPositionDoc(uint64_t position) noexcept {
  return static_cast<irs::doc_id_t>(position & kRowPositionDocMask);
}

inline irs::DocRemoval::Row RemovedRow(int64_t rowid,
                                       uint64_t position) noexcept {
  if (position == kNoRowPosition) {
    return {irs::DocRemoval::kNoSegment, 0, rowid};
  }
  return {RowPositionSegment(position), RowPositionDoc(position), rowid};
}

template<typename DocOf>
void FillRowPositions(std::optional<uint64_t> segment, duckdb::idx_t count,
                      DocOf&& doc_of, duckdb::Vector& out) {
  if (!segment) {
    auto& validity = duckdb::FlatVector::ValidityMutable(out);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      validity.SetInvalid(i);
    }
    return;
  }
  auto* positions = duckdb::FlatVector::GetDataMutable<uint64_t>(out);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    positions[i] = MakeRowPosition(*segment, doc_of(i));
  }
}

}  // namespace sdb::connector
