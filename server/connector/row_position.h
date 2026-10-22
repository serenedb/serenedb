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
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/types.hpp>

namespace sdb::connector {

inline constexpr uint32_t kRowPositionDocBits = 32;
inline constexpr uint64_t kRowPositionDocMask =
  (uint64_t{1} << kRowPositionDocBits) - 1;

inline uint64_t MakeRowPosition(uint32_t segment, irs::doc_id_t doc) noexcept {
  return uint64_t{segment} << kRowPositionDocBits | doc;
}

inline uint32_t RowPositionSegment(uint64_t position) noexcept {
  return static_cast<uint32_t>(position >> kRowPositionDocBits);
}

inline irs::doc_id_t RowPositionDoc(uint64_t position) noexcept {
  return static_cast<irs::doc_id_t>(position & kRowPositionDocMask);
}

template<typename DocOf>
void FillRowPositions(uint32_t segment, duckdb::idx_t count, DocOf&& doc_of,
                      duckdb::Vector& out) {
  if (segment == irs::DocRemoval::kNoSegment) {
    duckdb::ConstantVector::SetNull(out, duckdb::count_t(count));
    return;
  }
  auto* positions = duckdb::FlatVector::GetDataMutable<int64_t>(out);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    positions[i] = static_cast<int64_t>(MakeRowPosition(segment, doc_of(i)));
  }
  duckdb::FlatVector::SetSize(out, count);
}

}  // namespace sdb::connector
