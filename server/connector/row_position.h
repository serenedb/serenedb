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
#include <iresearch/types.hpp>
#include <span>

namespace sdb::connector {

inline constexpr uint32_t kRowPositionDocBits = 32;
inline constexpr uint64_t kRowPositionDocMask =
  (uint64_t{1} << kRowPositionDocBits) - 1;

inline uint64_t MakeRowPosition(uint32_t index, irs::doc_id_t doc) noexcept {
  return uint64_t{index} << kRowPositionDocBits | doc;
}

inline uint32_t RowPositionIndex(uint64_t position) noexcept {
  return static_cast<uint32_t>(position >> kRowPositionDocBits);
}

inline irs::doc_id_t RowPositionDoc(uint64_t position) noexcept {
  return static_cast<irs::doc_id_t>(position & kRowPositionDocMask);
}

inline void FillRowPositions(uint32_t index,
                             std::span<const irs::doc_id_t> docs,
                             duckdb::Vector& out) {
  auto positions = duckdb::FlatVector::Writer<duckdb::row_t>(out, docs.size());
  for (const auto doc : docs) {
    positions.WriteValue(
      static_cast<duckdb::row_t>(MakeRowPosition(index, doc)));
  }
}

inline void FillRowPositions(uint32_t index, irs::doc_id_t first,
                             const duckdb::SelectionVector* sel,
                             duckdb::idx_t count, duckdb::Vector& out) {
  if (!sel) {
    out.Sequence(static_cast<duckdb::row_t>(MakeRowPosition(index, first)), 1,
                 count);
    return;
  }
  auto positions = duckdb::FlatVector::Writer<duckdb::row_t>(out, count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    positions.WriteValue(static_cast<duckdb::row_t>(MakeRowPosition(
      index, first + static_cast<irs::doc_id_t>(sel->get_index(i)))));
  }
}

}  // namespace sdb::connector
