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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::column_internal {

inline bool BorrowsBlockStrings(const codecs::ColumnSegment& segment) {
  return segment.codec == codecs::CodecType::Uncompressed &&
         segment.type.InternalType() == duckdb::PhysicalType::VARCHAR;
}

inline void FetchSegmentRows(const codecs::ColumnSegment& segment,
                             codecs::ColumnFetchState& state,
                             const duckdb::idx_t* rows, duckdb::idx_t count,
                             duckdb::Vector& result,
                             const duckdb::SelectionVector& result_sel,
                             duckdb::idx_t result_offset) {
  SDB_ASSERT(count != 0 && rows[count - 1] < segment.count);
  segment.function.fetch_rows(segment, state, rows, count, result, result_sel,
                              result_offset);
}

}  // namespace irs::column_internal
