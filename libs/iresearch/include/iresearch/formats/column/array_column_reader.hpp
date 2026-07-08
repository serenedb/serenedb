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

#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/storage/statistics/array_stats.hpp>

#include "basics/assert.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"

namespace irs {

class ArrayColumnReader final : public ColumnReader {
 public:
  ArrayColumnReader(field_id id, duckdb::LogicalType type,
                    std::unique_ptr<ColumnReader> validity,
                    std::vector<std::unique_ptr<ColumnReader>> children)
    : ColumnReader{
        id, std::move(type), {}, std::move(validity), std::move(children)} {
    _row_count = (_array_size != 0 && !_children.empty())
                   ? _children.front()->RowCount() / _array_size
                   : 0;
    auto stats = duckdb::ArrayStats::CreateEmpty(_type);
    duckdb::ArrayStats::SetChildStats(
      stats, _children.front()->MergedStatistics().ToUnique());
    FinishStats(std::move(stats));
  }

  uint64_t GatherCursor(const ScanState& s) const noexcept final {
    if (_validity) {
      return _validity->ColumnReader::GatherCursor(s.child_states[0]);
    }
    SDB_ASSERT(!_children.empty() && s.child_states.size() > 1);
    const uint64_t child = _children.front()->GatherCursor(s.child_states[1]);
    return _array_size != 0 ? child / _array_size : child;
  }

  // The child lives in element space; blocks seal at row-group boundaries,
  // so element ends are exact multiples of the array size.
  uint64_t RowGroupEnd(uint64_t row) const noexcept final {
    SDB_ASSERT(_array_size != 0 && !_children.empty());
    return _children.front()->RowGroupEnd(row * _array_size) / _array_size;
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final {
    NewOutputVector(s);
    return ScanCount(s, result, count, /*result_offset=*/0);
  }

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final {
    duckdb::idx_t scan_count = count;
    if (_validity) {
      scan_count = _validity->ColumnReader::ScanCount(s.child_states[0], result,
                                                      count, result_offset);
    }
    auto& child = duckdb::ArrayVector::GetChildMutable(result);
    _children[0]->ScanCount(s.child_states[1], child, count * _array_size,
                            result_offset * _array_size);
    return scan_count;
  }

  void Skip(ScanState& s, duckdb::idx_t count) const final {
    if (_validity) {
      _validity->ColumnReader::Skip(s.child_states[0], count);
    }
    _children[0]->Skip(s.child_states[1], count * _array_size);
  }

  IRS_COLUMN_READER_GATHER_SCATTER
  IRS_COLUMN_READER_GATHER_DENSE
};

}  // namespace irs
