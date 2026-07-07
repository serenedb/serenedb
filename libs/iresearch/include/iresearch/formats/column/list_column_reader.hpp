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

#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/storage/statistics/list_stats.hpp>
#include <optional>

#include "basics/assert.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"

namespace irs {

class ListColumnReader final : public ColumnReader {
 public:
  ListColumnReader(field_id id, duckdb::LogicalType type,
                   std::vector<ColumnBlockMeta> segments,
                   std::unique_ptr<ColumnReader> validity,
                   std::vector<std::unique_ptr<ColumnReader>> children)
    : ColumnReader{id, std::move(type), std::move(segments),
                   std::move(validity), std::move(children)} {
    auto stats = duckdb::ListStats::CreateEmpty(_type);
    duckdb::ListStats::SetChildStats(
      stats, _children.front()->MergedStatistics().ToUnique());
    FinishStats(std::move(stats));
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final {
    NewOutputVector(s);
    return ScanCount(s, result, count, /*result_offset=*/0);
  }

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final {
    if (count == 0) {
      return 0;
    }
    // Nested list children can request more than a vector's worth of rows;
    // everything else reuses the state-owned scratch.
    std::optional<duckdb::Vector> big;
    if (count > STANDARD_VECTOR_SIZE) {
      big.emplace(duckdb::LogicalType::UBIGINT, count);
    } else if (!s.list_offsets) {
      s.list_offsets =
        std::make_unique<VectorScratch>(duckdb::LogicalType::UBIGINT);
    }
    duckdb::Vector& offsets = big ? *big : s.list_offsets->Reset();
    const auto scan_count =
      ScanVector(s, offsets, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    if (_validity) {
      _validity->ColumnReader::ScanCount(s.child_states[0], result, count,
                                         result_offset);
    }
    const auto* odata = duckdb::FlatVector::GetData<uint64_t>(offsets);
    const uint64_t last_entry = odata[scan_count - 1];
    auto* list_entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
    const uint64_t base = s.st.last_offset;
    const uint64_t child_base =
      result_offset != 0 ? duckdb::ListVector::GetListSize(result) : 0;
    list_entries[result_offset] =
      duckdb::list_entry_t{child_base, odata[0] - base};
    for (duckdb::idx_t i = 1; i < scan_count; ++i) {
      list_entries[result_offset + i] = duckdb::list_entry_t{
        child_base + (odata[i - 1] - base), odata[i] - odata[i - 1]};
    }
    const uint64_t child_scan_count = last_entry - base;
    duckdb::ListVector::Reserve(result, child_base + child_scan_count);
    if (child_scan_count > 0) {
      auto& child = duckdb::ListVector::GetChildMutable(result);
      _children[0]->ScanCount(s.child_states[1], child,
                              static_cast<duckdb::idx_t>(child_scan_count),
                              child_base);
    }
    s.st.last_offset = last_entry;
    duckdb::ListVector::SetListSize(result, child_base + child_scan_count);
    return scan_count;
  }

  void Skip(ScanState& s, duckdb::idx_t count) const final {
    if (_validity) {
      _validity->ColumnReader::Skip(s.child_states[0], count);
    }
    if (count > 1) {
      SkipRows(s, count - 1);
    }
    if (!s.list_offsets) {
      s.list_offsets =
        std::make_unique<VectorScratch>(duckdb::LogicalType::UBIGINT);
    }
    duckdb::Vector& offsets = s.list_offsets->Reset();
    const auto scan_count =
      ScanVector(s, offsets, 1, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count == 1);
    const uint64_t last_entry =
      duckdb::FlatVector::GetData<uint64_t>(offsets)[0];
    const uint64_t child_skip = last_entry - s.st.last_offset;
    s.st.last_offset = last_entry;
    if (child_skip > 0) {
      _children[0]->Skip(s.child_states[1],
                         static_cast<duckdb::idx_t>(child_skip));
    }
  }

  IRS_COLUMN_READER_GATHER_SCATTER
  IRS_COLUMN_READER_GATHER_DENSE
};

}  // namespace irs
