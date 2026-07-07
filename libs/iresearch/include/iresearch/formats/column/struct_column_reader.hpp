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

#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/storage/statistics/struct_stats.hpp>

#include "basics/assert.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"

namespace irs {

class StructColumnReader final : public ColumnReader {
 public:
  StructColumnReader(field_id id, duckdb::LogicalType type,
                     std::unique_ptr<ColumnReader> validity,
                     std::vector<std::unique_ptr<ColumnReader>> children)
    : ColumnReader{
        id, std::move(type), {}, std::move(validity), std::move(children)} {
    _row_count = !_children.empty() ? _children.front()->RowCount()
                                    : (_validity ? _validity->RowCount() : 0);
    auto stats = duckdb::StructStats::CreateEmpty(_type);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      duckdb::StructStats::SetChildStats(
        stats, fi, _children[fi]->MergedStatistics().ToUnique());
    }
    FinishStats(std::move(stats));
  }

  uint64_t GatherCursor(const ScanState& s) const noexcept final {
    if (_validity) {
      return _validity->ColumnReader::GatherCursor(s.child_states[0]);
    }
    SDB_ASSERT(!_children.empty() && s.child_states.size() > 1);
    return _children.front()->GatherCursor(s.child_states[1]);
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final {
    NewOutputVector(s);
    duckdb::idx_t scan_count = count;
    if (_validity) {
      SDB_ASSERT(!s.child_states.empty());
      scan_count =
        _validity->ColumnReader::Scan(s.child_states[0], result, count);
      if (result.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
        SDB_ASSERT(duckdb::ConstantVector::IsNull(result));
        return scan_count;
      }
    }
    auto& entries = duckdb::StructVector::GetEntries(result);
    SDB_ASSERT(entries.size() == _children.size());
    SDB_ASSERT(s.child_states.size() == _children.size() + 1);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->Scan(s.child_states[fi + 1], entries[fi], count);
    }
    duckdb::FlatVector::SetSize(result, scan_count);
    return scan_count;
  }

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final {
    duckdb::idx_t scan_count = count;
    if (_validity) {
      scan_count = _validity->ColumnReader::ScanCount(s.child_states[0], result,
                                                      count, result_offset);
    }
    auto& entries = duckdb::StructVector::GetEntries(result);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->ScanCount(s.child_states[fi + 1], entries[fi], count,
                               result_offset);
    }
    return scan_count;
  }

  void Skip(ScanState& s, duckdb::idx_t count) const final {
    if (_validity) {
      _validity->ColumnReader::Skip(s.child_states[0], count);
    }
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->Skip(s.child_states[fi + 1], count);
    }
  }

  IRS_COLUMN_READER_GATHER_SCATTER

  void GatherDense(ScanState& s, uint64_t anchor,
                   const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                   duckdb::idx_t span, duckdb::Vector& out) const final {
    SDB_ASSERT(hits > 0 && hits <= span && span <= STANDARD_VECTOR_SIZE);
    NewOutputVector(s);
    const uint64_t cur = GatherCursor(s);
    SDB_ASSERT(anchor >= cur, "GatherDense requires ascending rows");
    if (hits == span) {
      if (anchor > cur) {
        Skip(s, anchor - cur);
      }
      Scan(s, out, span);
      return;
    }
    if (hits * 32 < span) {
      column_internal::ScatterRuns(*this, s, anchor, sel, hits, out, 0);
      return;
    }
    if (_validity) {
      _validity->ColumnReader::GatherScatter(s.child_states[0], anchor, sel,
                                             hits, out, 0);
    }
    auto& entries = duckdb::StructVector::GetEntries(out);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->GatherDense(s.child_states[fi + 1], anchor, sel, hits,
                                 span, entries[fi]);
    }
  }
};

}  // namespace irs
