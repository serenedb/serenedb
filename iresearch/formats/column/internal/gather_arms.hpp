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

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::column_internal {

template<typename Kind>
void ScatterRuns(const Kind& self, ColumnReader::ScanState& s, uint64_t anchor,
                 const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                 duckdb::Vector& out, duckdb::idx_t at) {
  uint64_t pos = self.Kind::GatherCursor(s);
  duckdb::idx_t i = 0;
  while (i < hits) {
    const uint64_t target = anchor + sel.get_index(i);
    SDB_ASSERT(target >= pos, "GatherScatter requires ascending rows");
    duckdb::idx_t run = 1;
    while (i + run < hits &&
           sel.get_index(i + run) == sel.get_index(i + run - 1) + 1) {
      ++run;
    }
    if (target > pos) {
      self.Kind::Skip(s, target - pos);
    }
    self.Kind::ScanCount(s, out, run, at + i);
    pos = target + run;
    i += run;
  }
}

template<typename Kind>
void DenseRuns(const Kind& self, ColumnReader::ScanState& s, uint64_t anchor,
               const duckdb::SelectionVector& sel, duckdb::idx_t hits,
               duckdb::idx_t span, duckdb::Vector& out) {
  SDB_ASSERT(hits <= span && span <= STANDARD_VECTOR_SIZE);
  const uint64_t cur = self.Kind::GatherCursor(s);
  SDB_ASSERT(anchor >= cur, "GatherDense requires ascending rows");
  if (hits == span) {
    if (anchor > cur) {
      self.Kind::Skip(s, anchor - cur);
    }
    self.Kind::Scan(s, out, span);
    return;
  }
  if (hits * 32 >= span) {
    if (anchor > cur) {
      self.Kind::Skip(s, anchor - cur);
    }
    self.Kind::ScanCount(s, out, span, 0);
    out.Slice(sel, hits);
    return;
  }
  ScatterRuns(self, s, anchor, sel, hits, out, 0);
}

inline void GatherSelected(const ColumnReader& reader,
                           ColumnReader::ScanState& s, uint64_t anchor,
                           const duckdb::SelectionVector& sel,
                           duckdb::idx_t hits, duckdb::idx_t span,
                           duckdb::Vector& out, duckdb::idx_t at = 0,
                           bool whole = true) {
  if (whole && at == 0 && span <= STANDARD_VECTOR_SIZE) {
    reader.GatherDense(s, anchor, sel, hits, span, out);
  } else {
    reader.GatherScatter(s, anchor, sel, hits, out, at);
  }
}

template<typename Rows>
void GatherRows(const ColumnReader& reader, ColumnReader::ScanState& s,
                const Rows& rows, duckdb::Vector& result,
                duckdb::idx_t out_offset, bool whole_output = false) {
  const auto n = static_cast<duckdb::idx_t>(rows.size());
  SDB_ASSERT(n <= STANDARD_VECTOR_SIZE, "GatherRows fills one vector");
  if (n == 0) {
    return;
  }
  if (s.sel.data() == nullptr) {
    s.sel.Initialize(STANDARD_VECTOR_SIZE);
  }
  const auto anchor = static_cast<uint64_t>(rows[0]);
  for (duckdb::idx_t i = 0; i < n; ++i) {
    SDB_ASSERT(i == 0 || rows[i] > rows[i - 1],
               "GatherRows requires ascending rows");
    s.sel.set_index(i, static_cast<uint64_t>(rows[i]) - anchor);
  }
  const auto span =
    static_cast<duckdb::idx_t>(static_cast<uint64_t>(rows[n - 1]) - anchor + 1);
  GatherSelected(reader, s, anchor, s.sel, n, span, result, out_offset,
                 whole_output);
}

}  // namespace irs::column_internal

// GatherScatter/GatherDense bodies shared by the nested typed readers. The
// gather helpers take the concrete `*this` (devirtualised `Kind::method`
// calls), so the bodies can't live in a non-template base method; a macro
// dedups them.
#define IRS_COLUMN_READER_GATHER_SCATTER                                     \
  void GatherScatter(ScanState& s, uint64_t anchor,                          \
                     const duckdb::SelectionVector& sel, duckdb::idx_t hits, \
                     duckdb::Vector& out, duckdb::idx_t at) const final {    \
    if (at == 0) {                                                           \
      NewOutputVector(s);                                                    \
    }                                                                        \
    column_internal::ScatterRuns(*this, s, anchor, sel, hits, out, at);      \
  }

#define IRS_COLUMN_READER_GATHER_DENSE                                     \
  void GatherDense(ScanState& s, uint64_t anchor,                          \
                   const duckdb::SelectionVector& sel, duckdb::idx_t hits, \
                   duckdb::idx_t span, duckdb::Vector& out) const final {  \
    NewOutputVector(s);                                                    \
    column_internal::DenseRuns(*this, s, anchor, sel, hits, span, out);    \
  }
