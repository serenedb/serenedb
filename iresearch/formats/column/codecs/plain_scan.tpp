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

#include <absl/algorithm/container.h>

#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <optional>
#include <utility>

#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_scan.hpp"
#include "iresearch/formats/column/codecs/string_scan.tpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

template<typename Leaf>
struct PlainFetch final : duckdb::SegmentScanState {
  void Reset(duckdb::ColumnSegment&, data_ptr_t) noexcept {}

  void Read(data_ptr_t, const idx_t* rows, idx_t count, duckdb::Vector&,
            const duckdb::SelectionVector& sel, idx_t offset) {
    auto& order = frames.order;
    order.resize(count);
    auto* out = order.data();
    for (idx_t i = 0; i < count; ++i) {
      SDB_ENSURE(rows[i] < frames.header.row_count,
                 "col codec: row out of range");
      out[i] = {.entry = static_cast<uint32_t>(rows[i]),
                .at = offset + sel.get_index(i)};
    }
  }

  IRS_FORCE_INLINE std::optional<uint32_t> ReadOne(data_ptr_t,
                                                   idx_t row) const {
    SDB_ENSURE(row < frames.header.row_count, "col codec: row out of range");
    return static_cast<uint32_t>(row);
  }

  bool PlanBatch(const_data_ptr_t) {
    absl::c_reverse(frames.order);
    return false;
  }

  bool Whole() const noexcept {
    return frames.leaf.Walked() >= frames.header.entry_count;
  }

  FetchFrames<Leaf> frames;
};

template<typename L>
struct PlainScan final : duckdb::SegmentScanState {
  using Leaf = L;
  using Fetch = PlainFetch<L>;
  static constexpr Shape kShape = Shape::Plain;
  static constexpr auto kValidity =
    duckdb::CompressionValidity::REQUIRES_VALIDITY;

  PlainScan(duckdb::ColumnSegment& segment, duckdb::BufferHandle handle)
    : entries{segment, std::move(handle), true} {}

  void EnsureDictionary() { entries.DecodeAll(); }

  void ScanFlat(duckdb::Vector& result, idx_t result_offset, idx_t start,
                idx_t count) {
    entries.EnsureEach(count, [&](idx_t i) { return start + i; });
    entries.ReferenceHeap(result);
    auto writer =
      duckdb::FlatVector::Writer<string_t>(result, count, result_offset);
    for (idx_t i = 0; i < count; ++i) {
      writer.WriteStringRef(entries.values[start + i]);
    }
  }

  void ScanVector(duckdb::Vector& result, idx_t start, idx_t count) {
    ScanFlat(result, 0, start, count);
  }

  void Select(duckdb::Vector& result, idx_t start, idx_t,
              const duckdb::SelectionVector& rows, idx_t count) {
    entries.EnsureEach(count,
                       [&](idx_t i) { return start + rows.get_index(i); });
    entries.ReferenceHeap(result);
    auto writer = duckdb::FlatVector::Writer<string_t>(result, count);
    for (idx_t i = 0; i < count; ++i) {
      writer.WriteStringRef(entries.values[start + rows.get_index(i)]);
    }
  }

  void Filter(duckdb::Vector& result, idx_t start, idx_t count,
              duckdb::SelectionVector& sel, idx_t& sel_count,
              const duckdb::TableFilter&,
              duckdb::TableFilterState& filter_state) {
    ScanFlat(result, 0, start, count);
    duckdb::ColumnSegment::FilterSelection(sel, result, filter_state, count,
                                           sel_count);
  }

  EntryFrames<Shape::Plain, Leaf> entries;
};

}  // namespace irs::codecs::text
