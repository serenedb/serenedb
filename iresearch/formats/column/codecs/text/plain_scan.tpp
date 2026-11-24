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
#include <utility>

#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.tpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

template<typename Leaf>
struct PlainFetch final : SegmentScanState {
  void Reset(const ColumnSegment&) noexcept {}

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

  IRS_FORCE_INLINE uint32_t ReadOne(data_ptr_t, idx_t row) const {
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

template<typename Leaf>
struct PlainScan final : SegmentScanState {
  using Fetch = PlainFetch<Leaf>;
  static constexpr Shape kShape = Shape::Plain;
  static constexpr ByteCodec kCodec = Leaf::kCodec;
  static constexpr bool kDecodesWhole = Sorted(kCodec);
  static constexpr auto kValidity = CompressionValidity::RequiresValidity;

  PlainScan(const ColumnSegment& segment, duckdb::BufferHandle handle)
    : entries{segment, std::move(handle), true} {}

  void EnsureDictionary() { entries.DecodeAll(); }

  void ScanFlat(duckdb::Vector& result, idx_t result_offset, idx_t start,
                idx_t count) {
    const auto first = static_cast<uint32_t>(start);
    const auto end = static_cast<uint32_t>(start + count);
    if constexpr (requires(Leaf& leaf) {
                    leaf.DecodeInto(entries, result, result_offset, first, end);
                  }) {
      if (!entries.leaf->DecodeInto(entries, result, result_offset, first,
                                    end)) {
        ScanCached(result, result_offset, first, end);
      }
    } else {
      ScanCached(result, result_offset, first, end);
    }
  }

  void ScanCached(duckdb::Vector& result, idx_t result_offset, uint32_t first,
                  uint32_t end) {
    const auto entry_at = [&](idx_t i) { return first + i; };
    entries.leaf->Ensure(entries, end - first, entry_at, first, end);
    entries.WriteRefs(result, result_offset, end - first, entry_at);
  }

  void ScanVector(duckdb::Vector& result, idx_t start, idx_t count) {
    ScanFlat(result, 0, start, count);
  }

  void Select(duckdb::Vector& result, idx_t result_offset, idx_t start,
              idx_t span, const duckdb::SelectionVector& rows, idx_t count,
              duckdb::ScanVectorType) {
    const auto entry_at = [start, sel = rows](idx_t i) {
      return start + sel.get_index(i);
    };
    entries.leaf->Select(entries, result, result_offset, count, entry_at,
                         static_cast<uint32_t>(start),
                         static_cast<uint32_t>(start + span));
  }

  EntryFrames<Shape::Plain, Leaf> entries;
};

}  // namespace irs::codecs::text
