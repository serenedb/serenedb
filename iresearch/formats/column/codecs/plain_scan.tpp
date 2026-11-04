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
#include <duckdb/storage/table/column_segment.hpp>
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
    if (entries.Streams()) {
      Stream(result, result_offset, start, count);
      return;
    }
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
    SDB_ASSERT(!entries.Streams());
    entries.EnsureEach(count,
                       [&](idx_t i) { return start + rows.get_index(i); });
    entries.ReferenceHeap(result);
    auto writer = duckdb::FlatVector::Writer<string_t>(result, count);
    for (idx_t i = 0; i < count; ++i) {
      writer.WriteStringRef(entries.values[start + rows.get_index(i)]);
    }
  }

  void Stream(duckdb::Vector& result, idx_t result_offset, idx_t start,
              idx_t count) {
    SDB_ASSERT(start >= streamed);
    auto writer =
      duckdb::FlatVector::Writer<string_t>(result, count, result_offset);
    const auto stop = static_cast<uint32_t>(start + count);
    for (auto e = static_cast<uint32_t>(start); e < stop;) {
      if (frame == kNoFrame || e < entries.frames[frame].first_entry ||
          e >= entries.FrameEnd(frame)) {
        Open(entries.FrameOf(e));
      }
      ReferenceFrame(link, bytes, result);
      const auto first_entry = entries.frames[frame].first_entry;
      const auto end = std::min(stop, entries.FrameEnd(frame));
      const char* data = bytes->data.get();
      for (; e < end; ++e) {
        const auto k = e - first_entry;
        const uint32_t from = k == 0 ? 0 : ends[k - 1];
        writer.WriteStringRef(string_t{data + from, ends[k] - from});
      }
    }
    streamed = stop;
  }

  void Open(uint32_t f) {
    const bool first_frame =
      entries.header.flags == FrameDictionary::FirstFrame;
    std::string_view dictionary;
    if (f != 0 && first_frame) {
      if (!frame_zero) {
        frame_zero = Inflate(0, {}, nullptr);
      }
      dictionary = {frame_zero->data.get(), frame_zero->size};
    }
    ends.resize(entries.FrameEnd(f) - entries.frames[f].first_entry);
    bytes = Inflate(f, dictionary, ends.data());
    if (f == 0 && first_frame) {
      frame_zero = bytes;
    }
    frame = f;
  }

  duckdb::shared_ptr<FrameBytes> Inflate(uint32_t f,
                                         std::string_view dictionary,
                                         uint32_t* frame_ends) {
    auto out = duckdb::make_shared_ptr<FrameBytes>(entries.frames[f].raw_len);
    entries.leaf->Stream(entries, f, out->data.get(), dictionary, frame_ends);
    return out;
  }

  EntryFrames<Shape::Plain, Leaf> entries;
  uint32_t frame = kNoFrame;
  duckdb::shared_ptr<const FrameBytes> bytes;
  duckdb::shared_ptr<const FrameBytes> frame_zero;
  ScratchVector<uint32_t> ends;
  FrameLink link;
  idx_t streamed = 0;
};

}  // namespace irs::codecs::text
