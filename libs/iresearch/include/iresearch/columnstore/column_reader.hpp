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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector_buffer.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/data_pointer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iresearch/store/data_input.hpp"
#include "iresearch/types.hpp"

namespace duckdb {

class DatabaseInstance;
}

namespace irs {
namespace columnstore {

struct RgWindow {
  size_t rg = std::numeric_limits<size_t>::max();
  duckdb::idx_t begin = 0;
  duckdb::idx_t end = 0;
};

// Length of the longest run starting at rows[i] where consecutive
// elements differ by exactly 1, optionally capped by `upper_bound`
// (exclusive). Shared by every site that coalesces doc_id batches into
// contiguous sub-runs before issuing a Scan.
template<typename Rows>
inline size_t ConsecutiveRunLength(
  const Rows& rows, size_t i,
  uint64_t upper_bound = std::numeric_limits<uint64_t>::max()) noexcept {
  size_t run = 1;
  while (i + run < rows.size() &&
         static_cast<uint64_t>(rows[i + run]) ==
           static_cast<uint64_t>(rows[i + run - 1]) + 1 &&
         static_cast<uint64_t>(rows[i + run]) < upper_bound) {
    ++run;
  }
  return run;
}

class ColumnReader final {
 public:
  // Primitive ctor.
  ColumnReader(field_id id, std::string name, duckdb::LogicalType type,
               std::vector<duckdb::DataPointer> data_pointers,
               std::vector<duckdb::DataPointer> validity_pointers,
               IndexInput& in, duckdb::DatabaseInstance& db);

  // ARRAY<child, array_size> ctor. row_count is derived from the child.
  ColumnReader(field_id id, std::string name, duckdb::LogicalType type,
               std::vector<duckdb::DataPointer> validity_pointers,
               std::unique_ptr<ColumnReader> element_child, uint64_t array_size,
               IndexInput& in, duckdb::DatabaseInstance& db);

  // LIST<child> ctor. data_pointers carry per-row UBIGINT lengths; offsets
  // are recovered as cumulative sums per row group (cached in
  // _list_offsets_per_rg). Also used for MAP, which DuckDB encodes as
  // LIST<STRUCT<key,value>> at PhysicalType::LIST.
  ColumnReader(field_id id, std::string name, duckdb::LogicalType type,
               std::vector<duckdb::DataPointer> data_pointers,
               std::vector<duckdb::DataPointer> validity_pointers,
               std::unique_ptr<ColumnReader> element_child, IndexInput& in,
               duckdb::DatabaseInstance& db);

  // STRUCT ctor. No top-level data of its own; row count is taken from
  // child[0]. Each child mirrors duckdb::StructColumnData::sub_columns.
  ColumnReader(field_id id, std::string name, duckdb::LogicalType type,
               std::vector<duckdb::DataPointer> validity_pointers,
               std::vector<std::unique_ptr<ColumnReader>> struct_children,
               IndexInput& in, duckdb::DatabaseInstance& db);

  ColumnReader(const ColumnReader&) = delete;
  ColumnReader& operator=(const ColumnReader&) = delete;

  field_id Id() const noexcept { return _id; }
  std::string_view Name() const noexcept { return _name; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }

  uint64_t RowCount() const noexcept { return _row_count; }
  size_t RowGroupCount() const noexcept { return _data_pointers.size(); }
  uint64_t RowGroupOffset(size_t rg) const noexcept {
    return _data_offsets[rg];
  }
  uint64_t RowGroupRowCount(size_t rg) const noexcept {
    return _data_pointers[rg].tuple_count;
  }
  // True iff at least one row-group has a non-EMPTY validity codec.  An
  // all-valid column still ships per-RG validity DataPointers (with codec
  // = COMPRESSION_EMPTY) so the *array* is non-empty; callers care
  // whether any actual validity bits need to be scanned, so we cache the
  // real answer at construction.
  bool HasValidity() const noexcept { return _has_validity; }

  RgWindow Locate(uint64_t row_pos, RgWindow hint = {}) const noexcept;

  duckdb::unique_ptr<duckdb::ColumnSegment> OpenSegment(size_t rg) const;

  class ScanCursor {
   public:
    ScanCursor() noexcept = default;
    explicit ScanCursor(duckdb::unique_ptr<duckdb::ColumnSegment> seg) noexcept
      : _seg{std::move(seg)} {
      _seg->InitializeScan(_state);
    }

    ScanCursor(const ScanCursor&) = delete;
    ScanCursor& operator=(const ScanCursor&) = delete;
    ScanCursor(ScanCursor&&) noexcept = default;
    ScanCursor& operator=(ScanCursor&&) noexcept = default;

    // Re-initialize scan state on the *same* segment.
    //  For backward seeks (SeekTo is forward-only).
    void Rewind() noexcept {
      _state = duckdb::ColumnScanState{nullptr};
      _seg->InitializeScan(_state);
      _cursor = 0;
    }

    void SeekTo(uint64_t target) noexcept {
      SDB_ASSERT(target >= _cursor);
      if (target > _cursor) {
        _state.offset_in_column = target;
        _seg->Skip(_state);
        _cursor = target;
      }
    }

    void Scan(duckdb::idx_t count, duckdb::Vector& out_vec,
              duckdb::idx_t out_offset,
              duckdb::ScanVectorType scan_type =
                duckdb::ScanVectorType::SCAN_FLAT_VECTOR) {
      _seg->Scan(_state, count, out_vec, out_offset, scan_type);
      _state.offset_in_column += count;
      _state.internal_index += count;
      _cursor += count;
      // SCAN_ENTIRE_VECTOR lets the codec zero-copy into `out_vec` --
      // FixedSizeScan does `FlatVector::SetData(result, segment_data, ...)`
      // pointing the result Vector at the segment's pinned page.  Our
      // ScanCursor owns the segment and would drop the page the moment
      // we move to a new row-group, so we pin a second reference to the
      // BlockHandle and stash it on the result Vector's auxiliary data.
      // The buffer then stays in memory until the downstream consumer
      // resets the vector.
      if (scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR &&
          _seg->block) {
        auto& bm = duckdb::BufferManager::GetBufferManager(_seg->db);
        auto& block = _seg->block;
        auto handle = bm.Pin(block);
        out_vec.BufferMutable().AddAuxiliaryData(
          std::make_unique<duckdb::PinnedBufferHolder>(std::move(handle)));
      }
    }

    uint64_t Position() const noexcept { return _cursor; }
    explicit operator bool() const noexcept { return _seg != nullptr; }

   private:
    duckdb::unique_ptr<duckdb::ColumnSegment> _seg;
    duckdb::ColumnScanState _state{nullptr};
    uint64_t _cursor = 0;
  };

  class RangeScan {
   public:
    explicit RangeScan(const ColumnReader& reader,
                       bool validity_side = false) noexcept
      : _reader{&reader}, _validity{validity_side} {}

    RangeScan(const RangeScan&) = delete;
    RangeScan& operator=(const RangeScan&) = delete;
    RangeScan(RangeScan&&) noexcept = default;
    RangeScan& operator=(RangeScan&&) noexcept = default;

    // `may_use_entire` lets the caller opt in to SCAN_ENTIRE_VECTOR when
    // safe (single Scan call into `out`, no validity scan pre-write).
    // Multi-run scans (random doc_ids) must keep it false: a follow-up
    // SCAN_FLAT_VECTOR into the same vector would assert.
    void Scan(uint64_t row_pos, duckdb::idx_t count, duckdb::Vector& out,
              duckdb::idx_t out_offset, bool may_use_entire = false);

   private:
    const ColumnReader* _reader;
    bool _validity;
    ScanCursor _cursor;
    RgWindow _window;
  };

  // Default form: walk `rows`, coalesce consecutive doc_ids into runs
  // (one range.Scan per run).
  template<typename Rows>
  static void ScanRowsBatched(RangeScan& range, const Rows& rows,
                              duckdb::Vector& out, duckdb::idx_t out_offset) {
    if constexpr (requires { typename Rows::contiguous_range_tag; }) {
      // Caller has already attested the range is fully contiguous --
      // skip the per-element consecutive-check loop and issue one scan.
      // Since this is the only Scan call into `out`, SCAN_ENTIRE_VECTOR
      // is safe (no follow-up FLAT write that would assert on a DICT
      // result).
      if (rows.size() != 0) {
        range.Scan(rows[0], rows.size(), out, out_offset,
                   /*may_use_entire=*/true);
      }
      return;
    } else {
      // Random doc_ids: each run is a separate Scan call into the same
      // output Vector.  Force SCAN_FLAT_VECTOR (default), otherwise the
      // first run could leave the vector as DICTIONARY and the next
      // run's FLAT write would assert.
      size_t i = 0;
      while (i < rows.size()) {
        const size_t run_len = ConsecutiveRunLength(rows, i);
        range.Scan(rows[i], run_len, out, out_offset + i);
        i += run_len;
      }
    }
  }

  // ARRAY/LIST element child. nullptr for primitives.
  const ColumnReader* Child() const noexcept { return _child.get(); }
  uint64_t ArraySize() const noexcept { return _array_size; }

  // STRUCT field access. Empty for non-STRUCT.
  size_t StructFieldCount() const noexcept { return _struct_fields.size(); }
  const ColumnReader& StructField(size_t i) const noexcept {
    SDB_ASSERT(i < _struct_fields.size());
    return *_struct_fields[i];
  }

  // Stateful per-row-group cursor over a LIST/MAP column's cumulative
  // offsets. Caller advances through monotonically non-decreasing
  // (rg, in_rg) pairs; on each call, ReadListOffset yields the row's
  // element span [start, end) as column-global child positions.
  // The scratch UBIGINT vector is reused across calls so the hot loop
  // doesn't reallocate per row.
  struct ListOffsetState {
    size_t rg = std::numeric_limits<size_t>::max();
    ScanCursor cursor;
    uint64_t next_pos = 0;
    uint64_t prev_offset = 0;
    duckdb::Vector buf{duckdb::LogicalType::UBIGINT, 1};
  };
  void ReadListOffset(ListOffsetState& state, size_t rg, uint64_t in_rg,
                      uint64_t& start, uint64_t& end) const;

  // Batched form: read `count` consecutive cumulative offsets starting
  // at row `first_in_rg` in row group `rg`. Writes `count` values into
  // `out_buf` (must be UBIGINT, capacity >= count); returns the
  // pre-batch cumulative offset that anchors the run's start.
  // Callers must NOT mix this with ReadListOffset on the same state.
  uint64_t ReadListOffsets(ListOffsetState& state, size_t rg,
                           uint64_t first_in_rg, duckdb::idx_t count,
                           duckdb::Vector& out_buf) const;

  // Per-cursor point-access for roughly-monotonic per-doc lookups.
  // Caches the open ColumnSegment + fetch state across calls in the same
  // row group. Owns a Reopen'd IndexInput so concurrent cursors are safe.
  class PointReadCursor {
   public:
    PointReadCursor(const ColumnReader& reader,
                    std::unique_ptr<IndexInput> in) noexcept
      : _reader{&reader}, _in{std::move(in)} {}

    // Reads row[row_pos] into out[out_idx]. Does not populate validity;
    // use the Scan path when null bits are needed.
    void FetchRow(uint64_t row_pos, duckdb::Vector& out, duckdb::idx_t out_idx);

   private:
    const ColumnReader* _reader;
    std::unique_ptr<IndexInput> _in;
    duckdb::unique_ptr<duckdb::ColumnSegment> _segment;
    duckdb::ColumnFetchState _fetch_state;
    size_t _cached_rg = static_cast<size_t>(-1);
  };

  PointReadCursor NewPointCursor() const;

 private:
  // Validity side. Independent RG breakdown from data; only RangeScan
  // uses these (callers go through RangeScan with validity_side=true).
  RgWindow LocateValidity(uint64_t row_pos, RgWindow hint) const noexcept;
  duckdb::unique_ptr<duckdb::ColumnSegment> OpenValiditySegment(
    size_t vrg) const;
  friend class RangeScan;

  // Reads byte_size bytes for the codec from `in`; other state (codec
  // lookup, BufferManager pin, ColumnSegment build) is independent of the
  // input source.
  duckdb::unique_ptr<duckdb::ColumnSegment> OpenSegmentImpl(
    const duckdb::DataPointer& p, const duckdb::LogicalType& type,
    IndexInput& in) const;

  field_id _id;
  std::string _name;
  duckdb::LogicalType _type;
  std::vector<duckdb::DataPointer> _data_pointers;
  std::vector<duckdb::DataPointer> _validity_pointers;
  // Cumulative row offsets, one trailing sentinel == _row_count. Data and
  // validity have separate breakdowns: codec segment fragmentation differs.
  std::vector<uint64_t> _data_offsets;      // size = data_pointers + 1
  std::vector<uint64_t> _validity_offsets;  // size = validity_pointers + 1
  uint64_t _row_count = 0;
  bool _has_validity = false;  // any RG with non-EMPTY validity codec
  std::unique_ptr<ColumnReader> _child;
  uint64_t _array_size = 0;  // 0 for non-ARRAY
  std::vector<std::unique_ptr<ColumnReader>>
    _struct_fields;  // empty for non-STRUCT
  IndexInput* _in;
  duckdb::DatabaseInstance* _db;

  // Element-start prefix sums across LIST/MAP row groups, derived
  // eagerly from each segment's stats (max stored cumulative offset).
  std::vector<uint64_t> _rg_element_starts;
};

}  // namespace columnstore
}  // namespace irs
