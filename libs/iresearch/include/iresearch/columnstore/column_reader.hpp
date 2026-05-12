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
  bool HasValidity() const noexcept { return !_validity_pointers.empty(); }

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
              duckdb::idx_t out_offset) {
      _seg->Scan(_state, count, out_vec, out_offset,
                 duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
      _state.offset_in_column += count;
      _state.internal_index += count;
      _cursor += count;
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

    void Scan(uint64_t row_pos, duckdb::idx_t count, duckdb::Vector& out,
              duckdb::idx_t out_offset);

   private:
    const ColumnReader* _reader;
    bool _validity;
    ScanCursor _cursor;
    RgWindow _window;
  };

  template<typename Rows>
  static void ScanRowsBatched(RangeScan& range, const Rows& rows,
                              duckdb::Vector& out, duckdb::idx_t out_offset) {
    size_t i = 0;
    while (i < rows.size()) {
      size_t run_len = 1;
      while (i + run_len < rows.size() &&
             rows[i + run_len] - rows[i + run_len - 1] == 1) {
        ++run_len;
      }
      range.Scan(rows[i], run_len, out, out_offset + i);
      i += run_len;
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

  // LIST<T> per-row-group offsets. Cached prefix sum of size rg_rows + 1:
  // row i's element span is [offsets[i], offsets[i+1]).
  std::span<const uint64_t> ListOffsets(size_t rg) const;
  // Sum of lengths for row groups [0..rg). Cached.
  uint64_t RowGroupElementStart(size_t rg) const;

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
  std::unique_ptr<ColumnReader> _child;
  uint64_t _array_size = 0;  // 0 for non-ARRAY
  std::vector<std::unique_ptr<ColumnReader>>
    _struct_fields;  // empty for non-STRUCT
  IndexInput* _in;
  duckdb::DatabaseInstance* _db;

  // LIST lazy caches. ListOffsets(rg) populates _list_offsets_per_rg[rg]
  // as a prefix-sum span of size rg_rows + 1. _rg_element_starts[rg] has
  // sentinel uint64_t(-1) until computed.
  mutable std::vector<std::vector<uint64_t>> _list_offsets_per_rg;
  mutable std::vector<uint64_t> _rg_element_starts;
};

}  // namespace columnstore
}  // namespace irs
