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

class CsBlockManager;

struct RgWindow {
  size_t rg = std::numeric_limits<size_t>::max();
  duckdb::idx_t begin = 0;
  duckdb::idx_t end = 0;
};

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
  // Unified ctor. Type-shape determines which extra params are honoured:
  //   primitive  -> data_pointers + validity_pointers; others empty/0.
  //   ARRAY      -> validity_pointers + element_child + array_size>0;
  //                 data_pointers empty (no self data on disk).
  //   LIST/MAP   -> data_pointers (per-row UBIGINT lengths) +
  //                 validity_pointers + element_child; array_size 0.
  //   STRUCT     -> validity_pointers + struct_children;
  //                 data_pointers empty, element_child null.
  ColumnReader(field_id id, duckdb::LogicalType type,
               std::vector<duckdb::DataPointer> data_pointers,
               std::vector<duckdb::DataPointer> validity_pointers,
               std::unique_ptr<ColumnReader> element_child,
               std::vector<std::unique_ptr<ColumnReader>> struct_children,
               uint64_t array_size, IndexInput& in,
               duckdb::DatabaseInstance& db, CsBlockManager& block_manager);

  ColumnReader(const ColumnReader&) = delete;
  ColumnReader& operator=(const ColumnReader&) = delete;

  field_id Id() const noexcept { return _id; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }

  uint64_t RowCount() const noexcept { return _row_count; }
  // ARRAY parents own no top-level data, so their row groups are reported
  // via the validity side -- validity tuple_counts are always in parent
  // rows by construction, so partitioning stays correct even when the
  // child column's codec splits segments mid-row.
  size_t RowGroupCount() const noexcept {
    if (_type.id() == duckdb::LogicalTypeId::ARRAY) {
      return _validity_pointers.size();
    }
    return _data_pointers.size();
  }
  uint64_t RowGroupOffset(size_t rg) const noexcept {
    if (_type.id() == duckdb::LogicalTypeId::ARRAY) {
      return _validity_offsets[rg];
    }
    return _data_offsets[rg];
  }
  uint64_t RowGroupRowCount(size_t rg) const noexcept {
    if (_type.id() == duckdb::LogicalTypeId::ARRAY) {
      return _validity_pointers[rg].tuple_count;
    }
    return _data_pointers[rg].tuple_count;
  }
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

    void Scan(uint64_t row_pos, duckdb::idx_t count, duckdb::Vector& out,
              duckdb::idx_t out_offset, bool may_use_entire = false);

   private:
    const ColumnReader* _reader;
    bool _validity;
    ScanCursor _cursor;
    RgWindow _window;
  };

  template<typename Rows>
  static void ScanRowsBatched(RangeScan& range, const Rows& rows,
                              duckdb::Vector& out, duckdb::idx_t out_offset,
                              bool may_use_entire = false) {
    if constexpr (requires { typename Rows::contiguous_range_tag; }) {
      if (rows.size() != 0) {
        range.Scan(rows[0], rows.size(), out, out_offset, may_use_entire);
      }
      return;
    } else {
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

  struct ListOffsetState {
    size_t rg = std::numeric_limits<size_t>::max();
    ScanCursor cursor;
    uint64_t next_pos = 0;
    uint64_t prev_offset = 0;
    duckdb::Vector buf{duckdb::LogicalType::UBIGINT, 1};
  };
  void ReadListOffset(ListOffsetState& state, size_t rg, uint64_t in_rg,
                      uint64_t& start, uint64_t& end) const;

  uint64_t ReadListOffsets(ListOffsetState& state, size_t rg,
                           uint64_t first_in_rg, duckdb::idx_t count,
                           duckdb::Vector& out_buf) const;

  class PointReadCursor {
   public:
    PointReadCursor(const ColumnReader& reader,
                    std::unique_ptr<IndexInput> in) noexcept
      : _reader{&reader}, _in{std::move(in)} {}

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
  RgWindow LocateValidity(uint64_t row_pos, RgWindow hint) const noexcept;
  duckdb::unique_ptr<duckdb::ColumnSegment> OpenValiditySegment(
    size_t vrg) const;
  friend class RangeScan;

  duckdb::unique_ptr<duckdb::ColumnSegment> OpenSegmentImpl(
    const duckdb::DataPointer& p, const duckdb::LogicalType& type,
    IndexInput& in) const;

  field_id _id;
  duckdb::LogicalType _type;
  std::vector<duckdb::DataPointer> _data_pointers;
  std::vector<duckdb::DataPointer> _validity_pointers;
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
  CsBlockManager* _block_manager;
  // Element-start prefix sums across LIST/MAP row groups, derived
  // eagerly from each segment's stats (max stored cumulative offset).
  std::vector<uint64_t> _rg_element_starts;
};

}  // namespace columnstore
}  // namespace irs
