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

#include <algorithm>
#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector_buffer.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/data_pointer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iresearch/formats/column/internal/persistent_column_data.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class ColReader;

struct RgWindow {
  size_t rg = std::numeric_limits<size_t>::max();
  duckdb::idx_t begin = 0;
  duckdb::idx_t end = 0;
};

template<typename Rows>
inline size_t ConsecutiveRunLength(
  const Rows& rows, size_t i,
  uint64_t upper_bound = std::numeric_limits<uint64_t>::max()) noexcept {
  if constexpr (requires { typename Rows::contiguous_range_tag; }) {
    SDB_ASSERT(i < rows.size());
    const uint64_t begin = static_cast<uint64_t>(rows[i]);
    if (begin > upper_bound) {
      return 1;
    }
    return std::min(rows.size() - i, static_cast<size_t>(upper_bound - begin));
  }
  size_t run = 1;
  while (i + run < rows.size() &&
         static_cast<uint64_t>(rows[i + run]) ==
           static_cast<uint64_t>(rows[i + run - 1]) + 1 &&
         static_cast<uint64_t>(rows[i + run]) < upper_bound) {
    ++run;
  }
  return run;
}

class SharedPinHolder final : public duckdb::AuxiliaryDataHolder {
 public:
  explicit SharedPinHolder(std::shared_ptr<duckdb::BufferHandle> pin) noexcept
    : _pin{std::move(pin)} {}

 private:
  std::shared_ptr<duckdb::BufferHandle> _pin;
};

class ColumnReader final {
 public:
  struct VariantRgReader {
    uint64_t row_count = 0;
    VariantShredState shred_state = VariantShredState::Unshredded;
    std::unique_ptr<ColumnReader> unshredded;
    std::unique_ptr<ColumnReader> shredded;
  };

  ColumnReader(field_id id, duckdb::LogicalType type,
               std::vector<duckdb::DataPointer> data_pointers,
               std::vector<duckdb::DataPointer> validity_pointers,
               std::unique_ptr<ColumnReader> element_child,
               std::vector<std::unique_ptr<ColumnReader>> struct_children,
               uint64_t array_size, bool fully_shredded,
               duckdb::shared_ptr<duckdb::HyperLogLog> hyperloglog);

  ColumnReader(field_id id, duckdb::LogicalType type,
               std::vector<duckdb::DataPointer> validity_pointers,
               std::vector<VariantRgReader> variant_rgs,
               duckdb::shared_ptr<duckdb::HyperLogLog> hyperloglog);

  ColumnReader(const ColumnReader&) = delete;
  ColumnReader& operator=(const ColumnReader&) = delete;

  field_id Id() const noexcept { return _id; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }

  uint64_t RowCount() const noexcept { return _row_count; }
  bool HasValidity() const noexcept { return _has_validity; }
  bool FullyShredded() const noexcept { return _fully_shredded; }

  const duckdb::BaseStatistics& MergedStatistics() const noexcept {
    return *_stats;
  }

  const duckdb::HyperLogLog* HyperLogLog() const noexcept {
    return _hyperloglog.get();
  }

  size_t DataRgCount() const noexcept { return _data_pointers.size(); }
  size_t ValidityRgCount() const noexcept { return _validity_pointers.size(); }
  uint64_t ValidityRgFirstRow(size_t vrg) const noexcept {
    SDB_ASSERT(vrg < _validity_pointers.size());
    return _validity_offsets[vrg];
  }
  uint64_t ValidityRgRowCount(size_t vrg) const noexcept {
    SDB_ASSERT(vrg < _validity_pointers.size());
    return _validity_offsets[vrg + 1] - _validity_offsets[vrg];
  }
  bool IsValidityRgEmpty(size_t vrg) const noexcept {
    SDB_ASSERT(vrg < _validity_pointers.size());
    return _validity_pointers[vrg].compression_type ==
           duckdb::CompressionType::COMPRESSION_EMPTY;
  }

  RgWindow Locate(uint64_t row_pos, RgWindow hint = {}) const noexcept;

  duckdb::unique_ptr<duckdb::ColumnSegment> OpenSegment(size_t rg,
                                                        ReadContext& ctx) const;
  duckdb::unique_ptr<duckdb::ColumnSegment> OpenValiditySegment(
    size_t vrg, ReadContext& ctx) const;

  class ScanCursor {
   public:
    ScanCursor() noexcept = default;
    explicit ScanCursor(duckdb::unique_ptr<duckdb::ColumnSegment> seg)
      : _seg{std::move(seg)} {
      _seg->InitializeScan(_state);
      if (auto& block = _seg->GetBlockHandle()) {
        auto& bm = duckdb::BufferManager::GetBufferManager(_seg->GetDatabase());
        _pin = std::make_shared<duckdb::BufferHandle>(bm.Pin(block));
      }
    }

    ScanCursor(const ScanCursor&) = delete;
    ScanCursor& operator=(const ScanCursor&) = delete;
    ScanCursor(ScanCursor&&) noexcept = default;
    ScanCursor& operator=(ScanCursor&&) noexcept = default;

    // Re-initialize scan state on the same segment; SeekTo is forward-only,
    // so backward seeks must Reset() first.
    void Reset() noexcept {
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
      // Keep the segment's pinned block alive on the output Vector for BOTH
      // scan types. SCAN_FLAT_VECTOR can still reference segment memory for
      // non-inline string_t payloads (PK blobs, VARCHAR > 12 bytes), so
      // dropping the segment between Fetch and the caller consuming pk_data[k]
      // is a heap-use-after-free (ASAN-confirmed via SearchFullScanFunction ->
      // AppendPrimaryKey<PrimaryKeyI64I64>). The block is pinned once when the
      // cursor opens the segment; each chunk just shares that pin.
      if (_pin) {
        out_vec.BufferMutable().AddAuxiliaryData(
          std::make_unique<SharedPinHolder>(_pin));
      }
    }

    uint64_t Position() const noexcept { return _cursor; }
    explicit operator bool() const noexcept { return _seg != nullptr; }

   private:
    duckdb::unique_ptr<duckdb::ColumnSegment> _seg;
    duckdb::ColumnScanState _state{nullptr};
    std::shared_ptr<duckdb::BufferHandle> _pin;
    uint64_t _cursor = 0;
  };

  class RangeScan {
   public:
    RangeScan(const ColumnReader& reader, ReadContext& ctx,
              bool validity_side = false) noexcept
      : _reader{&reader}, _ctx{&ctx}, _validity{validity_side} {}

    RangeScan(const RangeScan&) = delete;
    RangeScan& operator=(const RangeScan&) = delete;
    RangeScan(RangeScan&&) noexcept = default;
    RangeScan& operator=(RangeScan&&) noexcept = default;

    void Scan(uint64_t row_pos, duckdb::idx_t count, duckdb::Vector& out,
              duckdb::idx_t out_offset, bool may_use_entire = false);

   private:
    const ColumnReader* _reader;
    ReadContext* _ctx;  // borrowed; owned by the caller's per-query state
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

  size_t VariantRgCount() const noexcept { return _variant_rgs.size(); }
  const VariantRgReader& VariantRg(size_t rg) const noexcept {
    SDB_ASSERT(rg < _variant_rgs.size());
    return _variant_rgs[rg];
  }
  RgWindow LocateVariantRg(uint64_t row, RgWindow hint = {}) const noexcept;

  class ListOffsetState {
   public:
    ListOffsetState(const ColumnReader& list_column, ReadContext& ctx) noexcept
      : _list_column{&list_column}, _ctx{&ctx} {}

    ListOffsetState(const ListOffsetState&) = delete;
    ListOffsetState& operator=(const ListOffsetState&) = delete;
    // Move-construct only (duckdb::Vector isn't move-assignable);
    // callers must build the state in-place, not assign over it.
    ListOffsetState(ListOffsetState&&) noexcept = default;
    ListOffsetState& operator=(ListOffsetState&&) = delete;

    void Read(size_t rg, uint64_t in_rg, uint64_t& start, uint64_t& end);
    uint64_t Read(size_t rg, uint64_t first_in_rg, duckdb::idx_t count,
                  duckdb::Vector& out_buf);

   private:
    const ColumnReader* _list_column;
    ReadContext* _ctx;  // borrowed
    size_t _rg = std::numeric_limits<size_t>::max();
    ScanCursor _cursor;
    uint64_t _next_pos = 0;
    uint64_t _prev_offset = 0;
    duckdb::Vector _buf{duckdb::LogicalType::UBIGINT, 1};
  };

  class PointReader {
   public:
    explicit PointReader(duckdb::DatabaseInstance& db) noexcept : _ctx{db} {}

    PointReader(const ColReader& col_reader, const ColumnReader& reader)
      : _ctx{col_reader}, _reader{&reader} {}

    PointReader(const PointReader&) = delete;
    PointReader& operator=(const PointReader&) = delete;

    void Reset(const ColReader& col_reader, const ColumnReader& reader);

    bool FetchRow(uint64_t row, duckdb::Vector& out, duckdb::idx_t out_offset);

   protected:
    bool FetchValidity(uint64_t row, duckdb::Vector& out,
                       duckdb::idx_t out_offset);
    void FetchData(uint64_t row, duckdb::Vector& out, duckdb::idx_t out_offset);

    ReadContext _ctx;
    const ColumnReader* _reader = nullptr;
    duckdb::unique_ptr<duckdb::ColumnSegment> _segment;
    duckdb::unique_ptr<duckdb::ColumnSegment> _validity_segment;
    duckdb::ColumnFetchState _fetch_state;
    duckdb::ColumnFetchState _validity_fetch_state;
    size_t _cached_rg = static_cast<size_t>(-1);
    size_t _cached_vrg = static_cast<size_t>(-1);
  };

  class BlobPointReader : public PointReader {
   public:
    using PointReader::PointReader;

    bytes_view FetchRow(uint64_t row);
    bytes_view FetchDoc(doc_id_t doc) {
      return FetchRow(static_cast<uint64_t>(doc) - doc_limits::min());
    }

    bool IsNullRow(uint64_t row);
    bool IsNullDoc(doc_id_t doc) {
      return IsNullRow(static_cast<uint64_t>(doc) - doc_limits::min());
    }

   private:
    duckdb::Vector _buf{duckdb::LogicalType::BLOB, 1};
  };

 private:
  RgWindow LocateValidity(uint64_t row_pos, RgWindow hint) const noexcept;
  friend class RangeScan;
  friend class PointReader;
  friend class BlobPointReader;
  friend class ListOffsetState;

  duckdb::unique_ptr<duckdb::ColumnSegment> OpenSegmentImpl(
    const duckdb::DataPointer& p, const duckdb::LogicalType& type,
    ReadContext& ctx) const;

  duckdb::BaseStatistics BuildMergedStatistics() const;

  field_id _id;
  duckdb::LogicalType _type;
  std::vector<duckdb::DataPointer> _data_pointers;
  std::vector<duckdb::DataPointer> _validity_pointers;
  std::vector<uint64_t> _data_offsets;      // size = data_pointers + 1
  std::vector<uint64_t> _validity_offsets;  // size = validity_pointers + 1
  uint64_t _row_count = 0;
  bool _has_validity = false;  // any RG with non-EMPTY validity codec
  bool _fully_shredded = true;
  std::unique_ptr<ColumnReader> _child;
  uint64_t _array_size = 0;  // 0 for non-ARRAY
  std::vector<std::unique_ptr<ColumnReader>>
    _struct_fields;  // empty for non-STRUCT
  // Element-start prefix sums across LIST/MAP row groups, derived
  // eagerly from each segment's stats (max stored cumulative offset).
  std::vector<uint64_t> _rg_element_starts;
  std::vector<VariantRgReader> _variant_rgs;
  duckdb::shared_ptr<duckdb::HyperLogLog> _hyperloglog;
  duckdb::unique_ptr<duckdb::BaseStatistics> _stats;
};

std::unique_ptr<ColumnReader> MakeColumnReader(field_id id,
                                               PersistentColumnData&& node);

}  // namespace irs
