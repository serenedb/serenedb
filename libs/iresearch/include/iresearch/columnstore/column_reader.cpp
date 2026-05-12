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

#include "iresearch/columnstore/column_reader.hpp"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/common/types.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/buffer/block_handle.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <utility>

#include "iresearch/columnstore/internal/overflow_string_io.hpp"
#include "iresearch/store/data_input.hpp"

namespace irs::columnstore {
namespace {

const duckdb::LogicalType kLengthsType{duckdb::LogicalTypeId::UBIGINT};
const duckdb::LogicalType kValidityType{duckdb::LogicalTypeId::VALIDITY};

bool AnyNonEmptyValidity(const std::vector<duckdb::DataPointer>& pointers) {
  for (const auto& p : pointers) {
    if (p.compression_type != duckdb::CompressionType::COMPRESSION_EMPTY) {
      return true;
    }
  }
  return false;
}

}  // namespace

ColumnReader::ColumnReader(field_id id, std::string name,
                           duckdb::LogicalType type,
                           std::vector<duckdb::DataPointer> data_pointers,
                           std::vector<duckdb::DataPointer> validity_pointers,
                           IndexInput& in, duckdb::DatabaseInstance& db)
  : _id{id},
    _name{std::move(name)},
    _type{std::move(type)},
    _data_pointers{std::move(data_pointers)},
    _validity_pointers{std::move(validity_pointers)},
    _has_validity{AnyNonEmptyValidity(_validity_pointers)},
    _in{&in},
    _db{&db} {
  auto build_offsets = [](const std::vector<duckdb::DataPointer>& pointers,
                          std::vector<uint64_t>& out) -> uint64_t {
    out.reserve(pointers.size() + 1);
    uint64_t total = 0;
    for (const auto& p : pointers) {
      out.push_back(total);
      total += p.tuple_count;
    }
    out.push_back(total);
    return total;
  };
  const uint64_t data_total = build_offsets(_data_pointers, _data_offsets);
  build_offsets(_validity_pointers, _validity_offsets);
  _row_count = data_total;
}

ColumnReader::ColumnReader(field_id id, std::string name,
                           duckdb::LogicalType type,
                           std::vector<duckdb::DataPointer> validity_pointers,
                           std::unique_ptr<ColumnReader> element_child,
                           uint64_t array_size, IndexInput& in,
                           duckdb::DatabaseInstance& db)
  : _id{id},
    _name{std::move(name)},
    _type{std::move(type)},
    _validity_pointers{std::move(validity_pointers)},
    _has_validity{AnyNonEmptyValidity(_validity_pointers)},
    _child{std::move(element_child)},
    _array_size{array_size},
    _in{&in},
    _db{&db} {
  SDB_ASSERT(_child);
  SDB_ASSERT(_array_size > 0);
  SDB_ASSERT((_child->RowCount() % _array_size) == 0);
  _row_count = _child->RowCount() / _array_size;
  _data_offsets.push_back(0);  // ARRAY has no self data; sentinel-only.
  _validity_offsets.reserve(_validity_pointers.size() + 1);
  uint64_t total = 0;
  for (const auto& p : _validity_pointers) {
    _validity_offsets.push_back(total);
    total += p.tuple_count;
  }
  _validity_offsets.push_back(total);
}

ColumnReader::ColumnReader(field_id id, std::string name,
                           duckdb::LogicalType type,
                           std::vector<duckdb::DataPointer> data_pointers,
                           std::vector<duckdb::DataPointer> validity_pointers,
                           std::unique_ptr<ColumnReader> element_child,
                           IndexInput& in, duckdb::DatabaseInstance& db)
  : _id{id},
    _name{std::move(name)},
    _type{std::move(type)},
    _data_pointers{std::move(data_pointers)},
    _validity_pointers{std::move(validity_pointers)},
    _has_validity{AnyNonEmptyValidity(_validity_pointers)},
    _child{std::move(element_child)},
    _in{&in},
    _db{&db} {
  SDB_ASSERT(_child);
  uint64_t total = 0;
  _data_offsets.reserve(_data_pointers.size() + 1);
  for (const auto& p : _data_pointers) {
    _data_offsets.push_back(total);
    total += p.tuple_count;
  }
  _data_offsets.push_back(total);
  _row_count = total;
  _validity_offsets.reserve(_validity_pointers.size() + 1);
  uint64_t vtotal = 0;
  for (const auto& p : _validity_pointers) {
    _validity_offsets.push_back(vtotal);
    vtotal += p.tuple_count;
  }
  _validity_offsets.push_back(vtotal);
  // Stored offsets are column-global cumulative; each RG's max stat is
  // the running total at end of that RG. `_rg_element_starts[r]` is the
  // child element start of row group r (== running total at end of RG
  // r-1); only used as the seed for ListOffsetState's first row.
  _rg_element_starts.reserve(_data_pointers.size() + 1);
  _rg_element_starts.push_back(0);
  for (const auto& p : _data_pointers) {
    if (p.tuple_count == 0) {
      _rg_element_starts.push_back(_rg_element_starts.back());
    } else {
      _rg_element_starts.push_back(
        duckdb::NumericStats::Max(p.statistics).GetValue<uint64_t>());
    }
  }
}

ColumnReader::ColumnReader(
  field_id id, std::string name, duckdb::LogicalType type,
  std::vector<duckdb::DataPointer> validity_pointers,
  std::vector<std::unique_ptr<ColumnReader>> struct_children, IndexInput& in,
  duckdb::DatabaseInstance& db)
  : _id{id},
    _name{std::move(name)},
    _type{std::move(type)},
    _validity_pointers{std::move(validity_pointers)},
    _has_validity{AnyNonEmptyValidity(_validity_pointers)},
    _struct_fields{std::move(struct_children)},
    _in{&in},
    _db{&db} {
  SDB_ASSERT(!_struct_fields.empty());
  for (const auto& f : _struct_fields) {
    SDB_ASSERT(f);
  }
  // Row count is determined by the first field; STRUCT has no own data.
  _row_count = _struct_fields.front()->RowCount();
  _data_offsets.push_back(0);  // sentinel only.
  _validity_offsets.reserve(_validity_pointers.size() + 1);
  uint64_t vtotal = 0;
  for (const auto& p : _validity_pointers) {
    _validity_offsets.push_back(vtotal);
    vtotal += p.tuple_count;
  }
  _validity_offsets.push_back(vtotal);
}

namespace {

RgWindow LocateInOffsets(uint64_t row_pos, const std::vector<uint64_t>& offsets,
                         RgWindow hint) noexcept {
  if (row_pos >= hint.end) {
    // Forward jump. hint.rg + 1 is the common sequential-forward step.
    const size_t next = hint.rg + 1;
    SDB_ASSERT(next + 1 < offsets.size());
    if (row_pos < offsets[next + 1]) {
      return {next, hint.end, offsets[next + 1]};
    }
    SDB_ASSERT(next + 2 < offsets.size());
    auto it =
      std::upper_bound(offsets.begin() + next + 2, offsets.end(), row_pos);
    const size_t rg = static_cast<size_t>(it - offsets.begin() - 1);
    return {rg, offsets[rg], offsets[rg + 1]};
  }
  if (row_pos < hint.begin) {
    // Backward jump: answer is strictly before hint.rg.
    SDB_ASSERT(hint.rg < offsets.size());
    auto it =
      std::upper_bound(offsets.begin(), offsets.begin() + hint.rg, row_pos);
    const size_t rg = static_cast<size_t>(it - offsets.begin() - 1);
    return {rg, offsets[rg], offsets[rg + 1]};
  }
  return hint;
}

}  // namespace

RgWindow ColumnReader::Locate(uint64_t row_pos, RgWindow hint) const noexcept {
  SDB_ASSERT(_type.id() != duckdb::LogicalTypeId::ARRAY &&
               _type.id() != duckdb::LogicalTypeId::STRUCT,
             "Locate has no meaning on parents with no top-level data");
  SDB_ASSERT(row_pos < _row_count);
  return LocateInOffsets(row_pos, _data_offsets, hint);
}

RgWindow ColumnReader::LocateValidity(uint64_t row_pos,
                                      RgWindow hint) const noexcept {
  SDB_ASSERT(row_pos < _row_count);
  return LocateInOffsets(row_pos, _validity_offsets, hint);
}

void ColumnReader::RangeScan::Scan(uint64_t row_pos, duckdb::idx_t count,
                                   duckdb::Vector& out,
                                   duckdb::idx_t out_offset,
                                   bool may_use_entire) {
  while (count > 0) {
    if (row_pos < _window.begin || _window.end <= row_pos) {
      _window = _validity ? _reader->LocateValidity(row_pos, _window)
                          : _reader->Locate(row_pos, _window);
      _cursor = ScanCursor{_validity ? _reader->OpenValiditySegment(_window.rg)
                                     : _reader->OpenSegment(_window.rg)};
    }
    _cursor.SeekTo(row_pos - _window.begin);
    const auto take = std::min<duckdb::idx_t>(count, _window.end - row_pos);
    const bool single_shot = (out_offset == 0 && take == count);
    const auto scan_type =
      (may_use_entire && single_shot && !_validity && !_reader->HasValidity())
        ? duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR
        : duckdb::ScanVectorType::SCAN_FLAT_VECTOR;
    _cursor.Scan(take, out, out_offset, scan_type);
    row_pos += take;
    count -= take;
    out_offset += take;
  }
}

duckdb::unique_ptr<duckdb::ColumnSegment> ColumnReader::OpenSegmentImpl(
  const duckdb::DataPointer& p, const duckdb::LogicalType& type,
  IndexInput& in) const {
  auto& cfg = duckdb::DBConfig::GetConfig(*_db);
  auto codec =
    cfg.TryGetCompressionFunction(p.compression_type, type.InternalType());
  SDB_ENSURE(codec, sdb::ERROR_INTERNAL,
             "columnstore: missing compression function for codec type ",
             static_cast<uint8_t>(p.compression_type));
  auto stats = p.statistics.Copy();
  const auto byte_size = static_cast<duckdb::idx_t>(p.block_pointer.offset);

  SDB_ENSURE(!p.segment_state, sdb::ERROR_INTERNAL,
             "columnstore: codec segment_state is not plumbed through "
             "OpenSegment (codec ",
             static_cast<uint8_t>(p.compression_type), ")");

  if (byte_size == 0 || p.block_pointer.block_id ==
                          static_cast<duckdb::block_id_t>(INVALID_BLOCK)) {
    return duckdb::make_uniq<duckdb::ColumnSegment>(
      *_db, /*block=*/nullptr, type, duckdb::ColumnSegmentType::PERSISTENT,
      static_cast<duckdb::idx_t>(p.tuple_count), *codec, std::move(stats),
      /*block_id=*/0, /*offset=*/0, byte_size,
      /*segment_state=*/nullptr);
  }

  auto& bm = duckdb::BufferManager::GetBufferManager(*_db);
  auto& block_manager = bm.GetTemporaryBlockManager();
  auto handle = bm.RegisterTransientMemory(byte_size, block_manager);
  auto buf = bm.Pin(handle);
  const uint64_t file_offset = p.block_pointer.block_id;
  in.ReadBytes(file_offset, reinterpret_cast<byte_type*>(buf.Ptr()), byte_size);
  auto segment = duckdb::make_uniq<duckdb::ColumnSegment>(
    *_db, std::move(handle), type, duckdb::ColumnSegmentType::PERSISTENT,
    static_cast<duckdb::idx_t>(p.tuple_count), *codec, std::move(stats),
    /*block_id=*/0, /*offset=*/0, byte_size,
    /*segment_state=*/nullptr);
  // Match the writer-side IndexOutputOverflowWriter so VARCHAR UNCOMPRESSED
  // overflow lookups read from .cs file offsets rather than DuckDB blocks.
  if (type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    if (auto seg_state = segment->GetSegmentState()) {
      auto& str_state =
        seg_state->Cast<duckdb::UncompressedStringSegmentState>();
      str_state.overflow_reader =
        duckdb::make_uniq<IndexInputOverflowReader>(in);
    }
  }
  return segment;
}

duckdb::unique_ptr<duckdb::ColumnSegment> ColumnReader::OpenSegment(
  size_t rg) const {
  if (_type.id() == duckdb::LogicalTypeId::LIST ||
      _type.id() == duckdb::LogicalTypeId::MAP) {
    return OpenSegmentImpl(_data_pointers[rg], kLengthsType, *_in);
  }
  return OpenSegmentImpl(_data_pointers[rg], _type, *_in);
}

duckdb::unique_ptr<duckdb::ColumnSegment> ColumnReader::OpenValiditySegment(
  size_t vrg) const {
  return OpenSegmentImpl(_validity_pointers[vrg], kValidityType, *_in);
}

void ColumnReader::ReadListOffset(ColumnReader::ListOffsetState& state,
                                  size_t rg, uint64_t in_rg, uint64_t& start,
                                  uint64_t& end) const {
  SDB_ASSERT(_type.id() == duckdb::LogicalTypeId::LIST ||
             _type.id() == duckdb::LogicalTypeId::MAP);
  if (state.rg != rg) {
    state.cursor = ScanCursor{OpenSegment(rg)};
    state.rg = rg;
    state.next_pos = 0;
    state.prev_offset = _rg_element_starts[rg];
  }
  SDB_ASSERT(in_rg >= state.next_pos);
  auto* buf_data = duckdb::FlatVector::GetDataMutable<uint64_t>(state.buf);
  while (state.next_pos < in_rg) {
    state.cursor.Scan(1, state.buf, 0);
    state.prev_offset = buf_data[0];
    ++state.next_pos;
  }
  state.cursor.Scan(1, state.buf, 0);
  end = buf_data[0];
  start = state.prev_offset;
  state.prev_offset = end;
  ++state.next_pos;
}

uint64_t ColumnReader::ReadListOffsets(ColumnReader::ListOffsetState& state,
                                       size_t rg, uint64_t first_in_rg,
                                       duckdb::idx_t count,
                                       duckdb::Vector& out_buf) const {
  SDB_ASSERT(_type.id() == duckdb::LogicalTypeId::LIST ||
             _type.id() == duckdb::LogicalTypeId::MAP);
  SDB_ASSERT(count > 0);
  if (state.rg != rg) {
    state.cursor = ScanCursor{OpenSegment(rg)};
    state.rg = rg;
    state.next_pos = 0;
    state.prev_offset = _rg_element_starts[rg];
  }
  SDB_ASSERT(first_in_rg >= state.next_pos);
  // Advance the cursor to `first_in_rg` one element at a time so the
  // pre-batch cumulative offset is captured for the run's anchor.
  auto* buf_data = duckdb::FlatVector::GetDataMutable<uint64_t>(state.buf);
  while (state.next_pos < first_in_rg) {
    state.cursor.Scan(1, state.buf, 0);
    state.prev_offset = buf_data[0];
    ++state.next_pos;
  }
  const uint64_t first_start = state.prev_offset;
  state.cursor.Scan(count, out_buf, 0);
  const auto* out_data = duckdb::FlatVector::GetData<uint64_t>(out_buf);
  state.prev_offset = out_data[count - 1];
  state.next_pos += count;
  return first_start;
}

ColumnReader::PointReadCursor ColumnReader::NewPointCursor() const {
  return PointReadCursor{*this, _in->Reopen()};
}

void ColumnReader::PointReadCursor::FetchRow(uint64_t row_pos,
                                             duckdb::Vector& out,
                                             duckdb::idx_t out_idx) {
  const auto window = _reader->Locate(row_pos);
  if (window.rg != _cached_rg) {
    if (_reader->Type().id() == duckdb::LogicalTypeId::LIST ||
        _reader->Type().id() == duckdb::LogicalTypeId::MAP) {
      _segment = _reader->OpenSegmentImpl(_reader->_data_pointers[window.rg],
                                          kLengthsType, *_in);
    } else {
      _segment = _reader->OpenSegmentImpl(_reader->_data_pointers[window.rg],
                                          _reader->_type, *_in);
    }
    _fetch_state = duckdb::ColumnFetchState{};
    _cached_rg = window.rg;
  }
  const uint64_t in_rg = row_pos - window.begin;
  _segment->FetchRow(_fetch_state, static_cast<duckdb::row_t>(in_rg), out,
                     out_idx);
}

}  // namespace irs::columnstore
