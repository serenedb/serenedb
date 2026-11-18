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

#include <absl/algorithm/container.h>

#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <functional>
#include <span>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/column_reader_scan.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

using column_internal::BorrowsBlockStrings;
using column_internal::FetchSegmentRows;

}  // namespace

ColumnReader::PointReader::PointReader(const ColReader& col_reader,
                                       const ColumnReader& col,
                                       bool random_access)
  : _ctx{col_reader, random_access},
    _reader{&col},
    _stored{!col.InMemory() && !col.Patched()},
    _data{&col},
    _validity{col.Validity()} {}

ColumnReader::PointReader::OpenBlock& ColumnReader::PointReader::Block(
  Blocks& blocks, uint64_t row) {
  auto& window = blocks.window;
  if (row < window.begin || row >= window.end) {
    window = blocks.reader->Locate(row, window);
  }
  auto& block = blocks.open[window.block];
  block.last_use = ++_uses;
  if (!block.segment) {
    const auto bytes = blocks.reader->_segments[window.block].byte_size;
    Evict(bytes);
    block.segment = blocks.reader->Open(window.block, _ctx);
    block.bytes = bytes;
    _open_bytes += bytes;
    _open.push_back(&block);
  }
  return block;
}

void ColumnReader::PointReader::Evict(uint64_t incoming) {
  constexpr uint64_t kOpenBytes = uint64_t{64} << 20;
  while (!_open.empty() && _open_bytes + incoming > kOpenBytes) {
    const auto oldest =
      absl::c_min_element(_open, [](const OpenBlock* l, const OpenBlock* r) {
        return l->last_use < r->last_use;
      });
    auto& block = **oldest;
    block.state = codecs::ColumnFetchState{};
    block.segment.reset();
    _open_bytes -= block.bytes;
    block.bytes = 0;
    *oldest = _open.back();
    _open.pop_back();
  }
}

IRS_FORCE_INLINE void ColumnReader::PointReader::FetchOne(
  Blocks& blocks, uint64_t row, duckdb::Vector& out, duckdb::idx_t out_offset) {
  auto& block = Block(blocks, row);
  const duckdb::idx_t local = row - blocks.window.begin;
  FetchSegmentRows(*block.segment, block.state, &local, 1, out,
                   *duckdb::FlatVector::IncrementalSelectionVector(),
                   out_offset);
}

bool ColumnReader::FetchPoint(PointReader& reader, uint64_t row,
                              duckdb::Vector& out,
                              duckdb::idx_t out_offset) const {
  return reader.FetchStored(row, out, out_offset);
}

bool ColumnReader::ValidPoint(PointReader& reader, uint64_t row,
                              duckdb::Vector& scratch) const {
  return reader.ValidStored(row, scratch);
}

bool ColumnReader::PointReader::FetchStored(uint64_t row, duckdb::Vector& out,
                                            duckdb::idx_t out_offset) {
  auto& valid = duckdb::FlatVector::ValidityMutable(out);
  if (row >= _reader->RowCount()) {
    valid.SetInvalid(out_offset);
    return false;
  }
  valid.SetValid(out_offset);
  if (_validity.reader) {
    FetchOne(_validity, row, out, out_offset);
    if (!valid.RowIsValid(out_offset)) {
      return false;
    }
  }
  FetchOne(_data, row, out, out_offset);
  return valid.RowIsValid(out_offset);
}

void ColumnReader::PointReader::FetchPoints(std::span<const uint32_t> ids,
                                            uint32_t first_id,
                                            duckdb::Vector& out,
                                            duckdb::idx_t out_offset) {
  SDB_ASSERT(out_offset + ids.size() <= STANDARD_VECTOR_SIZE);
  const bool strings =
    out.GetType().InternalType() == duckdb::PhysicalType::VARCHAR;
  for (size_t i = 0; i < ids.size(); ++i) {
    SDB_ASSERT(ids[i] >= first_id);
    const auto at = out_offset + i;
    if (!_reader->FetchPoint(*this, ids[i] - first_id, out, at) || !strings) {
      continue;
    }
    auto& value = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(out)[at];
    if (!value.IsInlined()) {
      value = duckdb::StringVector::AddStringOrBlob(out, value);
    }
  }
}

void ColumnReader::PointReader::FetchBlock(OpenBlock& block,
                                           duckdb::Vector& out,
                                           const duckdb::SelectionVector& sel,
                                           duckdb::idx_t offset) {
  FetchSegmentRows(*block.segment, block.state, _local.data(), _local.size(),
                   out, sel, offset);
  if (!BorrowsBlockStrings(*block.segment)) {
    return;
  }
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(out);
  for (size_t k = 0; k < _local.size(); ++k) {
    auto& value = data[offset + sel.get_index(k)];
    if (!value.IsInlined()) {
      value = duckdb::StringVector::AddStringOrBlob(out, value);
    }
  }
}

void ColumnReader::PointReader::FetchBlocks(Blocks& blocks,
                                            std::span<const uint32_t> ids,
                                            uint32_t first_id,
                                            duckdb::Vector& out,
                                            duckdb::idx_t out_offset) {
  for (size_t i = 0; i < ids.size();) {
    auto& block = Block(blocks, ids[i] - first_id);
    const uint64_t begin = blocks.window.begin + first_id;
    const uint64_t end = blocks.window.end + first_id;
    _local.clear();
    auto j = i;
    for (; j < ids.size() && ids[j] < end; ++j) {
      _local.push_back(ids[j] - begin);
    }
    FetchBlock(block, out, *duckdb::FlatVector::IncrementalSelectionVector(),
               out_offset + i);
    i = j;
  }
}

void ColumnReader::PointReader::FetchStoredRows(std::span<const uint32_t> ids,
                                                uint32_t first_id,
                                                duckdb::Vector& out,
                                                duckdb::idx_t out_offset) {
  SDB_ASSERT(out_offset + ids.size() <= STANDARD_VECTOR_SIZE);
  SDB_ASSERT(ids.empty() || ids.front() >= first_id);
  SDB_ASSERT(absl::c_adjacent_find(ids, std::greater_equal<>{}) == ids.end());
  auto& mask = duckdb::FlatVector::ValidityMutable(out);
  auto count = ids.size();
  for (size_t i = 0; i < count; ++i) {
    mask.SetValid(out_offset + i);
  }
  const uint64_t stored = _reader->RowCount() + first_id;
  while (count != 0 && ids[count - 1] >= stored) {
    mask.SetInvalid(out_offset + --count);
  }
  ids = ids.first(count);
  if (!_validity.reader) {
    FetchBlocks(_data, ids, first_id, out, out_offset);
    return;
  }
  FetchBlocks(_validity, ids, first_id, out, out_offset);
  _fetched.clear();
  for (size_t i = 0; i < count; ++i) {
    if (mask.RowIsValid(out_offset + i)) {
      _fetched.push_back(i);
    }
  }
  if (_fetched.size() == count) {
    FetchBlocks(_data, ids, first_id, out, out_offset);
    return;
  }
  auto& sel = column_internal::Allocated(_sel);
  for (size_t i = 0; i < _fetched.size();) {
    auto& block = Block(_data, ids[_fetched[i]] - first_id);
    const uint64_t begin = _data.window.begin + first_id;
    const uint64_t end = _data.window.end + first_id;
    _local.clear();
    auto j = i;
    for (; j < _fetched.size() && ids[_fetched[j]] < end; ++j) {
      sel.set_index(_local.size(), out_offset + _fetched[j]);
      _local.push_back(ids[_fetched[j]] - begin);
    }
    FetchBlock(block, out, sel, 0);
    i = j;
  }
}

bool ColumnReader::PointReader::ValidStored(uint64_t row,
                                            duckdb::Vector& scratch) {
  if (row >= _reader->RowCount()) {
    return false;
  }
  if (!_validity.reader) {
    return true;
  }
  auto& valid = duckdb::FlatVector::ValidityMutable(scratch);
  valid.SetValid(0);
  FetchOne(_validity, row, scratch, 0);
  return valid.RowIsValid(0);
}

}  // namespace irs
