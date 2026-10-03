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

#include "iresearch/formats/column/norm_writer.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <span>

#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

constexpr uint64_t kExceptionShare = 256;

uint32_t PickBits(std::span<const uint64_t, kNormMaxBits + 2> hist,
                  uint64_t rows, uint32_t max) noexcept {
  if (max == 0) {
    return 0;
  }
  uint64_t above = rows;
  for (uint32_t bits = 1; bits != kNormMaxBits; ++bits) {
    above -= hist[bits];
    if (bits % 8 == 0 && above * kExceptionShare <= rows) {
      return bits;
    }
  }
  return kNormMaxBits;
}

}  // namespace

NormColumnWriter::NormColumnWriter(field_id id, uint32_t row_group_size,
                                   IndexOutput& out)
  : _id{id},
    _out{&out},
    _meta{.row_group_size = row_group_size, .file_offset = out.Position()} {
  SDB_ASSERT(row_group_size != 0);
}

void NormColumnWriter::Append(uint64_t target_row, uint32_t value) {
  SDB_ASSERT(target_row >= RowCount(),
             "NormColumnWriter::Append target_row=", target_row,
             " below RowCount=", RowCount(), " on column ", _id);
  PadTo(target_row);
  _values.push_back(value);
  if (_values.size() == _meta.row_group_size) {
    FlushRowGroup();
  }
}

void NormColumnWriter::AppendValues(uint64_t target_row,
                                    std::span<const uint32_t> values) {
  SDB_ASSERT(target_row >= RowCount(),
             "NormColumnWriter::AppendValues target_row=", target_row,
             " below RowCount=", RowCount(), " on column ", _id);
  PadTo(target_row);
  while (!values.empty()) {
    const auto chunk =
      std::min<size_t>(values.size(), _meta.row_group_size - _values.size());
    _values.insert(_values.end(), values.begin(), values.begin() + chunk);
    values = values.subspan(chunk);
    if (_values.size() == _meta.row_group_size) {
      FlushRowGroup();
    }
  }
}

void NormColumnWriter::PadTo(uint64_t target) {
  while (RowCount() < target) {
    const auto chunk = std::min<uint64_t>(
      target - RowCount(), _meta.row_group_size - _values.size());
    _values.insert(_values.end(), chunk, 0);
    if (_values.size() == _meta.row_group_size) {
      FlushRowGroup();
    }
  }
}

void NormColumnWriter::FlushRowGroup() {
  if (_values.empty()) {
    return;
  }
  const uint64_t rows = _values.size();
  std::array<uint64_t, kNormMaxBits + 2> hist{};
  uint32_t max = 0;
  uint64_t sum = 0;
  uint64_t non_zero = 0;
  for (const auto v : _values) {
    ++hist[std::bit_width(uint64_t{v} + 1)];
    max = std::max(max, v);
    sum += v;
    non_zero += v != 0;
  }
  const auto bits = PickBits(hist, rows, max);
  const uint64_t first_row = _meta.row_count;
  _meta.row_groups.push_back({
    .bits = static_cast<uint8_t>(bits),
    .max = max,
    .sum = sum,
    .non_zero_count = non_zero,
    .file_offset = _out->Position(),
  });
  if (bits != 0) {
    const auto escape = static_cast<uint32_t>((uint64_t{1} << bits) - 1);
    for (size_t i = 0; i != rows; ++i) {
      auto v = _values[i];
      if (v >= escape) {
        _exception_rows.push_back(static_cast<uint32_t>(first_row + i));
        _exception_values.push_back(v);
        v = escape;
      }
      switch (bits) {
        case 8:
          _out->WriteByte(static_cast<byte_type>(v));
          break;
        case 16:
          _out->WriteU16(static_cast<uint16_t>(v));
          break;
        case 24:
          _out->WriteU16(static_cast<uint16_t>(v));
          _out->WriteByte(static_cast<byte_type>(v >> 16));
          break;
        default:
          _out->WriteU32(v);
      }
    }
  }
  _meta.row_count += rows;
  _values.clear();
}

void NormColumnWriter::Finalize() {
  FlushRowGroup();
  _values = {};
  if (_meta.row_groups.empty()) {
    return;
  }
  _out->WriteU64(0);
  const auto count = _exception_rows.size();
  if (count != 0) {
    SDB_ENSURE(count <= std::numeric_limits<uint32_t>::max(), "norm column ",
               _id, " has ", count, " exceptions");
    _meta.exceptions_offset = _out->Position();
    _meta.exceptions = static_cast<uint32_t>(count);
    const auto max =
      *std::max_element(_exception_values.begin(), _exception_values.end());
    _meta.exception_bytes =
      max <= std::numeric_limits<uint8_t>::max()
        ? 1
        : (max <= std::numeric_limits<uint16_t>::max() ? 2 : 4);
    const auto buckets = NormBuckets(_meta.row_count);
    size_t i = 0;
    for (uint64_t b = 0; b <= buckets; ++b) {
      while (i != count && (_exception_rows[i] >> kNormBucketShift) < b) {
        ++i;
      }
      _out->WriteU32(static_cast<uint32_t>(i));
    }
    for (const auto row : _exception_rows) {
      _out->WriteByte(static_cast<byte_type>(row & (kNormBucketRows - 1)));
    }
    for (size_t k = 0; k != kNormOffsetSlack; k += sizeof(uint64_t)) {
      _out->WriteU64(0);
    }
    for (const auto v : _exception_values) {
      switch (_meta.exception_bytes) {
        case 1:
          _out->WriteByte(static_cast<byte_type>(v));
          break;
        case 2:
          _out->WriteU16(static_cast<uint16_t>(v));
          break;
        default:
          _out->WriteU32(v);
      }
    }
    _out->WriteU32(0);
    _exception_rows = {};
    _exception_values = {};
  }
  _meta.size = _out->Position() - _meta.file_offset;
}

}  // namespace irs
