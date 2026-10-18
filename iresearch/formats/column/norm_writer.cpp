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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>
#include <span>

#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constexpr size_t kStagingRows = 4096;

uint8_t PickShift(uint64_t rows, uint64_t exceptions) noexcept {
  const auto span =
    std::max<uint64_t>(rows * kNormBucketTarget / exceptions, 1);
  return static_cast<uint8_t>(std::min<uint32_t>(
    static_cast<uint32_t>(std::bit_width(span)) - 1, kNormMaxShift));
}

}  // namespace

void NormStats::Add(std::span<const uint32_t> values) noexcept {
  rows += values.size();
  for (const auto v : values) {
    sum += v;
    non_zero += v != 0;
    wide8 += v >= NormFirstCode(8);
    wide16 += v >= NormFirstCode(16);
    min = std::min(min, v);
    max = std::max(max, v);
  }
}

void NormStats::Merge(const NormStats& other) noexcept {
  rows += other.rows;
  sum += other.sum;
  non_zero += other.non_zero;
  wide8 += other.wide8;
  wide16 += other.wide16;
  min = std::min(min, other.min);
  max = std::max(max, other.max);
}

NormLayout PickNormLayout(const NormStats& stats) noexcept {
  SDB_ASSERT(stats.rows != 0);
  if (stats.min == stats.max) {
    return {.value = stats.min};
  }
  if (stats.wide8 * kNormExceptionShare <= stats.rows) {
    return {.bits = 8, .exceptions = stats.wide8};
  }
  if (stats.wide16 * kNormExceptionShare <= stats.rows) {
    return {.bits = 16, .exceptions = stats.wide16};
  }
  return {.bits = 32};
}

NormColumnWriter::NormColumnWriter(field_id id, uint32_t row_group_size,
                                   IndexOutput& out)
  : _id{id}, _out{&out}, _row_group_size{row_group_size} {}

NormColumnWriter::NormColumnWriter(field_id id, uint32_t row_group_size,
                                   Open open)
  : _id{id}, _open{std::move(open)}, _row_group_size{row_group_size} {}

void NormColumnWriter::Append(uint64_t target_row, uint32_t value) {
  SDB_ASSERT(target_row >= RowCount(),
             "NormColumnWriter::Append target_row=", target_row,
             " below RowCount=", RowCount(), " on column ", _id);
  PadTo(target_row);
  _values.push_back(value);
  if (_values.size() == _row_group_size) {
    FlushRowGroup();
  }
}

void NormColumnWriter::PadTo(uint64_t target) {
  while (RowCount() < target) {
    SDB_ENSURE(_row_group_size != 0, "NormColumnWriter: streamed column ", _id,
               " holds ", RowCount(), " rows, cannot pad to ", target);
    const auto chunk =
      std::min<uint64_t>(target - RowCount(), _row_group_size - _values.size());
    _values.insert(_values.end(), chunk, 0);
    if (_values.size() == _row_group_size) {
      FlushRowGroup();
    }
  }
}

void NormColumnWriter::FlushRowGroup() {
  if (_values.empty()) {
    return;
  }
  NormStats stats;
  stats.Add(_values);
  Begin(PickNormLayout(stats), stats.rows, stats.max);
  Encode(_values);
  End();
  _values.clear();
}

void NormColumnWriter::OpenRegion(const NormStats& planned) {
  SDB_ASSERT(_row_group_size == 0 && !_open_region);
  if (planned.rows == 0) {
    return;
  }
  Begin(PickNormLayout(planned), planned.rows, planned.max);
}

void NormColumnWriter::Write(std::span<const uint32_t> values) {
  SDB_ASSERT(_open_region || values.empty());
  if (!values.empty()) {
    Encode(values);
  }
}

void NormColumnWriter::CloseRegion() {
  if (_open_region) {
    End();
  }
}

void NormColumnWriter::Begin(const NormLayout& layout, uint64_t rows,
                             uint32_t max) {
  if (layout.bits != 0 && _out == nullptr) {
    _out = &_open();
  }
  _open_region = true;
  _region = {};
  _region.bits = static_cast<uint8_t>(layout.bits);
  _region.value = layout.value;
  _region.file_offset = layout.bits == 0 ? 0 : _out->Position();
  if (layout.exceptions != 0) {
    _region.shift = PickShift(rows, layout.exceptions);
    _region.exception_bytes =
      max <= std::numeric_limits<uint16_t>::max() ? 2 : 4;
  }
  _bucket = 0;
  _local = 0;
  _bases.clear();
  _direct.clear();
  _overflow_rows.clear();
  _overflow_values.clear();
}

uint32_t NormColumnWriter::Escape(uint64_t row, uint32_t value) {
  const uint64_t bucket = row >> _region.shift;
  if (_bases.empty() || bucket != _bucket) {
    _bases.resize(bucket + 1, static_cast<uint32_t>(_direct.size()));
    _bucket = bucket;
    _local = 0;
  }
  if (_local == kNormDirect) {
    _overflow_rows.push_back(static_cast<uint32_t>(row));
    _overflow_values.push_back(value);
    return NormOverflowCode(_region.bits);
  }
  _direct.push_back(value);
  return NormFirstCode(_region.bits) + _local++;
}

template<size_t Bytes>
void NormColumnWriter::EncodeSlots(std::span<const uint32_t> values,
                                   uint64_t first_row) {
  const bool escapes = _region.exception_bytes != 0;
  const uint32_t first_code = NormFirstCode(Bytes * 8);
  for (size_t at = 0; at < values.size(); at += kStagingRows) {
    const auto chunk =
      values.subspan(at, std::min(kStagingRows, values.size() - at));
    _staging.resize(chunk.size() * Bytes);
    auto* out = _staging.data();
    for (size_t i = 0; i != chunk.size(); ++i) {
      auto v = chunk[i];
      if (escapes && v >= first_code) [[unlikely]] {
        v = Escape(first_row + at + i, v);
      }
      SDB_ASSERT(escapes || Bytes == 4 || v < first_code);
      if constexpr (Bytes == 1) {
        out[i] = static_cast<byte_type>(v);
      } else if constexpr (Bytes == 2) {
        absl::little_endian::Store16(out + i * 2, static_cast<uint16_t>(v));
      } else {
        absl::little_endian::Store32(out + i * 4, v);
      }
    }
    _out->WriteData(_staging.data(), _staging.size());
  }
}

void NormColumnWriter::Encode(std::span<const uint32_t> values) {
  const uint64_t first_row = _region.stats.rows;
  _region.stats.Add(values);
  switch (_region.bits) {
    case 0:
      SDB_ASSERT(std::all_of(values.begin(), values.end(),
                             [&](uint32_t v) { return v == _region.value; }));
      return;
    case 8:
      return EncodeSlots<1>(values, first_row);
    case 16:
      return EncodeSlots<2>(values, first_row);
    default:
      return EncodeSlots<4>(values, first_row);
  }
}

void NormColumnWriter::End() {
  _open_region = false;
  auto& region = _region;
  if (region.stats.rows == 0) {
    region = {};
    return;
  }
  const auto exceptions = _direct.size() + _overflow_rows.size();
  if (exceptions == 0) {
    region.shift = 0;
    region.exception_bytes = 0;
  } else {
    SDB_ENSURE(exceptions <= std::numeric_limits<uint32_t>::max(),
               "norm column ", _id, " has ", exceptions, " exceptions");
    region.exceptions = static_cast<uint32_t>(exceptions);
    region.overflow = static_cast<uint32_t>(_overflow_rows.size());
    region.table_offset = _out->Position();
    _bases.resize(NormBuckets(region), static_cast<uint32_t>(_direct.size()));
    for (const auto base : _bases) {
      _out->WriteU32(base);
    }
    for (const auto row : _overflow_rows) {
      _out->WriteU32(row);
    }
    for (const auto v : _overflow_values) {
      _out->WriteU32(v);
    }
    for (const auto v : _direct) {
      if (region.exception_bytes == 2) {
        _out->WriteU16(static_cast<uint16_t>(v));
      } else {
        _out->WriteU32(v);
      }
    }
    _out->WriteU32(0);
  }
  _meta.row_count += region.stats.rows;
  _meta.regions.push_back(region);
  region = {};
}

void NormColumnWriter::Finalize() {
  SDB_ASSERT(!_open_region);
  if (_row_group_size != 0) {
    FlushRowGroup();
    _values = {};
  }
  _bases = {};
  _direct = {};
  _overflow_rows = {};
  _overflow_values = {};
  _staging = {};
}

}  // namespace irs
