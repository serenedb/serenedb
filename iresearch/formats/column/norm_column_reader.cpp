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

#include "iresearch/formats/column/norm_column_reader.hpp"

#include <utility>

#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

NormColumnReader::NormColumnReader(field_id id, NormColumnMeta meta,
                                   IndexInput& in)
  : _id{id}, _pointers{std::move(meta.row_groups)} {
  _spans.resize(_pointers.size());
  _exceptions.resize(_pointers.size());
  if (!_pointers.empty()) {
    _rg_rows = meta.row_group_size;
    _total_row_count = meta.row_count;
  }

  size_t owned_total = 0;
  for (size_t rg = 0; rg < _pointers.size(); ++rg) {
    const auto& p = _pointers[rg];
    SDB_ASSERT(_total_sum + p.sum >= _total_sum,
               ".col reader norm running sum overflow on column id ", _id);
    _total_sum += p.sum;
    _total_non_zero += p.non_zero_count;
    _uniform_byte_size &= p.byte_size == _pointers.front().byte_size;
    _has_exceptions |= p.exceptions != 0;
    const auto byte_count = RowGroupRowCount(rg) * p.byte_size;
    const auto exception_count = size_t{p.exceptions} * kNormExceptionBytes;
    const auto total = byte_count + exception_count;
    if (total == 0) {
      continue;
    }
    if (const auto* ptr = in.ReadStable(p.file_offset, total); ptr) {
      _spans[rg] = std::span<const byte_type>{ptr, byte_count};
      _exceptions[rg] =
        std::span<const byte_type>{ptr + byte_count, exception_count};
    } else {
      _spans[rg] = {static_cast<const byte_type*>(nullptr), byte_count};
      _exceptions[rg] = {static_cast<const byte_type*>(nullptr),
                         exception_count};
      owned_total += total;
    }
  }
  if (owned_total != 0) {
    _owned.resize(owned_total);
    size_t offset = 0;
    for (size_t rg = 0; rg < _spans.size(); ++rg) {
      if (_spans[rg].data() != nullptr) {
        continue;
      }
      const auto byte_count = _spans[rg].size();
      const auto exception_count = _exceptions[rg].size();
      const auto total = byte_count + exception_count;
      if (total == 0) {
        continue;
      }
      auto* dst = _owned.data() + offset;
      in.ReadData(_pointers[rg].file_offset, dst, total);
      _spans[rg] = std::span<const byte_type>{dst, byte_count};
      _exceptions[rg] =
        std::span<const byte_type>{dst + byte_count, exception_count};
      offset += total;
    }
  }
}

uint32_t NormColumnReader::Get(uint64_t row_pos) const noexcept {
  const auto info = Locate(row_pos);
  SDB_ASSERT(!info.bytes.empty());
  const auto row = row_pos - info.first_row;
  const auto value =
    ReadNormValue(info.bytes.data() + row * info.byte_size, info.byte_size);
  if (value == kNormEscape && !info.exceptions.empty()) {
    return Exception(info.exceptions, row);
  }
  return value;
}

void NormColumnReader::Decode(size_t rg,
                              uint32_t* IRS_RESTRICT values) const noexcept {
  SDB_ASSERT(rg < _spans.size());
  const auto* IRS_RESTRICT bytes = _spans[rg].data();
  const auto rows = RowGroupRowCount(rg);
  switch (_pointers[rg].byte_size) {
    case 1:
      for (uint64_t i = 0; i != rows; ++i) {
        values[i] = bytes[i];
      }
      break;
    case 2:
      for (uint64_t i = 0; i != rows; ++i) {
        values[i] = absl::little_endian::Load16(bytes + i * 2);
      }
      break;
    default:
      SDB_ASSERT(_pointers[rg].byte_size == 4);
      for (uint64_t i = 0; i != rows; ++i) {
        values[i] = absl::little_endian::Load32(bytes + i * 4);
      }
      break;
  }
  const auto exceptions = _exceptions[rg];
  for (size_t i = 0; i != exceptions.size(); i += kNormExceptionBytes) {
    const auto row = absl::little_endian::Load32(exceptions.data() + i);
    SDB_ASSERT(row < rows);
    values[row] =
      absl::little_endian::Load32(exceptions.data() + i + sizeof(uint32_t));
  }
}

uint32_t NormColumnReader::Exception(std::span<const byte_type> exceptions,
                                     uint64_t row) noexcept {
  SDB_ASSERT(!exceptions.empty());
  size_t lo = 0;
  size_t hi = exceptions.size() / kNormExceptionBytes;
  while (lo < hi) {
    const auto mid = (lo + hi) / 2;
    const auto at = absl::little_endian::Load32(exceptions.data() +
                                                mid * kNormExceptionBytes);
    if (at < row) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  const auto* entry = exceptions.data() + lo * kNormExceptionBytes;
  SDB_ASSERT(lo < exceptions.size() / kNormExceptionBytes);
  SDB_ASSERT(absl::little_endian::Load32(entry) == row);
  return absl::little_endian::Load32(entry + sizeof(uint32_t));
}

}  // namespace irs
