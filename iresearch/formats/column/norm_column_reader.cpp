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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <algorithm>
#include <bit>

#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

NormColumnReader::NormColumnReader(field_id id, NormColumnMeta meta,
                                   IndexInput& in)
  : _id{id},
    _rg_rows{meta.row_group_size},
    _row_count{meta.row_count},
    _exceptions{meta.exceptions},
    _exception_bytes{meta.exception_bytes} {
  const auto* data = in.ReadStable(meta.file_offset, meta.size);
  if (data == nullptr) {
    _owned.resize(meta.size);
    in.ReadData(meta.file_offset, _owned.data(), meta.size);
    data = _owned.data();
  }
  _windows.reserve(meta.row_groups.size());
  for (size_t rg = 0; rg != meta.row_groups.size(); ++rg) {
    const auto& p = meta.row_groups[rg];
    _sum += p.sum;
    _non_zero += p.non_zero_count;
    _max_bits = std::max<uint32_t>(_max_bits, p.bits);
    const uint64_t first_row = rg * _rg_rows;
    const auto first_doc = static_cast<doc_id_t>(first_row + doc_limits::min());
    const auto rows = std::min(_rg_rows, _row_count - first_row);
    const auto* base = data + (p.file_offset - meta.file_offset) -
                       ((uint64_t{first_doc} * p.bits) >> 3);
    _windows.push_back({
      .base = base,
      .first_doc = first_doc,
      .end_doc = static_cast<doc_id_t>(first_doc + rows),
      .rg = rg,
      .bits = p.bits,
    });
    _uniform &=
      p.bits == _windows.front().bits && base == _windows.front().base;
  }
  if (_exceptions == 0) {
    return;
  }
  const auto buckets = NormBuckets(_row_count);
  _begin = data + (meta.exceptions_offset - meta.file_offset);
  _offsets = _begin + (buckets + 1) * sizeof(uint32_t);
  _values = _offsets + _exceptions + kNormOffsetSlack;
  uint32_t prev = 0;
  for (uint64_t b = 0; b <= buckets; ++b) {
    const auto at = absl::little_endian::Load32(_begin + b * sizeof(uint32_t));
    SDB_ENSURE(at >= prev && at - prev <= kNormBucketRows &&
                 (b != 0 || at == 0) && (b != buckets || at == _exceptions),
               ".col reader: norm exceptions index on column id ", _id,
               " is corrupt at bucket ", b);
    prev = at;
  }
}

std::span<const byte_type> NormColumnReader::RowGroupExtent(
  size_t rg) const noexcept {
  SDB_ASSERT(rg < _windows.size());
  const auto& w = _windows[rg];
  const auto* first = w.base + ((uint64_t{w.first_doc} * w.bits) >> 3);
  const auto* end = w.base + ((uint64_t{w.end_doc} * w.bits + 7) >> 3);
  return {first, static_cast<size_t>(end - first)};
}

uint32_t NormColumnReader::Exception(doc_id_t doc) const noexcept {
  SDB_ASSERT(_exceptions != 0);
  const uint64_t row = doc - doc_limits::min();
  const auto* bucket = _begin + (row >> kNormBucketShift) * sizeof(uint32_t);
  const auto lo = absl::little_endian::Load32(bucket);
  const auto n = absl::little_endian::Load32(bucket + sizeof(uint32_t)) - lo;
  const auto key = static_cast<byte_type>(row & (kNormBucketRows - 1));
  const auto* offsets = _offsets + lo;
  uint32_t k = 0;
#ifdef __AVX2__
  const __m256i needle = _mm256_set1_epi8(static_cast<char>(key));
  for (; k < n; k += 32) {
    auto m = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(offsets + k)),
      needle)));
    if (n - k < 32) {
      m &= (uint32_t{1} << (n - k)) - 1;
    }
    if (m != 0) {
      k += static_cast<uint32_t>(std::countr_zero(m));
      break;
    }
  }
#else
  while (k < n && offsets[k] != key) {
    ++k;
  }
#endif
  SDB_ASSERT(k < n, "norm exception for doc ", doc, " is missing");
  if (k >= n) [[unlikely]] {
    return 0;
  }
  const auto* value = _values + size_t{lo + k} * _exception_bytes;
  switch (_exception_bytes) {
    case 1:
      return *value;
    case 2:
      return absl::little_endian::Load16(value);
    default:
      return absl::little_endian::Load32(value);
  }
}

uint32_t NormColumnReader::Get(uint64_t row) const noexcept {
  SDB_ASSERT(row < _row_count);
  const auto doc = static_cast<doc_id_t>(row + doc_limits::min());
  const auto& w = Locate(doc);
  const auto value = NormSlot(w.base, w.bits, doc);
  if (_exceptions != 0 && w.bits != 0 && value == (uint64_t{1} << w.bits) - 1) {
    return Exception(doc);
  }
  return value;
}

void NormColumnReader::Decode(size_t rg,
                              uint32_t* IRS_RESTRICT values) const noexcept {
  SDB_ASSERT(rg < _windows.size());
  const auto& w = _windows[rg];
  const auto rows = w.end_doc - w.first_doc;
  if (w.bits == 0) {
    std::fill_n(values, rows, 0);
    return;
  }
  for (auto doc = w.first_doc; doc != w.end_doc; ++doc) {
    values[doc - w.first_doc] = NormSlot(w.base, w.bits, doc);
  }
  if (_exceptions == 0) {
    return;
  }
  const uint64_t first_row = w.first_doc - doc_limits::min();
  const uint64_t end_row = first_row + rows;
  for (auto b = first_row >> kNormBucketShift, end = NormBuckets(end_row);
       b != end; ++b) {
    const auto* bucket = _begin + b * sizeof(uint32_t);
    const auto hi = absl::little_endian::Load32(bucket + sizeof(uint32_t));
    for (auto i = absl::little_endian::Load32(bucket); i != hi; ++i) {
      const auto row = (b << kNormBucketShift) | _offsets[i];
      if (row < first_row || row >= end_row) {
        continue;
      }
      const auto* value = _values + size_t{i} * _exception_bytes;
      values[row - first_row] =
        _exception_bytes == 1
          ? *value
          : (_exception_bytes == 2 ? absl::little_endian::Load16(value)
                                   : absl::little_endian::Load32(value));
    }
  }
}

}  // namespace irs
