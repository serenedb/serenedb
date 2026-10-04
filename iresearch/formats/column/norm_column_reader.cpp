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

#include <algorithm>
#include <bit>

#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constexpr uint32_t kPageShift =
  static_cast<uint32_t>(std::countr_zero(file_utils::kPage));

}  // namespace

uint32_t NormRegion::Overflow(uint32_t row) const noexcept {
  uint32_t lo = 0;
  uint32_t hi = overflow;
  while (lo < hi) {
    const auto mid = lo + (hi - lo) / 2;
    if (absl::little_endian::Load32(overflow_rows + size_t{mid} * 4) < row) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  const bool found = lo < overflow && absl::little_endian::Load32(
                                        overflow_rows + size_t{lo} * 4) == row;
  SDB_ASSERT(found, "norm overflow exception for row ", row, " is missing");
  if (!found) [[unlikely]] {
    return 0;
  }
  return absl::little_endian::Load32(overflow_values + size_t{lo} * 4);
}

const byte_type* NormColumnReader::Map(IndexInput& in, uint64_t offset,
                                       uint64_t size) {
  if (const auto* data = in.ReadStable(offset, size)) {
    return data;
  }
  auto& owned =
    _owned.emplace_back(std::make_unique_for_overwrite<byte_type[]>(size));
  in.ReadData(offset, owned.get(), size);
  return owned.get();
}

NormColumnReader::NormColumnReader(field_id id, const NormColumnMeta& meta,
                                   IndexInput& in)
  : _id{id}, _row_count{meta.row_count} {
  _regions.reserve(meta.regions.size());
  _stats.reserve(meta.regions.size());
  auto first = doc_limits::min();
  for (const auto& m : meta.regions) {
    _stats.push_back(m.stats);
    auto& r = _regions.emplace_back();
    r.first_doc = first;
    r.end_doc = static_cast<doc_id_t>(first + m.stats.rows);
    r.bits = m.bits;
    r.value = m.value;
    _sum += m.stats.sum;
    _non_zero += m.stats.non_zero;
    first = r.end_doc;
    if (m.bits == 0) {
      continue;
    }
    const uint64_t bytes = m.bits / 8;
    const uint64_t size = NormSlotsSize(m);
    const auto* slots = Map(in, m.file_offset, size);
    r.base = slots - uint64_t{r.first_doc} * bytes;
    r.window = _windows.size();
    const uint64_t window_bytes = (uint64_t{1} << kNormWindowShift) * bytes;
    for (uint64_t at = 0; at < size; at += window_bytes) {
      _windows.emplace_back(slots + at, std::min(window_bytes, size - at));
    }
    r.windows = _windows.size() - r.window;
    r.first_page = reinterpret_cast<uintptr_t>(slots) >> kPageShift;
    r.page = _pages;
    _pages += (reinterpret_cast<uintptr_t>(slots + size - 1) >> kPageShift) -
              r.first_page + 1;
    if (m.exceptions == 0) {
      continue;
    }
    const auto buckets = NormBuckets(m);
    const auto* table = Map(in, m.table_offset, NormTableSize(m));
    _exceptions = true;
    r.exceptions = true;
    r.first_code = NormFirstCode(m.bits);
    r.shift = m.shift;
    r.direct = m.exceptions - m.overflow;
    r.overflow = m.overflow;
    r.exception_bytes = m.exception_bytes;
    r.bases = table;
    r.overflow_rows = table + buckets * sizeof(uint32_t);
    r.overflow_values = r.overflow_rows + uint64_t{m.overflow} * 4;
    r.values = r.overflow_values + uint64_t{m.overflow} * 4;
    uint32_t prev = 0;
    for (uint64_t b = 0; b != buckets; ++b) {
      const auto at = absl::little_endian::Load32(r.bases + b * 4);
      SDB_ENSURE(at >= prev && at <= r.direct,
                 ".col reader: norm exceptions index on column id ", _id,
                 " is corrupt at bucket ", b);
      prev = at;
    }
    for (uint32_t k = 0; k != m.overflow; ++k) {
      const auto row =
        absl::little_endian::Load32(r.overflow_rows + size_t{k} * 4);
      SDB_ENSURE(row < m.stats.rows &&
                   (k == 0 || row > absl::little_endian::Load32(
                                      r.overflow_rows + size_t{k - 1} * 4)),
                 ".col reader: norm overflow exceptions on column id ", _id,
                 " are corrupt at ", k);
    }
  }
}

const NormRegion& NormColumnReader::Locate(doc_id_t doc) const noexcept {
  SDB_ASSERT(doc >= doc_limits::min());
  SDB_ASSERT(doc - doc_limits::min() < _row_count);
  const auto it = std::upper_bound(
    _regions.begin(), _regions.end(), doc,
    [](doc_id_t d, const NormRegion& r) { return d < r.end_doc; });
  SDB_ASSERT(it != _regions.end());
  return *it;
}

uint32_t NormColumnReader::Get(uint64_t row) const noexcept {
  SDB_ASSERT(row < _row_count);
  const auto doc = static_cast<doc_id_t>(row + doc_limits::min());
  const auto& r = Locate(doc);
  const auto value = r.Slot(doc);
  return r.exceptions && value >= r.first_code ? r.Exception(doc, value)
                                               : value;
}

void NormColumnReader::Decode(doc_id_t first, size_t n,
                              uint32_t* IRS_RESTRICT values) const noexcept {
  if (n == 0) {
    return;
  }
  const auto& r = Locate(first);
  SDB_ASSERT(first + n <= r.end_doc);
  switch (r.bits) {
    case 0:
      std::fill_n(values, n, r.value);
      return;
    case 8: {
      const auto* p = r.base + first;
      for (size_t i = 0; i != n; ++i) {
        values[i] = p[i];
      }
    } break;
    case 16: {
      const auto* p = r.base + size_t{first} * 2;
      for (size_t i = 0; i != n; ++i) {
        values[i] = absl::little_endian::Load16(p + i * 2);
      }
    } break;
    default: {
      const auto* p = r.base + size_t{first} * 4;
      for (size_t i = 0; i != n; ++i) {
        values[i] = absl::little_endian::Load32(p + i * 4);
      }
    }
  }
  if (!r.exceptions) {
    return;
  }
  for (size_t i = 0; i != n; ++i) {
    if (values[i] >= r.first_code) [[unlikely]] {
      values[i] = r.Exception(static_cast<doc_id_t>(first + i), values[i]);
    }
  }
}

}  // namespace irs
