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
#include <span>
#include <utility>

#include "iresearch/formats/norm_reader_impl.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constexpr uint32_t kPageShift =
  static_cast<uint32_t>(std::countr_zero(duckdb::PageResidency::PAGE_SIZE));

NormRegionMeta ReadNormRegion(duckdb::BinaryDeserializer& d, field_id id,
                              uint64_t data_size) {
  NormRegionMeta r;
  r.stats.rows = d.ReadProperty<uint64_t>(0, "rows");
  r.stats.sum = d.ReadProperty<uint64_t>(1, "sum");
  r.stats.non_zero = d.ReadProperty<uint64_t>(2, "non_zero");
  r.stats.wide8 = d.ReadProperty<uint64_t>(3, "wide8");
  r.stats.wide16 = d.ReadProperty<uint64_t>(4, "wide16");
  r.stats.min = d.ReadProperty<uint32_t>(5, "min");
  r.stats.max = d.ReadProperty<uint32_t>(6, "max");
  r.bits = d.ReadProperty<uint8_t>(7, "bits");
  SDB_ENSURE(r.stats.rows != 0 && r.stats.rows <= doc_limits::kMaxDocs &&
               r.stats.min <= r.stats.max &&
               (r.bits == 0 || r.bits == 8 || r.bits == 16 || r.bits == 32),
             "norm region of column ", id, " is corrupt");
  if (r.bits == 0) {
    r.value = d.ReadProperty<uint32_t>(8, "value");
    return r;
  }
  r.file_offset = d.ReadProperty<uint64_t>(9, "file_offset");
  const auto slots = NormSlotsSize(r);
  SDB_ENSURE(r.file_offset + slots >= r.file_offset &&
               r.file_offset + slots <= data_size,
             "norm data of column ", id, " out of range (offset ",
             r.file_offset, ")");
  r.exceptions =
    d.ReadPropertyWithExplicitDefault<uint32_t>(10, "exceptions", 0);
  if (r.exceptions == 0) {
    return r;
  }
  r.overflow = d.ReadProperty<uint32_t>(11, "overflow");
  r.shift = d.ReadProperty<uint8_t>(12, "shift");
  r.exception_bytes = d.ReadProperty<uint8_t>(13, "exception_bytes");
  r.table_offset = d.ReadProperty<uint64_t>(14, "table_offset");
  SDB_ENSURE(r.bits != 32 && r.exceptions <= r.stats.rows &&
               r.overflow <= r.exceptions && r.shift <= kNormMaxShift &&
               (r.exception_bytes == 2 || r.exception_bytes == 4),
             "norm exceptions of column ", id, " are corrupt");
  const auto table = NormTableSize(r);
  SDB_ENSURE(r.table_offset + table >= r.table_offset &&
               r.table_offset + table <= data_size,
             "norm exceptions of column ", id, " out of range (offset ",
             r.table_offset, ")");
  return r;
}

}  // namespace

std::vector<NormColumnSpec> ReadNormColumns(duckdb::BinaryDeserializer& in,
                                            duckdb::field_id_t field,
                                            uint64_t data_size) {
  std::vector<NormColumnSpec> columns;
  in.ReadOptionalList(
    field, "norm_columns",
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::BinaryDeserializer& column) {
        auto& spec = columns.emplace_back();
        spec.id = static_cast<field_id>(column.ReadProperty<uint64_t>(0, "id"));
        column.ReadList(
          1, "regions",
          [&](duckdb::BinaryDeserializer::List& regions, duckdb::idx_t) {
            regions.ReadObject([&](duckdb::BinaryDeserializer& region) {
              const auto& r = spec.meta.regions.emplace_back(
                ReadNormRegion(region, spec.id, data_size));
              spec.meta.row_count += r.stats.rows;
            });
          });
        SDB_ENSURE(!spec.meta.regions.empty() &&
                     spec.meta.row_count <= doc_limits::kMaxDocs,
                   "norm column ", spec.id, " holds ", spec.meta.row_count,
                   " rows across ", spec.meta.regions.size(), " regions");
      });
    });
  return columns;
}

NormColumns::NormColumns(duckdb::shared_ptr<duckdb::InputFile> file,
                         std::span<const NormColumnSpec> columns)
  : _file{std::move(file)} {
  SDB_ASSERT(columns.empty() || _file);
  _columns.reserve(columns.size());
  for (const auto& [id, meta] : columns) {
    const bool added =
      _columns
        .try_emplace(id, std::make_unique<NormColumnReader>(id, meta, *_file))
        .second;
    SDB_ENSURE(added, "duplicate norm column ", id);
  }
}

NormReader::ptr NormColumns::norms(field_id field) const {
  const auto* column = Column(field);
  return column ? MakePersistedNormReader(*column) : NormReader::ptr{};
}

NormReader::ptr MakeNormReader(std::unique_ptr<const NormColumnReader> column) {
  const auto& reader = *column;
  return MakePersistedNormReader(reader, std::move(column));
}

uint32_t NormRegion::Overflow(uint32_t row) const {
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
  if (lo == overflow || absl::little_endian::Load32(
                          overflow_rows + size_t{lo} * 4) != row) [[unlikely]] {
    Corrupt(first_doc + row);
  }
  return absl::little_endian::Load32(overflow_values + size_t{lo} * 4);
}

void NormRegion::Corrupt(doc_id_t doc) const {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("norm exception for doc ", doc, " is missing"));
}

const byte_type* NormColumnReader::Map(const duckdb::InputFile& file,
                                       uint64_t offset, uint64_t size) {
  auto pinned = file.Pin(offset, size);
  const auto* const bytes = pinned.Ptr();
  if (pinned.IsValid()) {
    _pins.push_back(std::move(pinned));
  }
  return bytes;
}

NormColumnReader::NormColumnReader(field_id id, const NormColumnMeta& meta,
                                   const duckdb::InputFile& file)
  : _file{&file}, _id{id}, _row_count{meta.row_count}, _paged{file.IsPaged()} {
  const bool blocks = !file.IsMapped();
  containers::SmallVector<duckdb::FileRange, 16> ranges;
  for (const auto& m : meta.regions) {
    if (m.bits == 0) {
      continue;
    }
    if (blocks) {
      ranges.push_back({.offset = m.file_offset, .size = NormSlotsSize(m)});
    }
    if (m.exceptions != 0) {
      ranges.push_back({.offset = m.table_offset,
                        .size = blocks
                                  ? NormTableSize(m)
                                  : (uint64_t{NormBuckets(m)} + m.overflow) *
                                      sizeof(uint32_t)});
    }
  }
  file.Prefetch(std::span{ranges.data(), ranges.size()});
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
    const auto* slots = Map(file, m.file_offset, size);
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
    const auto* table = Map(file, m.table_offset, NormTableSize(m));
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
                 "norm exceptions index of column ", _id,
                 " is corrupt at bucket ", b);
      prev = at;
    }
    for (uint32_t k = 0; k != m.overflow; ++k) {
      const auto row =
        absl::little_endian::Load32(r.overflow_rows + size_t{k} * 4);
      SDB_ENSURE(row < m.stats.rows &&
                   (k == 0 || row > absl::little_endian::Load32(
                                      r.overflow_rows + size_t{k - 1} * 4)),
                 "norm overflow exceptions of column ", _id, " are corrupt at ",
                 k);
    }
  }
  if (_paged) {
    _residency.Reset(_pages);
  }
  IndexEnds();
}

void NormColumnReader::IndexEnds() {
  _ends.reserve(_regions.size());
  for (const auto& region : _regions) {
    _ends.push_back(region.end_doc);
  }
}

NormColumnReader::NormColumnReader(field_id id, std::vector<NormRegion> regions,
                                   std::vector<NormStats> stats,
                                   std::vector<std::vector<byte_type>> owned)
  : _file{nullptr},
    _id{id},
    _regions{std::move(regions)},
    _stats{std::move(stats)},
    _owned{std::move(owned)} {
  SDB_ASSERT(_regions.size() == _stats.size());
  for (const auto& stats : _stats) {
    _row_count += stats.rows;
    _sum += stats.sum;
    _non_zero += stats.non_zero;
  }
  IndexEnds();
}

void NormColumnConcat::Add(NormRegion region, const NormStats& stats) {
  if (stats.rows == 0) {
    return;
  }
  const auto end = static_cast<doc_id_t>(_next + stats.rows);
  if (!_regions.empty()) {
    auto& last = _regions.back();
    if (!last.exceptions && !region.exceptions && last.bits == region.bits &&
        (last.bits == 0 ? last.value == region.value
                        : last.base == region.base)) {
      last.end_doc = end;
      _stats.back().Merge(stats);
      _next = end;
      return;
    }
  }
  region.first_doc = _next;
  region.end_doc = end;
  region.window = 0;
  region.windows = 0;
  region.page = 0;
  region.first_page = 0;
  _next = end;
  _regions.push_back(region);
  _stats.push_back(stats);
}

void NormColumnConcat::Append(const NormColumnReader& column, size_t region) {
  auto moved = column.Region(region);
  if (moved.bits != 0) {
    moved.base +=
      (int64_t{moved.first_doc} - int64_t{_next}) * (moved.bits / 8);
  }
  Add(moved, column.Stats(region));
}

void NormColumnConcat::AppendZeros(uint64_t rows) {
  Add({}, {.rows = rows, .min = 0});
}

void NormColumnConcat::Append(std::span<const uint32_t> values) {
  if (values.empty()) {
    return;
  }
  NormStats stats;
  stats.Add(values);
  if (stats.min == stats.max) {
    Add({.value = stats.max}, stats);
    return;
  }
  const uint32_t bytes = stats.max <= std::numeric_limits<uint8_t>::max() ? 1
                         : stats.max <= std::numeric_limits<uint16_t>::max()
                           ? 2
                           : 4;
  auto& slots = _owned.emplace_back(values.size() * bytes);
  auto* out = slots.data();
  for (const auto value : values) {
    switch (bytes) {
      case 1:
        *out = static_cast<byte_type>(value);
        break;
      case 2:
        absl::little_endian::Store16(out, static_cast<uint16_t>(value));
        break;
      default:
        absl::little_endian::Store32(out, value);
    }
    out += bytes;
  }
  Add({.base = slots.data() - uint64_t{_next} * bytes, .bits = bytes * 8},
      stats);
}

std::unique_ptr<NormColumnReader> NormColumnConcat::Finish(field_id id) && {
  return std::make_unique<NormColumnReader>(
    id, std::move(_regions), std::move(_stats), std::move(_owned));
}

const NormRegion& NormColumnReader::Locate(doc_id_t doc) const noexcept {
  SDB_ASSERT(doc >= doc_limits::min());
  SDB_ASSERT(doc - doc_limits::min() < _row_count);
  const auto it = absl::c_upper_bound(_ends, doc);
  SDB_ASSERT(it != _ends.end());
  return _regions[static_cast<size_t>(it - _ends.begin())];
}

void NormColumnReader::Decode(doc_id_t first, size_t n,
                              uint32_t* IRS_RESTRICT values) const {
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
