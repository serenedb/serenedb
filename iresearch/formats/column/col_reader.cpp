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

#include "iresearch/formats/column/col_reader.hpp"

#include <absl/random/random.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/main/database.hpp>
#include <limits>
#include <map>
#include <utility>
#include <vector>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

IndexInput::ptr OpenColFile(const Directory& dir, std::string_view segment_name,
                            IOAdvice advice) {
  const std::string filename = FileName(segment_name);
  bool exists = false;
  if (!dir.exists(exists, filename)) {
    throw IoError{
      absl::StrCat("col reader: cannot stat .col file: ", filename)};
  }
  if (!exists) {
    return nullptr;
  }
  auto in = dir.open(filename, advice);
  if (!in) {
    throw IoError{
      absl::StrCat("col reader: cannot open .col file: ", filename)};
  }
  return in;
}

void CheckBlockRange(const ColumnBlockMeta& m, field_id id,
                     uint64_t footer_offset) {
  SDB_ENSURE(m.file_offset + m.byte_size <= footer_offset,
             ".col reader: column data on column id ", id,
             " out of range (offset ", m.file_offset, ", size ", m.byte_size,
             ")");
}

void CheckColumnMetaRanges(const ColumnMeta& meta, uint64_t footer_offset) {
  for (const auto& m : meta.data) {
    CheckBlockRange(m, meta.id, footer_offset);
  }
  for (const auto& m : meta.validity) {
    CheckBlockRange(m, meta.id, footer_offset);
  }
  for (const auto& d : meta.dictionaries) {
    SDB_ENSURE(d.byte_size != 0 && d.file_offset + d.byte_size <= footer_offset,
               ".col reader: dictionary on column id ", meta.id,
               " out of range (offset ", d.file_offset, ", size ", d.byte_size,
               ")");
  }
  for (const auto& c : meta.children) {
    CheckColumnMetaRanges(c, footer_offset);
  }
  for (const auto& rg : meta.variant_rgs) {
    if (rg.unshredded) {
      CheckColumnMetaRanges(*rg.unshredded, footer_offset);
    }
    if (rg.shredded) {
      CheckColumnMetaRanges(*rg.shredded, footer_offset);
    }
  }
}

NormRegionMeta DeserializeNormRegion(duckdb::BinaryDeserializer& d, field_id id,
                                     uint64_t footer_offset) {
  NormRegionMeta r;
  r.stats.rows = d.ReadProperty<uint64_t>(0, "rows");
  r.stats.sum = d.ReadProperty<uint64_t>(1, "sum");
  r.stats.non_zero = d.ReadProperty<uint64_t>(2, "non_zero");
  r.stats.wide8 = d.ReadProperty<uint64_t>(3, "wide8");
  r.stats.wide16 = d.ReadProperty<uint64_t>(4, "wide16");
  r.stats.min = d.ReadProperty<uint32_t>(5, "min");
  r.stats.max = d.ReadProperty<uint32_t>(6, "max");
  r.bits = d.ReadProperty<uint8_t>(7, "bits");
  SDB_ENSURE(r.stats.rows != 0 &&
               r.stats.rows <= doc_limits::eof() - doc_limits::min() &&
               r.stats.min <= r.stats.max &&
               (r.bits == 0 || r.bits == 8 || r.bits == 16 || r.bits == 32),
             ".col reader: norm region on column id ", id, " is corrupt");
  if (r.bits == 0) {
    r.value = d.ReadProperty<uint32_t>(8, "value");
    return r;
  }
  r.file_offset = d.ReadProperty<uint64_t>(9, "file_offset");
  const auto slots = NormSlotsSize(r);
  SDB_ENSURE(r.file_offset + slots >= r.file_offset &&
               r.file_offset + slots <= footer_offset,
             ".col reader: norm data on column id ", id,
             " out of range (offset ", r.file_offset, ")");
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
             ".col reader: norm exceptions on column id ", id, " are corrupt");
  const auto table = NormTableSize(r);
  SDB_ENSURE(r.table_offset + table >= r.table_offset &&
               r.table_offset + table <= footer_offset,
             ".col reader: norm exceptions on column id ", id,
             " out of range (offset ", r.table_offset, ")");
  return r;
}

NormColumnMeta DeserializeNormMeta(duckdb::BinaryDeserializer& d, field_id id,
                                   uint64_t footer_offset) {
  NormColumnMeta meta;
  d.ReadList(1, "regions",
             [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
               list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
                 auto& r = meta.regions.emplace_back(
                   DeserializeNormRegion(obj, id, footer_offset));
                 meta.row_count += r.stats.rows;
               });
             });
  SDB_ENSURE(!meta.regions.empty() &&
               meta.row_count <= doc_limits::eof() - doc_limits::min(),
             ".col reader: norm column id ", id, " holds ", meta.row_count,
             " rows across ", meta.regions.size(), " regions");
  return meta;
}

}  // namespace

ColReader::ColReader(const Directory& dir, std::string_view segment_name,
                     duckdb::DatabaseInstance& db, IOAdvice advice)
  : _db{&db}, _ctx{db, OpenColFile(dir, segment_name, advice)} {
  if (!_ctx.HasIn()) {
    return;
  }
  auto fin = _ctx.In().Dup();
  std::vector<ColumnMeta> metas;
  uint64_t file_id = 0;
  format_utils::ReadFooter(
    *fin, FileName(segment_name),
    [&](duckdb::BinaryDeserializer& footer, uint64_t data_size) {
      footer.Set<duckdb::DatabaseInstance&>(db);
      footer.ReadOptionalList(
        kColFieldColumns, "columns",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            auto& meta = metas.emplace_back(DeserializeColumnMeta(obj));
            CheckColumnMetaRanges(meta, data_size);
          });
        });
      footer.ReadOptionalList(
        kColFieldNormColumns, "norm_columns",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            const auto id =
              static_cast<field_id>(obj.ReadProperty<uint64_t>(0, "id"));
            auto column = std::make_unique<NormColumnReader>(
              id, DeserializeNormMeta(obj, id, data_size), _ctx.In());
            const bool ok = _norm_by_id.emplace(id, column.get()).second;
            SDB_ENSURE(ok, ".col footer: duplicate norm field_id ", id);
            _norm_columns.push_back(std::move(column));
          });
        });
      file_id = footer.ReadPropertyWithExplicitDefault<uint64_t>(
        kColFieldFileId, "file_id", 0);
      footer.Unset<duckdb::DatabaseInstance>();
    });
  if (file_id == 0) {
    file_id = NewColFileId();
  }
  for (auto& meta : metas) {
    auto col = ColumnReader::Make(std::move(meta), file_id);
    const auto id = col->Id();
    const bool ok = _by_id.emplace(id, col.get()).second;
    SDB_ENSURE(ok, ".col footer: duplicate column field_id ", id);
    _columns.push_back(std::move(col));
  }
}

ColReader::~ColReader() = default;

uint64_t NewColFileId() {
  absl::BitGen gen;
  return absl::Uniform<uint64_t>(absl::IntervalClosed, gen, 1,
                                 std::numeric_limits<uint64_t>::max());
}

const ColumnReader* ColReader::Column(field_id id) const noexcept {
  auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr : it->second;
}

const NormColumnReader* ColReader::NormColumn(field_id id) const noexcept {
  auto it = _norm_by_id.find(id);
  return it == _norm_by_id.end() ? nullptr : it->second;
}

}  // namespace irs
