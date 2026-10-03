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

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/main/database.hpp>
#include <map>
#include <utility>

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

NormColumnMeta DeserializeNormMeta(duckdb::BinaryDeserializer& d, field_id id,
                                   uint64_t footer_offset) {
  NormColumnMeta meta;
  meta.row_group_size = d.ReadProperty<uint32_t>(1, "row_group_size");
  meta.row_count = d.ReadProperty<uint64_t>(2, "row_count");
  d.ReadList(
    3, "row_groups",
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
        NormRowGroupMeta p;
        p.bits = obj.ReadProperty<uint8_t>(0, "bits");
        p.max = obj.ReadProperty<uint32_t>(1, "max");
        p.sum = obj.ReadProperty<uint64_t>(2, "sum");
        p.non_zero_count = obj.ReadProperty<uint64_t>(3, "non_zero_count");
        p.file_offset = obj.ReadProperty<uint64_t>(4, "file_offset");
        SDB_ENSURE(p.bits <= kNormMaxBits && p.bits % 8 == 0,
                   ".col reader: norm bits on column id ", id, ": ", p.bits);
        meta.row_groups.push_back(p);
      });
    });
  meta.file_offset = d.ReadProperty<uint64_t>(4, "file_offset");
  meta.size = d.ReadProperty<uint64_t>(5, "size");
  meta.exceptions =
    d.ReadPropertyWithExplicitDefault<uint32_t>(6, "exceptions", 0);
  if (meta.exceptions != 0) {
    meta.exceptions_offset = d.ReadProperty<uint64_t>(7, "exceptions_offset");
    meta.exception_bytes = d.ReadProperty<uint8_t>(8, "exception_bytes");
  }
  const uint64_t groups = meta.row_groups.size();
  const uint64_t rgs = meta.row_group_size;
  SDB_ENSURE(groups != 0 && rgs != 0 && meta.row_count > (groups - 1) * rgs &&
               meta.row_count <= groups * rgs &&
               meta.row_count <= doc_limits::eof() - doc_limits::min(),
             ".col reader: norm column id ", id, " holds ", meta.row_count,
             " rows across ", groups, " row groups of ", rgs);
  const auto end = meta.file_offset + meta.size;
  SDB_ENSURE(end >= meta.file_offset && end <= footer_offset,
             ".col reader: norm column id ", id, " out of range (offset ",
             meta.file_offset, ", size ", meta.size, ")");
  for (uint64_t rg = 0; rg < groups; ++rg) {
    const auto& p = meta.row_groups[rg];
    const auto rows = std::min(rgs, meta.row_count - rg * rgs);
    const auto first_doc = rg * rgs + doc_limits::min();
    const auto bytes = ((first_doc * p.bits & 7) + rows * p.bits + 7) / 8;
    SDB_ENSURE(p.file_offset >= meta.file_offset &&
                 p.file_offset + bytes + kNormSlotSlack <= end,
               ".col reader: norm data on column id ", id,
               " out of range (offset ", p.file_offset, ")");
  }
  if (meta.exceptions != 0) {
    SDB_ENSURE(meta.exceptions <= meta.row_count &&
                 (meta.exception_bytes == 1 || meta.exception_bytes == 2 ||
                  meta.exception_bytes == 4) &&
                 meta.exceptions_offset >= meta.file_offset &&
                 meta.exceptions_offset +
                     NormExceptionsSize(meta.row_count, meta.exceptions,
                                        meta.exception_bytes) <=
                   end,
               ".col reader: norm exceptions on column id ", id,
               " out of range (offset ", meta.exceptions_offset, ")");
  }
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
  format_utils::ReadFooter(
    *fin, FileName(segment_name),
    [&](duckdb::BinaryDeserializer& footer, uint64_t data_size) {
      footer.Set<duckdb::DatabaseInstance&>(db);
      footer.ReadOptionalList(
        kColFieldColumns, "columns",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            auto meta = DeserializeColumnMeta(obj);
            CheckColumnMetaRanges(meta, data_size);
            auto col = ColumnReader::Make(std::move(meta));
            const auto id = col->Id();
            const bool ok = _by_id.emplace(id, col.get()).second;
            SDB_ENSURE(ok, ".col footer: duplicate column field_id ", id);
            _columns.push_back(std::move(col));
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
      footer.Unset<duckdb::DatabaseInstance>();
    });
}

ColReader::~ColReader() = default;

const ColumnReader* ColReader::Column(field_id id) const noexcept {
  auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr : it->second;
}

const NormColumnReader* ColReader::NormColumn(field_id id) const noexcept {
  auto it = _norm_by_id.find(id);
  return it == _norm_by_id.end() ? nullptr : it->second;
}

}  // namespace irs
