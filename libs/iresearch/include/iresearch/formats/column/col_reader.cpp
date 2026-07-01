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

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/main/database.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/store/data_input.hpp"

namespace irs {
namespace {

IndexInput::ptr OpenColFile(const Directory& dir,
                            std::string_view segment_name) {
  const std::string filename = FileName(segment_name);
  bool exists = false;
  if (!dir.exists(exists, filename)) {
    throw IoError{
      absl::StrCat("col reader: cannot stat .col file: ", filename)};
  }
  if (!exists) {
    return nullptr;
  }
  auto in = dir.open(filename, IOAdvice::SEQUENTIAL);
  if (!in) {
    throw IoError{
      absl::StrCat("col reader: cannot open .col file: ", filename)};
  }
  return in;
}

void CheckBlockRange(const ColumnBlockMeta& m, field_id id,
                     uint64_t footer_offset) {
  SDB_ENSURE(m.file_offset + m.byte_size <= footer_offset,
             sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
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

}  // namespace

std::vector<NormRowGroupMeta> DeserializeNormMetas(duckdb::Deserializer& d,
                                                   field_id id,
                                                   uint64_t footer_offset) {
  std::vector<NormRowGroupMeta> pointers;
  d.ReadList(
    1, "row_groups", [&](duckdb::Deserializer::List& rgl, duckdb::idx_t /*j*/) {
      rgl.ReadObject([&](duckdb::Deserializer& po) {
        NormRowGroupMeta p;
        p.byte_size = po.ReadProperty<uint8_t>(0, "byte_size");
        p.row_count = po.ReadProperty<uint32_t>(1, "row_count");
        p.max = po.ReadProperty<uint32_t>(2, "max");
        p.sum = po.ReadProperty<uint64_t>(3, "sum");
        p.non_zero_count = po.ReadProperty<uint64_t>(4, "non_zero_count");
        p.file_offset = po.ReadProperty<uint64_t>(5, "file_offset");
        SDB_ENSURE(p.byte_size == 1 || p.byte_size == 2 || p.byte_size == 4,
                   sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
                   ".col reader: norm byte_size on column id ", id, ": ",
                   p.byte_size);
        SDB_ENSURE(p.row_count != 0, sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
                   ".col reader: norm row_count==0 on column id ", id);
        SDB_ENSURE(
          p.file_offset + static_cast<uint64_t>(p.row_count) * p.byte_size <=
            footer_offset,
          sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
          ".col reader: norm data on column id ", id, " out of range (offset ",
          p.file_offset, ")");
        pointers.push_back(p);
      });
    });
  return pointers;
}

ColReader::ColReader(const Directory& dir, std::string_view segment_name,
                     duckdb::DatabaseInstance& db)
  : _db{&db}, _ctx{db, OpenColFile(dir, segment_name)} {
  if (!_ctx.HasIn()) {
    return;
  }
  auto fin = _ctx.In().Dup();
  format_utils::CheckHeader(*fin, kFormatName, kFormatVersion, kFormatVersion);
  const uint64_t file_len = fin->Length();
  const uint64_t header_len =
    static_cast<uint64_t>(format_utils::HeaderLength(kFormatName));
  SDB_ENSURE(
    file_len > header_len + sizeof(uint64_t) + format_utils::kFooterLen,
    sdb::ERROR_SERVER_CORRUPTED_DATAFILE, ".col reader: truncated `.col` file ",
    segment_name, " (length ", file_len, ")");
  const uint64_t footer_offset_pos =
    file_len - format_utils::kFooterLen - sizeof(uint64_t);
  fin->Seek(footer_offset_pos);
  const uint64_t footer_offset = static_cast<uint64_t>(fin->ReadI64());
  SDB_ENSURE(footer_offset >= header_len && footer_offset < footer_offset_pos,
             sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
             ".col reader: corrupted `.col` file ", segment_name,
             ": footer offset ", footer_offset, " out of range [", header_len,
             ", ", footer_offset_pos, ")");
  fin->Seek(footer_offset);

  duckdb::BinaryDeserializer deserializer{*fin};
  deserializer.Set<duckdb::DatabaseInstance&>(db);
  deserializer.Begin();
  deserializer.ReadList(
    kFooterSlotColumns, "columns",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        auto meta = DeserializeColumnMeta(obj);
        CheckColumnMetaRanges(meta, footer_offset);
        auto col = ColumnReader::Make(std::move(meta));
        const auto id = col->Id();
        const bool ok = _by_id.emplace(id, col.get()).second;
        SDB_ENSURE(ok, sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
                   ".col footer: duplicate column field_id ", id);
        _columns.push_back(std::move(col));
      });
    });
  deserializer.ReadList(
    kFooterSlotNormColumns, "norm_columns",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        const auto id =
          static_cast<field_id>(obj.ReadProperty<uint64_t>(0, "id"));
        auto pointers = DeserializeNormMetas(obj, id, footer_offset);
        if (pointers.empty()) {
          return;
        }
        auto nr = std::make_unique<NormColumnReader>(id, std::move(pointers),
                                                     _ctx.In());
        const bool ok = _norm_by_id.emplace(id, nr.get()).second;
        SDB_ENSURE(ok, sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
                   ".col footer: duplicate norm field_id ", id);
        _norm_readers.push_back(std::move(nr));
      });
    });
  deserializer.End();
}

ColReader::~ColReader() = default;

const ColumnReader* ColReader::Column(field_id id) const noexcept {
  auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr : it->second;
}

bool ColReader::HasNormColumn(field_id id) const noexcept {
  return _norm_by_id.contains(id);
}

const NormColumnReader* ColReader::NormColumn(field_id id) const noexcept {
  auto it = _norm_by_id.find(id);
  return it == _norm_by_id.end() ? nullptr : it->second;
}

}  // namespace irs
