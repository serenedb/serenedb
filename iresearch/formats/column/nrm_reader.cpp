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

#include "iresearch/formats/column/nrm_reader.hpp"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <string>
#include <utility>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/nrm_writer.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

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
        p.byte_size = obj.ReadProperty<uint8_t>(0, "byte_size");
        p.max = obj.ReadProperty<uint32_t>(1, "max");
        p.sum = obj.ReadProperty<uint64_t>(2, "sum");
        p.non_zero_count = obj.ReadProperty<uint64_t>(3, "non_zero_count");
        p.file_offset = obj.ReadProperty<uint64_t>(4, "file_offset");
        p.exceptions =
          obj.ReadPropertyWithExplicitDefault<uint32_t>(5, "exceptions", 0);
        SDB_ENSURE(p.byte_size == 1 || p.byte_size == 2 || p.byte_size == 4,
                   ".nrm reader: norm byte_size on column id ", id, ": ",
                   p.byte_size);
        SDB_ENSURE(p.exceptions == 0 || p.byte_size == 1,
                   ".nrm reader: norm exceptions on column id ", id,
                   " with byte_size ", p.byte_size);
        meta.row_groups.push_back(p);
      });
    });
  const uint64_t groups = meta.row_groups.size();
  const uint64_t rgs = meta.row_group_size;
  SDB_ENSURE(groups != 0 && rgs != 0 && meta.row_count > (groups - 1) * rgs &&
               meta.row_count <= groups * rgs,
             ".nrm reader: norm column id ", id, " holds ", meta.row_count,
             " rows across ", groups, " row groups of ", rgs);
  for (uint64_t rg = 0; rg < groups; ++rg) {
    const auto& p = meta.row_groups[rg];
    const auto rows = std::min(rgs, meta.row_count - rg * rgs);
    SDB_ENSURE(p.exceptions <= rows &&
                 p.file_offset + rows * p.byte_size +
                     uint64_t{p.exceptions} * kNormExceptionBytes <=
                   footer_offset,
               ".nrm reader: norm data on column id ", id,
               " out of range (offset ", p.file_offset, ")");
  }
  return meta;
}

}  // namespace

NrmReader::NrmReader(const Directory& dir, std::string_view segment_name) {
  const auto filename = NrmFileName(segment_name);
  bool exists = false;
  if (!dir.exists(exists, filename)) {
    throw IoError{
      absl::StrCat("nrm reader: cannot stat .nrm file: ", filename)};
  }
  if (!exists) {
    return;
  }
  _in = dir.open(filename, IOAdvice::RANDOM);
  if (!_in) {
    throw IoError{
      absl::StrCat("nrm reader: cannot open .nrm file: ", filename)};
  }
  auto fin = _in->Dup();
  format_utils::ReadFooter(
    *fin, filename,
    [&](duckdb::BinaryDeserializer& footer, uint64_t data_size) {
      footer.ReadList(
        kNrmFieldColumns, "norm_columns",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            const auto id =
              static_cast<field_id>(obj.ReadProperty<uint64_t>(0, "id"));
            auto column = std::make_unique<NormColumnReader>(
              id, DeserializeNormMeta(obj, id, data_size), *_in);
            const bool ok = _by_id.emplace(id, column.get()).second;
            SDB_ENSURE(ok, ".nrm footer: duplicate norm field_id ", id);
            _columns.push_back(std::move(column));
          });
        });
    });
}

NrmReader::~NrmReader() = default;

const NormColumnReader* NrmReader::NormColumn(field_id id) const noexcept {
  auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr : it->second;
}

}  // namespace irs
