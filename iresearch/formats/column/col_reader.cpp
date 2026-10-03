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

}  // namespace

ColReader::ColReader(const Directory& dir, std::string_view segment_name,
                     duckdb::DatabaseInstance& db, IOAdvice advice)
  : _db{&db},
    _ctx{db, OpenColFile(dir, segment_name, advice)},
    _nrm{dir, segment_name} {
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
      footer.Unset<duckdb::DatabaseInstance>();
    });
}

ColReader::~ColReader() = default;

const ColumnReader* ColReader::Column(field_id id) const noexcept {
  auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr : it->second;
}

}  // namespace irs
