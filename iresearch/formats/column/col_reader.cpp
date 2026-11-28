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

#include <absl/algorithm/container.h>

#include <atomic>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/main/database.hpp>
#include <utility>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constinit std::atomic<uint64_t> gNextFileId{1};

bool InRange(uint64_t offset, uint64_t size, uint64_t end) noexcept {
  return size <= end && offset <= end - size;
}

template<typename... Parts>
[[noreturn]] void ThrowCorruptedFooter(const Parts&... parts) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED), ERR_MSG(parts...));
}

void CheckBlockRange(const ColumnBlockMeta& m, field_id id,
                     uint64_t footer_offset) {
  if (!(InRange(m.file_offset, m.byte_size, footer_offset) &&
        InRange(m.shared_offset, m.shared_size, footer_offset))) [[unlikely]] {
    ThrowCorruptedFooter(".col reader: column data on column id ", id,
                         " out of range (offset ", m.file_offset, ", size ",
                         m.byte_size, ")");
  }
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
    CheckColumnMetaRanges(*rg.unshredded, footer_offset);
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
  if (!(r.stats.rows != 0 && r.stats.rows <= doc_limits::kMaxDocs &&
        r.stats.min <= r.stats.max &&
        (r.bits == 0 || r.bits == 8 || r.bits == 16 || r.bits == 32)))
    [[unlikely]] {
    ThrowCorruptedFooter(".col reader: norm region on column id ", id,
                         " is corrupt");
  }
  if (r.bits == 0) {
    r.value = d.ReadProperty<uint32_t>(8, "value");
    return r;
  }
  r.file_offset = d.ReadProperty<uint64_t>(9, "file_offset");
  const auto slots = NormSlotsSize(r);
  if (!InRange(r.file_offset, slots, footer_offset)) [[unlikely]] {
    ThrowCorruptedFooter(".col reader: norm data on column id ", id,
                         " out of range (offset ", r.file_offset, ")");
  }
  r.exceptions =
    d.ReadPropertyWithExplicitDefault<uint32_t>(10, "exceptions", 0);
  if (r.exceptions == 0) {
    return r;
  }
  r.overflow = d.ReadProperty<uint32_t>(11, "overflow");
  r.shift = d.ReadProperty<uint8_t>(12, "shift");
  r.exception_bytes = d.ReadProperty<uint8_t>(13, "exception_bytes");
  r.table_offset = d.ReadProperty<uint64_t>(14, "table_offset");
  if (!(r.bits != 32 && r.exceptions <= r.stats.rows &&
        r.overflow <= r.exceptions && r.shift <= kNormMaxShift &&
        (r.exception_bytes == 2 || r.exception_bytes == 4))) [[unlikely]] {
    ThrowCorruptedFooter(".col reader: norm exceptions on column id ", id,
                         " are corrupt");
  }
  const auto table = NormTableSize(r);
  if (!InRange(r.table_offset, table, footer_offset)) [[unlikely]] {
    ThrowCorruptedFooter(".col reader: norm exceptions on column id ", id,
                         " out of range (offset ", r.table_offset, ")");
  }
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
  if (meta.regions.empty() || meta.row_count > doc_limits::kMaxDocs)
    [[unlikely]] {
    ThrowCorruptedFooter(".col reader: norm column id ", id, " holds ",
                         meta.row_count, " rows across ", meta.regions.size(),
                         " regions");
  }
  return meta;
}

}  // namespace

ColReader::ColReader(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
                     std::string_view root, std::string_view segment_name,
                     FileSizes sizes, duckdb::MappedFileAccess access)
  : _db{&db},
    _file{sizes.file != 0 ? fs.OpenInputFile(
                              fs.JoinPath(root, FileName(segment_name)), access)
                          : nullptr},
    _ctx{db, _file.get()} {
  ColFooter footer;
  if (!_file) {
    Adopt(std::move(footer));
    return;
  }
  format_utils::ReadFooter(
    *_file, FileName(segment_name), sizes,
    [&](duckdb::BinaryDeserializer& in, uint64_t data_size) {
      in.Set<duckdb::DatabaseInstance&>(db);
      in.ReadOptionalList(
        kColFieldColumns, "columns",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            auto& meta =
              footer.columns.emplace_back(DeserializeColumnMeta(obj));
            CheckColumnMetaRanges(meta, data_size);
          });
        });
      in.ReadOptionalList(
        kColFieldNormColumns, "norm_columns",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            const auto id =
              static_cast<field_id>(obj.ReadProperty<uint64_t>(0, "id"));
            footer.norms.push_back(
              {.id = id, .meta = DeserializeNormMeta(obj, id, data_size)});
          });
        });
      in.ReadOptionalList(
        kColFieldRowGroupStarts, "row_group_starts",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          const auto start = list.ReadElement<uint64_t>();
          if (!(footer.row_group_starts.empty()
                  ? start == 0
                  : start > footer.row_group_starts.back())) [[unlikely]] {
            ThrowCorruptedFooter(
              ".col footer: row group starts are not increasing from 0");
          }
          footer.row_group_starts.push_back(start);
        });
      in.Unset<duckdb::DatabaseInstance>();
    });
  Adopt(std::move(footer));
}

ColReader::ColReader(duckdb::DatabaseInstance& db,
                     duckdb::shared_ptr<duckdb::InputFile> file,
                     ColFooter footer)
  : _db{&db}, _file{std::move(file)}, _ctx{db, _file.get()} {
  Adopt(std::move(footer));
}

void ColReader::Adopt(ColFooter footer) {
  const auto file_id = gNextFileId.fetch_add(1, std::memory_order_relaxed);
  _owned.reserve(footer.columns.size());
  for (auto& meta : footer.columns) {
    Index(*_owned.emplace_back(ColumnReader::Make(std::move(meta), file_id)));
  }
  SDB_ASSERT(footer.norms.empty() || _file);
  for (auto& norm : footer.norms) {
    auto column =
      std::make_unique<NormColumnReader>(norm.id, std::move(norm.meta), *_file);
    if (!_norm_by_id.emplace(norm.id, column.get()).second) [[unlikely]] {
      ThrowCorruptedFooter(".col footer: duplicate norm field_id ", norm.id);
    }
    _norm_columns.push_back(std::move(column));
  }
  _row_group_starts = std::move(footer.row_group_starts);
}

ColReader::ColReader(duckdb::DatabaseInstance& db,
                     std::vector<std::unique_ptr<ColumnReader>> columns)
  : ColReader{db, std::move(columns), nullptr, {}} {}

ColReader::ColReader(duckdb::DatabaseInstance& db,
                     std::vector<std::unique_ptr<ColumnReader>> columns,
                     duckdb::shared_ptr<duckdb::InputFile> norms_file,
                     std::vector<NormColumnSpec> norms)
  : _db{&db},
    _file{std::move(norms_file)},
    _ctx{db, _file.get()},
    _owned{std::move(columns)} {
  for (const auto& col : _owned) {
    Index(*col);
  }
  SDB_ASSERT(norms.empty() || _file);
  for (auto& norm : norms) {
    auto column =
      std::make_unique<NormColumnReader>(norm.id, std::move(norm.meta), *_file);
    const bool ok = _norm_by_id.emplace(norm.id, column.get()).second;
    SDB_ENSURE(ok, "in-memory columns: duplicate norm field_id ", norm.id);
    _norm_columns.push_back(std::move(column));
  }
}

ColReader::ColReader(const ColReader& base,
                     std::vector<std::shared_ptr<const ColumnReader>> replaced)
  : _db{base._db},
    _ctx{*base._db},
    _base{&base},
    _replaced{std::move(replaced)},
    _norm_by_id{base._norm_by_id} {
  _columns.reserve(base._columns.size() + _replaced.size());
  for (const auto* column : base._columns) {
    const auto it = absl::c_find_if(_replaced, [&](const auto& patched) {
      return patched->Id() == column->Id();
    });
    Index(it == _replaced.end() ? *column : **it);
  }
  for (const auto& column : _replaced) {
    if (!base.HasColumn(column->Id())) {
      Index(*column);
    }
  }
}

ColReader::~ColReader() = default;

void ColReader::Index(const ColumnReader& column) {
  const bool ok = _by_id.emplace(column.Id(), &column).second;
  SDB_ENSURE(ok, "columns: duplicate column field_id ", column.Id());
  _columns.push_back(&column);
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
