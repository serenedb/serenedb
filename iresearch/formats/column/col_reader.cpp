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

}  // namespace

ColReader::ColReader(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
                     std::string_view root, std::string_view segment_name,
                     bool present, duckdb::MappedFileAccess access)
  : _db{&db},
    _file{present ? fs.OpenInputFile(fs.JoinPath(root, FileName(segment_name)),
                                     access)
                  : nullptr},
    _ctx{db, _file.get()} {
  ColFooter footer;
  if (!_file) {
    Adopt(std::move(footer));
    return;
  }
  format_utils::ReadFooter(
    *_file, FileName(segment_name),
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
  _row_group_starts = std::move(footer.row_group_starts);
}

ColReader::ColReader(duckdb::DatabaseInstance& db,
                     std::vector<std::unique_ptr<ColumnReader>> columns)
  : _db{&db}, _ctx{db, nullptr}, _owned{std::move(columns)} {
  for (const auto& col : _owned) {
    Index(*col);
  }
}

ColReader::ColReader(const ColReader& base,
                     std::vector<std::shared_ptr<const ColumnReader>> replaced)
  : _db{base._db},
    _ctx{*base._db},
    _base{&base},
    _replaced{std::move(replaced)} {
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

}  // namespace irs
