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

#include "iresearch/formats/column/col_writer.hpp"

#include <absl/strings/str_cat.h>

#include <cstring>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/database.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/error.h"
#include "basics/exceptions.h"
#include "basics/serialization.h"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"

namespace irs {

void SerializeNormColumn(duckdb::Serializer& s, const NormColumnWriter& nw) {
  s.WriteProperty<uint64_t>(0, "id", static_cast<uint64_t>(nw.Id()));
  const auto& ptrs = nw.Pointers();
  s.WriteList(1, "row_groups", ptrs.size(),
              [&](duckdb::Serializer::List& rgl, duckdb::idx_t j) {
                const auto& p = ptrs[j];
                rgl.WriteObject([&](duckdb::Serializer& po) {
                  po.WriteProperty<uint8_t>(0, "byte_size", p.byte_size);
                  po.WriteProperty<uint32_t>(1, "row_count", p.row_count);
                  po.WriteProperty<uint32_t>(2, "max", p.max);
                  po.WriteProperty<uint64_t>(3, "sum", p.sum);
                  po.WriteProperty<uint64_t>(4, "non_zero_count",
                                             p.non_zero_count);
                  po.WriteProperty<uint64_t>(5, "file_offset", p.file_offset);
                });
              });
}

ColWriter::ColWriter(Directory& dir, std::string_view segment_name,
                     duckdb::DatabaseInstance& db)
  : _dir{&dir},
    _segment_name{segment_name},
    _filename{FileName(segment_name)},
    _db{&db} {}

ColWriter::~ColWriter() {
  if (_out && !_committed) {
    Rollback();
  }
}

void ColWriter::EnsureOut() {
  if (_out) {
    return;
  }
  _out = _dir->create(_filename);
  if (!_out) {
    throw IoError{
      absl::StrCat("col writer: cannot create .col file: ", _filename)};
  }
  format_utils::WriteHeader(*_out, kFormatName, kFormatVersion);
  _write_ctx = std::make_unique<WriteContext>(*_db, *_out);
}

bool ColWriter::Empty() const noexcept {
  return _columns.empty() && _norm_writers.empty();
}

void ColWriter::SetFieldOptions(
  const IndexFieldOptions* field_options) noexcept {
  SDB_ASSERT(
    !_field_options || CompatibleFieldOptions(_field_options, field_options),
    "ColWriter::SetFieldOptions: encodings differ mid-segment");
  _field_options = field_options;
}

ColumnWriter& ColWriter::OpenColumnInternal(
  field_id id, duckdb::LogicalType type, bool skip_validity,
  uint32_t row_group_size, duckdb::CompressionType forced, bool hyperloglog) {
  SDB_ASSERT(row_group_size != 0);
  if (auto it = _by_id.find(id); it != _by_id.end()) {
    auto& existing = *it->second;
    SDB_ASSERT(
      existing._type == type && existing._row_group_size == row_group_size &&
        existing._skip_validity == skip_validity &&
        existing._forced == forced &&
        (existing._meta.hyperloglog != nullptr) == hyperloglog,
      "ColWriter::OpenColumn: re-opened id ", id, " with mismatched settings");
    return existing;
  }
  EnsureOut();
  auto col =
    std::make_unique<ColumnWriter>(*this, id, std::move(type), skip_validity,
                                   row_group_size, forced, hyperloglog);
  auto* ptr = col.get();
  _by_id.emplace(id, ptr);
  _columns.push_back(std::move(col));
  return *ptr;
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type) {
  ColumnOptions opts{};
  if (_field_options) {
    opts = _field_options->GetColumnOptions(id);
  }
  auto& cw =
    OpenColumnInternal(id, std::move(type), opts.skip_validity,
                       opts.row_group_size, opts.compression, opts.hyperloglog);
  if (opts.ivf_info) {
    _has_ivf_column = true;
  }
  return cw;
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type,
                                    bool skip_validity, uint32_t row_group_size,
                                    duckdb::CompressionType compression,
                                    bool hyperloglog) {
  return OpenColumnInternal(id, std::move(type), skip_validity, row_group_size,
                            compression, hyperloglog);
}

NormColumnWriter* ColWriter::OpenNormColumn(field_id id) {
  if (!_field_options) {
    return nullptr;
  }
  const auto opts = _field_options->GetNormColumnOptions(id);
  if (!field_limits::valid(opts.id)) {
    return nullptr;
  }
  return &OpenNormColumn(opts.id, opts.row_group_size);
}

NormColumnWriter& ColWriter::OpenNormColumn(field_id id,
                                            uint32_t row_group_size) {
  if (auto it = _norm_by_id.find(id); it != _norm_by_id.end()) {
    return *it->second;
  }
  EnsureOut();
  auto nw = std::make_unique<NormColumnWriter>(id, row_group_size, *_out);
  auto* ptr = nw.get();
  _norm_by_id.emplace(id, ptr);
  _norm_writers.push_back(std::move(nw));
  return *ptr;
}

void ColWriter::NoteIvfColumn() noexcept { _has_ivf_column = true; }

std::unique_ptr<IvfWriter> ColWriter::TakeIvf() noexcept {
  return std::move(_ivf);
}

void ColWriter::Rollback() noexcept { _out.reset(); }

void ColWriter::Commit(uint64_t target_row, IdxWriter* idx) {
  if (_committed) {
    return;
  }
  if (Empty() && !_out) {
    _committed = true;
    return;
  }
  for (auto& cw : _columns) {
    cw->SealRowGroup();
  }
  for (auto& nw : _norm_writers) {
    nw->PadTo(target_row);
    nw->Finalize();
  }
  std::vector<const NormColumnWriter*> norm_columns;
  norm_columns.reserve(_norm_writers.size());
  for (const auto& nw : _norm_writers) {
    if (!nw->Pointers().empty()) {
      norm_columns.push_back(nw.get());
    }
  }
  const uint64_t footer_offset = _out->Position();
  duckdb::BinarySerializer serializer{*_out, duckdb::VersionStorageOptions()};
  serializer.Begin();
  serializer.WriteList(kFooterSlotColumns, "columns", _columns.size(),
                       [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                         list.WriteObject([&](duckdb::Serializer& obj) {
                           SerializeColumnMeta(obj, _columns[i]->Meta());
                         });
                       });
  serializer.WriteList(kFooterSlotNormColumns, "norm_columns",
                       norm_columns.size(),
                       [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                         list.WriteObject([&](duckdb::Serializer& obj) {
                           SerializeNormColumn(obj, *norm_columns[i]);
                         });
                       });
  serializer.End();
  _out->WriteU64(footer_offset);
  format_utils::WriteFooter(*_out);
  if (_has_ivf_column) {
    SDB_ASSERT(idx,
               "ColWriter::Commit requires an IdxWriter when an IVF column "
               "is present");
    SDB_ASSERT(_field_options,
               "ColWriter::Commit: IVF column requires field options");
    _out->Flush();
    ColReader reader{*_dir, _segment_name, *_db};
    _ivf = std::make_unique<IvfWriter>();
    for (const auto& cw : _columns) {
      const auto opts = _field_options->GetColumnOptions(cw->Id());
      if (!opts.ivf_info) {
        continue;
      }
      const auto* col = reader.Column(cw->Id());
      if (!col) {
        continue;
      }
      _ivf->BuildColumn(*col, reader.Ctx(), *idx, *opts.ivf_info);
    }
  }
  _out.reset();
  _committed = true;
}

}  // namespace irs
