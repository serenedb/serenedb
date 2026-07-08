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
  return _columns.empty() && _norm_writers.empty() && _ivf_writers.empty();
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
    AttachIVF(id, *opts.ivf_info);
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

IvfWriter& ColWriter::AttachIVF(field_id column_id, IvfInfo info) {
  if (auto it = _ivf_by_id.find(column_id); it != _ivf_by_id.end()) {
    auto& existing = *it->second;
    SDB_ASSERT(existing.info == info,
               "ColWriter::AttachIVF: re-attach with mismatched IvfInfo on "
               "column ",
               column_id);
    return *existing.writer;
  }
  SDB_ASSERT(_by_id.contains(column_id), "ColWriter::AttachIVF: column ",
             column_id, " must be opened first");
  auto entry = std::make_unique<IvfEntry>();
  entry->column_id = column_id;
  entry->writer = std::make_unique<IvfWriter>(info);
  entry->info = std::move(info);
  auto& back = *_ivf_writers.emplace_back(std::move(entry));
  _ivf_by_id.emplace(column_id, &back);
  return *back.writer;
}

std::vector<std::unique_ptr<IvfWriter>> ColWriter::TakeIvfWriters() noexcept {
  std::vector<std::unique_ptr<IvfWriter>> out;
  out.reserve(_ivf_writers.size());
  for (auto& entry : _ivf_writers) {
    if (entry->writer) {
      out.push_back(std::move(entry->writer));
    }
  }
  return out;
}

void ColWriter::Rollback() noexcept { _out.reset(); }

void ColWriter::Commit(uint64_t target_row) {
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
  if (!_ivf_writers.empty()) {
    _out->Flush();
    ColReader reader{*_dir, _segment_name, *_db};
    for (auto& entry : _ivf_writers) {
      const auto* col = reader.Column(entry->column_id);
      if (!col) {
        continue;
      }
      entry->writer->Compute(*col, reader.Ctx());
    }
  }
  _out.reset();
  _committed = true;
}

}  // namespace irs
