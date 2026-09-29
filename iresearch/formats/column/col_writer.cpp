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
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/hnsw/hnsw_writer.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

void SerializeNormRegion(duckdb::BinarySerializer& s, const NormRegionMeta& r) {
  s.WriteProperty(0, "rows", r.stats.rows);
  s.WriteProperty(1, "sum", r.stats.sum);
  s.WriteProperty(2, "non_zero", r.stats.non_zero);
  s.WriteProperty(3, "wide8", r.stats.wide8);
  s.WriteProperty(4, "wide16", r.stats.wide16);
  s.WriteProperty(5, "min", r.stats.min);
  s.WriteProperty(6, "max", r.stats.max);
  s.WriteProperty(7, "bits", r.bits);
  if (r.bits == 0) {
    s.WriteProperty(8, "value", r.value);
    return;
  }
  s.WriteProperty(9, "file_offset", r.file_offset);
  s.WritePropertyWithDefault<uint32_t>(10, "exceptions", r.exceptions, 0);
  if (r.exceptions != 0) {
    s.WriteProperty(11, "overflow", r.overflow);
    s.WriteProperty(12, "shift", r.shift);
    s.WriteProperty(13, "exception_bytes", r.exception_bytes);
    s.WriteProperty(14, "table_offset", r.table_offset);
  }
}

void SerializeNormColumn(duckdb::BinarySerializer& s,
                         const NormColumnWriter& nw) {
  const auto& meta = nw.Meta();
  s.WriteProperty(0, "id", static_cast<uint64_t>(nw.Id()));
  s.WriteList(1, "regions", meta.regions.size(),
              [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                list.WriteObject([&](duckdb::BinarySerializer& obj) {
                  SerializeNormRegion(obj, meta.regions[i]);
                });
              });
}

}  // namespace

ColWriter::ColWriter(Directory& dir, std::string_view segment_name,
                     duckdb::DatabaseInstance& db, WriteTier tier)
  : _dir{&dir},
    _segment_name{segment_name},
    _filename{FileName(segment_name)},
    _db{&db},
    _tier{tier} {}

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
  _write_ctx = std::make_unique<WriteContext>(*_db, *_out);
}

bool ColWriter::Empty() const noexcept {
  return _columns.empty() && _ann_writers.empty();
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
  uint32_t row_group_size, duckdb::CompressionType forced, bool hyperloglog,
  ColCodecParams codec_params) {
  SDB_ASSERT(row_group_size != 0);
  if (auto it = _by_id.find(id); it != _by_id.end()) {
    auto& existing = *it->second;
    SDB_ASSERT(
      existing._type == type && existing._row_group_size == row_group_size &&
        existing._skip_validity == skip_validity &&
        existing._forced == forced && existing._codec_params == codec_params &&
        (existing._meta.hyperloglog != nullptr) == hyperloglog,
      "ColWriter::OpenColumn: re-opened id ", id, " with mismatched settings");
    return existing;
  }
  EnsureOut();
  auto col = std::make_unique<ColumnWriter>(*this, id, std::move(type),
                                            skip_validity, row_group_size,
                                            forced, hyperloglog, codec_params);
  auto* ptr = col.get();
  _by_id.emplace(id, ptr);
  _columns.push_back(std::move(col));
  return *ptr;
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type) {
  ColumnOptions opts{};
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  ColCodecParams codec_params;
  if (_field_options) {
    opts = _field_options->GetColumnOptions(id);
    row_group_size = _field_options->row_group_size;
    codec_params = _field_options->CodecParams(opts);
    if (_tier == WriteTier::Flush) {
      codec_params.objective = AutoObjective::Speed;
    }
  }
  auto& cw =
    OpenColumnInternal(id, std::move(type), opts.skip_validity, row_group_size,
                       opts.compression, opts.hyperloglog, codec_params);
  if (opts.ann_info) {
    AttachAnn(id, *opts.ann_info);
  }
  return cw;
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type,
                                    bool skip_validity, uint32_t row_group_size,
                                    duckdb::CompressionType compression,
                                    bool hyperloglog,
                                    ColCodecParams codec_params) {
  return OpenColumnInternal(id, std::move(type), skip_validity, row_group_size,
                            compression, hyperloglog, codec_params);
}

NormColumnWriter& ColWriter::AddNormColumn(field_id id,
                                           uint32_t row_group_size) {
  auto& writer = *_norms.emplace_back(std::make_unique<NormColumnWriter>(
    id, row_group_size, [this]() -> IndexOutput& {
      EnsureOut();
      return *_out;
    }));
  _norm_by_id.emplace(id, &writer);
  return writer;
}

NormColumnWriter& ColWriter::OpenNormColumn(field_id id,
                                            uint32_t row_group_size) {
  SDB_ASSERT(row_group_size != 0);
  if (auto it = _norm_by_id.find(id); it != _norm_by_id.end()) {
    return *it->second;
  }
  return AddNormColumn(id, row_group_size);
}

NormColumnWriter& ColWriter::StreamNormColumn(field_id id) {
  SDB_ASSERT(!_norm_by_id.contains(id), "ColWriter::StreamNormColumn: column ",
             id, " already open");
  return AddNormColumn(id, 0);
}

AnnWriter& ColWriter::AttachAnn(field_id column_id, AnnInfo info) {
  if (auto it = _ann_by_id.find(column_id); it != _ann_by_id.end()) {
    auto& existing = *it->second;
    SDB_ASSERT(existing.info == info,
               "ColWriter::AttachAnn: re-attach with mismatched AnnInfo on "
               "column ",
               column_id);
    return *existing.writer;
  }
  SDB_ASSERT(_by_id.contains(column_id), "ColWriter::AttachAnn: column ",
             column_id, " must be opened first");
  auto entry = std::make_unique<AnnEntry>();
  entry->column_id = column_id;
  switch (info.kind) {
    case AnnKind::Ivf:
      entry->writer = std::make_unique<IvfWriter>(info);
      break;
    case AnnKind::Hnsw:
      entry->writer = std::make_unique<HnswWriter>(info);
      break;
    default:
      SDB_UNREACHABLE();
  }
  entry->info = std::move(info);
  auto& back = *_ann_writers.emplace_back(std::move(entry));
  _ann_by_id.emplace(column_id, &back);
  return *back.writer;
}

void ColWriter::SetIdxWriter(IdxWriter& idx) noexcept {
  for (auto& entry : _ann_writers) {
    if (entry->writer) {
      entry->writer->SetIdxWriter(idx);
    }
  }
}

std::vector<std::unique_ptr<AnnWriter>> ColWriter::TakeAnnWriters() noexcept {
  std::vector<std::unique_ptr<AnnWriter>> out;
  out.reserve(_ann_writers.size());
  for (auto& entry : _ann_writers) {
    if (entry->writer) {
      out.push_back(std::move(entry->writer));
    }
  }
  return out;
}

void ColWriter::Rollback() noexcept { _out.reset(); }

constexpr auto kNoCancel = [] { return true; };

bool ColWriter::Commit(uint64_t target_row) {
  return Commit(target_row, kNoCancel);
}

yaclib::Task<bool> ColWriter::ComputeAnn(const AnnBuildEnv* env) {
  return ComputeAnn(env, kNoCancel);
}

bool ColWriter::Commit(uint64_t target_row,
                       absl::FunctionRef<bool()> progress) {
  if (_committed) {
    return true;
  }
  std::vector<const NormColumnWriter*> norms;
  for (auto& writer : _norms) {
    if (writer->RowCount() < target_row) {
      writer->PadTo(target_row);
    }
    writer->Finalize();
    SDB_ASSERT(writer->Meta().row_count == target_row,
               "ColWriter::Commit: norm column ", writer->Id(), " holds ",
               writer->Meta().row_count, " rows, segment ", target_row);
    if (!writer->Meta().regions.empty()) {
      norms.push_back(writer.get());
    }
  }
  if (Empty() && !_out && norms.empty()) {
    _committed = true;
    return true;
  }
  EnsureOut();
  for (auto& cw : _columns) {
    if (!progress()) {
      return false;
    }
    cw->SealRowGroup();
  }
  format_utils::WriteFooter(*_out, [&](duckdb::BinarySerializer& footer) {
    if (!_columns.empty()) {
      footer.WriteList(
        kColFieldColumns, "columns", _columns.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          list.WriteObject([&](duckdb::BinarySerializer& obj) {
            SerializeColumnMeta(obj, _columns[i]->Meta());
          });
        });
    }
    if (!norms.empty()) {
      footer.WriteList(
        kColFieldNormColumns, "norm_columns", norms.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          list.WriteObject([&](duckdb::BinarySerializer& obj) {
            SerializeNormColumn(obj, *norms[i]);
          });
        });
    }
    footer.WriteProperty<uint64_t>(kColFieldFileId, "file_id", NewColFileId());
  });
  _out.reset();
  _committed = true;
  return true;
}

yaclib::Task<bool> ColWriter::ComputeAnn(const AnnBuildEnv* env,
                                         absl::FunctionRef<bool()> progress) {
  if (!_committed || _ann_writers.empty()) {
    co_return true;
  }
  ColReader reader{*_dir, _segment_name, *_db};
  for (auto& entry : _ann_writers) {
    if (!progress()) {
      co_return false;
    }
    const auto* col = reader.Column(entry->column_id);
    if (!col) {
      continue;
    }
    co_await entry->writer->Compute(*col, reader.Ctx(), env);
  }
  co_return true;
}

}  // namespace irs
