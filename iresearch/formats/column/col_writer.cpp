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

#include <absl/algorithm/container.h>

#include <cstddef>
#include <cstring>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/database.hpp>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

#ifndef IRESEARCH_NO_VECTOR
#include "iresearch/formats/hnsw/hnsw_writer.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"
#endif

namespace irs {
namespace {

uint64_t BlockRows(const std::vector<ColumnBlockMeta>& blocks) {
  uint64_t rows = 0;
  for (const auto& block : blocks) {
    rows += block.tuple_count;
  }
  return rows;
}

uint64_t MetaRows(const ColumnMeta& meta) {
  return BlockRows(meta.data.empty() ? meta.validity : meta.data);
}

void AppendPart(ColumnMeta& meta, ColumnMeta& part) {
  SDB_ASSERT(part.list_parts.empty() && part.variant_rgs.empty() &&
             !part.distinct);
  if (meta.type.id() == duckdb::LogicalTypeId::INVALID) {
    meta.id = part.id;
    meta.type = std::move(part.type);
    meta.compression_level = part.compression_level;
  }
  if (meta.children.empty()) {
    meta.children.resize(part.children.size());
  }
  SDB_ASSERT(meta.children.size() == part.children.size());
  if (meta.type.InternalType() == duckdb::PhysicalType::LIST &&
      !meta.data.empty()) {
    meta.list_parts.push_back(ListPart{
      .row = BlockRows(meta.data),
      .code = MetaRows(meta.children[1]),
      .elem = MetaRows(meta.children[0]),
    });
  }
  absl::c_move(part.data, std::back_inserter(meta.data));
  absl::c_move(part.validity, std::back_inserter(meta.validity));
  for (size_t i = 0; i != part.children.size(); ++i) {
    AppendPart(meta.children[i], part.children[i]);
  }
}

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
                         const NormColumnSpec& norm) {
  const auto& meta = norm.meta;
  s.WriteProperty(0, "id", static_cast<uint64_t>(norm.id));
  s.WriteList(1, "regions", meta.regions.size(),
              [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                list.WriteObject([&](duckdb::BinarySerializer& obj) {
                  SerializeNormRegion(obj, meta.regions[i]);
                });
              });
}

}  // namespace

ColWriter::ColWriter(duckdb::DatabaseInstance& db, FileTracker& tracker,
                     WriteTier tier)
  : _tracker{tracker},
    _filename{FileName(tracker.Name())},
    _db{&db},
    _tier{tier} {}

ColWriter::~ColWriter() {
  if (_file && !_committed) {
    Rollback();
  }
}

void ColWriter::EnsureOut() {
  if (_file) {
    return;
  }
  _file = std::make_unique<ColFileWriter>(
    format_utils::OpenOutput(_tracker.Fs(), _tracker.Root(), _filename));
  _tracker.Add(kColFile);
  _write_ctx = std::make_unique<WriteContext>(_db->GetBufferManager(), *_file);
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

ColumnWriter* ColWriter::StagedColumn(field_id id) noexcept {
  SDB_ASSERT(_deferred);
  const auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr
                            : _columns[it->second]->parts.front().writer.get();
}

ColWriter::Column& ColWriter::AddColumn(field_id id, uint32_t parts,
                                        bool whole) {
  SDB_ASSERT(whole || !_deferred);
  if (!_deferred) {
    EnsureOut();
  }
  const bool added = _by_id.try_emplace(id, _columns.size()).second;
  SDB_ASSERT(added);
  return *_columns.emplace_back(std::make_unique<Column>(parts, whole));
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type,
                                    ColumnWriterOptions options) {
  SDB_ASSERT(options.row_group_size != 0);
  options.codec_params.tier = _tier;
  if (const auto it = _by_id.find(id); it != _by_id.end()) {
    auto& column = *_columns[it->second];
    SDB_ASSERT(column.whole && column.parts.front().writer);
    auto& existing = *column.parts.front().writer;
    SDB_ASSERT(existing.Type() == type && existing.Options() == options);
    return existing;
  }
  auto& writer = AddColumn(id, 1, true).parts.front().writer;
  writer = std::make_unique<ColumnWriter>(*this, id, std::move(type), options,
                                          _deferred, 0);
  return *writer;
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type) {
  ColumnWriterOptions options;
  if (_field_options) {
    const auto opts = _field_options->GetColumnOptions(id);
    options = {
      .skip_validity = opts.skip_validity,
      .row_group_size = _field_options->row_group_size,
      .compression = opts.compression,
      .distinct = opts.distinct,
      .codec_params = _field_options->CodecParams(opts),
    };
  }
  auto& cw = OpenColumn(id, std::move(type), options);
  if (_field_options) {
    for (const auto ann : _field_options->AnnFields(id)) {
      AttachAnn(id, *_field_options->GetColumnOptions(ann).ann_info);
    }
  }
  return cw;
}

ColumnWriter& ColWriter::OpenedColumn(field_id id) noexcept {
  auto& column = ColumnOf(id);
  SDB_ASSERT(column.whole && column.parts.front().writer);
  return *column.parts.front().writer;
}

void ColWriter::CloseColumn(field_id id) {
  auto& column = ColumnOf(id);
  SDB_ASSERT(column.whole);
  Close(column.parts.front());
}

void ColWriter::PlanParts(field_id id, uint32_t parts) {
  AddColumn(id, parts, false);
}

ColWriter::Column& ColWriter::ColumnOf(field_id id) {
  const auto it = _by_id.find(id);
  SDB_ASSERT(it != _by_id.end());
  return *_columns[it->second];
}

ColWriter::ColumnEntry& ColWriter::PartOf(field_id id, uint32_t part) {
  auto& column = ColumnOf(id);
  SDB_ASSERT(!column.whole && part < column.parts.size());
  return column.parts[part];
}

void ColWriter::Close(ColumnEntry& entry) {
  SDB_ASSERT(entry.writer);
  entry.writer->Finish();
  entry.meta = entry.writer->TakeMeta();
  entry.writer.reset();
}

ColumnWriter& ColWriter::OpenColumnPart(field_id id, uint32_t part,
                                        uint64_t first_row,
                                        duckdb::LogicalType type,
                                        const ColumnWriterOptions& options) {
  SDB_ASSERT(options.row_group_size != 0 &&
             options.distinct == DistinctStats::None);
  auto& writer = PartOf(id, part).writer;
  SDB_ASSERT(!writer);
  auto part_options = options;
  part_options.codec_params.tier = _tier;
  writer = std::make_unique<ColumnWriter>(*this, id, std::move(type),
                                          part_options, false, first_row);
  return *writer;
}

void ColWriter::CloseColumnPart(field_id id, uint32_t part) {
  Close(PartOf(id, part));
}

ColumnMeta ColWriter::Assemble(Column& column) {
  if (column.whole) {
    auto& entry = column.parts.front();
    SDB_ASSERT(!entry.writer);
    return std::move(entry.meta);
  }
  ColumnMeta meta;
  for (auto& part : column.parts) {
    SDB_ASSERT(!part.writer &&
               part.meta.type.id() != duckdb::LogicalTypeId::INVALID);
    AppendPart(meta, part.meta);
  }
  return meta;
}

NormColumnWriter& ColWriter::AddNormColumn(field_id id,
                                           uint32_t row_group_size) {
  auto& writer = *_norms.emplace_back(std::make_unique<NormColumnWriter>(
    id, _deferred ? 0 : row_group_size, [this]() -> ColFileWriter& {
      EnsureOut();
      return *_file;
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
  if (_deferred) {
    SDB_ASSERT(_norm_row_group_size == 0 ||
               _norm_row_group_size == row_group_size);
    _norm_row_group_size = row_group_size;
  }
  return AddNormColumn(id, row_group_size);
}

NormColumnWriter& ColWriter::StreamNormColumn(field_id id) {
  SDB_ASSERT(!_deferred);
  SDB_ASSERT(!_norm_by_id.contains(id), "ColWriter::StreamNormColumn: column ",
             id, " already open");
  return AddNormColumn(id, 0);
}

AnnWriter& ColWriter::AttachAnn(field_id column_id, AnnInfo info) {
  if (auto it = _ann_by_id.find(info.index_id); it != _ann_by_id.end()) {
    auto& existing = _ann_writers[it->second];
    SDB_ASSERT(existing.info == info && existing.column_id == column_id,
               "ColWriter::AttachAnn: re-attach with mismatched AnnInfo on "
               "column ",
               column_id);
    return *existing.writer;
  }
  SDB_ASSERT(_by_id.contains(column_id), "ColWriter::AttachAnn: column ",
             column_id, " must be opened first");
  std::unique_ptr<AnnWriter> writer;
  switch (info.kind) {
#ifndef IRESEARCH_NO_VECTOR
    case AnnKind::Ivf:
      writer = std::make_unique<IvfWriter>(info);
      break;
    case AnnKind::Hnsw:
      writer = std::make_unique<HnswWriter>(info, _hnsw_bases);
      break;
#endif
    default:
      SDB_UNREACHABLE();
  }
  auto& entry =
    _ann_writers.emplace_back(column_id, std::move(info), std::move(writer));
  _ann_by_id.emplace(entry.info.index_id, _ann_writers.size() - 1);
  return *entry.writer;
}

bool ColWriter::HasAnn(field_id column_id) const noexcept {
  return absl::c_any_of(_ann_writers, [&](const AnnEntry& entry) {
    return entry.column_id == column_id;
  });
}

void ColWriter::SetIdxWriter(IdxWriter& idx) noexcept {
  for (auto& entry : _ann_writers) {
    entry.writer->SetIdxWriter(idx);
  }
}

std::vector<std::unique_ptr<AnnWriter>> ColWriter::TakeAnnWriters() noexcept {
  std::vector<std::unique_ptr<AnnWriter>> out;
  out.reserve(_ann_writers.size());
  for (auto& entry : _ann_writers) {
    out.push_back(std::move(entry.writer));
  }
  return out;
}

void ColWriter::Rollback() noexcept {
  _reader.reset();
  _write_ctx.reset();
  _file.reset();
}

void ColWriter::ReorderNorms(uint64_t rows,
                             std::span<const uint32_t> new2old) {
  SDB_ASSERT(_deferred && !_norms_reordered);
  _norms_reordered = true;
  for (auto& writer : _norms) {
    writer->PadTo(rows);
    writer->Reorder(new2old, _norm_row_group_size);
  }
}

void ColWriter::Reorder(uint64_t rows, std::span<const uint32_t> new2old) {
  SDB_ASSERT(new2old.empty() || new2old.size() == rows);
  for (auto& column : _columns) {
    auto& writer = *column->parts.front().writer;
    writer.PadNullsTo(rows);
    writer.Reorder(new2old);
  }
  ReorderNorms(rows, new2old);
}

std::vector<irs::StagedColumn> ColWriter::Snapshot(uint64_t rows) const {
  std::vector<irs::StagedColumn> columns;
  columns.reserve(_columns.size());
  for (const auto& column : _columns) {
    const auto& entry = column->parts.front();
    SDB_ASSERT(column->whole && entry.writer);
    const auto& writer = *entry.writer;
    columns.push_back(irs::StagedColumn{.id = writer.Id(),
                                        .type = writer.Type(),
                                        .chunks = writer.Snapshot(rows),
                                        .distinct = writer.Options().distinct});
  }
  return columns;
}

std::vector<NormColumnSpec> ColWriter::SealNorms(uint64_t rows) {
  if (_deferred && !_norms_reordered) {
    ReorderNorms(rows, {});
  }
  std::vector<NormColumnSpec> norms;
  norms.reserve(_norms.size());
  for (auto& writer : _norms) {
    if (writer->RowCount() < rows) {
      writer->PadTo(rows);
    }
    writer->Finalize();
    if (!writer->Meta().regions.empty()) {
      norms.push_back({.id = writer->Id(), .meta = writer->Meta()});
    }
  }
  if (!norms.empty()) {
    EnsureOut();
  }
  if (_file) {
    _file->Flush();
  }
  return norms;
}

size_t ColWriter::StagedBytes() const noexcept {
  size_t bytes = 0;
  for (const auto& column : _columns) {
    for (const auto& part : column->parts) {
      if (part.writer) {
        bytes += part.writer->StagedBytes();
      }
    }
  }
  if (_deferred && !_norms_reordered) {
    for (const auto& norm : _norms) {
      bytes += norm->StagedBytes();
    }
  }
  return bytes;
}

constexpr auto kNoCancel = [] { return true; };

bool ColWriter::Commit(uint64_t target_row, std::span<const uint32_t> new2old) {
  return Commit(target_row, kNoCancel, new2old);
}

uint64_t ColWriter::RowGroupRows(uint64_t row,
                                 uint64_t uniform) const noexcept {
  const auto next = absl::c_upper_bound(_row_group_starts, row);
  return next == _row_group_starts.end() ? uniform - row % uniform
                                         : *next - row;
}

yaclib::Task<bool> ColWriter::ComputeAnn(const AnnBuildEnv* env) {
  return ComputeAnn(env, kNoCancel);
}

bool ColWriter::Commit(uint64_t target_row, absl::FunctionRef<bool()> progress,
                       std::span<const uint32_t> new2old) {
  if (_committed) {
    return true;
  }
  SDB_ASSERT(new2old.empty() || _deferred);
  if (!new2old.empty()) {
    Reorder(target_row, new2old);
  } else if (_deferred && !_norms_reordered) {
    ReorderNorms(target_row, {});
  }
  ColFooter footer;
  for (auto& writer : _norms) {
    if (writer->RowCount() < target_row) {
      writer->PadTo(target_row);
    }
    writer->Finalize();
    SDB_ASSERT(writer->Meta().row_count == target_row,
               "ColWriter::Commit: norm column ", writer->Id(), " holds ",
               writer->Meta().row_count, " rows, segment ", target_row);
    if (!writer->Meta().regions.empty()) {
      footer.norms.push_back({.id = writer->Id(), .meta = writer->TakeMeta()});
    }
  }
  if (Empty() && !_file && footer.norms.empty()) {
    _reader = std::make_unique<ColReader>(*_db, nullptr, ColFooter{});
    _committed = true;
    return true;
  }
  EnsureOut();
  for (auto& column : _columns) {
    auto& entry = column->parts.front();
    if (!column->whole || !entry.writer) {
      continue;
    }
    if (!progress()) {
      return false;
    }
    Close(entry);
  }
  auto row_groups =
    _row_group_starts.empty() ? size_t{0} : _row_group_starts.size() - 1;
  SDB_IF_FAILURE("irs::ColWriterOmitsRowGroupStarts") { row_groups = 0; }
  footer.row_group_starts.assign(_row_group_starts.begin(),
                                 _row_group_starts.begin() + row_groups);
  footer.columns.reserve(_columns.size());
  for (auto& column : _columns) {
    footer.columns.push_back(Assemble(*column));
  }
  auto out = _file->Finish([&](duckdb::BinarySerializer& out) {
    out.WriteOptionalList(
      kColFieldColumns, "columns", footer.columns.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          SerializeColumnMeta(obj, footer.columns[i]);
        });
      });
    out.WriteOptionalList(
      kColFieldNormColumns, "norm_columns", footer.norms.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          SerializeNormColumn(obj, footer.norms[i]);
        });
      });
    out.WriteOptionalList(
      kColFieldRowGroupStarts, "row_group_starts",
      footer.row_group_starts.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        list.WriteElement(footer.row_group_starts[i]);
      });
  });
  _tracker.Written(out->Position());
  _reader = std::make_unique<ColReader>(*_db, format_utils::CloseOutput(*out),
                                        std::move(footer));
  _committed = true;
  return true;
}

yaclib::Task<bool> ColWriter::ComputeAnn(const AnnBuildEnv* env,
                                         absl::FunctionRef<bool()> progress) {
  if (!_committed || _ann_writers.empty()) {
    co_return true;
  }
  SDB_ASSERT(_reader);
  const auto& reader = *_reader;
  ReadContext ctx{reader};
  for (auto& entry : _ann_writers) {
    if (!progress()) {
      co_return false;
    }
    const auto* col = reader.Column(entry.column_id);
    if (!col) {
      continue;
    }
    co_await entry.writer->Compute(*col, ctx, env);
  }
  co_return true;
}

yaclib::Task<> ColWriter::ComputeAnn(field_id column, ColReader& reader,
                                     const AnnBuildEnv* env) {
  SDB_ASSERT(_committed);
  const auto* col = reader.Column(column);
  if (!col) {
    co_return {};
  }
  for (auto& entry : _ann_writers) {
    if (entry.column_id == column) {
      co_await entry.writer->Compute(*col, reader.Ctx(), env);
    }
  }
  co_return {};
}

}  // namespace irs
