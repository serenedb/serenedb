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
#include <absl/strings/str_cat.h>

#include <cstring>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/database.hpp>
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

ColWriter::ColWriter(duckdb::DatabaseInstance& db, FileTracker& tracker)
  : _tracker{tracker}, _filename{FileName(tracker.Name())}, _db{&db} {}

ColWriter::~ColWriter() {
  if (_out && !_committed) {
    Rollback();
  }
}

void ColWriter::EnsureOut() {
  if (_out) {
    return;
  }
  _out = format_utils::OpenOutput(_tracker.Fs(), _tracker.Root(), _filename);
  _tracker.Add(kColFile);
  _write_ctx = std::make_unique<WriteContext>(
    *_db, *_out,
    _field_options ? _field_options->block_size : DEFAULT_BLOCK_ALLOC_SIZE);
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

ColumnWriter& ColWriter::OpenColumnInternal(field_id id,
                                            duckdb::LogicalType type,
                                            bool skip_validity,
                                            uint32_t row_group_size,
                                            duckdb::CompressionType forced,
                                            DistinctStats distinct) {
  SDB_ASSERT(row_group_size != 0);
  if (auto it = _by_id.find(id); it != _by_id.end()) {
    auto& existing = *it->second;
    SDB_ASSERT(
      existing._type == type && existing._row_group_size == row_group_size &&
        existing._skip_validity == skip_validity &&
        existing._forced == forced && existing._distinct == distinct,
      "ColWriter::OpenColumn: re-opened id ", id, " with mismatched settings");
    return existing;
  }
  if (!_deferred) {
    EnsureOut();
  }
  auto col =
    std::make_unique<ColumnWriter>(*this, id, std::move(type), skip_validity,
                                   row_group_size, forced, distinct, _deferred);
  auto* ptr = col.get();
  _by_id.emplace(id, ptr);
  _columns.push_back(std::move(col));
  return *ptr;
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type) {
  ColumnOptions opts{};
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  if (_field_options) {
    opts = _field_options->GetColumnOptions(id);
    row_group_size = _field_options->row_group_size;
  }
  auto& cw =
    OpenColumnInternal(id, std::move(type), opts.skip_validity, row_group_size,
                       opts.compression, opts.distinct);
  if (_field_options) {
    for (const auto ann : _field_options->AnnFields(id)) {
      AttachAnn(id, *_field_options->GetColumnOptions(ann).ann_info);
    }
  }
  return cw;
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type,
                                    bool skip_validity, uint32_t row_group_size,
                                    duckdb::CompressionType compression,
                                    DistinctStats distinct) {
  return OpenColumnInternal(id, std::move(type), skip_validity, row_group_size,
                            compression, distinct);
}

AnnWriter& ColWriter::AttachAnn(field_id column_id, AnnInfo info) {
  if (auto it = _ann_by_id.find(info.index_id); it != _ann_by_id.end()) {
    auto& existing = *it->second;
    SDB_ASSERT(existing.info == info && existing.column_id == column_id,
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
#ifndef IRESEARCH_NO_VECTOR
    case AnnKind::Ivf:
      entry->writer = std::make_unique<IvfWriter>(info);
      break;
    case AnnKind::Hnsw:
      entry->writer = std::make_unique<HnswWriter>(info, _hnsw_bases);
      break;
#endif
    default:
      SDB_UNREACHABLE();
  }
  entry->info = std::move(info);
  auto& back = *_ann_writers.emplace_back(std::move(entry));
  _ann_by_id.emplace(back.info.index_id, &back);
  return *back.writer;
}

bool ColWriter::HasAnn(field_id column_id) const noexcept {
  return absl::c_any_of(_ann_writers, [&](const auto& entry) {
    return entry->column_id == column_id;
  });
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

void ColWriter::Rollback() noexcept {
  _reader.reset();
  _out.reset();
}

std::vector<StagedColumn> ColWriter::Snapshot(uint64_t rows) const {
  std::vector<StagedColumn> columns;
  columns.reserve(_columns.size());
  for (const auto& cw : _columns) {
    columns.push_back(StagedColumn{.id = cw->Id(),
                                   .type = cw->Type(),
                                   .chunks = cw->Snapshot(rows),
                                   .distinct = cw->_distinct});
  }
  return columns;
}

size_t ColWriter::StagedBytes() const noexcept {
  size_t bytes = 0;
  for (const auto& cw : _columns) {
    bytes += cw->StagedBytes();
  }
  return bytes;
}

constexpr auto kNoCancel = [] { return true; };

bool ColWriter::Commit(uint64_t target_row) {
  return Commit(target_row, kNoCancel);
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

bool ColWriter::Commit(uint64_t target_row,
                       absl::FunctionRef<bool()> progress) {
  if (_committed) {
    return true;
  }
  if (Empty() && !_out) {
    _reader = std::make_unique<ColReader>(*_db, nullptr, ColFooter{});
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
  auto row_groups =
    _row_group_starts.empty() ? size_t{0} : _row_group_starts.size() - 1;
  SDB_IF_FAILURE("irs::ColWriterOmitsRowGroupStarts") { row_groups = 0; }
  ColFooter footer;
  footer.block_size = _write_ctx->GetBlockAllocSize();
  footer.row_group_starts.assign(_row_group_starts.begin(),
                                 _row_group_starts.begin() + row_groups);
  footer.columns.reserve(_columns.size());
  for (auto& cw : _columns) {
    footer.columns.push_back(cw->TakeMeta());
  }
  format_utils::WriteFooter(*_out, [&](duckdb::BinarySerializer& out) {
    out.WriteOptionalList(
      kColFieldColumns, "columns", footer.columns.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          SerializeColumnMeta(obj, footer.columns[i]);
        });
      });
    out.WritePropertyWithDefault<uint64_t>(kColFieldBlockSize, "block_size",
                                           footer.block_size,
                                           DEFAULT_BLOCK_ALLOC_SIZE);
    out.WriteOptionalList(
      kColFieldRowGroupStarts, "row_group_starts",
      footer.row_group_starts.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        list.WriteElement(footer.row_group_starts[i]);
      });
  });
  _tracker.Written(_out->Position());
  auto file = format_utils::CloseOutput(*_out);
  _out.reset();
  _reader =
    std::make_unique<ColReader>(*_db, std::move(file), std::move(footer));
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
    const auto* col = reader.Column(entry->column_id);
    if (!col) {
      continue;
    }
    co_await entry->writer->Compute(*col, ctx, env);
  }
  co_return true;
}

}  // namespace irs
