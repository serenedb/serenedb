////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/index/segment_writer.hpp"

#include <absl/algorithm/container.h>

#include <duckdb/storage/buffer_manager.hpp>

#include "index_meta.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/hot_column_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/inverter/columnar_flush.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/async.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

duckdb::DatabaseInstance& DerefDb(duckdb::DatabaseInstance* db) {
  SDB_ASSERT(db != nullptr);
  return *db;
}

}  // namespace

doc_id_t SegmentWriter::begin(doc_id_t batch_size) {
  SDB_ASSERT(batch_size > 0);
  SDB_ENSURE(buffered_docs() + batch_size <= doc_limits::kMaxDocs,
             "a segment of more than ", doc_limits::kMaxDocs, " documents");
  _valid = true;

  _batch_first_doc_id = LastDocId() + 1;
  _docs += batch_size;

  return _batch_first_doc_id;
}

std::unique_ptr<SegmentWriter> SegmentWriter::make(
  Directory& dir, const SegmentWriterOptions& options) {
  return std::make_unique<SegmentWriter>(ConstructToken{}, dir, options);
}

size_t SegmentWriter::memory_active() const noexcept {
  return _docs_mask.ByteSize() + _fields.MemoryActive();
}

size_t SegmentWriter::memory_reserved() const noexcept {
  return sizeof(SegmentWriter) + _docs_mask.ByteCapacity() +
         _fields.MemoryReserved();
}

bool SegmentWriter::remove(doc_id_t doc_id) noexcept {
  if (!doc_limits::valid(doc_id)) {
    return false;
  }
  if (buffered_docs() <= doc_id - doc_limits::min()) {
    return false;
  }
  return _docs_mask.Add(doc_id);
}

SegmentWriter::SegmentWriter(ConstructToken, Directory& dir,
                             const SegmentWriterOptions& options) noexcept
  : _dir{dir},
    _scorer{options.scorer},
    _fields{InverterMemory{
      duckdb::BufferManager::GetBufferManager(DerefDb(options.db))
        .GetBufferAllocator(),
      options.resource_manager}},
    _db{DerefDb(options.db)},
    _fallback_field_options{options.field_options},
    _ann_env{options.ann_env},
    _hnsw_bases{options.hnsw_bases} {}

void SegmentWriter::FlushFields(FlushState& state,
                                std::span<const BasicTermReader* const> extra) {
  SDB_ASSERT(_field_writer);

  try {
    _fields.Flush(*_field_writer, state, extra);
  } catch (...) {
    _field_writer.reset();
    throw;
  }
}

void SegmentWriter::flush(SegmentMeta& meta, DocumentMaskBuilder& docs_mask) {
  FlushState state{
    .dir = &_dir,
    .norms = this,
    .name = _seg_name,
    .scorer = _scorer,
    .doc_count = buffered_docs(),
  };

  IdxWriter idx{_dir, _seg_name};

  std::vector<std::unique_ptr<AnnWriter>> ann_writers;
  if (_col_writer) {
    _col_writer->SetIdxWriter(idx);
    _fields.FinalizeNorms();
    CopyColumns();
    _col_writer->Commit(buffered_docs());
    if (_ann_env != nullptr) {
      GetBlocking(_col_writer->ComputeAnn(_ann_env).ToFuture());
    } else {
      GetReady(_col_writer->ComputeAnn(/*env=*/nullptr).ToFuture());
    }
    ann_writers = _col_writer->TakeAnnWriters();
    _col_writer.reset();
  }

  if (state.doc_count != 0) {
    _col_reader =
      std::make_unique<ColReader>(_dir, _seg_name, _db, IOAdvice::SEQUENTIAL);
    _field_writer->SetIdxWriter(idx);
    std::optional<ReadContext> ivf_ctx;
    const auto cluster_readers =
      PrepareIvfClusterReaders(ann_writers, _col_reader.get(), ivf_ctx);
    FlushFields(state, cluster_readers);
  }

  for (const auto& w : ann_writers) {
    if (w) {
      w->Flush();
    }
  }

  _col_reader.reset();

  idx.Commit();

  docs_mask = std::move(_docs_mask);

  meta.docs_count = state.doc_count;
  meta.live_docs_count =
    meta.docs_count - static_cast<doc_id_t>(docs_mask.Count());
  meta.kinds = SegmentFileKinds(_dir.FlushTracked(meta.byte_size));
}

void SegmentWriter::CopyRows(const ColReader& source,
                             std::span<const field_id> columns, uint64_t first,
                             uint64_t count) {
  SDB_ASSERT(_copied_rows.empty() || absl::c_equal(_copied_columns, columns));
  if (_copied_rows.empty()) {
    _copied_columns.assign(columns.begin(), columns.end());
  } else if (auto& last = _copied_rows.back();
             last.source == &source && last.first + last.count == first) {
    last.count += count;
    return;
  }
  _copied_rows.push_back({.source = &source, .first = first, .count = count});
}

void SegmentWriter::CopyColumns() {
  if (_copied_rows.empty()) {
    return;
  }
  SDB_ASSERT(absl::c_accumulate(_copied_rows, uint64_t{0},
                                [](uint64_t rows, const CopiedRows& run) {
                                  return rows + run.count;
                                }) == buffered_docs());
  for (const auto id : _copied_columns) {
    ColumnWriter* writer = nullptr;
    uint64_t row = 0;
    for (const auto& run : _copied_rows) {
      const auto* column = run.source->Column(id);
      if (column && run.first < column->RowCount()) {
        if (!writer) {
          writer = &_col_writer->OpenColumn(id, column->Type());
        }
        ReadContext context{*run.source};
        writer->CopyRows(row, *column, context, run.first,
                         std::min(run.count, column->RowCount() - run.first));
      }
      row += run.count;
    }
    if (writer) {
      writer->PadNullsTo(row);
    }
  }
}

std::shared_ptr<const SegmentReaderImpl> SegmentWriter::SealPiece(uint64_t id) {
  const auto docs = buffered_docs();
  SDB_ASSERT(docs != 0);
  SDB_ASSERT(_copied_rows.empty());
  std::vector<std::unique_ptr<ColumnReader>> columns;
  std::vector<NormColumnSpec> norms;
  size_t column_bytes = 0;
  if (_col_writer) {
    _fields.FinalizeNorms();
    norms = _col_writer->SealNorms(docs);
    column_bytes = _col_writer->StagedBytes();
    HotChunkStats stats;
    for (auto& column : _col_writer->Snapshot(docs)) {
      columns.push_back(HotColumnReader::Make(_db, std::move(column), &stats));
    }
  }
  auto fields = std::make_shared<PieceFields>();
  FlushState state{.dir = &_dir, .name = _seg_name, .doc_count = docs};
  PieceFieldWriter field_writer{*fields, /*compaction=*/false};
  _fields.Flush(field_writer, state, {});
  SegmentMeta meta;
  meta.name = _seg_name;
  meta.docs_count = static_cast<doc_id_t>(docs);
  if (!_docs_mask.Empty()) {
    meta.docs_mask = std::make_shared<const DocumentMask>(
      DocumentMaskBuilder{_docs_mask}.Finish());
  }
  meta.live_docs_count = meta.docs_count - RemovalCount(meta);
  const auto files = _dir.FlushTracked(meta.byte_size);
  meta.byte_size += column_bytes + fields->Bytes();
  fields->Open(ReaderState{.dir = &_dir, .meta = &meta},
               field_writer.Features());
  IndexInput::ptr norms_in;
  if (!norms.empty()) {
    norms_in = _dir.open(FileName(_seg_name), IOAdvice::RANDOM);
  }
  for (const auto& file : files) {
    _dir.remove(file);
  }
  return SegmentReaderImpl::OpenPiece(
    meta, id,
    std::make_unique<ColReader>(_db, std::move(columns), std::move(norms_in),
                                std::move(norms)),
    std::move(fields));
}

void SegmentWriter::SetFieldOptions(
  std::shared_ptr<const IndexFieldOptions> options) noexcept {
  if (!options) {
    return;
  }
  // On resume the col/field writers hold a raw view of the previous (equal)
  // options; re-point them before the assignment drops its last owner.
  const auto* next = options.get();
  if (_initialized) {
    if (_col_writer) {
      _col_writer->SetFieldOptions(next);
    }
    _fields.SetFieldOptions(next);
  }
  _field_options = std::move(options);
}

void SegmentWriter::ResetState() noexcept {
  _initialized = false;
  _docs = 0;
  _docs_mask.Clear();
  _batch_first_doc_id = doc_limits::eof();
  _fields.Reset();
  _col_reader.reset();
  if (_col_writer) {
    _col_writer->Rollback();
    _col_writer.reset();
  }
  _copied_columns.clear();
  _copied_rows.clear();
  _dir.RemoveTracked();
}

void SegmentWriter::reset() noexcept {
  ResetState();
  // Release the override so a pooled writer never pins a snapshot's index past
  // the operation. (A drop waits on the storage refcount, so a still-pinned
  // index only defers the drop to the next recycle, never dangles.)
  _field_options.reset();
}

void SegmentWriter::reset(const SegmentMeta& meta) {
  // Keep _field_options: the caller set it for this segment; the col/field
  // writers below open against ActiveFieldOptions().
  ResetState();

  _seg_name = meta.name;

  if (!_field_writer) {
    _field_writer =
      std::make_unique<term_dict::FieldWriter>(/*compaction=*/false);
  }

  const auto* active = ActiveFieldOptions();
  _col_writer = std::make_unique<ColWriter>(_dir, meta.name, _db);
  _col_writer->SetDeferredSeal(_deferred_columns);
  _col_writer->SetFieldOptions(active);
  _col_writer->SetHnswBases(_hnsw_bases);
  _fields.SetColWriter(_col_writer.get());
  _fields.SetFieldOptions(active);

  _initialized = true;
}

}  // namespace irs
