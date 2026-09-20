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

#include <duckdb/storage/buffer_manager.hpp>

#include "index_meta.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"
#include "iresearch/index/inverter/columnar_flush.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/async.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

duckdb::DatabaseInstance& DerefDb(duckdb::DatabaseInstance* db) {
  SDB_ASSERT(db != nullptr);
  return *db;
}

}  // namespace

doc_id_t SegmentWriter::begin(uint64_t tick, doc_id_t batch_size) {
  SDB_ASSERT(LastDocId() < doc_limits::eof());
  _valid = true;
  SDB_ASSERT(batch_size > 0);

  _batch_first_doc_id = LastDocId() + 1;
  _docs_context.Append(batch_size, tick);

  return _batch_first_doc_id;
}

std::unique_ptr<SegmentWriter> SegmentWriter::make(
  Directory& dir, const SegmentWriterOptions& options) {
  return std::make_unique<SegmentWriter>(ConstructToken{}, dir, options);
}

size_t SegmentWriter::memory_active() const noexcept {
  return _docs_context.MemoryActive() + _docs_mask.ByteSize() +
         _fields.MemoryActive();
}

size_t SegmentWriter::memory_reserved() const noexcept {
  return sizeof(SegmentWriter) + _docs_context.MemoryReserved() +
         _docs_mask.ByteSize() + _fields.MemoryReserved();
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
    _docs_context{options.resource_manager},
    _fields{InverterMemory{
      duckdb::BufferManager::GetBufferManager(DerefDb(options.db))
        .GetBufferAllocator(),
      options.resource_manager}},
    _db{DerefDb(options.db)},
    _fallback_field_options{options.field_options},
    _ann_env{options.ann_env} {}

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

void SegmentWriter::flush(IndexSegment& segment, DocumentMask& docs_mask) {
  auto& meta = segment.meta;

  FlushState state{
    .dir = &_dir,
    .norms = this,
    .name = _seg_name,
    .scorer = _scorer,
    .doc_count = buffered_docs(),
  };

  IdxWriter idx{_dir, _seg_name, _db};

  std::vector<std::unique_ptr<AnnWriter>> ann_writers;
  if (_col_writer) {
    _col_writer->SetIdxWriter(idx);
    _fields.FinalizeNorms();
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
    _col_reader = std::make_unique<ColReader>(_dir, _seg_name, _db);
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
  meta.files = _dir.FlushTracked(meta.byte_size);
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
  _dir.ClearTracked();
  _docs_context.Clear();
  _docs_mask.Clear();
  _batch_first_doc_id = doc_limits::eof();
  _fields.Reset();
  _col_reader.reset();
  if (_col_writer) {
    _col_writer->Rollback();
    _col_writer.reset();
  }
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
    auto& rm = _docs_context.ResourceManager();
    _field_writer = std::make_unique<burst_trie::FieldWriter>(
      meta.codec->get_postings_writer(/*compaction=*/false, rm),
      /*compaction=*/false, rm);
  }

  const auto* active = ActiveFieldOptions();
  _col_writer = std::make_unique<ColWriter>(_dir, meta.name, _db);
  _col_writer->SetFieldOptions(active);
  _fields.SetColWriter(_col_writer.get());
  _fields.SetFieldOptions(active);

  _initialized = true;
}

}  // namespace irs
