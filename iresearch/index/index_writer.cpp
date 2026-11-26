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

#include "index_writer.hpp"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iterator>
#include <span>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "iresearch/index/file_names.hpp"
#include "iresearch/index/merge_writer.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/index/segment_writer.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/group_commit.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

std::vector<DocMap> MergeMaps(CompactionView candidates,
                              const MergeWriter& merger,
                              const SegmentReaderImpl& merged) {
  std::vector<DocMap> maps;
  maps.reserve(candidates.size());
  for (size_t i = 0; i < candidates.size(); ++i) {
    const auto& segment =
      *utils::downCast<SegmentReader>(*candidates[i]).GetImpl();
    maps.push_back({.source = segment.Id(),
                    .target = merged.Id(),
                    .mask = segment.SharedDocsMask(),
                    .rank_end = segment.Meta().live_docs_count,
                    .base = merger[i].remap.base_id});
  }
  return maps;
}

struct SyncFiles {
  duckdb::FileSystem* fs;
  duckdb::vector<std::string> paths;
};

void AddSyncFiles(SyncFiles& out, std::string_view root,
                  const SegmentMeta& meta) {
  for (const auto& name : SegmentFiles(meta.name, meta.kinds)) {
    out.paths.push_back(out.fs->JoinPath(root, name));
  }
}

void SyncBatch(GroupCommit<SyncFiles>::Batch batch) {
  std::vector<SyncFiles> by_fs;
  for (const auto* files : batch) {
    auto it = absl::c_find_if(
      by_fs, [&](const SyncFiles& same) { return same.fs == files->fs; });
    if (it == by_fs.end()) {
      it = by_fs.insert(by_fs.end(), SyncFiles{.fs = files->fs});
    }
    it->paths.insert(it->paths.end(), files->paths.begin(), files->paths.end());
  }
  for (const auto& [fs, paths] : by_fs) {
    fs->SyncFiles(paths);
  }
}

GroupCommit<SyncFiles>& FlushSyncs() {
  static GroupCommit<SyncFiles> syncs;
  return syncs;
}

void CompactFiles(duckdb::FileSystem& fs, std::string_view root,
                  const SegmentMeta& meta) {
  duckdb::vector<std::string> paths;
  for (const auto& name : SegmentFiles(meta.name, meta.kinds)) {
    paths.push_back(fs.JoinPath(root, name));
  }
  try {
    SDB_IF_FAILURE("IndexWriter::CompactOutOfMemory") {
      throw duckdb::OutOfMemoryException("intentional debug error");
    }
    fs.Compact(paths);
  } catch (const duckdb::OutOfMemoryException& e) {
    SDB_DEBUG(IRESEARCH, "Keeping segment '", meta.name,
              "' in blocks: ", duckdb::ErrorData{e}.RawMessage());
  }
}

}  // namespace

IndexWriter::Document::Document(SegmentContext& segment, doc_id_t batch_size)
  : _writer{*segment.writer} {
  SDB_ASSERT(segment.writer != nullptr);
  _doc_id = _writer.begin(batch_size);
  SDB_ASSERT(irs::doc_limits::valid(_doc_id));
}

IndexWriter::Document::~Document() noexcept { Finish(); }

void IndexWriter::Document::Finish() noexcept {
  try {
    _writer.commit();
  } catch (...) {
    _writer.rollback();
  }
}

std::span<const IndexWriter::FlushedSegment>
IndexWriter::Transaction::FlushSegments(bool sync) {
  if (!_segment) {
    return {};
  }
  Flush();
  const auto flushed = std::span{_segment->flushed};
  if (!sync) {
    return flushed;
  }
  SyncFiles files{.fs = &_writer->_fs};
  for (const auto& entry : flushed) {
    AddSyncFiles(files, _writer->_root, entry.meta);
  }
  duckdb::TaskScheduler::BlockingScope blocking;
  FlushSyncs().Submit(files, SyncBatch);
  return flushed;
}

std::shared_ptr<const SegmentReaderImpl> IndexWriter::Transaction::SealPiece(
  uint64_t id) {
  SDB_ASSERT(_segment && _hot);
  _segment->writer->AdmitFlush();
  return _segment->writer->SealPiece(id);
}

void IndexWriter::Transaction::Abort() noexcept {
  if (!_segment) {
    return;
  }
  _segment->Reset();
  _segment->writer->ReturnMemory();
  _segment.reset();
}

void IndexWriter::Transaction::Flush() {
  if (!_segment) {
    return;
  }
  _segment->writer->AdmitFlush();
  absl::Cleanup reset = [&] { _segment->Reset(); };
  _segment->Flush();
  std::move(reset).Cancel();
}

void IndexWriter::Transaction::UpdateSegment(doc_id_t batch_size) {
  SDB_ASSERT(Valid());
  if (!_segment) {
    _segment = _writer->GetSegmentContext();
    _segment->writer->BindMemory(*_memory);
  }
  auto& writer = *_segment->writer;
  if (writer.initialized()) [[likely]] {
    if (_hot) {
      return;
    }
    const bool options_match =
      CompatibleFieldOptions(writer.ActiveFieldOptions(), _field_options.get());
    if (options_match && !_writer->FlushRequired(writer, batch_size)) {
      writer.SetFieldOptions(_field_options);
      return;
    }
    SDB_TRACE(IRESEARCH, "Flushing segment '", writer.name(),
              "', docs=", writer.buffered_docs(),
              ", memory=", writer.memory_active(),
              ", docs limit=", _writer->_segment_limits.Docs(),
              ", memory limit=", _writer->_segment_limits.Memory(),
              ", options_match=", options_match);
    Flush();
  }
  writer.SetFieldOptions(_field_options);
  writer.SetDeferredColumns(_hot);
  _segment->Prepare(_hot ? _hot_segment : _writer->NextSegmentId());
}

void IndexWriter::Transaction::SetHot(uint64_t segment_id) noexcept {
  SDB_ASSERT(!_segment);
  _hot = true;
  _hot_segment = segment_id;
  _writer->ReserveSegmentId(segment_id);
}

bool IndexWriter::FlushRequired(const SegmentWriter& segment,
                                doc_id_t batch_size) const noexcept {
  return _segment_limits.Memory() <= segment.memory_active() ||
         _segment_limits.Docs() < segment.buffered_docs() + batch_size;
}

IndexWriter::SegmentContext::SegmentContext(IndexWriter& owner,
                                            const SegmentWriterOptions& options)
  : owner{owner},
    writer{SegmentWriter::make(owner._db, owner._fs, owner._root, options)} {}

IndexWriter::SegmentContext::ptr IndexWriter::SegmentContext::make(
  IndexWriter& owner, const SegmentWriterOptions& options) {
  return std::make_unique<SegmentContext>(owner, options);
}

void IndexWriter::SegmentContext::Flush() {
  if (!writer->initialized()) {
    return;
  }
  if (writer->buffered_docs() == 0) {
    writer->reset();
    return;
  }
  SDB_ASSERT(writer->buffered_docs() <= doc_limits::eof());

  Finally reset_writer = [&]() noexcept { writer->reset(); };

  DocumentMaskBuilder docs_mask;
  SDB_IF_FAILURE("IndexWriter::Flush") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  auto written = writer->flush(writer_meta, docs_mask);
  if (writer_meta.live_docs_count == 0) {
    writer_meta = {};
    return;
  }
  SDB_ASSERT(writer_meta.kinds != 0);
  SDB_ASSERT(writer_meta.live_docs_count <= writer_meta.docs_count);
  auto reader = owner.OpenWritten(writer_meta, std::move(written));
  flushed.emplace_back(std::move(writer_meta), std::move(docs_mask),
                       std::move(reader));
}

void IndexWriter::SegmentContext::Prepare(uint64_t segment_id) {
  SDB_ASSERT(!writer->initialized());
  writer_meta = {};
  writer_meta.name = FileName(segment_id);
  writer->reset(writer_meta);
}

void IndexWriter::SegmentContext::Reset() noexcept {
  flushed.clear();
  if (writer->initialized()) {
    writer->reset();
  }
}

IndexWriter::IndexWriter(ConstructToken, duckdb::DatabaseInstance& db,
                         duckdb::FileSystem& fs, std::string_view root,
                         size_t segment_pool_size,
                         const SegmentOptions& segment_limits,
                         const IndexReaderOptions& reader_options)
  : _topk_scorer{reader_options.scorer},
    _db{db},
    _fs{fs},
    _root{root},
    _reader_options{reader_options},
    _segment_limits{segment_limits},
    _segment_writer_pool{segment_pool_size} {}

IndexWriter::ptr IndexWriter::Make(duckdb::DatabaseInstance& db,
                                   duckdb::FileSystem& fs,
                                   std::string_view root,
                                   IndexWriterOptions options) {
  std::unique_ptr<HnswBases> hnsw_bases;
  if (!options.reader_options.hnsw_bases) {
    hnsw_bases = std::make_unique<HnswBases>();
    options.reader_options.hnsw_bases = hnsw_bases.get();
  }
  auto writer = std::make_shared<IndexWriter>(
    ConstructToken{}, db, fs, root, options.segment_pool_size,
    SegmentOptions{options}, options.reader_options);
  writer->_ann_env = options.ann_env;
  writer->_hnsw_bases = std::move(hnsw_bases);
  writer->_released = std::move(options.released);
  writer->_pack = options.pack;
  writer->_seg_counter = options.segment_ids
                           ? std::move(options.segment_ids)
                           : std::make_shared<std::atomic_uint64_t>(0);
  if (options.column_options || options.norm_column_id) {
    writer->_field_options = std::make_shared<const FunctionFieldOptions>(
      std::move(options.column_options), std::move(options.norm_column_id),
      options.row_group_size, std::move(options.anns));
  }
  return writer;
}

uint64_t IndexWriter::NextSegmentId() noexcept {
  return _seg_counter->fetch_add(1, std::memory_order_relaxed) + 1;
}

uint64_t IndexWriter::CurrentSegmentId() const noexcept {
  return _seg_counter->load(std::memory_order_relaxed);
}

void ReserveSegmentId(std::atomic_uint64_t& ids, uint64_t id) noexcept {
  for (auto current = ids.load(std::memory_order_relaxed);
       current < id &&
       !ids.compare_exchange_weak(current, id, std::memory_order_relaxed);) {
  }
}

void IndexWriter::ReserveSegmentId(uint64_t id) noexcept {
  irs::ReserveSegmentId(*_seg_counter, id);
}

void IndexWriter::MergeClaim::Release() noexcept {
  if (writer == nullptr) {
    return;
  }
  absl::MutexLock lock{&writer->_compacting.lock};
  for (const auto* candidate : candidates) {
    writer->_compacting.segments.erase(candidate->Meta().name);
  }
  writer = nullptr;
  candidates.clear();
}

auto IndexWriter::MergeAsync(const CompactionPolicy& policy,
                             CompositeReader source,
                             const IndexFieldOptions* field_options,
                             const MergeWriter::FlushProgress& progress,
                             const AnnBuildEnv* env) -> yaclib::Future<Merged> {
  Merged merged;
  merged.source = std::move(source);
  const auto& reader = merged.source;
  {
    absl::MutexLock lock{&_compacting.lock};
    CompactingSegments unavailable = _compacting.segments;
    const auto floor = _compacting.floor;
    if (floor != 0) {
      for (const auto& segment : reader) {
        uint64_t id = 0;
        const auto& name = segment.Meta().name;
        if (ParseSegmentId(name, id) && id <= floor) {
          unavailable.insert(name);
        }
      }
    }
    Compaction candidates;
    policy(candidates, reader, unavailable);
    if (floor != 0) {
      std::erase_if(candidates, [floor](const SubReader* candidate) {
        uint64_t id = 0;
        return ParseSegmentId(candidate->Meta().name, id) && id <= floor;
      });
    }
    uint64_t docs = 0;
    candidates.erase(absl::c_find_if(candidates,
                                     [&](const SubReader* candidate) {
                                       docs += candidate->live_docs_count();
                                       return docs > doc_limits::kMaxDocs;
                                     }),
                     candidates.end());
    if (candidates.empty() ||
        (candidates.size() == 1 && !HasRemovals(candidates.front()->Meta()) &&
         candidates.front()->PatchedDocs() == 0)) {
      co_return merged;
    }
    for (const auto* candidate : candidates) {
      if (_compacting.segments.contains(candidate->Meta().name)) {
        co_return merged;
      }
    }
    for (const auto* candidate : candidates) {
      _compacting.segments.emplace(candidate->Meta().name);
    }
    merged.claim = MergeClaim{*this, std::move(candidates)};
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_irs_compaction_registered");
  co_return co_await FlushMerge(std::move(merged), NextSegmentId(),
                                field_options, progress, env);
}

auto IndexWriter::WriteAsync(CompositeReader source, uint64_t id,
                             const IndexFieldOptions* field_options,
                             const MergeWriter::FlushProgress& progress,
                             const AnnBuildEnv* env) -> yaclib::Future<Merged> {
  Merged merged;
  merged.source = std::move(source);
  Compaction candidates;
  for (const auto& segment : merged.source) {
    candidates.emplace_back(&segment);
  }
  if (candidates.empty()) {
    co_return merged;
  }
  merged.claim.candidates = std::move(candidates);
  co_return co_await FlushMerge(std::move(merged), id, field_options, progress,
                                env);
}

IndexWriter::Merged IndexWriter::MergePieces(
  CompositeReader source, uint64_t id, const IndexFieldOptions* field_options) {
  Merged merged;
  merged.source = std::move(source);
  auto& candidates = merged.claim.candidates;
  for (const auto& segment : merged.source) {
    candidates.emplace_back(&segment);
  }
  if (candidates.empty()) {
    return merged;
  }
  SegmentMeta meta;
  meta.name = FileName(id);
  LocalBudget budget{gRootBudget};
  MergeWriter merger{_db, _fs, _root,
                     GetSegmentWriterOptions(true, field_options), budget};
  merger.Reset(candidates.begin(), candidates.end());
  merged.reader = merger.FlushPiece(meta, id);
  if (merged.reader) {
    merged.maps = MergeMaps(candidates, merger, *merged.reader);
  }
  return merged;
}

auto IndexWriter::FlushMerge(Merged merged, uint64_t id,
                             const IndexFieldOptions* field_options,
                             const MergeWriter::FlushProgress& progress,
                             const AnnBuildEnv* env) -> yaclib::Future<Merged> {
  const auto& candidates = merged.claim.candidates;
  SegmentMeta meta;
  meta.name = FileName(id);
  LocalBudget budget{gRootBudget};
  MergeWriter merger{_db, _fs, _root,
                     GetSegmentWriterOptions(true, field_options), budget};
  merger.Reset(candidates.begin(), candidates.end());
  auto files = co_await merger.Flush(meta, progress, env);
  if (!files) {
    co_return merged;
  }
  merged.reader = OpenWritten(
    meta, {.files = std::move(files), .inputs = merger.TakeInputs()});
  merged.maps = MergeMaps(candidates, merger, *merged.reader);
  SyncFiles sync{.fs = &_fs};
  AddSyncFiles(sync, _root, meta);
  _fs.SyncFiles(sync.paths);
  merged.meta = std::move(meta);
  co_return merged;
}

std::shared_ptr<const SegmentReaderImpl> IndexWriter::OpenWritten(
  const SegmentMeta& meta, WrittenSegment written) {
  if (!_pack) {
    return SegmentReaderImpl::Open(_db, _fs, _root, meta, _reader_options,
                                   std::move(written.files),
                                   std::move(written.inputs));
  }
  written.inputs = {};
  CompactFiles(_fs, _root, meta);
  return SegmentReaderImpl::Open(_db, _fs, _root, meta, _reader_options,
                                 std::move(written.files));
}

void IndexWriter::CompactionFloorGuard::Release() noexcept {
  if (_writer == nullptr) {
    return;
  }
  absl::MutexLock lock{&_writer->_compacting.lock};
  _writer->_compacting.floor = 0;
  _writer = nullptr;
}

bool IndexWriter::CompactionFloorGuard::Arm() noexcept {
  SDB_ASSERT(_writer);
  SDB_ASSERT(_floor == kPendingFloor);
  absl::MutexLock lock{&_writer->_compacting.lock};
  if (!_writer->_compacting.segments.empty()) {
    // A compaction is between selecting candidates and minting its output id,
    // so it could still stamp a fresh id on segments below the floor we are
    // about to set. The caller should retry once it finishes.
    return false;
  }
  _floor = _writer->CurrentSegmentId();
  _writer->_compacting.floor = _floor;
  return true;
}

IndexWriter::CompactionFloorGuard IndexWriter::ClaimCompactionFloor() {
  absl::MutexLock lock{&_compacting.lock};
  SDB_ASSERT(_compacting.floor == 0);
  _compacting.floor = kPendingFloor;
  return {*this, kPendingFloor};
}

bool IndexWriter::Compacting() {
  absl::MutexLock lock{&_compacting.lock};
  return !_compacting.segments.empty();
}

std::shared_ptr<IndexWriter::SegmentContext> IndexWriter::GetSegmentContext() {
  const auto options =
    GetSegmentWriterOptions(false, /*field_options=*/nullptr);
  std::shared_ptr<SegmentContext> segment =
    _segment_writer_pool.emplace(*this, options);
  if (_segment_limits.Memory() < segment->writer->memory_reserved()) {
    segment->writer = SegmentWriter::make(_db, _fs, _root, options);
  }
  return segment;
}

SegmentWriterOptions IndexWriter::GetSegmentWriterOptions(
  bool compaction, const IndexFieldOptions* field_options) const noexcept {
  // The merge passes its own view; a segment write installs its owning override
  // later via SetFieldOptions, so non-owning here.
  return {
    .scorer = _topk_scorer,
    .field_options = field_options ? field_options : _field_options.get(),
    .ann_env = compaction ? nullptr : _ann_env,
    .hnsw_bases = _reader_options.hnsw_bases,
    .released = _released,
  };
}

}  // namespace irs
