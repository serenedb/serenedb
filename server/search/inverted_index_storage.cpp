////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "search/inverted_index_storage.h"

#include <absl/cleanup/cleanup.h>
#include <absl/time/time.h>

#include <chrono>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <filesystem>
#include <iresearch/formats/index_meta_reader.hpp>
#include <iresearch/formats/index_meta_writer.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/merge_order.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/fs_directory.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serializer.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <memory>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "connector/term_dict.h"
#include "query/transaction.h"
#include "search/scorer_options.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

InvertedIndexStorage::InvertedIndexStorage(
  std::shared_ptr<catalog::DatabaseDirectory> directory, bool in_memory,
  bool read_only, duckdb::idx_t db_id, duckdb::idx_t index_id,
  const catalog::InvertedIndexSettings& options,
  const std::optional<irs::ScorerOptions>& top_k_scorer,
  const irs::SortSpec& sort, bool is_new)
  : MaintainedStore{index_id},
    _index_id{index_id},
    _db_id{db_id},
    _directory{std::move(directory)},
    _in_memory{in_memory},
    _read_only{read_only},
    _is_new{is_new},
    _segment_memory_max{options.segment_memory_max},
    _segment_docs_max{options.segment_docs_max},
    _sort{sort},
    _search{GetSearchEngine()},
    _durability{db_id, absl::StrCat("inverted index ", index_id)} {
  SetTasksSettings(options);
  if (!sort.Empty()) {
    _tasks_settings.merge_inputs_max = irs::kSortedMergeInputsMax;
  }

  SDB_ASSERT(index_id != 0);

  if (const auto& scorer_options = top_k_scorer) {
    _topk_scorer = MakeScorer(*scorer_options);
  }
  if (is_new) {
    Open();
  }
}

void InvertedIndexStorage::Open() {
  _delete_log_open = _is_new;
  irs::ResourceManagementOptions resource_manager;
  resource_manager.transactions = _writers_memory;
  resource_manager.readers = _readers_memory;
  resource_manager.compactions = _compactions_memory;
  resource_manager.file_descriptors = _file_descriptors_count;
  auto opened = OpenStorageDirectory(*_directory, _index_id, _is_new,
                                     _in_memory, resource_manager);
  _dir = std::move(opened.directory);
  _absent = opened.absent;

  if (_read_only) {
    return;
  }

  irs::IndexWriterOptions writer_options;
  writer_options.ann_env = &AnnBuildEnv();
  writer_options.segment_memory_max = _segment_memory_max;
  writer_options.segment_docs_max = _segment_docs_max;
  writer_options.on_flushed =
    [id = GetId()](std::span<const irs::SubReader* const> segments) {
      MergeRegistry::Instance().RecordFlushes(id, segments);
    };
#ifdef SDB_DEV
  // Dev safety net: a second IndexWriter opening the same index directory (a
  // lifecycle bug -- two storages/loops on one dir) fails to acquire the lock
  // with a clean error instead of silently corrupting segment files.
  writer_options.lock_repository = true;
#else
  writer_options.lock_repository = false;  // single-process server owns the dir
#endif
  writer_options.sort = _sort;
  writer_options.reader_options = ReaderOptions();
  writer_options.db = writer_options.reader_options.db;
  writer_options.key_field = connector::term_dict::kPKFieldId;
  // No column/norm options are configured on the writer: the per-column
  // encoding config travels with each operation instead. A write hands its own
  // DDL snapshot's InvertedIndex (SegmentWriter::SetFieldOptions, via the
  // serenedb transaction) and a merge hands the compaction task's snapshot
  // index (CompactUnsafe). The long-lived writer therefore never reaches into
  // the live catalog, so a concurrent DROP can no longer dangle it.

  SDB_IF_FAILURE("segment_1000_docs_max") {
    writer_options.segment_docs_max = 1000;
  }

  auto [writer, stamp] = _durability.OpenWriter(
    *_dir, Path(), opened.on_disk, _is_new, std::move(writer_options),
    [this] { return GetFileManifest(); });
  _writer = std::move(writer);

  auto reader = _writer->GetSnapshot();
  SDB_ASSERT(reader);

  SetFileManifest(stamp.manifest);
  StoreInvertedIndexSnapshot(std::make_shared<InvertedIndexSnapshot>(
    std::move(reader), std::move(stamp.manifest)));
}

irs::IndexReaderOptions InvertedIndexStorage::ReaderOptions() const {
  irs::IndexReaderOptions options;
  options.db = &irs::DuckDBEngine::Instance().instance();
  options.scorer = _topk_scorer.get();
  return options;
}

void InvertedIndexStorage::OpenReader() {
  auto [reader, stamp] = IndexDurability::OpenReader(*_dir, ReaderOptions());
  SetFileManifest(stamp.manifest);
  StoreInvertedIndexSnapshot(std::make_shared<InvertedIndexSnapshot>(
    std::move(reader), std::move(stamp.manifest)));
}

StorageDirectory OpenStorageDirectory(
  const catalog::DatabaseDirectory& database, duckdb::idx_t oid, bool is_new,
  bool in_memory, const irs::ResourceManagementOptions& resources) {
  std::optional<std::filesystem::path> path;
  if (!in_memory) {
    path = is_new ? database.CreateStorage(oid) : database.OpenStorage(oid);
  }
  if (!path) {
    return {.directory = std::make_unique<irs::MemoryDirectory>(
              irs::DirectoryAttributes{}, resources),
            .absent = !in_memory};
  }
  return {.directory = std::make_unique<irs::MMapDirectory>(
            *path, irs::DirectoryAttributes{}, resources),
          .on_disk = true};
}

InvertedIndexStorage::~InvertedIndexStorage() {
  _writer.reset();
  _dir.reset();
  if (_dropped.load(std::memory_order_acquire)) {
    catalog::DatabaseDirectory::RemoveStorage(std::move(_directory), _index_id);
  }
}

void InvertedIndexStorage::ApplyOptions(
  const catalog::InvertedIndexSettings& options) {
  SetTasksSettings(options);
  _segment_memory_max = options.segment_memory_max;
  _segment_docs_max = options.segment_docs_max;
  if (!_writer) {
    return;
  }

  irs::SegmentOptions segment_options;
  segment_options.segment_count_max = 0;
  segment_options.segment_memory_max = options.segment_memory_max;
  segment_options.segment_docs_max = options.segment_docs_max;
  _writer->Options(segment_options);
}

auto InvertedIndexStorage::ReindexClaim::TryAcquire(
  InvertedIndexStorage& storage) -> ReindexClaim {
  absl::MutexLock lock{&storage._reindex_mutex};
  if (storage._reindex_in_flight || storage._reindex_waiters != 0) {
    return ReindexClaim{nullptr};
  }
  storage._reindex_in_flight = true;
  return ReindexClaim{&storage};
}

auto InvertedIndexStorage::ReindexClaim::Acquire(
  InvertedIndexStorage& storage, absl::FunctionRef<bool()> cancelled,
  absl::Duration poll) -> ReindexClaim {
  absl::MutexLock lock{&storage._reindex_mutex};
  ++storage._reindex_waiters;
  absl::Cleanup leave = [&]() ABSL_NO_THREAD_SAFETY_ANALYSIS noexcept {
    --storage._reindex_waiters;
  };
  while (storage._reindex_in_flight) {
    if (storage._reindex_cv.WaitWithTimeout(&storage._reindex_mutex, poll) &&
        cancelled()) {
      return ReindexClaim{nullptr};
    }
  }
  storage._reindex_in_flight = true;
  return ReindexClaim{&storage};
}

InvertedIndexStorage::ReindexClaim::~ReindexClaim() {
  if (!_storage) {
    return;
  }
  absl::MutexLock lock{&_storage->_reindex_mutex};
  _storage->_reindex_in_flight = false;
  _storage->_reindex_cv.SignalAll();
}

absl::Status InvertedIndexStorage::Refresh(
  const irs::ProgressReportCallback& progress) {
  RefreshResult code = RefreshResult::Undefined;
  auto res = RefreshUnsafe(/*wait=*/true, progress, code).res;
  if (code == RefreshResult::Done) {
    NudgeCompaction();
  }
  return res;
}

StoreStats InvertedIndexStorage::UpdateStatsUnsafe(
  InvertedIndexSnapshotPtr inverted_index_snapshot) const {
  auto stats = StoreStats::FromReader(inverted_index_snapshot->reader);
  stats.numBufferedDocs = _writer ? _writer->BufferedDocs() : 0;
  _maintenance.Fill(stats);
  return stats;
}

CleanupResult InvertedIndexStorage::CleanupUnsafe() {
  auto begin = std::chrono::steady_clock::now();
  auto removed = _durability.Cleanup(*_dir);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  _maintenance.RecordCleanup(removed.status(), time_ms);
  if (!removed.ok()) {
    return {.res = std::move(removed).status(), .time_ms = time_ms};
  }
  return {.time_ms = time_ms, .removed = *removed};
}

auto InvertedIndexStorage::CompactUnsafeAsync(
  const irs::CompactionPolicy& policy, const irs::MergeProgress& progress,
  bool& empty_compaction, const irs::IndexFieldOptions* field_options,
  const irs::AnnBuildEnv* env, MergeOrigin origin,
  irs::CompactionResult* outcome, irs::MergeExecutor* executor,
  irs::LoneSegment lone) -> yaclib::Future<ResultWithTime> {
  MergeRecorder recorder{GetId(), origin};
  const auto recorded = recorder.Wrap(policy);
  co_return co_await AwaitMerge(recorder, empty_compaction, outcome, [&] {
    return _writer->CompactAsync(recorded, field_options, progress, env,
                                 executor, lone);
  });
}

auto InvertedIndexStorage::RunMergeAsync(
  irs::MergeTicket ticket, const irs::MergeProgress& progress,
  bool& empty_compaction, const irs::IndexFieldOptions* field_options,
  const irs::AnnBuildEnv* env, uint64_t merge_id,
  irs::CompactionResult* outcome, irs::MergeExecutor* executor)
  -> yaclib::Future<ResultWithTime> {
  MergeRecorder recorder{GetId(), MergeOrigin::Background, merge_id};
  recorder.Begin(ticket.Candidates());
  co_return co_await AwaitMerge(recorder, empty_compaction, outcome, [&] {
    return _writer->RunMerge(std::move(ticket), field_options, progress, env,
                             executor);
  });
}

ResultWithTime InvertedIndexStorage::RefreshUnsafe(
  bool wait, const irs::ProgressReportCallback& progress, RefreshResult& code) {
  SDB_ASSERT(_writer);
  auto begin = std::chrono::steady_clock::now();
  auto result = _durability.Refresh(
    *_writer, wait, progress, code,
    [&] {
      StoreInvertedIndexSnapshot(std::make_shared<InvertedIndexSnapshot>(
        _writer->GetSnapshot(), GetFileManifest()));
    },
    [&] { PublishSnapshot(); });
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();

  SDB_IF_FAILURE("Search::FailOnCommit") {
    result = absl::InternalError("debug failure point");
  }
  SDB_IF_FAILURE("Search::CrashAfterCommit") { SDB_IMMEDIATE_ABORT(); }

  _maintenance.RecordCommit(result, code, time_ms);
  return {std::move(result), time_ms};
}

auto InvertedIndexStorage::AwaitMerge(
  MergeRecorder& recorder, bool& empty_compaction,
  irs::CompactionResult* outcome,
  absl::FunctionRef<yaclib::Future<irs::CompactionResult>()> merge)
  -> yaclib::Future<ResultWithTime> {
  const auto begin = std::chrono::steady_clock::now();
  empty_compaction = false;
  auto result = absl::OkStatus();
  try {
    auto res = co_await merge();
    if (!res) {
      SDB_DEBUG(SEARCH, "compaction of Search index '", GetId(),
                "' merged nothing: cancelled, or every document was removed");
      empty_compaction = true;
    } else if (res.error != irs::CompactionError::Busy) {
      empty_compaction = res.size == 0;
    }
    if (outcome) {
      *outcome = res;
    }
    recorder.Finish(std::move(res));
  } catch (const std::exception& e) {
    const auto message = duckdb::ErrorData{e}.RawMessage();
    recorder.Fail(message);
    result = absl::InternalError(
      absl::StrCat("caught exception while executing compaction policy "
                   "on Search index '",
                   GetId(), "': ", message));
  } catch (...) {
    recorder.Fail("unknown exception");
    result = absl::InternalError(
      absl::StrCat("caught exception while executing compaction policy "
                   "on Search index '",
                   GetId(), "'"));
  }
  const uint64_t time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count();
  _maintenance.RecordCompaction(result, empty_compaction, time_ms);
  co_return ResultWithTime{std::move(result), time_ms};
}

void InvertedIndexStorage::PublishSnapshot() {
  auto reader = _writer->GetSnapshot();
  SDB_ASSERT(reader);
  const auto reader_size = reader->size();
  const auto docs_count = reader->docs_count();
  const auto live_docs_count = reader->live_docs_count();

  auto data = std::make_shared<InvertedIndexSnapshot>(std::move(reader),
                                                      GetFileManifest());
  StoreInvertedIndexSnapshot(data);

  UpdateStatsUnsafe(std::move(data));

  SDB_DEBUG(SEARCH, "successful sync of Search index '", GetId(),
            "', segments '", reader_size, "', docs count '", docs_count,
            "', live docs count '", live_docs_count, "', wal position '",
            _durability.Position().generation, ":",
            _durability.Position().offset, "'");
}

void InvertedIndexStorage::PrepareCheckpoint(uint64_t iteration) {
  _durability.PrepareCheckpoint(*_writer, iteration);
}

void InvertedIndexStorage::FinishCheckpoint() {
  if (_durability.FinishCheckpoint(*_writer)) {
    PublishSnapshot();
  }
}

std::vector<int64_t> InvertedIndexStorage::AppendDeleteLog(
  std::vector<int64_t> rows) {
  duckdb::lock_guard<duckdb::mutex> lock{_delete_log_mutex};
  if (!_delete_log_open.load(std::memory_order_relaxed)) {
    return rows;
  }
  const auto begin = _delete_log_rowid_begin.load(std::memory_order_acquire);
  const auto end = _delete_log_rowid_end.load(std::memory_order_relaxed);
  std::vector<int64_t> logged;
  std::vector<int64_t> native;
  for (const auto row : rows) {
    (row < begin || row >= end ? native : logged).push_back(row);
  }
  if (!logged.empty()) {
    _delete_log.emplace_back(std::move(logged));
  }
  return native;
}

std::vector<irs::TermRemoval> InvertedIndexStorage::TakeDeleteLog() {
  duckdb::lock_guard<duckdb::mutex> lock{_delete_log_mutex};
  _delete_log_open.store(false, std::memory_order_release);
  return std::exchange(_delete_log, {});
}

template<typename Remove>
void InvertedIndexStorage::CommitRemovalWith(const Remove& remove) {
  auto trx = GetTransaction();
  remove(trx);
  trx.RegisterFlush();
  if (!trx.Commit()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("inverted index ", _index_id,
                            ": failed to commit a removal: out of memory"));
  }
}

void InvertedIndexStorage::CommitRemoval(
  std::shared_ptr<const irs::DocRemoval> removal) {
  CommitRemovalWith([&](auto& trx) { trx.Remove(std::move(removal)); });
}

void InvertedIndexStorage::CommitRemoval(
  std::vector<irs::TermRemoval> removals) {
  CommitRemovalWith([&](auto& trx) {
    for (auto& removal : removals) {
      trx.Remove(std::move(removal));
    }
  });
}

void InvertedIndexStorage::CommitRemoveAll() {
  CommitRemovalWith([](auto& trx) { trx.RemoveAll(); });
}

void ThrowIfRefreshFailed(const absl::Status& status,
                          std::string_view index_name) {
  if (!status.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("failed to refresh index '", index_name,
                            "': ", status.message()));
  }
}

}  // namespace sdb::search
