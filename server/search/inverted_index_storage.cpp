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
#include <iresearch/formats/index_meta_reader.hpp>
#include <iresearch/formats/index_meta_writer.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/store/directory.hpp>
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
#include "query/transaction.h"
#include "search/scorer_options.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

InvertedIndexStorage::InvertedIndexStorage(
  std::optional<catalog::StorageDirectory> parent,
  duckdb::AttachedDatabase& database, duckdb::idx_t index_id,
  const catalog::InvertedIndexSettings& options,
  const std::optional<irs::ScorerOptions>& top_k_scorer, bool is_new)
  : _index_id{index_id},
    _db_id{database.oid},
    _parent{std::move(parent)},
    _search{GetSearchEngine()},
    _durability{database, absl::StrCat("inverted index ", index_id), _parent,
                index_id, StorageOpenOf(database, is_new)} {
  SetTasksSettings(options);

  SDB_ASSERT(index_id != 0);

  _delete_log_open = is_new;

  if (const auto& scorer_options = top_k_scorer) {
    _topk_scorer = MakeScorer(*scorer_options);
  }

  if (database.IsReadOnly()) {
    return;
  }
  if (StorageOpenOf(database, is_new) == StorageOpen::Defer) {
    _pending_options = options;
    return;
  }
  OpenWriter(is_new, options);
}

void InvertedIndexStorage::OpenPendingWriter() {
  SDB_ASSERT(_pending_options);
  const auto options = *std::exchange(_pending_options, std::nullopt);
  if (_durability.Locate(*_parent, _index_id)) {
    OpenWriter(false, options);
  }
}

void InvertedIndexStorage::OpenWriter(
  bool is_new, const catalog::InvertedIndexSettings& options) {
  irs::IndexWriterOptions writer_options;
  writer_options.ann_env = &AnnBuildEnv();
  writer_options.segment_memory_max = options.segment_memory_max;
  writer_options.segment_docs_max = options.segment_docs_max;
#ifdef SDB_DEV
  // Dev safety net: a second IndexWriter opening the same index directory (a
  // lifecycle bug -- two storages/loops on one dir) fails to acquire the lock
  // with a clean error instead of silently corrupting segment files.
  writer_options.lock_repository = true;
#else
  writer_options.lock_repository = false;  // single-process server owns the dir
#endif
  writer_options.reader_options = ReaderOptions();
  writer_options.db = writer_options.reader_options.db;
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
    is_new, std::move(writer_options), [this] { return GetFileManifest(); });
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
  auto [reader, stamp] = _durability.OpenReader(ReaderOptions());
  SetFileManifest(stamp.manifest);
  StoreInvertedIndexSnapshot(std::make_shared<InvertedIndexSnapshot>(
    std::move(reader), std::move(stamp.manifest)));
}

InvertedIndexStorage::~InvertedIndexStorage() {
  _writer.reset();
  if (_parent && _dropped.load(std::memory_order_acquire)) {
    _parent->RemoveStorage(_index_id);
  }
}

void InvertedIndexStorage::ApplyOptions(
  const catalog::InvertedIndexSettings& options) {
  SetTasksSettings(options);
  if (_pending_options) {
    *_pending_options = options;
    return;
  }
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
  return RefreshUnsafe(/*wait=*/true, progress, code).res;
}

absl::Status InvertedIndexStorage::OptimizeInMemory() {
  SDB_ASSERT(_writer);
  if (auto status = Refresh(); !status.ok()) {
    return status;
  }
  if (auto status = _durability.CompactFiles(*_writer); !status.ok()) {
    return status;
  }
  return Refresh();
}

StoreStats InvertedIndexStorage::UpdateStatsUnsafe(
  InvertedIndexSnapshotPtr inverted_index_snapshot) const {
  auto stats = StoreStats::FromReader(inverted_index_snapshot->reader);
  stats.numBufferedDocs = _writer ? _writer->BufferedDocs() : 0;
  _maintenance.Fill(stats);
  return stats;
}

ResultWithTime InvertedIndexStorage::CleanupUnsafe() {
  auto begin = std::chrono::steady_clock::now();
  auto result = _durability.Cleanup();
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  _maintenance.RecordCleanup(result, time_ms);
  return {std::move(result), time_ms};
}

auto InvertedIndexStorage::CompactUnsafeAsync(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env)
  -> yaclib::Future<ResultWithTime> {
  auto begin = std::chrono::steady_clock::now();
  auto result = co_await CompactUnsafeImpl(policy, progress, empty_compaction,
                                           field_options, env);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  _maintenance.RecordCompaction(result, empty_compaction, time_ms);
  co_return ResultWithTime{std::move(result), time_ms};
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

auto InvertedIndexStorage::CompactUnsafeImpl(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env)
  -> yaclib::Future<absl::Status> {
  empty_compaction = false;

  try {
    const auto res =
      co_await _writer->CompactAsync(policy, field_options, progress, env);
    if (res.error == irs::CompactionError::Fail) {
      SDB_DEBUG(SEARCH, "compaction of Search index '", GetId(),
                "' merged nothing: cancelled, or every document was removed");
      empty_compaction = true;
      co_return absl::OkStatus();
    }
    if (res.error == irs::CompactionError::Busy) {
      co_return absl::OkStatus();
    }

    empty_compaction = (res.size == 0);
  } catch (const std::exception& e) {
    co_return absl::InternalError(
      absl::StrCat("caught exception while executing compaction policy "
                   "on Search index '",
                   GetId(), "': ", duckdb::ErrorData{e}.RawMessage()));
  } catch (...) {
    co_return absl::InternalError(
      absl::StrCat("caught exception while executing compaction policy "
                   "on Search index '",
                   GetId(), "'"));
  }
  co_return absl::OkStatus();
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

bool InvertedIndexStorage::AppendDeleteLog(std::vector<int64_t>&& rows) {
  duckdb::lock_guard<duckdb::mutex> lock{_delete_log_mutex};
  if (!_delete_log_open.load(std::memory_order_relaxed)) {
    return false;
  }
  _delete_log.push_back(std::move(rows));
  return true;
}

std::vector<int64_t> InvertedIndexStorage::TakeDeleteLog() {
  std::vector<std::vector<int64_t>> batches;
  {
    duckdb::lock_guard<duckdb::mutex> lock{_delete_log_mutex};
    _delete_log_open.store(false, std::memory_order_release);
    batches = std::exchange(_delete_log, {});
  }
  size_t total = 0;
  for (const auto& batch : batches) {
    total += batch.size();
  }
  std::vector<int64_t> rows;
  rows.reserve(total);
  for (const auto& batch : batches) {
    rows.insert(rows.end(), batch.begin(), batch.end());
  }
  return rows;
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
