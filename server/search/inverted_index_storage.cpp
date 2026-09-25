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
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <filesystem>
#include <iresearch/error/error.hpp>
#include <iresearch/formats/index_meta_reader.hpp>
#include <iresearch/formats/index_meta_writer.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
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
#include "query/transaction.h"
#include "search/scorer_options.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

duckdb::shared_ptr<duckdb::AttachedDatabase> AttachedDatabaseById(
  duckdb::idx_t id) {
  auto& manager =
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance());
  for (auto& attached : manager.GetDatabases()) {
    if (attached->oid == id) {
      return attached;
    }
  }
  return nullptr;
}

constexpr duckdb::field_id_t kFieldWalGeneration = 1;
constexpr duckdb::field_id_t kFieldWalOffset = 2;
constexpr duckdb::field_id_t kFieldManifest = 3;
constexpr duckdb::field_id_t kFieldCheckpoint = 4;

struct Stamp {
  irs::SourcePosition position;
  std::shared_ptr<const FileManifest> manifest;
  uint64_t checkpoint = 0;
};

Stamp ReadStamp(duckdb::BinaryDeserializer& in) {
  Stamp stamp;
  stamp.position.generation =
    in.ReadProperty<uint64_t>(kFieldWalGeneration, "wal_generation");
  stamp.position.offset =
    in.ReadProperty<uint64_t>(kFieldWalOffset, "wal_offset");
  const bool has_manifest =
    in.OnOptionalPropertyBegin(kFieldManifest, "manifest");
  if (has_manifest) {
    stamp.manifest = FileManifest::Read(in);
  }
  in.OnOptionalPropertyEnd(has_manifest);
  stamp.checkpoint = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kFieldCheckpoint, "checkpoint", 0);
  return stamp;
}

void ResolveCheckpointSave(irs::Directory& dir, duckdb::idx_t db_id) {
  std::string pending;
  if (!irs::index_meta::LastPendingFile(dir, pending)) {
    return;
  }
  uint64_t checkpoint = 0;
  try {
    irs::index_meta::ReadPayload(dir, pending,
                                 [&](duckdb::BinaryDeserializer& in) {
                                   checkpoint = ReadStamp(in).checkpoint;
                                 });
  } catch (const irs::IndexError&) {
    checkpoint = 0;
  }
  if (checkpoint != 0) {
    const auto store = AttachedDatabaseById(db_id);
    SDB_ENSURE(store, "inverted index: database ", db_id,
               " of a pending checkpoint save is not attached");
    if (checkpoint <=
        store->GetStorageManager().GetBlockManager().GetCheckpointIteration()) {
      const auto committed = irs::index_meta::FileName(
        irs::index_meta::ParsePendingGeneration(pending));
      if (!dir.rename(pending, committed)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_IO_ERROR),
          ERR_MSG("failed to rename '", pending, "' to '", committed, "'"));
      }
      return;
    }
  }
  if (!dir.remove(pending)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("failed to remove '", pending, "'"));
  }
}

}  // namespace

InvertedIndexStorage::InvertedIndexStorage(
  std::shared_ptr<catalog::DatabaseDirectory> directory, bool in_memory,
  duckdb::idx_t db_id, duckdb::idx_t index_id,
  const catalog::InvertedIndexSettings& options,
  const std::optional<irs::ScorerOptions>& top_k_scorer, bool is_new)
  : _index_id{index_id},
    _db_id{db_id},
    _directory{std::move(directory)},
    _search{GetSearchEngine()} {
  _tasks_settings.refresh_interval_msec = options.refresh_interval_ms;
  _tasks_settings.compaction_interval_msec = options.compaction_interval_ms;
  _tasks_settings.reindex_interval_msec = options.reindex_interval_ms;
  _tasks_settings.cleanup_interval_step = options.cleanup_interval_step;
  _tasks_settings.compaction_max_segments = options.compaction_max_segments;
  _tasks_settings.compaction_max_segments_bytes =
    options.compaction_max_segments_bytes;
  _tasks_settings.compaction_floor_segment_bytes =
    options.compaction_floor_segment_bytes;

  SDB_ASSERT(index_id != 0);

  _delete_log_open = is_new;
  irs::ResourceManagementOptions resource_manager;
  resource_manager.transactions = _writers_memory;
  resource_manager.readers = _readers_memory;
  resource_manager.compactions = _compactions_memory;
  resource_manager.file_descriptors = _file_descriptors_count;
  auto opened = OpenStorageDirectory(*_directory, index_id, is_new, in_memory,
                                     resource_manager);
  _dir = std::move(opened.directory);
  _absent = opened.absent;
  _on_disk = opened.on_disk;
  const bool reopen = opened.on_disk && !is_new;
  const auto open_mode =
    reopen ? (irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate)
           : irs::OpenMode::kOmCreate;

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
  writer_options.db = &irs::DuckDBEngine::Instance().instance();
  writer_options.reader_options.db = writer_options.db;
  // No column/norm options are configured on the writer: the per-column
  // encoding config travels with each operation instead. A write hands its own
  // DDL snapshot's InvertedIndex (SegmentWriter::SetFieldOptions, via the
  // serenedb transaction) and a merge hands the compaction task's snapshot
  // index (CompactUnsafe). The long-lived writer therefore never reaches into
  // the live catalog, so a concurrent DROP can no longer dangle it.

  if (const auto& options = top_k_scorer) {
    _topk_scorer = MakeScorer(*options);
    writer_options.reader_options.scorer = _topk_scorer.get();
  }

  writer_options.meta_payload_writer =
    [this](const irs::SourcePosition& position, duckdb::BinarySerializer& out) {
      _position = std::max(_position, position);
      out.WriteProperty<uint64_t>(kFieldWalGeneration, "wal_generation",
                                  _position.generation);
      out.WriteProperty<uint64_t>(kFieldWalOffset, "wal_offset",
                                  _position.offset);
      const auto manifest = GetFileManifest();
      out.OnOptionalPropertyBegin(kFieldManifest, "manifest",
                                  manifest != nullptr);
      if (manifest) {
        manifest->Write(out);
      }
      out.OnOptionalPropertyEnd(manifest != nullptr);
      _pending_checkpoint = _checkpoint != 0
                              ? _checkpoint
                              : RunningCheckpoint(_position.generation);
      out.WritePropertyWithDefault<uint64_t>(kFieldCheckpoint, "checkpoint",
                                             _pending_checkpoint, 0);
    };

  Stamp stamp;
  writer_options.meta_payload_reader = [&](duckdb::BinaryDeserializer& in) {
    stamp = ReadStamp(in);
  };

  SDB_IF_FAILURE("segment_1000_docs_max") {
    writer_options.segment_docs_max = 1000;
  }

  if (reopen) {
    ResolveCheckpointSave(*_dir, _db_id);
  }
  _writer = irs::IndexWriter::Make(*_dir, open_mode, std::move(writer_options));

  if (!reopen) {
    _writer->RefreshCommit();
  }

  auto reader = _writer->GetSnapshot();
  SDB_ASSERT(reader);

  if (reopen) {
    _position = stamp.position;
    SetFileManifest(stamp.manifest);
  }
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
  _tasks_settings.refresh_interval_msec = options.refresh_interval_ms;
  _tasks_settings.compaction_interval_msec = options.compaction_interval_ms;
  _tasks_settings.reindex_interval_msec = options.reindex_interval_ms;
  _tasks_settings.cleanup_interval_step = options.cleanup_interval_step;
  _tasks_settings.compaction_max_segments = options.compaction_max_segments;
  _tasks_settings.compaction_max_segments_bytes =
    options.compaction_max_segments_bytes;
  _tasks_settings.compaction_floor_segment_bytes =
    options.compaction_floor_segment_bytes;

  irs::SegmentOptions segment_options;
  segment_options.segment_count_max = 0;
  segment_options.segment_memory_max = options.segment_memory_max;
  segment_options.segment_docs_max = options.segment_docs_max;
  SDB_ASSERT(_writer);
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

void InvertedIndexStorage::Refresh(
  const irs::ProgressReportCallback& progress) {
  RefreshResult code = RefreshResult::Undefined;
  std::ignore = RefreshUnsafe(/*wait=*/true, progress, code);
  if (code == RefreshResult::Done) {
    NudgeCompaction();
  }
}

StoreStats InvertedIndexStorage::UpdateStatsUnsafe(
  InvertedIndexSnapshotPtr inverted_index_snapshot) const {
  auto stats = StoreStats::FromReader(inverted_index_snapshot->reader);
  stats.numBufferedDocs = _writer->BufferedDocs();
  _maintenance.Fill(stats);
  return stats;
}

ResultWithTime InvertedIndexStorage::CleanupUnsafe() {
  auto begin = std::chrono::steady_clock::now();
  auto result = CleanupUnsafeImpl();
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  _maintenance.RecordCleanup(result, time_ms);
  return {std::move(result), time_ms};
}

absl::Status InvertedIndexStorage::CleanupUnsafeImpl() {
  try {
    SyncDirectory();
    irs::directory_utils::RemoveAllUnreferenced(*_dir);
  } catch (const std::exception& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while cleaning up Search index '", GetId(),
                   "': ", e.what()));
  } catch (...) {
    return absl::InternalError(absl::StrCat(
      "caught exception while cleaning up Search index '", GetId(), "'"));
  }
  return absl::OkStatus();
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
  auto begin = std::chrono::steady_clock::now();
  auto result = RefreshUnsafeImpl(wait, progress, code);
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
      co_return absl::InternalError(absl::StrCat(
        "failure while executing compaction policy on Search index '", GetId(),
        "'"));
    }
    if (res.error == irs::CompactionError::Busy) {
      co_return absl::OkStatus();
    }

    empty_compaction = (res.size == 0);
  } catch (const std::exception& e) {
    co_return absl::InternalError(
      absl::StrCat("caught exception while executing compaction policy "
                   "on Search index '",
                   GetId(), "': ", e.what()));
  } catch (...) {
    co_return absl::InternalError(
      absl::StrCat("caught exception while executing compaction policy "
                   "on Search index '",
                   GetId(), "'"));
  }
  co_return absl::OkStatus();
}

absl::Status InvertedIndexStorage::RefreshUnsafeImpl(
  bool wait, const irs::ProgressReportCallback& progress, RefreshResult& code) {
  code = RefreshResult::NoChanges;

  try {
    std::unique_lock refresh_lock{_refresh_mutex, std::try_to_lock};
    if (!refresh_lock.owns_lock()) {
      if (!wait) {
        SDB_TRACE(SEARCH, "Refresh for Search index '", GetId(),
                  "' is already in progress, skipping");

        code = RefreshResult::InProgress;
        return absl::OkStatus();
      }

      SDB_TRACE(SEARCH, "Refresh for Search index '", GetId(),
                "' is already in progress, waiting");

      refresh_lock.lock();
    }
    while (wait && _pending_checkpoint != 0 && CanPersist()) {
      refresh_lock.unlock();
      WaitForCheckpoint();
      refresh_lock.lock();
    }

    if (!CanPersist()) {
      return absl::OkStatus();
    }
    bool were_changes = false;
    try {
      were_changes = _writer->RefreshBegin({
        .progress = progress,
        .reopen_reader = /* TODO(codeworse) */ false,
      });
      if (were_changes) {
        _directory_dirty.store(true, std::memory_order_relaxed);
      }
      if (were_changes && IsOutOfSync()) {
        _writer->RefreshAbort();
        _pending_checkpoint = 0;
        return absl::OkStatus();
      }
      if (were_changes && _pending_checkpoint == 0) {
        _writer->RefreshFinish();
        _directory_dirty.store(true, std::memory_order_relaxed);
      }
    } catch (...) {
      _pending_checkpoint = 0;
      MarkOutOfSync();
      throw;
    }
    if (!were_changes) {
      SDB_TRACE(SEARCH, "Refresh for Search index '", GetId(),
                "' is no changes");
      StoreInvertedIndexSnapshot(std::make_shared<InvertedIndexSnapshot>(
        _writer->GetSnapshot(), GetFileManifest()));
      return absl::OkStatus();
    }
    if (_pending_checkpoint != 0) {
      code = RefreshResult::InProgress;
      if (wait) {
        refresh_lock.unlock();
        WaitForCheckpoint();
      }
      return absl::OkStatus();
    }
    code = RefreshResult::Done;
    PublishSnapshot();
  } catch (const irs::SqlException& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while refreshing Search index '", GetId(),
                   "': ", e.message()));
  } catch (const std::exception& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while refreshing Search index '", GetId(),
                   "': ", e.what()));
  } catch (...) {
    return absl::InternalError(absl::StrCat(
      "caught exception while refreshing Search index '", GetId(), "'"));
  }
  return absl::OkStatus();
}

bool InvertedIndexStorage::CanPersist() const {
  if (IsOutOfSync()) {
    return false;
  }
  const auto store = AttachedDatabaseById(_db_id);
  return !store || (!duckdb::ValidChecker::IsInvalidated(*store) &&
                    !duckdb::ValidChecker::IsInvalidated(store->GetDatabase()));
}

uint64_t InvertedIndexStorage::RunningCheckpoint(uint64_t generation) const {
  const auto store = AttachedDatabaseById(_db_id);
  if (!store || !store->HasStorageManager()) {
    return 0;
  }
  const auto header =
    store->GetStorageManager().GetBlockManager().GetCheckpointIteration();
  return generation > header ? generation : 0;
}

void InvertedIndexStorage::WaitForCheckpoint() const {
  if (const auto store = AttachedDatabaseById(_db_id);
      store && store->HasStorageManager()) {
    std::ignore =
      duckdb::DuckTransactionManager::Get(*store).SharedCheckpointLock();
  }
}

void InvertedIndexStorage::SyncDirectory() {
  if (!_on_disk ||
      !_directory_dirty.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  try {
    catalog::SyncDirectory(Path());
  } catch (...) {
    _directory_dirty.store(true, std::memory_order_relaxed);
    throw;
  }
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
            _position.generation, ":", _position.offset, "'");
}

void InvertedIndexStorage::PrepareCheckpoint(uint64_t iteration) {
  std::lock_guard lock{_refresh_mutex};
  SDB_ASSERT(_pending_checkpoint == 0 || _pending_checkpoint == iteration);
  _checkpoint = iteration;
  absl::Cleanup reset = [&]() noexcept { _checkpoint = 0; };
  try {
    if (_writer->RefreshBegin()) {
      _directory_dirty.store(true, std::memory_order_relaxed);
    }
  } catch (...) {
    _pending_checkpoint = 0;
    MarkOutOfSync();
    throw;
  }
  SyncDirectory();
  SDB_IF_FAILURE("crash_after_index_checkpoint_prepare") {
    SDB_IMMEDIATE_ABORT();
  }
  if (IsOutOfSync()) {
    if (_pending_checkpoint != 0) {
      _writer->RefreshAbort();
      _pending_checkpoint = 0;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("inverted index ", _index_id,
                            " is out of sync with its store table; refusing "
                            "to checkpoint"));
  }
}

void InvertedIndexStorage::FinishCheckpoint() {
  std::lock_guard lock{_refresh_mutex};
  SDB_IF_FAILURE("crash_before_index_checkpoint_finish") {
    SDB_IMMEDIATE_ABORT();
  }
  if (_pending_checkpoint == 0) {
    return;
  }
  try {
    _writer->RefreshFinish();
  } catch (const std::exception& e) {
    SDB_FATAL(
      SEARCH, "inverted index ", _index_id,
      " cannot commit the index save of a written checkpoint: ", e.what());
  }
  _pending_checkpoint = 0;
  _directory_dirty.store(true, std::memory_order_relaxed);
  PublishSnapshot();
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

}  // namespace sdb::search
