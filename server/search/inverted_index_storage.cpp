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

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/time/time.h>

#include <chrono>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <filesystem>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/fs_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <memory>
#include <system_error>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "basics/serializer.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/scorer_options.h"
#include "catalog/store/store.h"
#include "query/transaction.h"
#include "search/tick_domain.h"
#include "search/wal_recovery.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

bool ReadSegmentMeta(irs::bytes_view payload, Tick& tick,
                     int64_t& iceberg_snapshot_id,
                     WalCursor& wal_cursor) noexcept {
  // [tick:8][iceberg_snapshot_id:8][wal_generation:8][wal_offset:8].
  constexpr size_t kSize = 4 * sizeof(uint64_t);
  if (payload.size() != kSize) {
    return false;
  }
  tick = absl::big_endian::Load64(payload.data());
  iceberg_snapshot_id = static_cast<int64_t>(
    absl::big_endian::Load64(payload.data() + sizeof(uint64_t)));
  wal_cursor.generation =
    absl::big_endian::Load64(payload.data() + 2 * sizeof(uint64_t));
  wal_cursor.offset =
    absl::big_endian::Load64(payload.data() + 3 * sizeof(uint64_t));
  return true;
}

}  // namespace

void InvertedIndexStorage::RecordFlushCursor(Tick tick,
                                             WalCursor cursor) noexcept {
  duckdb::lock_guard<duckdb::mutex> lock{_flush_cursors_mutex};
  _flush_cursors.insert_or_assign(tick, cursor);
}

WalCursor InvertedIndexStorage::CursorAtOrBelow(Tick tick) noexcept {
  duckdb::lock_guard<duckdb::mutex> lock{_flush_cursors_mutex};
  auto it = _flush_cursors.upper_bound(tick);
  if (it == _flush_cursors.begin()) {
    return {};
  }
  --it;
  const WalCursor cursor = it->second;
  // Entries strictly below the returned one can never be the highest at/below a
  // future bound for THIS index, so drop them. Safe because the table is
  // per-index: no other index relies on these entries.
  _flush_cursors.erase(_flush_cursors.begin(), it);
  return cursor;
}

std::filesystem::path InvertedIndexStorage::GetPath(ObjectId db_id,
                                                    ObjectId schema_id,
                                                    ObjectId table_id,
                                                    ObjectId index_id) {
  SDB_ASSERT(db_id.isSet());
  auto path = search::GetSearchEngine().GetPersistedPath(db_id);
  if (schema_id.isSet()) {
    path /= absl::StrCat(schema_id);
  }
  if (table_id.isSet()) {
    SDB_ASSERT(schema_id.isSet());
    path /= absl::StrCat(table_id);
  }
  if (index_id.isSet()) {
    SDB_ASSERT(table_id.isSet());
    path /= absl::StrCat(index_id);
  }
  return path;
}

std::shared_ptr<InvertedIndexStorage> InvertedIndexStorage::Create(
  ObjectId id, const catalog::InvertedIndex& index, bool is_new) {
  return std::make_shared<InvertedIndexStorage>(id, index, is_new);
}

InvertedIndexStorage::InvertedIndexStorage(ObjectId id,
                                           const catalog::InvertedIndex& index,
                                           bool is_new)
  : _index_id{index.GetId()}, _search{GetSearchEngine()} {
  const auto& options = index.GetOptions();
  _tasks_settings.refresh_interval_msec = options.refresh_interval_ms;
  _tasks_settings.compaction_interval_msec = options.compaction_interval_ms;
  _tasks_settings.cleanup_interval_step = options.cleanup_interval_step;

  const auto schema_id = index.GetParentId();
  const auto db_id =
    catalog::GetCatalog().GetCatalogSnapshot()->GetDatabaseId(index);
  const auto index_id = index.GetId();
  SDB_ASSERT(index_id.isSet());
  std::filesystem::path path =
    GetPath(db_id, schema_id, index.GetRelationId(), index_id);
  // TODO(mbkkt) maybe we should use create_directories result instead of
  // exists?
  std::error_code ec;
  bool path_exists = std::filesystem::exists(path, ec);
  if (ec) {
    SDB_THROW(ERROR_INTERNAL, "Failed to check existence of path '",
              path.string(), "' while initializing data store '", _index_id,
              "': ", ec.message());
  }
  if (!path_exists) {
    std::filesystem::create_directories(path, ec);
    if (ec) {
      SDB_THROW(ERROR_INTERNAL, "Failed to create directory '", path.string(),
                "' while initializing data store '", _index_id,
                "': ", ec.message());
    }
  }
  auto codec = irs::formats::Get("1_5simd");
  const auto open_mode =
    path_exists ? (irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate)
                : irs::OpenMode::kOmCreate;

  // New indexes start at the current tick; existing directories override
  // both values from the persisted segment meta below.
  _recovery_tick = TickDomain::Instance().Current();
  _last_durable_tick = _recovery_tick;
  irs::ResourceManagementOptions resource_manager;
  resource_manager.transactions = _writers_memory;
  resource_manager.readers = _readers_memory;
  resource_manager.compactions = _compactions_memory;
  resource_manager.file_descriptors = _file_descriptors_count;
  _dir = std::make_unique<irs::MMapDirectory>(path, irs::DirectoryAttributes{},
                                              resource_manager);

  irs::IndexWriterOptions writer_options;
  writer_options.segment_memory_max = 256 * (size_t{1} << 20);  // 256MB
#ifdef SDB_DEV
  // Dev safety net: a second IndexWriter opening the same index directory (a
  // lifecycle bug -- two storages/loops on one dir) fails to acquire the lock
  // with a clean error instead of silently corrupting segment files.
  writer_options.lock_repository = true;
#else
  writer_options.lock_repository = false;  // single-process server owns the dir
#endif
  writer_options.db = &sdb::DuckDBEngine::Instance().instance();
  writer_options.reader_options.db = writer_options.db;
  // No column/norm options are configured on the writer: the per-column
  // encoding config travels with each operation instead. A write hands its own
  // DDL snapshot's InvertedIndex (SegmentWriter::SetFieldOptions, via the
  // serenedb transaction) and a merge hands the compaction task's snapshot
  // index (CompactUnsafe). The long-lived writer therefore never reaches into
  // the live catalog, so a concurrent DROP can no longer dangle it.

  if (const auto& options = index.GetTopKScorer()) {
    _topk_scorer = catalog::MakeScorer(*options);
    writer_options.reader_options.scorer = _topk_scorer.get();
  }

  writer_options.meta_payload_provider = [this](uint64_t tick,
                                                irs::bstring& out) {
    if (_phase == Phase::Creating) {
      tick = TickDomain::Instance().Current();
    }
    _last_durable_tick = std::max(_last_durable_tick, tick);
    uint64_t tick_be = absl::big_endian::FromHost(_last_durable_tick);
    out.append(reinterpret_cast<const irs::byte_type*>(&tick_be),
               sizeof(tick_be));
    uint64_t iceberg_be =
      absl::big_endian::FromHost(static_cast<uint64_t>(_iceberg_snapshot_id));
    out.append(reinterpret_cast<const irs::byte_type*>(&iceberg_be),
               sizeof(iceberg_be));
    // Durable WAL cursor, stamped consistently with the durable tick we just
    // persisted above. `tick` here is the exact tick this flush made durable
    // (FlushContext::FlushPending's flushed_tick in the Recovering/Creating
    // phase, before_refresh in the Active phase -- both are the highest tick
    // covered by these segments), and _last_durable_tick is its running max.
    // The matching cursor is the highest per-index commit entry at/below that
    // tick. A 0/absent lookup means no recorded commit fell at/below it, so
    // keep the prior durable cursor rather than regressing it to 0. Checkpoint
    // refreshes set _pending_wal_cursor up front (next generation, offset 0)
    // and disable this stamping. Recovery replays only operations at or past
    // the stamped cursor.
    if (_stamp_cursor_from_flush) {
      if (const auto cursor = CursorAtOrBelow(_last_durable_tick);
          cursor.generation != 0 || cursor.offset != 0) {
        _pending_wal_cursor = cursor;
      }
    }
    uint64_t gen_be =
      absl::big_endian::FromHost(_pending_wal_cursor.generation);
    out.append(reinterpret_cast<const irs::byte_type*>(&gen_be),
               sizeof(gen_be));
    uint64_t offset_be = absl::big_endian::FromHost(_pending_wal_cursor.offset);
    out.append(reinterpret_cast<const irs::byte_type*>(&offset_be),
               sizeof(offset_be));
    return true;
  };

  SDB_IF_FAILURE("segment_1000_docs_max") {
    writer_options.segment_docs_max = 1000;
  }

  _writer = irs::IndexWriter::Make(*_dir, codec, open_mode, writer_options);

  if (!path_exists) {
    // Initialize empty index
    _writer->RefreshCommit();
  }

  auto reader = _writer->GetSnapshot();
  SDB_ASSERT(reader);

  if (path_exists) {
    auto payload = irs::GetPayload(reader.Meta().index_meta);
    if (!payload.empty()) {
      if (!ReadSegmentMeta(payload, _recovery_tick, _iceberg_snapshot_id,
                           _recovery_wal_cursor)) {
        SDB_WARN(SEARCH, "Failed to read segment meta from inverted index '",
                 GetId().id(), "'");
      }
      _last_durable_tick = _recovery_tick;
    }
  }
  std::atomic_store(&_snapshot,
                    std::make_shared<InvertedIndexSnapshot>(std::move(reader)));
}

void InvertedIndexStorage::TruncateCommit(TruncateGuard&& guard, Tick tick,
                                          query::Transaction* user_txn)
  ABSL_NO_THREAD_SAFETY_ANALYSIS {
  SDB_IF_FAILURE("SereneSearchTruncateFailure") { SDB_THROW(ERROR_DEBUG); }

  SDB_ASSERT(_writer);

  // If we're inside a user transaction, drop its per-conn iresearch staging
  // for this storage (the new-arch analog of the old SearchTrxState cookie
  // cleanup). Throws away pending operations -- Clear will overwrite them
  // anyway -- and forces release of any active segment context so the
  // _writer->Clear below doesn't deadlock waiting for it.
  if (user_txn != nullptr) {
    user_txn->EraseSearchTransaction(GetId());
  }

  // Allow callers to pass an empty guard -- self-lock in that case so this
  // is callable from paths that didn't go through TruncateBegin (e.g. WAL
  // recovery).
  if (!guard.mutex) {
    guard = TruncateBegin();
  }
  SDB_ASSERT(guard.mutex.get() == &_refresh_mutex);

  // Roll _last_durable_tick back to its prior value if the iresearch Clear
  // throws partway through. Once Clear returns, we cancel the rollback and
  // commit to the new tick -- same ordering as the legacy code.
  absl::Cleanup clear_guard = [&, last = _last_durable_tick]() noexcept {
    _last_durable_tick = last;
  };
  try {
    _writer->Clear(tick);
    std::move(clear_guard).Cancel();
    // payload will not be called if index already empty
    _last_durable_tick = std::max(tick, _last_durable_tick);

    auto reader = _writer->GetSnapshot();
    SDB_ASSERT(reader);

    // update reader
    auto data = std::make_shared<InvertedIndexSnapshot>(std::move(reader));
    StoreInvertedIndexSnapshot(data);

    UpdateStatsUnsafe(std::move(data));
  } catch (const std::exception& e) {
    SDB_ERROR(SEARCH, "caught exception while truncating Search index '",
              GetId().id(), "': ", e.what());
    throw;
  } catch (...) {
    SDB_WARN(SEARCH, "caught exception while truncating Search index '",
             GetId().id(), "'");
    throw;
  }
}

void InvertedIndexStorage::StartTasks() {
  _search.StartTasks(shared_from_this());
}

void InvertedIndexStorage::Refresh(
  const irs::ProgressReportCallback& progress) {
  RefreshResult code = RefreshResult::Undefined;
  std::ignore = RefreshUnsafe(/*wait=*/true, progress, code);
}

void InvertedIndexStorage::CheckpointRefresh() {
  RefreshResult code = RefreshResult::Undefined;
  std::ignore = RefreshUnsafe(/*wait=*/true, nullptr, code,
                              /*for_checkpoint=*/true);
}

InvertedIndexStorage::Stats InvertedIndexStorage::UpdateStatsUnsafe(
  InvertedIndexSnapshotPtr inverted_index_snapshot) const {
  Stats stats;
  if (inverted_index_snapshot) {
    auto& reader = inverted_index_snapshot->reader;
    SDB_ASSERT(reader);
    auto& segments = reader->Meta().index_meta.segments;
    stats.numSegments = segments.size();
    stats.numDocs = reader->docs_count();
    stats.numLiveDocs = reader->live_docs_count();
    stats.numFiles = 1 + stats.numSegments;
    for (const auto& segment : segments) {
      const auto& meta = segment.meta;
      stats.indexSize += meta.byte_size;
      stats.numFiles += meta.files.size();
    }
  }
  if (_writer) {
    stats.numBufferedDocs = _writer->BufferedDocs();
  }
  stats.numFailedCommits = _num_failed_commits.load(std::memory_order_relaxed);
  stats.numFailedCleanups =
    _num_failed_cleanups.load(std::memory_order_relaxed);
  stats.numFailedConsolidations =
    _num_failed_consolidations.load(std::memory_order_relaxed);
  stats.avgCommitTimeMs = _avg_commit_time_ms.Average();
  stats.avgCleanupTimeMs = _avg_cleanup_time_ms.Average();
  stats.avgConsolidationTimeMs = _avg_consolidation_time_ms.Average();
  return stats;
}

InvertedIndexStorage::ResultWithTime InvertedIndexStorage::CleanupUnsafe() {
  auto begin = std::chrono::steady_clock::now();
  auto result = CleanupUnsafeImpl();
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  if (!result.ok()) {
    _num_failed_cleanups.fetch_add(1, std::memory_order_relaxed);
  } else {
    _avg_cleanup_time_ms.Record(time_ms);
  }
  return {std::move(result), time_ms};
}

Result InvertedIndexStorage::CleanupUnsafeImpl() {
  try {
    irs::directory_utils::RemoveAllUnreferenced(*_dir);
  } catch (const std::exception& e) {
    return {ERROR_INTERNAL, "caught exception while cleaning up Search index '",
            GetId().id(), "': ", e.what()};
  } catch (...) {
    return {ERROR_INTERNAL, "caught exception while cleaning up Search index '",
            GetId().id(), "'"};
  }
  return {};
}

InvertedIndexStorage::ResultWithTime InvertedIndexStorage::CompactUnsafe(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options) {
  auto begin = std::chrono::steady_clock::now();
  auto result =
    CompactUnsafeImpl(policy, progress, empty_compaction, field_options);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  if (!result.ok()) {
    _num_failed_consolidations.fetch_add(1, std::memory_order_relaxed);
  } else if (!empty_compaction) {
    _avg_consolidation_time_ms.Record(time_ms);
  }
  return {std::move(result), time_ms};
}

InvertedIndexStorage::ResultWithTime InvertedIndexStorage::RefreshUnsafe(
  bool wait, const irs::ProgressReportCallback& progress, RefreshResult& code,
  bool for_checkpoint) {
  auto begin = std::chrono::steady_clock::now();
  auto result = RefreshUnsafeImpl(wait, progress, code, for_checkpoint);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();

  SDB_IF_FAILURE("Search::FailOnCommit") { result.reset(ERROR_DEBUG); }
  SDB_IF_FAILURE("Search::CrashAfterCommit") { SDB_IMMEDIATE_ABORT(); }

  if (!result.ok()) {
    _num_failed_commits.fetch_add(1, std::memory_order_relaxed);
  } else if (code == RefreshResult::Done) {
    _avg_commit_time_ms.Record(time_ms);
  }

  return {std::move(result), time_ms};
}

Result InvertedIndexStorage::CompactUnsafeImpl(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options) {
  empty_compaction = false;

  if (!policy) {
    return {ERROR_BAD_PARAMETER,
            "unset compaction policy while executing compaction policy "
            "on Search index '",
            GetId().id(), "'"};
  }

  try {
    const auto res = _writer->Compact(policy, field_options, nullptr, progress);
    if (res.error == irs::CompactionError::Fail) {
      return {ERROR_INTERNAL,
              "failure while executing compaction policy on Search index '",
              GetId().id(), "'"};
    }
    if (res.error == irs::CompactionError::Busy) {
      empty_compaction = false;
      return {};
    }

    empty_compaction = (res.size == 0);
  } catch (const std::exception& e) {
    return {ERROR_INTERNAL,
            "caught exception while executing compaction policy ",
            "on Search index '",
            GetId().id(),
            "': ",
            e.what()};
  } catch (...) {
    return {ERROR_INTERNAL,
            "caught exception while executing compaction policy ",
            "on Search index '", GetId().id(), "'"};
  }
  return {};
}

Result InvertedIndexStorage::RefreshUnsafeImpl(
  bool wait, const irs::ProgressReportCallback& progress, RefreshResult& code,
  bool for_checkpoint) {
  code = RefreshResult::NoChanges;

  try {
    std::unique_lock refresh_lock{_refresh_mutex, std::try_to_lock};
    if (!refresh_lock.owns_lock()) {
      if (!wait) {
        SDB_TRACE(SEARCH, "Refresh for Search index '", GetId().id(),
                  "' is already in progress, skipping");

        code = RefreshResult::InProgress;
        return {};
      }

      SDB_TRACE(SEARCH, "Refresh for Search index '", GetId().id(),
                "' is already in progress, waiting");

      refresh_lock.lock();
    }

    const auto before_refresh = TickDomain::Instance().Current();
    SDB_ASSERT(_last_durable_tick <= before_refresh);

    // Stamp the EXACT durable WAL cursor consistently with the durable tick
    // this refresh persists. RefreshCommit (below) flushes every staged batch
    // with tick <= refresh_tick and, inside the meta payload provider, persists
    // _last_durable_tick == the highest tick covered by the flushed segments.
    // The per-index table recorded, per settled CommitSearch BEFORE the batch
    // became flushable and after the store WAL was durable, the WAL end offset
    // of that commit; commits serialize, so the cursor that matches the durable
    // tick is the entry of the highest tick at/below it. The payload provider
    // performs that CursorAtOrBelow(_last_durable_tick) lookup just before
    // persisting (it has the exact durable tick in hand), gated by
    // _stamp_cursor_from_flush.
    //
    // A checkpoint-driven refresh runs the moment before the checkpoint
    // truncates the store WAL and bumps the iteration to
    // GetCheckpointIteration()
    // + 1. The post-checkpoint WAL starts fresh, so stamp that next generation
    // with offset 0: the next boot loads iteration+1 and the cursor generation
    // matches (the live iteration is still N here, but the persisted header
    // will be N+1). The payload provider must NOT overwrite that, so disable
    // the flush-driven stamping.
    _stamp_cursor_from_flush = !for_checkpoint;
    absl::Cleanup stamp_guard = [&]() noexcept {
      _stamp_cursor_from_flush = false;
    };
    if (for_checkpoint) {
      if (auto store =
            duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
              .GetDatabase(duckdb::Identifier{catalog::kStoreDatabaseName})) {
        const auto next_gen = store->GetStorageManager()
                                .GetBlockManager()
                                .GetCheckpointIteration() +
                              1;
        _pending_wal_cursor = WalCursor{next_gen, 0};
      }
    }
    absl::Cleanup refresh_guard = [&, last = _last_durable_tick]() noexcept {
      _last_durable_tick = last;
    };

    const auto refresh_tick = [&] {
      switch (_phase) {
        case Phase::Creating:
        case Phase::Recovering:
          return irs::writer_limits::kMaxTick;
        case Phase::Active:
          return before_refresh;
      }
    }();
    const bool were_changes = _writer->RefreshCommit({
      .tick = refresh_tick,
      .progress = progress,
      .reopen_reader = /* TODO(codeworse) */ false,
    });
    // get new reader
    auto reader = _writer->GetSnapshot();
    SDB_ASSERT(reader != nullptr);
    std::move(refresh_guard).Cancel();
    if (!were_changes) {
      SDB_TRACE(SEARCH, "Refresh for Search index '", GetId().id(),
                "' is no changes, tick ", before_refresh, "'");
      if (_phase != Phase::Recovering) {
        _last_durable_tick = before_refresh;
      }
      StoreInvertedIndexSnapshot(
        std::make_shared<InvertedIndexSnapshot>(std::move(reader)));
      return {};
    }
    SDB_ASSERT(_phase != Phase::Active || _last_durable_tick == before_refresh);
    code = RefreshResult::Done;

    // update reader
    SDB_ASSERT(GetInvertedIndexSnapshot());
    SDB_ASSERT(GetInvertedIndexSnapshot()->reader != reader);
    const auto reader_size = reader->size();
    const auto docs_count = reader->docs_count();
    const auto live_docs_count = reader->live_docs_count();

    auto data = std::make_shared<InvertedIndexSnapshot>(std::move(reader));
    StoreInvertedIndexSnapshot(data);

    UpdateStatsUnsafe(std::move(data));

    SDB_DEBUG(SEARCH, "successful sync of Search index '", GetId().id(),
              "', segments '", reader_size, "', docs count '", docs_count,
              "', live docs count '", live_docs_count,
              "', last operation tick '", _last_durable_tick, "'");
  } catch (const basics::Exception& e) {
    return {e.code(), "caught exception while refreshing Search index '",
            GetId().id(), "': ", e.message()};
  } catch (const std::exception& e) {
    return {ERROR_INTERNAL, "caught exception while refreshing Search index '",
            GetId().id(), "': ", e.what()};
  } catch (...) {
    return {ERROR_INTERNAL, "caught exception while refreshing Search index '",
            GetId().id(), "'"};
  }
  return {};
}

void InvertedIndexStorage::FinishCreation() {
  std::lock_guard lock{_refresh_mutex};
  if (_phase == Phase::Active) {
    return;
  }
  _phase = Phase::Active;
}

InvertedIndexStorage::Stats InvertedIndexStorage::GetStats() const {
  return UpdateStatsUnsafe(GetInvertedIndexSnapshot());
}

}  // namespace sdb::search
