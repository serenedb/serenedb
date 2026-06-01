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

#include "search/inverted_index_shard.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/time/time.h>
#include <vpack/serializer.h>

#include <chrono>
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
#include "basics/logger/logger.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/scorer_options.h"
#include "metrics/gauge.h"
#include "metrics/guard.h"
#include "query/transaction.h"
#include "rest_server/flush_feature.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_recovery_manager.h"
#include "search/task.h"
#include "search/wal_recovery.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

uint64_t ComputeAvg(std::atomic<uint64_t>& time_num, uint64_t new_time) {
  constexpr uint64_t kWindowSize{10};
  const auto old_time_num =
    time_num.fetch_add((new_time << 32U) + 1, std::memory_order_relaxed);
  const auto old_time = old_time_num >> 32U;
  const auto old_num = old_time_num & std::numeric_limits<uint32_t>::max();
  if (old_num >= kWindowSize) {
    time_num.fetch_sub(((old_time / old_num) << 32U) + 1,
                       std::memory_order_relaxed);
  }
  return (old_time + new_time) / (old_num + 1);
}

bool ReadCommitMeta(irs::bytes_view payload, Tick& tick,
                    int64_t& iceberg_snapshot_id) noexcept {
  // [tick:8][iceberg_snapshot_id:8]; 0 = not pinned.
  constexpr size_t kExpectedSize = 2 * sizeof(uint64_t);
  if (payload.size() != kExpectedSize) {
    return false;
  }
  tick = absl::big_endian::Load64(payload.data());
  iceberg_snapshot_id = static_cast<int64_t>(
    absl::big_endian::Load64(payload.data() + sizeof(uint64_t)));
  return true;
}

}  // namespace

std::filesystem::path InvertedIndexShard::GetPath(ObjectId db_id,
                                                  ObjectId schema_id,
                                                  ObjectId table_id,
                                                  ObjectId index_id,
                                                  ObjectId shard_id) {
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
  if (shard_id.isSet()) {
    SDB_ASSERT(index_id.isSet());
    path /= absl::StrCat(shard_id);
  }
  return path;
}

std::shared_ptr<InvertedIndexShard> InvertedIndexShard::Create(
  ObjectId id, const catalog::InvertedIndex& index, bool is_new) {
  return std::make_shared<InvertedIndexShard>(id, index, is_new);
}

InvertedIndexShard::InvertedIndexShard(ObjectId id,
                                       const catalog::InvertedIndex& index,
                                       bool is_new)
  : IndexShard{id, index.GetId(), catalog::ObjectType::InvertedIndexShard},
    _engine{GetServerEngine()},
    _search{GetSearchEngine()},
    _state{std::make_shared<ThreadPoolState>()} {
  const auto& options = index.GetOptions();
  _tasks_settings.refresh_interval_msec = options.refresh_interval_ms;
  _tasks_settings.compaction_interval_msec = options.compaction_interval_ms;
  _tasks_settings.cleanup_interval_step = options.cleanup_interval_step;

  const auto schema_id = index.GetParentId();
  const auto db_id = catalog::CatalogFeature::instance()
                       .Global()
                       .GetCatalogSnapshot()
                       ->GetDatabaseId(index);
  const auto index_id = index.GetId();
  SDB_ASSERT(index_id.isSet());
  std::filesystem::path path =
    GetPath(db_id, schema_id, index.GetRelationId(), index_id, GetId());
  // TODO(mbkkt) maybe we should use create_directories result instead of
  // exists?
  std::error_code ec;
  bool path_exists = std::filesystem::exists(path, ec);
  if (ec) {
    SDB_THROW(ERROR_INTERNAL, "Failed to check existence of path '",
              path.string(), "' while initializing data store '", _id,
              "': ", ec.message());
  }
  if (!path_exists) {
    std::filesystem::create_directories(path, ec);
    if (ec) {
      SDB_THROW(ERROR_INTERNAL, "Failed to create directory '", path.string(),
                "' while initializing data store '", _id, "': ", ec.message());
    }
  }
  auto codec = irs::formats::Get("1_5simd");
  const auto open_mode =
    path_exists ? (irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate)
                : irs::OpenMode::kOmCreate;

  // Set up recovery tick based on engine state (default for new index)
  switch (_engine.recoveryState()) {
    case RecoveryState::Before:
      [[fallthrough]];
    case RecoveryState::Done:
      _recovery_tick = _engine.recoveryTick();
      break;
    case RecoveryState::InProgress:
      _recovery_tick = _engine.releasedTick();
      break;
  }
  _last_committed_tick = _recovery_tick;
  irs::ResourceManagementOptions resource_manager;
  resource_manager.transactions = _writers_memory;
  resource_manager.readers = _readers_memory;
  resource_manager.compactions = _compactions_memory;
  resource_manager.file_descriptors = _file_descriptors_count;
  resource_manager.cached_columns =
    &GetSearchEngine().getCachedColumnsManager();
  _dir = std::make_unique<irs::MMapDirectory>(path, irs::DirectoryAttributes{},
                                              resource_manager);

  irs::IndexWriterOptions writer_options;
  writer_options.segment_memory_max = 256 * (size_t{1} << 20);  // 256MB
  writer_options.lock_repository = false;  // RocksDB has its own lock
  writer_options.db = &sdb::DuckDBEngine::Instance().instance();
  writer_options.reader_options.db = writer_options.db;
  writer_options.column_options = [&](irs::field_id id) -> irs::ColumnOptions {
    if (const auto* entry = index.FindEntry(id)) {
      return {
        .row_group_size = entry->row_group_size,
        .compression = entry->compression,
        .hnsw_info = index.GetHNSWInfo(id),
      };
    }
    if (static_cast<catalog::Column::Id>(id) ==
        catalog::Column::kGeneratedPKId) {
      return {
        .skip_validity = true,
        .row_group_size = index.GetOptions().row_group_size,
      };
    }
    const auto* features = index.FindSyntheticFeatures(id);
    SDB_ASSERT(features, "column callback for unknown column: ", id);
    SDB_ASSERT(!features->HasFeatures(irs::IndexFeatures::Norm),
               "norm-role synthetic id must not reach column callback: ", id);
    return {
      .skip_validity = true,
      .row_group_size = index.GetOptions().row_group_size,
    };
  };
  writer_options.norm_column_options =
    [&](std::string_view name) -> irs::NormColumnOptions {
    static constexpr size_t kIdSize = sizeof(uint64_t);
    SDB_ASSERT(name.size() == kIdSize + 1);
    const auto raw =
      static_cast<uint64_t>(absl::big_endian::Load64(name.data()));
    const auto* entry = index.FindEntry(static_cast<irs::field_id>(raw));
    SDB_ASSERT(entry != nullptr, ERROR_INTERNAL,
               "norm callback for unknown id: ", raw);
    SDB_ASSERT(entry->synthetic_column,
               "norm callback fired without a catalog reservation; id: ", raw);
    SDB_ASSERT(entry->features.HasFeatures(irs::IndexFeatures::Norm),
               "norm callback fired but catalog features lack Norm; id: ", raw);
    return {
      .id = *entry->synthetic_column,
      .row_group_size = entry->norm_row_group_size,
    };
  };

  if (const auto& options = index.GetTopKScorer()) {
    _topk_scorer = catalog::MakeScorer(*options);
    writer_options.reader_options.scorer = _topk_scorer.get();
  }

  writer_options.meta_payload_provider = [this](uint64_t tick,
                                                irs::bstring& out) {
    if (_phase == Phase::Creating) {
      tick = _engine.currentTick();
    }
    _last_committed_tick = std::max(_last_committed_tick, tick);
    uint64_t tick_be = absl::big_endian::FromHost(_last_committed_tick);
    out.append(reinterpret_cast<const irs::byte_type*>(&tick_be),
               sizeof(tick_be));
    uint64_t iceberg_be =
      absl::big_endian::FromHost(static_cast<uint64_t>(_iceberg_snapshot_id));
    out.append(reinterpret_cast<const irs::byte_type*>(&iceberg_be),
               sizeof(iceberg_be));
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
      if (!ReadCommitMeta(payload, _recovery_tick, _iceberg_snapshot_id)) {
        SDB_WARN(SEARCH, "Failed to read commit meta from inverted index '",
                 GetId().id(), "'");
      }
      _last_committed_tick = _recovery_tick;
    }
  }
  auto engine_snapshot = _engine.currentSnapshot();
  if (!engine_snapshot) {
    SDB_THROW(ERROR_INTERNAL, "Search index '", _id,
              "' cannot acquire snapshot");
  }
  _snapshot = std::make_shared<InvertedIndexSnapshot>(
    std::move(reader), std::move(engine_snapshot));

  _flush_subscription = std::make_shared<LowerBoundSubscription>(
    _recovery_tick,
    absl::StrCat("flush subscription for inverted index '", _id, "'"));
}

void InvertedIndexShard::WriteInternal(vpack::Builder& /*b*/) const {}

void InvertedIndexShard::TruncateCommit(TruncateGuard&& guard, Tick tick,
                                        query::Transaction* user_txn)
  ABSL_NO_THREAD_SAFETY_ANALYSIS {
  // Bump _num_failed_commits if anything below throws so the metric stays
  // consistent with the legacy SearchDataStore::truncateCommit path.
  bool ok = false;
  irs::Finally compute_metrics = [&]() noexcept {
    // We don't measure time because we believe that it should tend to zero
    if (!ok && _num_failed_commits != nullptr) {
      _num_failed_commits->fetch_add(1, std::memory_order_relaxed);
    }
  };

  SDB_IF_FAILURE("SereneSearchTruncateFailure") { SDB_THROW(ERROR_DEBUG); }

  SDB_ASSERT(_writer);

  // If we're inside a user transaction, drop its per-conn iresearch staging
  // for this shard (the new-arch analog of the old SearchTrxState cookie
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
  SDB_ASSERT(guard.mutex.get() == &_commit_mutex);

  // Roll _last_committed_tick back to its prior value if the iresearch Clear
  // throws partway through. Once Clear returns, we cancel the rollback and
  // commit to the new tick -- same ordering as the legacy code.
  absl::Cleanup clear_guard = [&, last = _last_committed_tick]() noexcept {
    _last_committed_tick = last;
  };
  try {
    _writer->Clear(tick);
    std::move(clear_guard).Cancel();
    // payload will not be called if index already empty
    _last_committed_tick = std::max(tick, _last_committed_tick);

    auto engine_snapshot = _engine.currentSnapshot();
    if (!engine_snapshot) [[unlikely]] {
      // we reuse the previous storage snapshot here. Technically not right
      // (it's most likely outdated) but the index is empty so it makes no
      // difference -- we won't materialize anything anyway.
      auto prev = GetInvertedIndexSnapshot();
      if (prev) {
        engine_snapshot = prev->snapshot;
      }
      if (!engine_snapshot) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to get engine snapshot while truncating Search "
                  "index '",
                  GetId().id(), "'");
      }
    }
    auto reader = _writer->GetSnapshot();
    SDB_ASSERT(reader);

    // update reader
    auto data = std::make_shared<InvertedIndexSnapshot>(
      std::move(reader), std::move(engine_snapshot));
    StoreInvertedIndexSnapshot(data);

    UpdateStatsUnsafe(std::move(data));

    auto& subscription =
      basics::downCast<LowerBoundSubscription>(*_flush_subscription);
    subscription.tick(_last_committed_tick);
    ok = true;
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

void InvertedIndexShard::ScheduleCompaction(absl::Duration delay) {
  CompactionTask task{shared_from_this(), [] { return /* TODO */ true; }};

  _state->pending_compactions.fetch_add(1, std::memory_order_release);
  std::move(task).Schedule(delay).Detach();
}

void InvertedIndexShard::ScheduleCommit(absl::Duration delay) {
  RefreshTask task{shared_from_this(), false};

  _state->pending_commits.fetch_add(1, std::memory_order_release);
  std::move(task).Schedule(delay).Detach();
}

yaclib::Future<> InvertedIndexShard::CommitWait() {
  RefreshTask task{shared_from_this(), true};
  _state->pending_commits.fetch_add(1, std::memory_order_release);
  return std::move(task).Schedule();
}

InvertedIndexShard::Stats InvertedIndexShard::UpdateStatsUnsafe(
  InvertedIndexSnapshotPtr inverted_index_snapshot) const {
  SDB_ASSERT(inverted_index_snapshot);
  auto& reader = inverted_index_snapshot->reader;
  SDB_ASSERT(reader);
  if (_mapped_memory) {
    _mapped_memory->store(reader.CountMappedMemory(),
                          std::memory_order_relaxed);
  }
  auto& segments = reader->Meta().index_meta.segments;

  Stats stats;
  stats.numSegments = segments.size();
  stats.numDocs = reader->docs_count();
  stats.numLiveDocs = reader->live_docs_count();
  stats.numFiles = 1 + stats.numSegments;
  for (const auto& segment : segments) {
    const auto& meta = segment.meta;
    stats.indexSize += meta.byte_size;
    stats.numFiles += meta.files.size();
  }
  if (_metric_stats) {
    _metric_stats->store(stats);
  }
  return stats;
}

InvertedIndexShard::ResultWithTime InvertedIndexShard::CleanupUnsafe() {
  auto begin = std::chrono::steady_clock::now();
  auto result = CleanupUnsafeImpl();
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  if (bool ok = result.ok(); ok && _avg_cleanup_time_ms != nullptr) {
    _avg_cleanup_time_ms->store(ComputeAvg(_cleanup_time_num, time_ms),
                                std::memory_order_relaxed);
  } else if (!ok && _num_failed_cleanups != nullptr) {
    _num_failed_cleanups->fetch_add(1, std::memory_order_relaxed);
  }
  return {std::move(result), time_ms};
}

Result InvertedIndexShard::CleanupUnsafeImpl() {
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

InvertedIndexShard::ResultWithTime InvertedIndexShard::CompactUnsafe(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction) {
  auto begin = std::chrono::steady_clock::now();
  auto result = CompactUnsafeImpl(policy, progress, empty_compaction);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  if (bool ok = result.ok(); ok && _avg_compaction_time_ms != nullptr) {
    _avg_compaction_time_ms->store(ComputeAvg(_compaction_time_num, time_ms),
                                   std::memory_order_relaxed);
  } else if (!ok && _num_failed_compactions != nullptr) {
    _num_failed_compactions->fetch_add(1, std::memory_order_relaxed);
  }
  return {std::move(result), time_ms};
}

InvertedIndexShard::ResultWithTime InvertedIndexShard::CommitUnsafe(
  bool wait, const irs::ProgressReportCallback& progress, CommitResult& code) {
  auto begin = std::chrono::steady_clock::now();
  auto result = CommitUnsafeImpl(wait, progress, code);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();

  SDB_IF_FAILURE("Search::FailOnCommit") {
    // intentionally mark the commit as failed
    result.reset(ERROR_DEBUG);
  }
  SDB_IF_FAILURE("Search::CrashAfterCommit") { SDB_IMMEDIATE_ABORT(); }

  if (bool ok = result.ok(); !ok && _num_failed_commits != nullptr) {
    _num_failed_commits->fetch_add(1, std::memory_order_relaxed);
  } else if (ok && code == CommitResult::Done &&
             _avg_commit_time_ms != nullptr) {
    _avg_commit_time_ms->store(ComputeAvg(_commit_time_num, time_ms),
                               std::memory_order_relaxed);
  }
  return {std::move(result), time_ms};
}

Result InvertedIndexShard::CompactUnsafeImpl(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction) {
  empty_compaction = false;

  if (!policy) {
    return {ERROR_BAD_PARAMETER,
            "unset compaction policy while executing compaction policy "
            "on Search index '",
            GetId().id(), "'"};
  }

  try {
    const auto res = _writer->Compact(policy, nullptr, progress);
    if (!res) {
      return {ERROR_INTERNAL,
              "failure while executing compaction policy on Search index '",
              GetId().id(), "'"};
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

Result InvertedIndexShard::CommitUnsafeImpl(
  bool wait, const irs::ProgressReportCallback& progress, CommitResult& code) {
  code = CommitResult::NoChanges;

  try {
    std::unique_lock commit_lock{_commit_mutex, std::try_to_lock};
    if (!commit_lock.owns_lock()) {
      if (!wait) {
        SDB_TRACE(SEARCH, "Commit for Search index '", GetId().id(),
                  "' is already in progress, skipping");

        code = CommitResult::InProgress;
        return {};
      }

      SDB_TRACE(SEARCH, "Commit for Search index '", GetId().id(),
                "' is already in progress, waiting");

      commit_lock.lock();
    }

    auto engine_snapshot = _engine.currentSnapshot();
    if (!engine_snapshot) [[unlikely]] {
      return {ERROR_INTERNAL,
              "Failed to get engine snapshot while committing "
              "Search index '",
              GetId().id(), "'"};
    }
    const auto before_commit =
      engine_snapshot->GetSnapshot()->GetSequenceNumber();
    SDB_ASSERT(_last_committed_tick <= before_commit);
    absl::Cleanup commit_guard = [&, last = _last_committed_tick]() noexcept {
      _last_committed_tick = last;
    };

    const auto commit_tick = [&] {
      switch (_phase) {
        case Phase::Creating:
        case Phase::Recovering:
          return irs::writer_limits::kMaxTick;
        case Phase::Active:
          return before_commit;
      }
    }();
    const bool were_changes = _writer->RefreshCommit({
      .tick = commit_tick,
      .progress = progress,
      .reopen_columnstore = /* TODO(codeworse) */ false,
    });
    // get new reader
    auto reader = _writer->GetSnapshot();
    SDB_ASSERT(reader != nullptr);
    std::move(commit_guard).Cancel();
    if (!were_changes) {
      SDB_TRACE(SEARCH, "Commit for Search index '", GetId().id(),
                "' is no changes, tick ", before_commit, "'");
      // While Recovering, the flush subscription must not claim more than
      // what's actually flushed -- otherwise rocksdb could truncate WAL we
      // still need to replay on a later restart.
      if (_phase != Phase::Recovering) {
        _last_committed_tick = before_commit;
        auto& subscription =
          basics::downCast<LowerBoundSubscription>(*_flush_subscription);
        subscription.tick(_last_committed_tick);
      }
      StoreInvertedIndexSnapshot(std::make_shared<InvertedIndexSnapshot>(
        std::move(reader), std::move(engine_snapshot)));
      return {};
    }
    SDB_ASSERT(_phase != Phase::Active ||
               _last_committed_tick == before_commit);
    code = CommitResult::Done;

    // update reader
    SDB_ASSERT(GetInvertedIndexSnapshot());
    SDB_ASSERT(GetInvertedIndexSnapshot()->reader != reader);
    const auto reader_size = reader->size();
    const auto docs_count = reader->docs_count();
    const auto live_docs_count = reader->live_docs_count();

    auto data = std::make_shared<InvertedIndexSnapshot>(
      std::move(reader), std::move(engine_snapshot));
    StoreInvertedIndexSnapshot(data);

    auto& subscription =
      basics::downCast<LowerBoundSubscription>(*_flush_subscription);
    subscription.tick(_last_committed_tick);

    UpdateStatsUnsafe(std::move(data));

    SDB_DEBUG(SEARCH, "successful sync of Search index '", GetId().id(),
              "', segments '", reader_size, "', docs count '", docs_count,
              "', live docs count '", live_docs_count,
              "', last operation tick '", _last_committed_tick, "'");
  } catch (const basics::Exception& e) {
    return {e.code(), "caught exception while committing Search index '",
            GetId().id(), "': ", e.message()};
  } catch (const std::exception& e) {
    return {ERROR_INTERNAL, "caught exception while committing Search index '",
            GetId().id(), "': ", e.what()};
  } catch (...) {
    return {ERROR_INTERNAL, "caught exception while committing Search index '",
            GetId().id(), "'"};
  }
  return {};
}

void InvertedIndexShard::FinishCreation() {
  std::lock_guard lock{_commit_mutex};
  if (_phase == Phase::Active) {
    return;
  }
  _phase = Phase::Active;
  FlushFeature::instance().registerFlushSubscription(_flush_subscription);
}

void InvertedIndexShard::RecoveryCommit(Tick tick) {
  _last_committed_tick = tick;
  auto& subscription =
    basics::downCast<LowerBoundSubscription>(*_flush_subscription);
  subscription.tick(_last_committed_tick);
}

InvertedIndexShard::Stats InvertedIndexShard::GetStats() const {
  if (_metric_stats) {
    return _metric_stats->load();
  }
  auto snapshot = GetInvertedIndexSnapshot();
  if (!snapshot) {
    return {};
  }
  return UpdateStatsUnsafe(std::move(snapshot));
}

}  // namespace sdb::search
