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

#include <absl/cleanup/cleanup.h>
#include <absl/time/time.h>

#include <chrono>
#include <filesystem>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/fs_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <memory>
#include <system_error>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "metrics/gauge.h"
#include "metrics/guard.h"
#include "rest_server/flush_feature.h"
#include "rest_server/serened_single.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_recovery_manager.h"
#include "search/task.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/search_engine.h"
#include "vpack/serializer.h"

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

bool ReadTick(irs::bytes_view payload, Tick& tick) noexcept {
  // Payload format: [tick:8]
  constexpr size_t kExpectedSize = sizeof(uint64_t);
  if (payload.size() != kExpectedSize) {
    return false;
  }
  tick = absl::big_endian::Load64(payload.data());
  return true;
}

}  // namespace

std::filesystem::path InvertedIndexShard::GetPath(ObjectId db, ObjectId schema,
                                                  ObjectId id) {
  std::filesystem::path path = GetSearchEngine().GetPersistedPath(db);
  path /= absl::StrCat(schema);
  path /= absl::StrCat(id);
  return path;
}

std::shared_ptr<InvertedIndexShard> InvertedIndexShard::Create(
  const catalog::InvertedIndex& index, InvertedIndexShardOptions options,
  bool is_new) {
  auto shard =
    std::make_shared<InvertedIndexShard>(PrivateTag{}, index, options, is_new);
  // TODO(Dronpane) use actual is_new value when indexing of existing table
  // would be implemented
  shard->InitPostRecovery(false);
  return shard;
}

InvertedIndexShard::InvertedIndexShard(PrivateTag,
                                       const catalog::InvertedIndex& index,
                                       InvertedIndexShardOptions options,
                                       bool is_new)
  : IndexShard{index},
    _engine{GetServerEngine()},
    _search{GetSearchEngine()},
    _state{std::make_shared<ThreadPoolState>()},
    _options{std::move(options)} {
  _tasks_settings.commit_interval_msec = _options.commit_interval_ms;
  _tasks_settings.consolidation_interval_msec =
    _options.consolidation_interval_ms;
  _tasks_settings.cleanup_interval_step = 10;
  auto& server = SerenedServer::Instance();

  const auto db_id = index.GetDatabaseId();
  const auto schema_id = index.GetSchemaId();
  const auto index_id = index.GetId();
  SDB_ASSERT(index_id.isSet());
  std::filesystem::path path = _search.GetPersistedPath(db_id);
  path /= absl::StrCat(schema_id);
  path /= absl::StrCat(index_id);
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
  resource_manager.consolidations = _consolidations_memory;
  resource_manager.file_descriptors = _file_descriptors_count;
  resource_manager.cached_columns =
    &GetSearchEngine().getCachedColumnsManager();
  _dir = std::make_unique<irs::MMapDirectory>(path, irs::DirectoryAttributes{},
                                              resource_manager);

  irs::IndexWriterOptions writer_options;
  writer_options.segment_memory_max = 256 * (size_t{1} << 20);  // 256MB
  writer_options.lock_repository = false;  // RocksDB has its own lock

  writer_options.meta_payload_provider = [this](uint64_t tick,
                                                irs::bstring& out) {
    if (_is_creation) {
      tick = _engine.currentTick();
    }
    _last_committed_tick = std::max(_last_committed_tick, tick);

    // Write payload: [tick:8]
    tick = absl::big_endian::FromHost(_last_committed_tick);

    out.append(reinterpret_cast<const irs::byte_type*>(&tick), sizeof(tick));
    return true;
  };

  _writer = irs::IndexWriter::Make(*_dir, codec, open_mode, writer_options);

  if (!path_exists) {
    // Initialize empty index
    _writer->Commit();
  }

  auto reader = _writer->GetSnapshot();
  SDB_ASSERT(reader);

  // For existing index, read recovery tick from persisted payload
  if (path_exists) {
    auto payload = irs::GetPayload(reader.Meta().index_meta);
    if (!payload.empty()) {
      if (!ReadTick(payload, _recovery_tick)) {
        SDB_WARN("xxxxx", Logger::SEARCH,
                 "Failed to read recovery tick from inverted index '",
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

  if (!server.hasFeature<RocksDBRecoveryManager>()) {
    return;
  }
}

void InvertedIndexShard::InitPostRecovery(bool is_new) {
  auto& server = SerenedServer::Instance();
  auto res =
    server.getFeature<RocksDBRecoveryManager>().registerPostRecoveryCallback(
      [weak_self = weak_from_this(), is_new]() -> Result {
        auto self = weak_self.lock();
        if (!self) {
          // Index was dropped during recovery
          return {};
        }

        SDB_ASSERT(!self->_engine.inRecovery());

        const auto recovery_tick = self->_engine.recoveryTick();

        // Check for out-of-sync condition
        if (self->_recovery_tick > recovery_tick) {
          SDB_WARN("xxxxx", Logger::SEARCH, "Inverted index '",
                   self->GetId().id(), "' is recovered at tick ",
                   self->_recovery_tick, " greater than storage engine tick ",
                   recovery_tick,
                   ", it seems WAL tail was lost and index is out "
                   "of sync");

          if (self->SetOutOfSync()) {
            self->MarkOutOfSyncUnsafe();
          }
        }

        // Register flush subscription if we are loading existing index
        // If not finishCreation would be called later when indexing finishes
        if (!is_new) {
          self->FinishCreation();
        }

        // Start background maintenance tasks
        self->StartTasks();

        return {};
      });
  if (res.fail()) {
    SDB_THROW(std::move(res));
  }
}

void InvertedIndexShard::WriteInternal(vpack::Builder& builder) const {
  vpack::WriteTuple(builder, _options);
}

Snapshot InvertedIndexShard::GetSnapshot() const {
  if (FailQueriesOnOutOfSync() && IsOutOfSync()) {
    SDB_THROW(ERROR_CLUSTER_AQL_COLLECTION_OUT_OF_SYNC, "Search index '",
              GetId(), "' is out of sync and needs to be recreated");
  }

  return {shared_from_this(), GetInvertedIndexSnapshot()};
}

void InvertedIndexShard::ScheduleConsolidation(absl::Duration delay) {
  ConsolidationTask task{shared_from_this(), [self = shared_from_this()] {
                           return /* TODO */ true;
                         }};

  _state->pending_consolidations.fetch_add(1, std::memory_order_release);
  std::move(task).Schedule(delay).Detach();
}

void InvertedIndexShard::ScheduleCommit(absl::Duration delay) {
  CommitTask task{shared_from_this()};

  _state->pending_commits.fetch_add(1, std::memory_order_release);
  std::move(task).Schedule(delay).Detach();
}

yaclib::Future<> InvertedIndexShard::CommitWait() {
  CommitTask task{shared_from_this()};
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

InvertedIndexShard::ResultWithTime InvertedIndexShard::ConsolidateUnsafe(
  const irs::ConsolidationPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_consolidation) {
  auto begin = std::chrono::steady_clock::now();
  auto result = ConsolidateUnsafeImpl(policy, progress, empty_consolidation);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  if (bool ok = result.ok(); ok && _avg_consolidation_time_ms != nullptr) {
    _avg_consolidation_time_ms->store(
      ComputeAvg(_consolidation_time_num, time_ms), std::memory_order_relaxed);
  } else if (!ok && _num_failed_consolidations != nullptr) {
    _num_failed_consolidations->fetch_add(1, std::memory_order_relaxed);
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

  if (result.fail() && SetOutOfSync()) {
    try {
      MarkOutOfSyncUnsafe();
    } catch (const std::exception& e) {
      // We couldn't persist the outOfSync flag,
      // but we can't mark the inverted index shard as "not outOfSync" again.
      // Not much we can do except logging.
      SDB_WARN("xxxxx", Logger::SEARCH,
               "failed to store 'outOfSync' flag for Search index '",
               GetId().id(), "': ", e.what());
    }
  }

  if (bool ok = result.ok(); !ok && _num_failed_commits != nullptr) {
    _num_failed_commits->fetch_add(1, std::memory_order_relaxed);
  } else if (ok && code == CommitResult::Done &&
             _avg_commit_time_ms != nullptr) {
    _avg_commit_time_ms->store(ComputeAvg(_commit_time_num, time_ms),
                               std::memory_order_relaxed);
  }
  return {std::move(result), time_ms};
}

Result InvertedIndexShard::ConsolidateUnsafeImpl(
  const irs::ConsolidationPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_consolidation) {
  empty_consolidation = false;

  if (!policy) {
    return {ERROR_BAD_PARAMETER,
            "unset consolidation policy while executing consolidation policy "
            "on Search index '",
            GetId().id(), "'"};
  }

  try {
    const auto res = _writer->Consolidate(policy, nullptr, progress);
    if (!res) {
      return {ERROR_INTERNAL,
              "failure while executing consolidation policy on Search index '",
              GetId().id(), "'"};
    }

    empty_consolidation = (res.size == 0);
  } catch (const std::exception& e) {
    return {ERROR_INTERNAL,
            "caught exception while executing consolidation policy ",
            "on Search index '",
            GetId().id(),
            "': ",
            e.what()};
  } catch (...) {
    return {ERROR_INTERNAL,
            "caught exception while executing consolidation policy ",
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
        SDB_TRACE("xxxxx", Logger::SEARCH, "Commit for Search index '",
                  GetId().id(), "' is already in progress, skipping");

        code = CommitResult::InProgress;
        return {};
      }

      SDB_TRACE("xxxxx", Logger::SEARCH, "Commit for Search index '",
                GetId().id(), "' is already in progress, waiting");

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
    const bool were_changes = _writer->Commit({
      .tick = _is_creation ? irs::writer_limits::kMaxTick : before_commit,
      .progress = progress,
      .reopen_columnstore = /* TODO(codeworse) */ false,
    });
    // get new reader
    auto reader = _writer->GetSnapshot();
    SDB_ASSERT(reader != nullptr);
    std::move(commit_guard).Cancel();
    if (!were_changes) {
      SDB_TRACE("xxxxx", Logger::SEARCH, "Commit for Search index '",
                GetId().id(), "' is no changes, tick ", before_commit, "'");
      _last_committed_tick = before_commit;
      // no changes, can release the latest tick before commit
      auto& subscription =
        basics::downCast<LowerBoundSubscription>(*_flush_subscription);
      subscription.tick(_last_committed_tick);
      StoreInvertedIndexSnapshot(std::make_shared<InvertedIndexSnapshot>(
        std::move(reader), std::move(engine_snapshot)));
      return {};
    }
    SDB_ASSERT(_is_creation || _last_committed_tick == before_commit);
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

    SDB_DEBUG("xxxxx", Logger::SEARCH, "successful sync of Search index '",
              GetId().id(), "', segments '", reader_size, "', docs count '",
              docs_count, "', live docs count '", live_docs_count,
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

bool InvertedIndexShard::SetOutOfSync() noexcept {
  SDB_ASSERT(!ServerState::instance()->IsCoordinator());
  auto error = _error.load(std::memory_order_relaxed);
  return error == Error::NoError &&
         _error.compare_exchange_strong(error, Error::OutOfSync,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed);
}

void InvertedIndexShard::MarkOutOfSyncUnsafe() {
  _search.trackOutOfSyncLink();

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto snapshot = catalog.Local().GetSnapshot();
  auto c = snapshot->GetObject<catalog::Table>(GetId());
  auto shard = snapshot->GetTableShard(GetId());
  if (!c) {
    return;
  }

  _engine.ChangeTable(*c, *shard);
}

bool InvertedIndexShard::IsOutOfSync() const noexcept {
  return _error.load(std::memory_order_relaxed) == Error::OutOfSync;
}

bool InvertedIndexShard::FailQueriesOnOutOfSync() const noexcept {
  return _search.failQueriesOnOutOfSync();
}

void InvertedIndexShard::FinishCreation() {
  std::lock_guard lock{_commit_mutex};
  if (std::exchange(_is_creation, false)) {
    auto& server = SerenedServer::Instance();
    if (server.hasFeature<FlushFeature>()) {
      server.getFeature<FlushFeature>().registerFlushSubscription(
        _flush_subscription);
    }
  }
}

void InvertedIndexShard::RecoveryCommit(Tick tick) {
  _last_committed_tick = tick;
  auto& subscription =
    basics::downCast<LowerBoundSubscription>(*_flush_subscription);
  subscription.tick(_last_committed_tick);
}

}  // namespace sdb::search
