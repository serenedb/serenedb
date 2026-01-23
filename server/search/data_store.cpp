#include "search/data_store.h"

#include <absl/cleanup/cleanup.h>

#include <iresearch/store/fs_directory.hpp>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "metrics/gauge.h"
#include "metrics/guard.h"
#include "search/task.h"
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
}  // namespace

DataStore::DataStore(const DataStoreOptions& options)
  : _options(options),
    _dir{std::make_unique<irs::FSDirectory>(_options.path)} {}

Snapshot DataStore::GetSnapshot() const {
  if (FailQueriesOnOutOfSync() && IsOutOfSync()) {
    SDB_THROW(ERROR_CLUSTER_AQL_COLLECTION_OUT_OF_SYNC,
              absl::StrCat("Search index '", GetId(),
                           "' is out of sync and needs to be recreated"));
  }

  return {shared_from_this(), GetDataSnapshot()};
}

void DataStore::ScheduleCommit(absl::Duration delay) {
  CommitTask task{GetIndexId(), shared_from_this(), _state};

  _state->pending_commits.fetch_add(1, std::memory_order_release);
  task.Schedule(delay);
}

void DataStore::ScheduleConsolidation(absl::Duration delay) {
  ConsolidationTask task{
    GetIndexId(), shared_from_this(), _state,
    [self = shared_from_this()] { return /* TODO */ true; }};

  _state->pending_consolidations.fetch_add(1, std::memory_order_release);
  task.Schedule(delay);
}

DataStore::Stats DataStore::UpdateStatsUnsafe(
  DataSnapshotPtr data_snapshot) const {
  SDB_ASSERT(data_snapshot);
  auto& reader = data_snapshot->reader;
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
  stats.numFiles = 1 + stats.numSegments;  // +1 for segments file
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

DataStore::ResultWithTime DataStore::CleanupUnsafe() {
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

Result DataStore::CleanupUnsafeImpl() {
  try {
    irs::directory_utils::RemoveAllUnreferenced(*_dir);
  } catch (const std::exception& e) {
    return {ERROR_INTERNAL,
            absl::StrCat("caught exception while cleaning up Search index '",
                         GetId().id(), "': ", e.what())};
  } catch (...) {
    return {ERROR_INTERNAL,
            absl::StrCat("caught exception while cleaning up Search index '",
                         GetId().id(), "'")};
  }
  return {};
}

DataStore::ResultWithTime DataStore::CommitUnsafe(
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
      // but we can't mark the data store as "not outOfSync" again.
      // Not much we can do except logging.
      SDB_WARN("xxxxx", Logger::SEARCH,
               "failed to store 'outOfSync' flag for Search index '", GetId(),
               "': ", e.what());
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

Result DataStore::CommitUnsafeImpl(bool wait,
                                   const irs::ProgressReportCallback& progress,
                                   CommitResult& code) {
  code = CommitResult::NoChanges;

  try {
    std::unique_lock commit_lock{_commit_mutex, std::try_to_lock};
    if (!commit_lock.owns_lock()) {
      if (!wait) {
        SDB_TRACE("xxxxx", Logger::SEARCH, "Commit for Search index '", GetId(),
                  "' is already in progress, skipping");

        code = CommitResult::InProgress;
        return {};
      }

      SDB_TRACE("xxxxx", Logger::SEARCH, "Commit for Search index '", GetId(),
                "' is already in progress, waiting");

      commit_lock.lock();
    }

    auto engine_snapshot = _engine->currentSnapshot();
    if (!engine_snapshot) [[unlikely]] {
      return {ERROR_INTERNAL,
              absl::StrCat("Failed to get engine snapshot while committing "
                           "Search index '",
                           GetId().id(), "'")};
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
      .reopen_columnstore = /* TODO */ {},
    });

    auto reader = _writer->GetSnapshot();
    SDB_ASSERT(reader != nullptr);
    std::move(commit_guard).Cancel();
    // auto& subscription =
    //   basics::downCast<LowerBoundSubscription>(*_flush_subscription);
    if (!were_changes) {
      SDB_TRACE("xxxxx", Logger::SEARCH, "Commit for Search index '", GetId(),
                "' is no changes, tick ", before_commit, "'");
      _last_committed_tick = before_commit;
      // no changes, can release the latest tick before commit
      // subscription.tick(_last_committed_tick);
      // TODO(mbkkt) make_shared can throw!
      StoreDataSnapshot(std::make_shared<DataSnapshot>(
        std::move(reader), std::move(engine_snapshot)));
      return {};
    }
    SDB_ASSERT(_is_creation || _last_committed_tick == before_commit);
    code = CommitResult::Done;

    SDB_ASSERT(GetSnapshot().GetDirectoryReader() != reader);
    const auto reader_size = reader->size();
    const auto docs_count = reader->docs_count();
    const auto live_docs_count = reader->live_docs_count();

    auto data = std::make_shared<DataSnapshot>(std::move(reader),
                                               std::move(engine_snapshot));
    StoreDataSnapshot(data);

    // subscription.tick(_last_committed_tick);

    UpdateStatsUnsafe(std::move(data));

    SDB_DEBUG("xxxxx", Logger::SEARCH, "successful sync of Search index '",
              GetId(), "', segments '", reader_size, "', docs count '",
              docs_count, "', live docs count '", live_docs_count,
              "', last operation tick '", _last_committed_tick, "'");
  } catch (const basics::Exception& e) {
    return {e.code(),
            absl::StrCat("caught exception while committing Search index '",
                         GetId().id(), "': ", e.message())};
  } catch (const std::exception& e) {
    return {ERROR_INTERNAL,
            absl::StrCat("caught exception while committing Search index '",
                         GetId().id(), "': ", e.what())};
  } catch (...) {
    return {ERROR_INTERNAL,
            absl::StrCat("caught exception while committing Search index '",
                         GetId().id(), "'")};
  }
  return {};
}

DataStore::ResultWithTime DataStore::ConsolidateUnsafe(
  const DataStoreMeta::ConsolidationPolicy& policy,
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

Result DataStore::ConsolidateUnsafeImpl(
  const DataStoreMeta::ConsolidationPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_consolidation) {
  empty_consolidation = false;  // TODO Why?

  if (!policy.policy()) {
    return {
      ERROR_BAD_PARAMETER,
      absl::StrCat(
        "unset consolidation policy while executing consolidation policy '",
        policy.properties().toString(), "' on Search index '",
        GetIndexId().id(), "'")};
  }

  try {
    const auto res = _writer->Consolidate(policy.policy(), nullptr, progress);
    if (!res) {
      return {ERROR_INTERNAL,
              absl::StrCat("failure while executing consolidation policy '",
                           policy.properties().toString(),
                           "' on Search index '", GetIndexId().id(), "'")};
    }

    empty_consolidation = (res.size == 0);
  } catch (const std::exception& e) {
    return {
      ERROR_INTERNAL,
      absl::StrCat("caught exception while executing consolidation policy '",
                   policy.properties().toString(), "' on Search index '",
                   GetIndexId().id(), "': ", e.what())};
  } catch (...) {
    return {
      ERROR_INTERNAL,
      absl::StrCat("caught exception while executing consolidation policy '",
                   policy.properties().toString(), "' on Search index '",
                   GetIndexId().id(), "'")};
  }
  return {};
}

bool DataStore::SetOutOfSync() noexcept {
  SDB_ASSERT(!ServerState::instance()->IsCoordinator());
  auto error = _error.load(std::memory_order_relaxed);
  return error == Error::NoError &&
         _error.compare_exchange_strong(error, Error::OutOfSync,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed);
}

void DataStore::MarkOutOfSyncUnsafe() {
  // Only once per link:
  // 1. increase metric for number of OutOfSync links
  // 2. persist OutOfSync flag in RocksDB
  // note: if this fails, it will throw an exception
  SDB_ASSERT(_search != nullptr);
  _search->trackOutOfSyncLink();

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto snapshot = catalog.Local().GetSnapshot();
  auto c = snapshot->GetObject<catalog::Table>(GetId());
  auto shard = snapshot->GetTableShard(GetId());
  if (!c) {
    // already deleted
    return;
  }

  _engine->ChangeTable(*c, *shard);
}

bool DataStore::IsOutOfSync() const noexcept {
  // The OutOfSync flag is expected to be set either
  // during the recovery phase, or when a commit goes wrong
  return _error.load(std::memory_order_relaxed) == Error::OutOfSync;
}

bool DataStore::FailQueriesOnOutOfSync() const noexcept {
  return _search->failQueriesOnOutOfSync();
}

}  // namespace sdb::search
