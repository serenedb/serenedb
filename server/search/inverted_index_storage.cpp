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
#include "search/task.h"
#include "search/tick_domain.h"
#include "search/wal_recovery.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

bool ReadSegmentMeta(irs::bytes_view payload, Tick& tick,
                     int64_t& iceberg_snapshot_id,
                     uint64_t& wal_cursor) noexcept {
  // [tick:8][iceberg_snapshot_id:8][wal_cursor:8].
  constexpr size_t kSize = 3 * sizeof(uint64_t);
  if (payload.size() != kSize) {
    return false;
  }
  tick = absl::big_endian::Load64(payload.data());
  iceberg_snapshot_id = static_cast<int64_t>(
    absl::big_endian::Load64(payload.data() + sizeof(uint64_t)));
  wal_cursor = absl::big_endian::Load64(payload.data() + 2 * sizeof(uint64_t));
  return true;
}

}  // namespace

std::filesystem::path InvertedIndexStorage::GetPath(ObjectId db_id,
                                                    ObjectId schema_id,
                                                    ObjectId table_id,
                                                    ObjectId index_id,
                                                    ObjectId storage_id) {
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
  if (storage_id.isSet()) {
    SDB_ASSERT(index_id.isSet());
    path /= absl::StrCat(storage_id);
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
  : _index_id{index.GetId()},
    _search{GetSearchEngine()},
    _state{std::make_shared<ThreadPoolState>()} {
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
    GetPath(db_id, schema_id, index.GetRelationId(), index_id, GetId());
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
  writer_options.lock_repository = false;  // single-process server owns the dir
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
    [&](irs::field_id id) -> irs::NormColumnOptions {
    const auto* entry = index.FindEntry(id);
    SDB_ASSERT(entry != nullptr, ERROR_INTERNAL,
               "norm callback for unknown id: ", id);
    SDB_ASSERT(irs::field_limits::valid(entry->synthetic_column),
               "norm callback fired without a catalog reservation; id: ", id);
    SDB_ASSERT(entry->features.HasFeatures(irs::IndexFeatures::Norm),
               "norm callback fired but catalog features lack Norm; id: ", id);
    return {
      .id = entry->synthetic_column,
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
    // Durable WAL cursor: the max committed store-WAL position captured before
    // this refresh's flush watermark (see RefreshUnsafeImpl). Recovery replays
    // only operations past it.
    uint64_t cursor_be = absl::big_endian::FromHost(_pending_wal_cursor);
    out.append(reinterpret_cast<const irs::byte_type*>(&cursor_be),
               sizeof(cursor_be));
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
  _snapshot = std::make_shared<InvertedIndexSnapshot>(std::move(reader));
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

void InvertedIndexStorage::ScheduleCompaction(absl::Duration delay) {
  CompactionTask task{shared_from_this(), [] { return /* TODO */ true; }};

  _state->pending_compactions.fetch_add(1, std::memory_order_release);
  std::move(task).Schedule(delay).Detach();
}

void InvertedIndexStorage::ScheduleRefresh(absl::Duration delay) {
  RefreshTask task{shared_from_this(), false};

  _state->pending_refreshes.fetch_add(1, std::memory_order_release);
  std::move(task).Schedule(delay).Detach();
}

void InvertedIndexStorage::Refresh() {
  RefreshResult code = RefreshResult::Undefined;
  std::ignore = RefreshUnsafe(/*wait=*/true, nullptr, code);
}

InvertedIndexStorage::Stats InvertedIndexStorage::UpdateStatsUnsafe(
  InvertedIndexSnapshotPtr inverted_index_snapshot) const {
  SDB_ASSERT(inverted_index_snapshot);
  auto& reader = inverted_index_snapshot->reader;
  SDB_ASSERT(reader);
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
  return stats;
}

InvertedIndexStorage::ResultWithTime InvertedIndexStorage::CleanupUnsafe() {
  auto begin = std::chrono::steady_clock::now();
  auto result = CleanupUnsafeImpl();
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
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
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction) {
  auto begin = std::chrono::steady_clock::now();
  auto result = CompactUnsafeImpl(policy, progress, empty_compaction);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  return {std::move(result), time_ms};
}

InvertedIndexStorage::ResultWithTime InvertedIndexStorage::RefreshUnsafe(
  bool wait, const irs::ProgressReportCallback& progress, RefreshResult& code) {
  auto begin = std::chrono::steady_clock::now();
  auto result = RefreshUnsafeImpl(wait, progress, code);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();

  SDB_IF_FAILURE("Search::FailOnCommit") { result.reset(ERROR_DEBUG); }
  SDB_IF_FAILURE("Search::CrashAfterCommit") { SDB_IMMEDIATE_ABORT(); }

  return {std::move(result), time_ms};
}

Result InvertedIndexStorage::CompactUnsafeImpl(
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

Result InvertedIndexStorage::RefreshUnsafeImpl(
  bool wait, const irs::ProgressReportCallback& progress, RefreshResult& code) {
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

    // Capture the durable WAL cursor BEFORE the flush watermark. Read the store
    // WAL position (generation + byte offset) directly: RegisterSearchFlush (at
    // feed time) guarantees this refresh waits for every transaction whose WAL
    // bytes are already written, so everything at/below this offset is flushed
    // here -> a safe durable cursor. Conservative: registered transactions with
    // a higher offset get flushed too but aren't claimed, and the recovery
    // boundary overlap is absorbed idempotently by FinishReplay's
    // delete-then-insert.
    if (auto store =
          duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
            .GetDatabase(std::string{catalog::kStoreDatabaseName})) {
      auto& sm = store->GetStorageManager();
      _pending_wal_cursor = PackWalCursor(
        sm.GetBlockManager().GetCheckpointIteration(), sm.GetWALSize());
    }
    const auto before_refresh = TickDomain::Instance().Current();
    SDB_ASSERT(_last_durable_tick <= before_refresh);
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
  auto snapshot = GetInvertedIndexSnapshot();
  if (!snapshot) {
    return {};
  }
  return UpdateStatsUnsafe(std::move(snapshot));
}

}  // namespace sdb::search
