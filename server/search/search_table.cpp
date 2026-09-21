////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "search/search_table.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <chrono>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <mutex>
#include <system_error>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "connector/column_id.h"
#include "search/inverted_index_storage.h"
#include "search/scorer_options.h"
#include "search/task.h"
#include "server/utils/lifecycle.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

std::filesystem::path SearchTable::GetPath(duckdb::idx_t db_id,
                                           duckdb::idx_t schema_id,
                                           duckdb::idx_t table_id) {
  SDB_ASSERT(db_id != 0);
  SDB_ASSERT(schema_id != 0);
  SDB_ASSERT(table_id != 0);
  // Same on-disk layout as an inverted index minus the trailing index level --
  // reuse its path generator with the index unset.
  // TODO(Dronplane): unify as generic SearchStorage with all common stuff
  return InvertedIndexStorage::GetPath(db_id, schema_id, table_id,
                                       /*index_id=*/0);
}

std::filesystem::path SearchTable::GetWalPath(duckdb::idx_t db_id) {
  SDB_ASSERT(db_id != 0);
  auto path = GetSearchEngine().GetPersistedPath(db_id);
  path /= "wal";
  return path;
}

std::filesystem::path SearchTable::GetChunkDir(duckdb::idx_t db_id,
                                               duckdb::idx_t table_id) {
  SDB_ASSERT(table_id != 0);
  auto path = GetWalPath(db_id);
  path /= "chunks";
  path /= std::to_string(table_id);
  return path;
}

SearchTable::SearchTable(duckdb::idx_t db_id, duckdb::idx_t schema_id,
                         duckdb::idx_t table_id, bool is_new,
                         const catalog::SearchTableOptions& options)
  : _table_id{table_id},
    _db_id{db_id},
    _schema_id{schema_id},
    _is_new{is_new},
    _segment_memory_max{options.segment_memory_max},
    _row_group_size{options.row_group_size} {
  if (!options.optimize_top_k.empty()) {
    _topk_options = ParseScorerExpression(nullptr, options.optimize_top_k);
    _topk_scorer = MakeScorer(*_topk_options);
  }
  RebuildConfig();
  OpenWriter();

  _maint_settings.refresh_interval_msec = options.refresh_interval_ms;
  _maint_settings.compaction_interval_msec = options.compaction_interval_ms;
  _maint_settings.cleanup_interval_step = options.cleanup_interval_step;
}

SearchTable::~SearchTable() {
  _writer.reset();
  _dir.reset();
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  if (!lifecycle::IsStopping()) {
    GetSearchEngine().GetDbWal(_db_id).DeregisterShard(_table_id);
  }
  RemoveDroppedStorageDir(GetChunkDir(_db_id, _table_id), 2);
  RemoveDroppedStorageDir(GetPath(_db_id, _schema_id, _table_id), 2);
}

void SearchTable::OpenWriter() {
  auto path = GetPath(_db_id, _schema_id, GetTableId());

  std::error_code ec;
  bool path_exists = std::filesystem::exists(path, ec);
  if (ec) {
    THROW_SQL_ERROR(ERR_MSG("Failed to check existence of path '",
                            path.string(),
                            "' while initializing search table for table ",
                            GetTableId(), ": ", ec.message()));
  }
  if (!path_exists) {
    std::filesystem::create_directories(path, ec);
    if (ec) {
      THROW_SQL_ERROR(ERR_MSG("Failed to create directory '", path.string(),
                              "' while initializing search table for table ",
                              GetTableId(), ": ", ec.message()));
    }
  }

  auto codec = irs::formats::Get("1_5simd");
  const bool reopen = path_exists && !_is_new;
  const auto open_mode = reopen
                           ? (irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate)
                           : irs::OpenMode::kOmCreate;

  irs::ResourceManagementOptions resource_manager;
  _dir = std::make_unique<irs::MMapDirectory>(path, irs::DirectoryAttributes{},
                                              resource_manager);

  irs::IndexWriterOptions writer_options;
  writer_options.segment_memory_max = _segment_memory_max;
  writer_options.lock_repository = false;
  writer_options.db = &irs::DuckDBEngine::Instance().instance();
  writer_options.reader_options.db = writer_options.db;
  if (_topk_scorer) {
    writer_options.reader_options.scorer = _topk_scorer.get();
  }

  writer_options.meta_payload_provider = [this](uint64_t tick,
                                                irs::bstring& out) {
    _last_committed_tick = std::max(_last_committed_tick, tick);
    uint64_t tick_be = absl::big_endian::FromHost(_last_committed_tick);
    out.append(reinterpret_cast<const irs::byte_type*>(&tick_be),
               sizeof(tick_be));
    return true;
  };

  _writer = irs::IndexWriter::Make(*_dir, codec, open_mode, writer_options);

  auto& db_manager =
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance());
  const auto claim = [&](irs::field_id id) {
    if (id <= connector::kMaxRealColumnIdValue) {
      db_manager.ClaimOid(id);
    }
  };
  for (const auto& segment : _writer->GetSnapshot()) {
    for (const auto id : segment.field_ids()) {
      claim(id);
    }
    if (const auto* columns = segment.GetColReader()) {
      for (const auto& column : columns->Columns()) {
        claim(column->Id());
      }
    }
  }

  if (reopen) {
    auto reader = _writer->GetSnapshot();
    auto payload = irs::GetPayload(reader.Meta().index_meta);
    if (payload.size() >= sizeof(uint64_t)) {
      _last_committed_tick = absl::big_endian::Load64(payload.data());
    }
  }

  _wal = &GetSearchEngine().GetDbWal(_db_id);

  if (_is_new) {
    // A brand-new shard has no WAL records, so seed its committed tick at the
    // database WAL's current tick (not 0) -- otherwise an unused table would
    // pin the shared WAL's GC floor.
    _last_committed_tick = _wal->CurrentTick();
  }
  _wal->RegisterShard(GetTableId(), _last_committed_tick);

  if (_is_new) {
    _writer->RefreshCommit();
  }
}

void SearchTable::StartTasks() {
#ifdef SDB_DEV
  const bool already = _tasks_started.exchange(true);
  SDB_ASSERT(!already, "SearchTable::StartTasks called twice for table ",
             GetTableId());
#endif
  // Launch this table's refresh + compaction loops on the shared background
  // scheduler. Called only after recovery or CREATE/CTAS finalize, so a
  // background commit's WAL GC never races replay.
  GetSearchEngine().StartTasks(shared_from_this());
}

ResultWithTime SearchTable::RefreshUnsafe(
  bool wait, const irs::ProgressReportCallback& /*progress*/,
  RefreshResult& code) {
  const auto begin = std::chrono::steady_clock::now();
  code = RefreshResult::NoChanges;
  auto result = absl::OkStatus();
  try {
    std::unique_lock<absl::Mutex> lock{_refresh_mutex, std::try_to_lock};
    if (!lock.owns_lock()) {
      if (wait) {
        lock.lock();
      } else {
        code = RefreshResult::InProgress;  // another refresh/VACUUM is running
      }
    }
    if (lock.owns_lock()) {
      // Snapshot the WAL tick before publishing: a RefreshCommit that reports
      // no changes proves this shard has nothing un-published up to that tick,
      // and any later batch lands at a higher tick, so advancing to it never
      // over-claims.
      const auto tick_before = _wal->CurrentTick();
      if (_writer->RefreshCommit()) {
        _wal->OnShardCommit(GetTableId(), _last_committed_tick);
        code = RefreshResult::Done;
      } else {
        _wal->OnShardCommit(GetTableId(), tick_before);
      }
    }
  } catch (const std::exception& e) {
    result = absl::InternalError(absl::StrCat(
      "refresh failed for search table ", GetTableId(), ": ", e.what()));
  }
  const uint64_t time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count();
  SDB_IF_FAILURE("Search::FailOnCommit") {
    result = absl::InternalError("debug failure point");
  }
  _maintenance.RecordCommit(result, code, time_ms);
  return {std::move(result), time_ms};
}

StoreStats SearchTable::GetStats() const {
  auto stats = StoreStats::FromReader(_writer->GetSnapshot());
  stats.numBufferedDocs = _writer->BufferedDocs();
  _maintenance.Fill(stats);
  return stats;
}

unsigned SearchTable::RegisterWriter() { return _writers.Register(); }

void SearchTable::DeregisterWriter(unsigned slot) noexcept {
  _writers.Deregister(slot);
}

void SearchTable::DrainPriorWriters(absl::FunctionRef<bool()> cancelled) {
  SDB_ASSERT(_build_in_flight.load(std::memory_order_acquire),
             "DrainPriorWriters requires a held BuildClaim");
  if (!_writers.Drain(cancelled, kWriterWaitPoll)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_QUERY_CANCELED),
      ERR_MSG("canceled while waiting for write transactions "
              "on search table ",
              _table_id, " that started before the index was declared"));
  }
}

void SearchTable::OpenDeleteLog() {
  absl::MutexLock lock{&_delete_log_mutex};
  _delete_log.clear();
  _delete_log_open.store(true, std::memory_order_release);
}

void SearchTable::AppendDeleteLog(std::span<const int64_t> rows) {
  if (rows.empty()) {
    return;
  }
  absl::MutexLock lock{&_delete_log_mutex};
  if (!_delete_log_open.load(std::memory_order_relaxed)) {
    return;
  }
  _delete_log.insert(_delete_log.end(), rows.begin(), rows.end());
}

std::vector<int64_t> SearchTable::TakeDeleteLog() {
  absl::MutexLock lock{&_delete_log_mutex};
  return std::exchange(_delete_log, {});
}

void SearchTable::CloseDeleteLog() {
  absl::MutexLock lock{&_delete_log_mutex};
  _delete_log_open.store(false, std::memory_order_release);
  _delete_log.clear();
}

std::shared_ptr<const catalog::InvertedIndexConfig> SearchTable::Config()
  const {
  std::shared_lock lock(_table_lock);
  return _config;
}

void SearchTable::RebuildConfig() {
  auto merged = std::make_shared<catalog::InvertedIndexConfig>();
  merged->pk = {.index_term = true, .column = catalog::PkColumnKind::None};
  merged->top_k_scorer = _topk_options;
  merged->row_group_size = _row_group_size;
  for (const auto& index : _configs) {
    for (const auto& [id, field] : index.config->fields) {
      merged->fields.emplace(id, field);
    }
    merged->keys.insert(merged->keys.end(), index.config->keys.begin(),
                        index.config->keys.end());
  }
  _config = std::move(merged);
}

void SearchTable::MergeIndexConfig(
  duckdb::idx_t index_oid,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  std::unique_lock lock(_table_lock);
  _configs.push_back({index_oid, std::move(config)});
  RebuildConfig();
}

void SearchTable::RemoveIndexConfig(duckdb::idx_t index_oid) {
  std::unique_lock lock(_table_lock);
  std::erase_if(
    _configs, [&](const IndexConfig& index) { return index.oid == index_oid; });
  RebuildConfig();
}

auto SearchTable::CompactUnsafeAsync(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env)
  -> yaclib::Future<ResultWithTime> {
  const auto begin = std::chrono::steady_clock::now();
  empty_compaction = false;
  auto result = absl::OkStatus();
  try {
    // iresearch serializes Compact against refresh/DML internally, so a long
    // merge never blocks the refresh chain.
    const auto res = co_await _writer->CompactAsync(policy, field_options,
                                                    nullptr, progress, env);
    if (!res) {
      result = absl::InternalError(
        absl::StrCat("compaction failed for search table ", GetTableId()));
    } else {
      empty_compaction = (res.size == 0);  // nothing merged -> idle round
    }
  } catch (const std::exception& e) {
    result = absl::InternalError(absl::StrCat(
      "consolidation failed for search table ", GetTableId(), ": ", e.what()));
  }
  const uint64_t time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count();
  _maintenance.RecordCompaction(result, empty_compaction, time_ms);
  co_return ResultWithTime{std::move(result), time_ms};
}

ResultWithTime SearchTable::CleanupUnsafe() {
  const auto begin = std::chrono::steady_clock::now();
  auto result = absl::OkStatus();
  try {
    irs::directory_utils::RemoveAllUnreferenced(*_dir);
  } catch (const std::exception& e) {
    result = absl::InternalError(absl::StrCat(
      "cleanup failed for search table ", GetTableId(), ": ", e.what()));
  }
  const uint64_t time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count();
  _maintenance.RecordCleanup(result, time_ms);
  return {std::move(result), time_ms};
}

void SearchTable::VacuumRefresh() {
  RefreshResult code = RefreshResult::Undefined;
  RefreshUnsafe(/*wait=*/true, nullptr, code);
  CleanupUnsafe();
}

void SearchTable::VacuumCompact() {
  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  static const irs::MergeWriter::FlushProgress kProgress = [] { return true; };
  RefreshResult code = RefreshResult::Undefined;
  RefreshUnsafe(/*wait=*/true, nullptr, code);
  bool empty = false;
  const auto field_options = Config();
  CompactUnsafe(kFullMerge, kProgress, empty, field_options.get());
  if (!empty) {
    RefreshUnsafe(/*wait=*/true, nullptr, code);
  }
  CleanupUnsafe();
}

}  // namespace sdb::search
