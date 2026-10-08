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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <chrono>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/store/directory_attributes.hpp>
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
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "connector/column_id.h"
#include "connector/search_sink_writer.hpp"
#include "search/inverted_index_storage.h"
#include "search/scorer_options.h"
#include "search/task.h"
#include "search/tick_domain.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

catalog::CompressionByColumn SearchTable::DeclaredCompression(
  const duckdb::ColumnList& columns) {
  catalog::CompressionByColumn compression;
  for (const auto& column : columns.Logical()) {
    if (column.CompressionType() != duckdb::CompressionType::COMPRESSION_AUTO) {
      compression.emplace(column.Oid(), column.CompressionType());
    }
  }
  return compression;
}

namespace {

constexpr duckdb::field_id_t kFieldTick = 0;

}  // namespace

struct SearchTable::ReplaySession {
  struct PendingAdopt {
    std::string meta_file;
    uint64_t queries_before;
  };

  irs::IndexWriter::Transaction trx;
  std::unique_ptr<connector::SearchSinkInsertBaseImpl> sink;
  uint64_t max_tick = 0;
  std::vector<PendingAdopt> adopts;
};

uint64_t SearchTable::ReadCommittedTick(duckdb::BinaryDeserializer& payload) {
  return payload.ReadProperty<uint64_t>(kFieldTick, "tick");
}

SearchTable::SearchTable(std::shared_ptr<catalog::DatabaseDirectory> directory,
                         bool in_memory, duckdb::idx_t table_id, bool is_new,
                         const catalog::SearchTableOptions& options,
                         catalog::CompressionByColumn compression)
  : _table_id{table_id},
    _directory{std::move(directory)},
    _is_new{is_new},
    _segment_memory_max{options.segment_memory_max},
    _row_group_size{options.row_group_size},
    _compression{std::move(compression)} {
  if (!options.optimize_top_k.empty()) {
    _topk_options = ParseScorerExpression(nullptr, options.optimize_top_k);
    _topk_scorer = MakeScorer(*_topk_options);
  }
  RebuildConfig();
  OpenWriter(in_memory);
  ApplyOptions(options);
}

void SearchTable::ApplyOptions(const catalog::SearchTableOptions& options) {
  _maint_settings.refresh_interval_msec = options.refresh_interval_ms;
  _maint_settings.compaction_interval_msec = options.compaction_interval_ms;
  _maint_settings.cleanup_interval_step = options.cleanup_interval_step;
  _maint_settings.compaction_max_segments = options.compaction_max_segments;
  _maint_settings.compaction_max_segments_bytes =
    options.compaction_max_segments_bytes;
  _maint_settings.compaction_floor_segment_bytes =
    options.compaction_floor_segment_bytes;
}

SearchTable::~SearchTable() {
  _replay.reset();
  _writer.reset();
  _dir.reset();
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  catalog::DatabaseDirectory::RemoveStorage(std::move(_directory), _table_id);
}

void SearchTable::OpenWriter(bool in_memory) {
  irs::ResourceManagementOptions resource_manager;
  auto opened = OpenStorageDirectory(*_directory, GetTableId(), _is_new,
                                     in_memory, resource_manager);
  _dir = std::move(opened.directory);
  _absent = opened.absent;

  const bool reopen = opened.on_disk && !_is_new;
  const auto open_mode =
    reopen ? (irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate)
           : irs::OpenMode::kOmCreate;

  irs::IndexWriterOptions writer_options;
  writer_options.segment_memory_max = _segment_memory_max;
  // A shard loaded from disk may hold flushed-but-unpublished segments the WAL
  // references, so Make() must not unlink them; FinishRecovery cleans up once
  // replay is done. A new shard's directory is empty, so it keeps the default.
  writer_options.cleanup_on_open = _is_new;
  writer_options.lock_repository = false;
  writer_options.db = &irs::DuckDBEngine::Instance().instance();
  writer_options.reader_options.db = writer_options.db;
  if (_topk_scorer) {
    writer_options.reader_options.scorer = _topk_scorer.get();
  }

  writer_options.meta_payload_writer = [this](uint64_t tick,
                                              duckdb::BinarySerializer& out) {
    const auto committed = std::max(CommittedTick(), tick);
    _last_committed_tick.store(committed, std::memory_order_release);
    out.WriteProperty<uint64_t>(kFieldTick, "tick", committed);
  };
  writer_options.meta_payload_reader = [this](duckdb::BinaryDeserializer& in) {
    _last_committed_tick.store(ReadCommittedTick(in),
                               std::memory_order_release);
  };

  _writer = irs::IndexWriter::Make(*_dir, open_mode, std::move(writer_options));

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

  if (_is_new) {
    _last_committed_tick.store(TickDomain::Instance().Current(),
                               std::memory_order_release);
    _writer->RefreshCommit();
  }
  TickDomain::Instance().SeedAtLeast(CommittedTick());
}

void SearchTable::StartTasks() {
#ifdef SDB_DEV
  const bool already = _tasks_started.exchange(true);
  SDB_ASSERT(!already, "SearchTable::StartTasks called twice for table ",
             GetTableId());
#endif
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
      _refresh_mutex.AssertHeld();
      SDB_PARK_ONCE_ON_FAILURE("pause_search_refresh_after_tick");
      if (_writer->RefreshCommit()) {
        code = RefreshResult::Done;
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
  _build_truncate_tick.store(0, std::memory_order_relaxed);
  _delete_log_open.store(true, std::memory_order_release);
}

void SearchTable::RecordTruncateForBuild(uint64_t tick) {
  absl::MutexLock lock{&_delete_log_mutex};
  if (!_delete_log_open.load(std::memory_order_relaxed)) {
    return;
  }
  if (_build_truncate_tick.load(std::memory_order_relaxed) < tick) {
    _build_truncate_tick.store(tick, std::memory_order_release);
  }
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
  merged->declared_compression = _compression;
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
  _configs.emplace_back(index_oid, std::move(config));
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
    const auto res =
      co_await _writer->CompactAsync(policy, field_options, progress, env);
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

void SearchTable::VacuumCompact(uint32_t target_segments) {
  static const irs::MergeWriter::FlushProgress kProgress = [] { return true; };
  const auto target = std::max<uint32_t>(1, target_segments);
  const auto field_options = Config();
  RefreshResult code = RefreshResult::Undefined;
  RefreshUnsafe(/*wait=*/true, nullptr, code);
  for (size_t pass = 0; pass < 8; ++pass) {
    std::vector<std::vector<std::string>> buckets(target);
    {
      const auto snapshot = _writer->GetSnapshot();
      if (snapshot.size() <= target) {
        break;
      }
      for (size_t i = 0; i < snapshot.size(); ++i) {
        buckets[i % target].emplace_back(snapshot[i].Meta().name);
      }
    }
    bool merged = false;
    for (auto& names : buckets) {
      const irs::CompactionPolicy bucket =
        [&names](irs::Compaction& candidates, const irs::IndexReader& reader,
                 const irs::CompactingSegments& busy) {
          for (size_t i = 0; i < reader.size(); ++i) {
            const auto& segment = reader[i];
            const auto& name = segment.Meta().name;
            if (!busy.contains(name) && absl::c_linear_search(names, name)) {
              candidates.emplace_back(&segment);
            }
          }
        };
      bool empty = false;
      CompactUnsafe(bucket, kProgress, empty, field_options.get());
      merged |= !empty;
    }
    RefreshUnsafe(/*wait=*/true, nullptr, code);
    if (!merged) {
      break;
    }
  }
  CleanupUnsafe();
}

void SearchTable::Publish() {
  RefreshResult code = RefreshResult::Undefined;
  const auto result = RefreshUnsafe(/*wait=*/true, nullptr, code);
  if (!result.res.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("search table ", _table_id,
                            ": publish failed: ", result.res.message()));
  }
}

SearchTable::ReplaySession* SearchTable::BeginReplay(uint64_t tick) {
  if (tick <= CommittedTick()) {
    return nullptr;
  }
  if (!_replay) {
    _replay = std::make_unique<ReplaySession>(GetTransaction());
  }
  _replay->max_tick = std::max(_replay->max_tick, tick);
  return _replay.get();
}

void SearchTable::ReplayInsert(duckdb::ClientContext& context,
                               duckdb::Catalog& catalog,
                               std::span<const connector::ColumnId> column_ids,
                               uint64_t tick, duckdb::DataChunk& chunk,
                               uint64_t row_start) {
  auto* session = BeginReplay(tick);
  if (!session) {
    return;
  }
  if (!session->sink) {
    session->sink = connector::MakeSearchTableInsertSink(session->trx, *this,
                                                         catalog, context);
  }
  connector::WriteChunkToSearchSink(*session->sink, chunk, column_ids,
                                    row_start, _table_id, context);
}

void SearchTable::ReplayDelete(uint64_t tick, duckdb::DataChunk& chunk) {
  auto* session = BeginReplay(tick);
  if (!session) {
    return;
  }
  auto& rows = chunk.data[0];
  rows.Flatten();
  connector::RemoveGeneratedRows(
    session->trx, std::span<const int64_t>{
                    duckdb::FlatVector::GetData<int64_t>(rows), chunk.size()});
}

void SearchTable::ReplayTruncate(uint64_t tick) {
  if (auto* session = BeginReplay(tick)) {
    session->trx.Remove(std::make_shared<irs::All>());
  }
}

void SearchTable::ReplayAdoptSegments(uint64_t tick,
                                      std::vector<std::string> segments) {
  auto* session = BeginReplay(tick);
  if (!session) {
    return;
  }
  for (auto& segment : segments) {
    session->adopts.push_back({std::move(segment), session->trx.GetQueries()});
  }
}

void SearchTable::FinishReplay() {
  if (_replay) {
    auto& session = *_replay;
    session.sink.reset();
    const uint64_t queries = session.trx.GetQueries();
    SDB_FATAL_IF(SEARCH, session.max_tick <= queries,
                 "search-table WAL recovery: tick ", session.max_tick,
                 " cannot cover ", queries, " removals for table ", _table_id);
    const uint64_t first_tick = session.max_tick - queries;
    for (const auto& pending : session.adopts) {
      const uint64_t tick = first_tick + pending.queries_before;
      const bool adopted = AdoptSegment(pending.meta_file, tick);
      SDB_FATAL_IF(SEARCH, !adopted,
                   "search-table WAL recovery: failed to adopt segment '",
                   pending.meta_file, "' for table ", _table_id,
                   " tick=", tick);
    }
    const bool committed = session.trx.Commit(session.max_tick);
    SDB_FATAL_IF(SEARCH, !committed,
                 "search-table WAL recovery: iresearch trx Commit failed for "
                 "table ",
                 _table_id, " tick=", session.max_tick);
    _writer->RefreshCommit({.tick = session.max_tick});
    TickDomain::Instance().SeedAtLeast(session.max_tick);
    _replay.reset();
  }
  FinishRecovery();
}

}  // namespace sdb::search
