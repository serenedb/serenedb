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
#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/debugging.hpp>
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
#include "connector/search_remove_filter.hpp"
#include "connector/search_sink_writer.hpp"
#include "search/inverted_index_storage.h"
#include "search/scorer_options.h"
#include "search/task.h"
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

struct SearchTable::ReplaySession {
  struct PendingAdopt {
    std::string meta_file;
    uint64_t queries_before;
  };

  irs::IndexWriter::Transaction trx;
  std::unique_ptr<connector::SearchSinkInsertBaseImpl> sink;
  std::vector<PendingAdopt> adopts;
};

SearchTable::SearchTable(std::shared_ptr<catalog::DatabaseDirectory> directory,
                         bool in_memory, bool read_only, duckdb::idx_t db_id,
                         duckdb::idx_t table_id, bool is_new,
                         const catalog::SearchTableOptions& options,
                         catalog::CompressionByColumn compression)
  : _db_id{db_id},
    _table_id{table_id},
    _directory{std::move(directory)},
    _is_new{is_new},
    _segment_memory_max{options.segment_memory_max},
    _row_group_size{options.row_group_size},
    _compression{std::move(compression)},
    _durability{db_id, absl::StrCat("search table ", table_id)} {
  if (!options.optimize_top_k.empty()) {
    _topk_options = ParseScorerExpression(nullptr, options.optimize_top_k);
    _topk_scorer = MakeScorer(*_topk_options);
  }
  RebuildConfig(std::vector<IndexConfig>{}, std::nullopt);
  if (read_only) {
    OpenDirectory();
  } else {
    OpenWriter(in_memory);
  }
  ApplyOptions(options);
}

SearchTable::~SearchTable() {
  _replay.reset();
  _writer.reset();
  _reader = {};
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

  irs::IndexWriterOptions writer_options;
  writer_options.ann_env = &AnnBuildEnv();
  writer_options.segment_memory_max = _segment_memory_max;
  writer_options.cleanup_on_open = _is_new;
  writer_options.lock_repository = false;
  writer_options.reader_options = ReaderOptions();
  writer_options.db = writer_options.reader_options.db;

  _writer = _durability
              .OpenWriter(*_dir, Path(), opened.on_disk, _is_new,
                          std::move(writer_options))
              .first;

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
}

void SearchTable::OpenDirectory() {
  irs::ResourceManagementOptions resource_manager;
  auto opened = OpenStorageDirectory(*_directory, GetTableId(), false, false,
                                     resource_manager);
  _dir = std::move(opened.directory);
  _absent = opened.absent;
}

void SearchTable::OpenReader() {
  _reader = IndexDurability::OpenReader(*_dir, ReaderOptions()).first;
}

irs::IndexReaderOptions SearchTable::ReaderOptions() const {
  irs::IndexReaderOptions options;
  options.db = &irs::DuckDBEngine::Instance().instance();
  options.scorer = _topk_scorer.get();
  return options;
}

void SearchTable::StartTasks() {
  if (!_writer) {
    return;
  }
#ifdef SDB_DEV
  const bool already = _tasks_started.exchange(true);
  SDB_ASSERT(!already, "SearchTable::StartTasks called twice for table ",
             GetTableId());
#endif
  GetSearchEngine().StartTasks(shared_from_this());
}

ResultWithTime SearchTable::RefreshUnsafe(
  bool wait, const irs::ProgressReportCallback& progress, RefreshResult& code) {
  const auto begin = std::chrono::steady_clock::now();
  auto result =
    _durability.Refresh(*_writer, wait, progress, code, [] {}, [] {});
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
  auto stats = StoreStats::FromReader(GetDirectoryReader());
  stats.numBufferedDocs = _writer ? _writer->BufferedDocs() : 0;
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
  _build_truncate = {};
  _delete_log_open.store(true, std::memory_order_release);
}

void SearchTable::RecordTruncateForBuild(const irs::SourcePosition& position) {
  absl::MutexLock lock{&_delete_log_mutex};
  if (!_delete_log_open.load(std::memory_order_relaxed)) {
    return;
  }
  _build_truncate = std::max(_build_truncate, position);
}

void SearchTable::AppendDeleteLog(std::span<const RowRange> rows) {
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

std::shared_ptr<const catalog::InvertedIndexConfig> SearchTable::ScanConfig()
  const {
  std::shared_lock lock(_table_lock);
  return _scan_config;
}

std::shared_ptr<const catalog::InvertedIndexConfig> SearchTable::MergeConfigs(
  std::span<const IndexConfig> configs,
  std::optional<duckdb::idx_t> skip) const {
  std::vector<catalog::InvertedIndexKey> keys;
  catalog::InvertedIndexFields fields;
  for (const auto& index : configs) {
    if (index.oid == skip) {
      continue;
    }
    for (const auto& [id, field] : index.config->fields) {
      fields.emplace(id, field);
    }
    keys.insert(keys.end(), index.config->keys.begin(),
                index.config->keys.end());
  }
  auto merged = std::make_shared<catalog::InvertedIndexConfig>(
    std::move(keys), std::move(fields));
  merged->pk = {.index_term = true, .column = catalog::PkColumnKind::None};
  merged->top_k_scorer = _topk_options;
  merged->row_group_size = _row_group_size;
  merged->declared_compression = _compression;
  return merged;
}

template<typename Configs>
void SearchTable::RebuildConfig(Configs&& configs,
                                std::optional<duckdb::idx_t> building_index) {
  auto config = MergeConfigs(configs, std::nullopt);
  auto scan_config =
    building_index ? MergeConfigs(configs, building_index) : config;
  SDB_IF_FAILURE("SearchTable::RebuildConfig") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  _configs = std::forward<Configs>(configs);
  _building_index = building_index;
  _config = std::move(config);
  _scan_config = std::move(scan_config);
}

void SearchTable::MergeIndexConfig(
  duckdb::idx_t index_oid,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  std::unique_lock lock(_table_lock);
  auto configs = _configs;
  configs.emplace_back(index_oid, std::move(config));
  RebuildConfig(std::move(configs), _building_index);
}

SearchTable::BuildClaim SearchTable::BeginIndexBuild(
  duckdb::idx_t index_oid,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  if (_build_in_flight.exchange(true, std::memory_order_acq_rel)) {
    return {};
  }
  BuildClaim claim{*this, index_oid};
  std::unique_lock lock(_table_lock);
  SDB_ASSERT(!_building_index);
  auto configs = _configs;
  configs.emplace_back(index_oid, std::move(config));
  RebuildConfig(std::move(configs), index_oid);
  return claim;
}

void SearchTable::FinishIndexBuild(duckdb::idx_t index_oid) noexcept {
  std::unique_lock lock(_table_lock);
  if (_building_index != index_oid) {
    return;
  }
  _building_index.reset();
  _scan_config = _config;
}

void SearchTable::AbortIndexBuild(duckdb::idx_t index_oid) noexcept {
  std::unique_lock lock(_table_lock);
  if (_building_index != index_oid) {
    return;
  }
  _building_index.reset();
  std::erase_if(
    _configs, [&](const IndexConfig& index) { return index.oid == index_oid; });
  _config = _scan_config;
}

void SearchTable::BuildClaim::Finish() noexcept { Release(/*finished=*/true); }

void SearchTable::BuildClaim::Release(bool finished) noexcept {
  if (!_table) {
    return;
  }
  if (finished) {
    _table->FinishIndexBuild(_index_oid);
  } else {
    _table->AbortIndexBuild(_index_oid);
  }
  _table->_build_in_flight.store(false, std::memory_order_release);
  _table = nullptr;
}

void SearchTable::RemoveIndexConfig(duckdb::idx_t index_oid) {
  std::unique_lock lock(_table_lock);
  if (std::erase_if(_configs, [&](const IndexConfig& index) {
        return index.oid == index_oid;
      }) == 0) {
    return;
  }
  if (_building_index == index_oid) {
    _building_index.reset();
  }
  // only for remove we want removal to stay in case of merge configs failure
  // as this is DROP INDEX / Rollback and catalog already marked index as gone
  // so nobody will repeat this removal. This will leave field config not
  // updated but at least any next DLL will fix this and "stalled" fields will
  // be not emitted anymore. If we leave _configs without deletion this will be
  // not fixed until restart.
  RebuildConfig(_configs, _building_index);
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
      SDB_DEBUG(SEARCH, "compaction of search table ", GetTableId(),
                " merged nothing: cancelled, or every document was removed");
      empty_compaction = true;
    } else {
      empty_compaction = (res.size == 0);  // nothing merged -> idle round
    }
  } catch (const std::exception& e) {
    result = absl::InternalError(
      absl::StrCat("consolidation failed for search table ", GetTableId(), ": ",
                   duckdb::ErrorData{e}.RawMessage()));
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
  auto result = _durability.Cleanup(*_dir);
  const uint64_t time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count();
  _maintenance.RecordCleanup(result, time_ms);
  return {std::move(result), time_ms};
}

absl::Status SearchTable::VacuumRefresh() {
  RefreshResult code = RefreshResult::Undefined;
  if (auto res = RefreshUnsafe(/*wait=*/true, nullptr, code).res; !res.ok()) {
    return res;
  }
  return CleanupUnsafe().res;
}

absl::Status SearchTable::VacuumCompact(uint32_t target_segments) {
  static const irs::MergeWriter::FlushProgress kProgress = [] { return true; };
  const auto target = std::max<uint32_t>(1, target_segments);
  const auto field_options = Config();
  RefreshResult code = RefreshResult::Undefined;
  if (auto res = RefreshUnsafe(/*wait=*/true, nullptr, code).res; !res.ok()) {
    return res;
  }
  const ManualCompactionSlot slot;
  const irs::AnnBuildEnv* env = slot.AnnEnv();
  for (size_t pass = 0; pass < 8; ++pass) {
    std::vector<std::vector<std::string>> buckets;
    {
      const auto snapshot = _writer->GetSnapshot();
      if (snapshot.size() <= target) {
        break;
      }
      buckets.resize(target);
      for (size_t i = 0; i < snapshot.size(); ++i) {
        buckets[i % target].emplace_back(snapshot[i].Meta().name);
      }
    }
    absl::Status status;
    bool merged = false;
    for (const auto& names : buckets) {
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
      status.Update(
        irs::GetBlocking(CompactUnsafeAsync(bucket, kProgress, empty,
                                            field_options.get(), env))
          .res);
      merged = merged || !empty;
    }
    if (!status.ok()) {
      return status;
    }
    if (auto res = RefreshUnsafe(/*wait=*/true, nullptr, code).res; !res.ok()) {
      return res;
    }
    if (!merged) {
      break;
    }
  }
  return CleanupUnsafe().res;
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

SearchTable::ReplaySession* SearchTable::BeginReplay(
  duckdb::AttachedDatabase& db) {
  if (!_writer) {
    return nullptr;
  }
  auto& manager = duckdb::DuckTransactionManager::Get(db);
  const irs::SourcePosition position{manager.GetReplayGeneration(),
                                     manager.GetReplayCommitOffset()};
  if (position.offset != 0 && position < _durability.Position()) {
    return nullptr;
  }
  if (!_replay) {
    _replay = std::make_unique<ReplaySession>(GetTransaction());
  }
  return _replay.get();
}

void SearchTable::ReplayInsert(duckdb::ClientContext& context,
                               duckdb::Catalog& catalog,
                               std::span<const connector::ColumnId> column_ids,
                               duckdb::DataChunk& chunk, uint64_t row_start) {
  auto* session = BeginReplay(catalog.GetAttached());
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

void SearchTable::ReplayDelete(duckdb::AttachedDatabase& db,
                               duckdb::DataChunk& chunk) {
  auto* session = BeginReplay(db);
  if (!session) {
    return;
  }
  auto& rows = chunk.data[0];
  SDB_ASSERT(rows.GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
  connector::RemoveGeneratedRows(
    session->trx, std::span<const int64_t>{
                    duckdb::FlatVector::GetData<int64_t>(rows), chunk.size()});
}

void SearchTable::ReplayDeleteRanges(duckdb::AttachedDatabase& db,
                                     std::vector<RowRange> rows) {
  auto* session = BeginReplay(db);
  if (!session) {
    return;
  }
  NormalizeRowRanges(rows);
  if (auto removal = connector::MakeRowRangeRemoval(rows)) {
    session->trx.Remove(std::move(removal));
  }
}

void SearchTable::ReplayTruncate(duckdb::AttachedDatabase& db) {
  if (auto* session = BeginReplay(db)) {
    session->trx.Remove(std::make_shared<irs::All>());
  }
}

void SearchTable::ReplayAdoptSegments(duckdb::AttachedDatabase& db,
                                      std::vector<std::string> segments) {
  auto* session = BeginReplay(db);
  if (!session) {
    return;
  }
  for (auto& segment : segments) {
    session->adopts.push_back({std::move(segment), session->trx.GetQueries()});
  }
}

bool SearchTable::FinishReplay(duckdb::AttachedDatabase& db) {
  if (!_replay) {
    return false;
  }
  auto& session = *_replay;
  session.sink.reset();
  auto& storage = db.GetStorageManager();
  const irs::SourcePosition end{
    storage.GetBlockManager().GetCheckpointIteration(), storage.GetWALSize()};
  const auto queries = session.trx.GetQueries();
  const auto tick = NextTick(queries);
  for (const auto& pending : session.adopts) {
    try {
      _writer->AdoptSegment(pending.meta_file,
                            tick - queries + pending.queries_before, end);
    } catch (const std::exception& e) {
      SDB_FATAL(SEARCH, "search-table WAL recovery: failed to adopt segment '",
                pending.meta_file, "' for table ", _table_id, ": ",
                duckdb::ErrorData{e}.RawMessage());
    }
  }
  SDB_FATAL_IF(SEARCH, !session.trx.Commit(tick, end),
               "search-table WAL recovery: iresearch trx Commit failed for "
               "table ",
               _table_id);
  _durability.MarkStaged(end);
  _replay.reset();
  return true;
}

}  // namespace sdb::search
