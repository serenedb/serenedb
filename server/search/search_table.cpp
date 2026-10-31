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
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/table_log_storage.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "search/compaction/copy_check.h"
#include "search/compaction/merge_planner.h"
#include "search/inverted_index_storage.h"
#include "search/row_position_wal.h"
#include "search/scorer_options.h"
#include "search/task.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

irs::containers::FlatHashSet<irs::field_id> TableColumns(
  const duckdb::ColumnList& columns) {
  irs::containers::FlatHashSet<irs::field_id> ids;
  for (const auto& column : columns.Logical()) {
    ids.insert(connector::TableColumnId(column));
  }
  return ids;
}

template<typename Config>
std::vector<Config> WithConfig(std::vector<Config> configs, Config config) {
  const auto it = absl::c_upper_bound(
    configs, config.oid,
    [](duckdb::idx_t oid, const Config& other) { return oid < other.oid; });
  configs.insert(it, std::move(config));
  return configs;
}

}  // namespace

catalog::CompressionByColumn SearchTable::DeclaredCompression(
  const duckdb::ColumnList& columns) {
  catalog::CompressionByColumn compression;
  for (const auto& column : columns.Logical()) {
    if (column.CompressionType() != duckdb::CompressionType::COMPRESSION_AUTO) {
      compression.emplace(
        column.Oid(),
        catalog::DeclaredCodec{.type = column.CompressionType(),
                               .compression_level = column.CompressionLevel()});
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
  irs::DocRemoval removal;
  irs::containers::FlatHashMap<uint64_t, std::vector<irs::DocMap>> doc_maps;
  std::unique_ptr<connector::SearchSinkInsertBaseImpl> sink;
  std::vector<PendingAdopt> adopts;
};

SearchTable::SearchTable(std::shared_ptr<catalog::DatabaseDirectory> directory,
                         bool in_memory, bool read_only, duckdb::idx_t db_id,
                         duckdb::idx_t table_id, bool is_new,
                         const catalog::SearchTableOptions& options,
                         const duckdb::ColumnList& columns, irs::SortSpec sort)
  : MaintainedStore{table_id, std::move(sort)},
    _db_id{db_id},
    _table_id{table_id},
    _directory{std::move(directory)},
    _is_new{is_new},
    _in_memory{in_memory},
    _read_only{read_only},
    _segment_memory_max{options.segment_memory_max},
    _row_group_size{options.row_group_size},
    _table_columns{TableColumns(columns)},
    _compression{DeclaredCompression(columns)},
    _durability{db_id, absl::StrCat("search table ", table_id)} {
  if (!options.optimize_top_k.empty()) {
    _topk_options = ParseScorerExpression(nullptr, options.optimize_top_k);
    _topk_scorer = MakeScorer(*_topk_options);
  }
  RebuildConfig(std::vector<IndexConfig>{}, std::nullopt);
  if (is_new) {
    Open();
  }
  ApplyOptions(options);
}

void SearchTable::Open() {
  if (_read_only) {
    OpenDirectory();
  } else {
    OpenWriter();
  }
}

void SearchTable::ApplyOptions(const catalog::SearchTableOptions& options) {
  SetTasksSettings(options);
  NudgeCompaction();
}

void SearchTable::SetDeclaredCompression(
  catalog::CompressionByColumn compression) {
  absl::MutexLock lock{&_layout_mutex};
  _compression = std::move(compression);
  RebuildConfig(_layout.configs, _layout.building_index);
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

void SearchTable::OpenWriter() {
  SDB_ASSERT(!_writer);
  irs::ResourceManagementOptions resource_manager;
  auto opened = OpenStorageDirectory(*_directory, GetTableId(), _is_new,
                                     _in_memory, resource_manager);
  _dir = std::move(opened.directory);
  _absent = opened.absent;

  irs::IndexWriterOptions writer_options;
  writer_options.ann_env = &AnnBuildEnv();
  writer_options.segment_memory_max = _segment_memory_max;
  writer_options.cleanup_on_open = _is_new;
  writer_options.lock_repository = false;
  SetWriterOptions(writer_options);
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
  SDB_ASSERT(_writer);
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
  SDB_ASSERT(BuildInFlight());
  if (!_writers.Drain(cancelled, kWriterWaitPoll)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_QUERY_CANCELED),
      ERR_MSG("canceled while waiting for write transactions "
              "on search table ",
              _table_id, " that started before the index was declared"));
  }
}

void SearchTable::WatchTruncates() {
  absl::MutexLock lock{&_truncate_mutex};
  _build_truncate = {};
}

void SearchTable::RecordTruncateForBuild(const irs::SourcePosition& position) {
  absl::MutexLock lock{&_truncate_mutex};
  _build_truncate = std::max(_build_truncate, position);
}

std::shared_ptr<const catalog::InvertedIndexConfig> SearchTable::Config()
  const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return _layout.config;
}

std::shared_ptr<const catalog::InvertedIndexConfig> SearchTable::ScanConfig()
  const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return _layout.scan_config;
}

bool SearchTable::IsCurrentConfig(const irs::IndexFieldOptions* options) const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return _layout.compaction_config.get() == options;
}

std::shared_ptr<const catalog::InvertedIndexConfig>
SearchTable::CompactionConfig() {
  absl::MutexLock lock{&_layout_mutex};
  if (_layout.compaction_stale) {
    RebuildConfig(_layout.configs, _layout.building_index);
  }
  return _layout.compaction_config;
}

std::shared_ptr<const catalog::InvertedIndexConfig> SearchTable::MergeConfigs(
  std::span<const IndexConfig> configs, std::span<const IndexConfig> retired,
  std::optional<duckdb::idx_t> skip) const {
  std::vector<catalog::InvertedIndexKey> keys;
  catalog::InvertedIndexFields fields;
  auto declared_compression = _compression;
  const auto add = [&](const IndexConfig& index) {
    for (const auto& [id, field] : index.config->fields) {
      fields.emplace(id, field);
    }
    keys.insert(keys.end(), index.config->keys.begin(),
                index.config->keys.end());
    for (const auto& key : index.config->keys) {
      if (key.field_id == key.column_id ||
          !irs::field_limits::valid(key.column_id)) {
        continue;
      }
      const auto* field = index.config->FindEntry(key.field_id);
      if (!field) {
        continue;
      }
      const auto& options = field->column_options;
      const bool own_codec =
        options.compression != duckdb::CompressionType::COMPRESSION_AUTO;
      if (!own_codec && !options.hyperloglog) {
        continue;
      }
      auto& declared = declared_compression[key.column_id];
      if (own_codec) {
        declared.type = options.compression;
        declared.compression_level = options.compression_level;
      }
      declared.hyperloglog = declared.hyperloglog || options.hyperloglog;
    }
  };
  for (const auto& index : configs) {
    if (index.oid != skip) {
      add(index);
    }
  }
  absl::c_for_each(retired, add);
  auto merged = std::make_shared<catalog::InvertedIndexConfig>(
    std::move(keys), std::move(fields));
  merged->pk = {.index_term = false, .column = catalog::PkColumnKind::None};
  merged->top_k_scorer = _topk_options;
  merged->row_group_size = _row_group_size;
  merged->declared_compression = std::move(declared_compression);
  merged->table_columns = _table_columns;
  return merged;
}

template<typename Configs>
void SearchTable::RebuildConfig(Configs&& configs,
                                std::optional<duckdb::idx_t> building_index) {
  auto config = MergeConfigs(configs, {}, std::nullopt);
  auto scan_config =
    building_index ? MergeConfigs(configs, {}, building_index) : config;
  auto compaction_config =
    _layout.retired.empty()
      ? config
      : MergeConfigs(configs, _layout.retired, std::nullopt);
  SDB_IF_FAILURE("SearchTable::RebuildConfig") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  _layout.configs = std::forward<Configs>(configs);
  _layout.building_index = building_index;
  _layout.config = std::move(config);
  _layout.scan_config = std::move(scan_config);
  _layout.compaction_config = std::move(compaction_config);
  _layout.compaction_stale = false;
}

void SearchTable::MergeIndexConfig(
  duckdb::idx_t index_oid,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  absl::MutexLock lock{&_layout_mutex};
  RebuildConfig(
    WithConfig(_layout.configs, IndexConfig{index_oid, std::move(config)}),
    _layout.building_index);
}

void SearchTable::ClaimLayout(duckdb::ClientContext& context,
                              std::string_view name) {
  if (auto* connection = connector::GetSereneDBContextPtr(context)) {
    connection->ClaimSearchLayout(
      shared_from_this(),
      duckdb::MetaTransaction::Get(context).global_transaction_id, name);
  }
}

bool SearchTable::AcquireLayout(duckdb::transaction_t owner,
                                std::string_view name) {
  SDB_ASSERT(owner != kNoLayoutOwner);
  absl::MutexLock lock{&_layout_mutex};
  if (_layout.owner == owner) {
    return false;
  }
  if (_layout.owner != kNoLayoutOwner) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("search table \"", name,
              "\" is being changed by another transaction"),
      ERR_HINT("Retry once that transaction commits or rolls back."));
  }
  _layout.owner = owner;
  return true;
}

void SearchTable::ReleaseLayout(duckdb::transaction_t owner) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  SDB_ASSERT(_layout.owner == owner);
  _layout.owner = kNoLayoutOwner;
}

SearchTable::BuildClaim SearchTable::BeginIndexBuild(
  duckdb::idx_t index_oid,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  BuildClaim claim{*this, index_oid};
  absl::MutexLock lock{&_layout_mutex};
  SDB_ASSERT(!_layout.building_index);
  RebuildConfig(
    WithConfig(_layout.configs, IndexConfig{index_oid, std::move(config)}),
    index_oid);
  return claim;
}

void SearchTable::FinishIndexBuild(duckdb::idx_t index_oid) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  if (_layout.building_index != index_oid) {
    return;
  }
  _layout.building_index.reset();
  _layout.scan_config = _layout.config;
}

void SearchTable::AbortIndexBuild(duckdb::idx_t index_oid) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  if (_layout.building_index != index_oid) {
    return;
  }
  _layout.building_index.reset();
  std::erase_if(_layout.configs, [&](const IndexConfig& index) {
    return index.oid == index_oid;
  });
  _layout.config = _layout.scan_config;
  _layout.compaction_stale = true;
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
  _table = nullptr;
}

bool SearchTable::BuildInFlight() const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return static_cast<bool>(_layout.building_index);
}

void SearchTable::RemoveIndexConfig(duckdb::idx_t index_oid) {
  absl::MutexLock lock{&_layout_mutex};
  const auto it = absl::c_find_if(
    _layout.configs,
    [&](const IndexConfig& index) { return index.oid == index_oid; });
  if (it == _layout.configs.end()) {
    return;
  }
  _layout.retired.push_back(*it);
  _layout.configs.erase(it);
  if (_layout.building_index == index_oid) {
    _layout.building_index.reset();
  }
  // only for remove we want removal to stay in case of merge configs failure
  // as this is DROP INDEX / Rollback and catalog already marked index as gone
  // so nobody will repeat this removal. This will leave field config not
  // updated but at least any next DLL will fix this and "stalled" fields will
  // be not emitted anymore. If we leave _configs without deletion this will be
  // not fixed until restart.
  RebuildConfig(_layout.configs, _layout.building_index);
}

void SearchTable::ReleaseRetiredConfig(duckdb::idx_t index_oid) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  if (std::erase_if(_layout.retired, [&](const IndexConfig& index) {
        return index.oid == index_oid;
      }) != 0) {
    _layout.compaction_stale = true;
  }
}

auto SearchTable::CompactUnsafeAsync(
  const irs::CompactionPolicy& policy, const irs::MergeProgress& progress,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env,
  MergeOrigin origin, irs::MergeExecutor& executor, irs::LoneSegment lone)
  -> yaclib::Future<MergeResult> {
  MergeRecorder recorder{GetTableId(), origin};
  const irs::CompactionPolicy current_config_policy =
    [&](irs::Compaction& candidates, const irs::IndexReader& index,
        const irs::CompactingSegments& compacting_segments) {
      if (IsCurrentConfig(field_options)) {
        policy(candidates, index, compacting_segments);
      }
    };
  const auto recorded = recorder.Wrap(current_config_policy);
  auto result = co_await AwaitMerge([&] {
    return _writer->CompactAsync(recorded, field_options, progress, env,
                                 &executor, lone);
  });
  if (result.res.ok()) {
    recorder.Finish(result.outcome);
  } else {
    recorder.Fail(result.error);
  }
  co_return result;
}

auto SearchTable::RunMergeAsync(irs::MergeTicket ticket,
                                const irs::MergeProgress& progress,
                                const irs::IndexFieldOptions* field_options,
                                const irs::AnnBuildEnv* env,
                                irs::MergeExecutor& executor)
  -> yaclib::Future<MergeResult> {
  co_return co_await AwaitMerge([&] {
    return _writer->RunMerge(std::move(ticket), field_options, progress, env,
                             &executor);
  });
}

auto SearchTable::AwaitMerge(
  absl::FunctionRef<yaclib::Future<irs::CompactionResult>()> merge)
  -> yaclib::Future<MergeResult> {
  const auto begin = std::chrono::steady_clock::now();
  bool empty_compaction = false;
  MergeResult result;
  try {
    // iresearch serializes Compact against refresh/DML internally, so a long
    // merge never blocks the refresh chain.
    result.outcome = co_await merge();
    if (!result.outcome) {
      SDB_DEBUG(SEARCH, "compaction of search table ", GetTableId(),
                " merged nothing: cancelled, or every document was removed");
      empty_compaction = true;
    } else {
      empty_compaction = result.outcome.size == 0;
    }
  } catch (const std::exception& e) {
    result.error = duckdb::ErrorData{e}.RawMessage();
    result.res =
      absl::InternalError(absl::StrCat("consolidation failed for search table ",
                                       GetTableId(), ": ", result.error));
  }
  result.time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count();
  _maintenance.RecordCompaction(result.res, empty_compaction, result.time_ms);
  co_return result;
}

CleanupResult SearchTable::CleanupUnsafe() {
  const auto begin = std::chrono::steady_clock::now();
  auto removed = _durability.Cleanup(*_dir);
  const uint64_t time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count();
  _maintenance.RecordCleanup(removed.status(), time_ms);
  if (!removed.ok()) {
    return {.res = std::move(removed).status(), .time_ms = time_ms};
  }
  return {.time_ms = time_ms, .removed = *removed};
}

absl::Status SearchTable::VacuumRefresh() {
  RefreshResult code = RefreshResult::Undefined;
  if (auto res = RefreshUnsafe(/*wait=*/true, nullptr, code).res; !res.ok()) {
    return res;
  }
  if (code == RefreshResult::Done) {
    NudgeCompaction();
  }
  return CleanupUnsafe().res;
}

absl::Status SearchTable::VacuumCompact(MergeMode mode,
                                        uint32_t target_segments,
                                        const irs::MergeProgress& progress,
                                        irs::MergeExecutor& executor) {
  const auto lease = GetSearchEngine().Merges().TakeOver(MergeRelation());
  RefreshResult code = RefreshResult::Undefined;
  if (auto res = RefreshUnsafe(/*wait=*/true, nullptr, code).res; !res.ok()) {
    return res;
  }
  const ManualCompactionSlot slot;
  const irs::AnnBuildEnv* env = slot.AnnEnv();
  for (auto more = true; more;) {
    const auto field_options = CompactionConfig();
    const CopyChecker checker{*field_options, *executor.Scheduler()};
    const auto plan =
      PlanMergeRound(_writer->GetSnapshot(),
                     MergeRoundRequest{.mode = mode,
                                       .target_segments = target_segments,
                                       .row_group_size = _row_group_size,
                                       .inputs_max = MergeInputsMax(),
                                       .copy_check = &checker});
    more = plan.partial;
    for (const auto& planned : plan.merges) {
      if (!progress()) {
        more = false;
        break;
      }
      std::vector<std::string> names;
      names.reserve(planned.inputs.size());
      for (const auto& segment : planned.inputs) {
        names.emplace_back(segment.name);
      }
      if (auto res = irs::GetReady(
                       CompactUnsafeAsync(
                         ExplicitPolicy(names), progress, field_options.get(),
                         env, MergeOrigin::Vacuum, executor, planned.lone))
                       .res;
          !res.ok()) {
        return res;
      }
      SDB_PARK_ONCE_ON_FAILURE("pause_search_compact_before_publish");
      if (auto res = RefreshUnsafe(/*wait=*/true, nullptr, code).res;
          !res.ok()) {
        return res;
      }
      if (auto res = CleanupUnsafe().res; !res.ok()) {
        return res;
      }
    }
    more = more && _writer->GetSnapshot().size() < plan.segments;
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
                               duckdb::DataChunk& chunk) {
  auto* session = BeginReplay(catalog.GetAttached());
  if (!session) {
    return;
  }
  if (!session->sink) {
    session->sink = connector::MakeSearchTableInsertSink(session->trx, *this,
                                                         catalog, context);
  }
  connector::WriteChunkToSearchSink(*session->sink, chunk, column_ids,
                                    _table_id, context);
}

void SearchTable::ReplayDeleteRowPositions(
  duckdb::AttachedDatabase& db, std::shared_ptr<const duckdb::data_t> entry,
  std::span<const duckdb::WALFrozenRowPositionGroup> groups) {
  auto* session = BeginReplay(db);
  if (!session) {
    return;
  }
  if (groups.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("Malformed doc removal: it names no segment"));
  }
  for (const auto& group : groups) {
    session->removal.groups.push_back(
      {.segment = group.segment,
       .docs =
         irs::DocumentMask{{entry, group.docs.data()}, group.docs.size()}});
  }
}

void SearchTable::ReplayRowPositionMaps(
  duckdb::AttachedDatabase& db, std::shared_ptr<const duckdb::data_t> entry,
  uint64_t source, std::span<const duckdb::WALFrozenRowPositionMap> maps) {
  auto* session = BeginReplay(db);
  if (!session) {
    return;
  }
  auto& doc_maps = session->doc_maps[source];
  doc_maps.clear();
  doc_maps.reserve(maps.size());
  for (const auto& map : maps) {
    if (source >= map.target) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATA_CORRUPTED),
        ERR_MSG("Malformed doc map: it does not lead to a newer segment"));
    }
    doc_maps.push_back({.source = source,
                        .target = map.target,
                        .mask = map.mask.empty()
                                  ? nullptr
                                  : std::make_shared<const irs::DocumentMask>(
                                      std::shared_ptr<const irs::byte_type>{
                                        entry, map.mask.data()},
                                      map.mask.size()),
                        .rank_begin = map.rank_begin,
                        .rank_end = map.rank_end,
                        .base = map.base});
  }
}

void SearchTable::ReplayTruncate(duckdb::AttachedDatabase& db) {
  if (auto* session = BeginReplay(db)) {
    session->trx.RemoveAll();
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
  if (!session.removal.groups.empty()) {
    session.trx.Remove(ReplayRemoval(std::move(session.removal),
                                     _writer->GetSnapshot(), session.doc_maps));
  }
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
