////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "search/store.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/memory_file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/table_log_storage.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "catalog/catalog.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/search_sink_writer.hpp"
#include "connector/term_dict.h"
#include "pg/connection_context.h"
#include "search/replay.h"
#include "search/scorer_options.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

catalog::CompressionByColumn DeclaredCompression(
  const duckdb::ColumnList& columns) {
  catalog::CompressionByColumn compression;
  for (const auto& column : columns.Logical()) {
    if (column.CompressionType() != duckdb::CompressionType::COMPRESSION_AUTO) {
      compression.emplace(connector::TableColumnId(column),
                          column.CompressionType());
    }
  }
  return compression;
}

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

uint64_t MillisSince(std::chrono::steady_clock::time_point begin) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::steady_clock::now() - begin)
    .count();
}

std::string StorageRoot(duckdb::AttachedDatabase& database,
                        const catalog::StorageDirectory* storage,
                        duckdb::idx_t id, bool is_new) {
  if (database.GetStorageManager().InMemory()) {
    return absl::StrCat(duckdb::MemoryFileSystem::PREFIX, database.oid, "/",
                        id);
  }
  SDB_ASSERT(storage);
  if (is_new) {
    catalog::CreateDirectory(storage->Path());
  }
  return storage->Path();
}

}  // namespace

struct Store::ReplaySession {
  ReplaySession(duckdb::AttachedDatabase& db, Store& store)
    : replay{db, store} {}

  Replay replay;
  std::unique_ptr<connector::SearchSinkInsertBaseImpl> sink;
};

Store::Store(std::unique_ptr<catalog::StorageDirectory> storage,
             duckdb::AttachedDatabase& database, duckdb::idx_t id,
             const catalog::InvertedIndexSettings& options,
             const std::optional<irs::ScorerOptions>& top_k_scorer, bool is_new)
  : _id{id},
    _db_id{database.oid},
    _storage{std::move(storage)},
    _read_only{database.IsReadOnly()},
    _segment_memory_max{options.segment_memory_max},
    _segment_docs_max{options.segment_docs_max},
    _topk_options{top_k_scorer},
    _durability{database, absl::StrCat("inverted index ", id),
                StorageRoot(database, _storage.get(), id, is_new)} {
  SDB_ASSERT(id != 0);
  SetTasksSettings(options);
  if (_topk_options) {
    _topk_scorer = MakeScorer(*_topk_options);
  }
  _delete_log_open = is_new;
  if (is_new) {
    OpenWriter(true);
  }
}

Store::Store(std::unique_ptr<catalog::StorageDirectory> storage,
             duckdb::AttachedDatabase& database, duckdb::idx_t id, bool is_new,
             const catalog::SearchTableOptions& options,
             const duckdb::ColumnList& columns)
  : _id{id},
    _db_id{database.oid},
    _storage{std::move(storage)},
    _read_only{database.IsReadOnly()},
    _segment_memory_max{options.segment_memory_max},
    _durability{database, absl::StrCat("search table ", id),
                StorageRoot(database, _storage.get(), id, is_new)},
    _table_columns{TableColumns(columns)},
    _row_group_size{options.row_group_size},
    _compression{DeclaredCompression(columns)} {
  if (!options.optimize_top_k.empty()) {
    _topk_options = ParseScorerExpression(nullptr, options.optimize_top_k);
    _topk_scorer = MakeScorer(*_topk_options);
  }
  SetTasksSettings(options);
  RebuildConfig(std::vector<IndexConfig>{}, std::nullopt);
  _delete_log_open = is_new;
  if (is_new) {
    OpenWriter(true);
  }
}

Store::~Store() {
  _replay.reset();
  _writer.reset();
}

irs::IndexReaderOptions Store::ReaderOptions() const {
  irs::IndexReaderOptions options;
  options.scorer = _topk_scorer.get();
  return options;
}

bool Store::Open() {
  if (!catalog::StorageFileSystem().DirectoryExists(_durability.Root())) {
    return false;
  }
  if (_read_only) {
    OpenReader();
  } else {
    OpenWriter(false);
  }
  return true;
}

void Store::OpenWriter(bool is_new) {
  SDB_ASSERT(!_writer);
  irs::IndexWriterOptions writer_options;
  writer_options.ann_env = &AnnBuildEnv();
  writer_options.segment_memory_max = _segment_memory_max;
  writer_options.segment_docs_max = _segment_docs_max;
  writer_options.reader_options = ReaderOptions();
  IndexDurability::ManifestSource manifest;
  if (IsSearchTable()) {
    writer_options.cleanup_on_open = is_new;
    writer_options.lock_repository = false;
  } else {
#ifdef SDB_DEV
    // Dev safety net: a second IndexWriter opening the same index directory (a
    // lifecycle bug -- two storages/loops on one dir) fails to acquire the lock
    // with a clean error instead of silently corrupting segment files.
    writer_options.lock_repository = true;
#else
    writer_options.lock_repository = false;
#endif
    writer_options.key_column = connector::term_dict::kPKFieldId;
    SDB_IF_FAILURE("segment_1000_docs_max") {
      writer_options.segment_docs_max = 1000;
    }
    if (IsBackupActive()) {
      writer_options.cleanup_on_open = false;
    }
    manifest = [this] { return GetFileManifest(); };
  }

  auto [writer, stamp] = _durability.OpenWriter(
    is_new, std::move(writer_options), std::move(manifest));
  _writer = std::move(writer);
  SetFileManifest(std::move(stamp.manifest));

  if (IsSearchTable()) {
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
  PublishSnapshot();
}

void Store::OpenReader() {
  auto [reader, stamp] = _durability.OpenReader(ReaderOptions());
  SetFileManifest(stamp.manifest);
  std::atomic_store(&_snapshot,
                    std::make_shared<StoreSnapshot>(std::move(reader),
                                                    std::move(stamp.manifest)));
}

void Store::PublishSnapshot() {
  std::atomic_store(&_snapshot, std::make_shared<StoreSnapshot>(
                                  _writer->GetSnapshot(), GetFileManifest()));
}

void Store::ApplyOptions(const catalog::InvertedIndexSettings& options) {
  SetTasksSettings(options);
  _segment_memory_max = options.segment_memory_max;
  _segment_docs_max = options.segment_docs_max;
  if (!_writer) {
    return;
  }
  irs::SegmentOptions segment_options;
  segment_options.segment_count_max = 0;
  segment_options.segment_memory_max = options.segment_memory_max;
  segment_options.segment_docs_max = options.segment_docs_max;
  _writer->Options(segment_options);
}

void Store::StartTasks() {
  if (!_writer) {
    return;
  }
#ifdef SDB_DEV
  const bool already = _tasks_started.exchange(true);
  SDB_ASSERT(!already, "Store::StartTasks called twice for store ", _id);
#endif
  GetSearchEngine().StartTasks(shared_from_this());
}

auto Store::TryClaimReindex() noexcept -> ReindexClaim {
  uint32_t idle = 0;
  if (!_reindex_state.compare_exchange_strong(idle, kReindexing)) {
    return {};
  }
  return ReindexClaim{this};
}

auto Store::ClaimReindex(duckdb::ClientContext& context) -> ReindexClaim {
  _reindex_state.fetch_add(kReindexWaiter);
  absl::Cleanup leave = [this] { _reindex_state.fetch_sub(kReindexWaiter); };
  absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
  context.Await([this] { return !(_reindex_state.load() & kReindexing); });
  [[maybe_unused]] const auto previous = _reindex_state.fetch_or(kReindexing);
  SDB_ASSERT(!(previous & kReindexing));
  return ReindexClaim{this};
}

void Store::ReindexReleaser::operator()(Store* store) const noexcept {
  SDB_ASSERT(store);
  if (store->_reindex_state.fetch_and(~kReindexing) >= kReindexWaiter) {
    duckdb::ClientContext::Notify();
  }
}

ResultWithTime Store::RefreshUnsafe(bool wait,
                                    const irs::ProgressReportCallback& progress,
                                    RefreshResult& code) {
  SDB_ASSERT(_writer);
  const auto begin = std::chrono::steady_clock::now();
  const auto enabled = [&] {
    return _tasks_settings.refresh_interval_msec.load() != 0;
  };
  const RefreshGate scheduled{_settings_mutex, enabled};
  auto result = _durability.Refresh(
    *_writer, wait, progress, code, [&] { PublishSnapshot(); },
    [&] { PublishSnapshot(); }, wait ? nullptr : &scheduled);
  const auto time_ms = MillisSince(begin);
  SDB_IF_FAILURE("Search::FailOnCommit") {
    result = absl::InternalError("debug failure point");
  }
  SDB_IF_FAILURE("Search::CrashAfterCommit") { SDB_IMMEDIATE_ABORT(); }
  _maintenance.RecordCommit(result, code, time_ms);
  return {std::move(result), time_ms};
}

absl::Status Store::Refresh(const irs::ProgressReportCallback& progress) {
  RefreshResult code = RefreshResult::Undefined;
  return RefreshUnsafe(/*wait=*/true, progress, code).res;
}

absl::Status Store::OptimizeInMemory() {
  SDB_ASSERT(_writer);
  if (auto status = Refresh(); !status.ok()) {
    return status;
  }
  if (auto status = _durability.CompactFiles(*_writer); !status.ok()) {
    return status;
  }
  return Refresh();
}

void Store::Publish() {
  RefreshResult code = RefreshResult::Undefined;
  const auto result = RefreshUnsafe(/*wait=*/true, nullptr, code);
  if (!result.res.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("search store ", _id,
                            ": publish failed: ", result.res.message()));
  }
}

ResultWithTime Store::CleanupUnsafe() {
  const auto maintenance = ClaimMaintenance();
  if (!maintenance) {
    return {absl::OkStatus(), 0};
  }
  const auto begin = std::chrono::steady_clock::now();
  auto result = _durability.Cleanup();
  const auto time_ms = MillisSince(begin);
  _maintenance.RecordCleanup(result, time_ms);
  return {std::move(result), time_ms};
}

auto Store::CompactUnsafeAsync(utils::DrainGate::Pass maintenance,
                               const irs::CompactionPolicy& policy,
                               const irs::MergeWriter::FlushProgress& progress,
                               bool& empty_compaction,
                               const irs::IndexFieldOptions* field_options,
                               const irs::AnnBuildEnv* env)
  -> yaclib::Future<ResultWithTime> {
  empty_compaction = false;
  if (!maintenance) {
    co_return ResultWithTime{BackupInProgressError(), 0};
  }
  const irs::MergeWriter::FlushProgress interruptible = [&progress] {
    return !IsBackupActive() && progress();
  };
  const auto begin = std::chrono::steady_clock::now();
  auto result = co_await CompactUnsafeImpl(
    policy, interruptible, empty_compaction, field_options, env);
  const auto time_ms = MillisSince(begin);
  if (result.ok() && empty_compaction && IsBackupActive()) {
    co_return ResultWithTime{BackupInProgressError(), time_ms};
  }
  _maintenance.RecordCompaction(result, empty_compaction, time_ms);
  co_return ResultWithTime{std::move(result), time_ms};
}

auto Store::CompactUnsafeImpl(const irs::CompactionPolicy& policy,
                              const irs::MergeWriter::FlushProgress& progress,
                              bool& empty_compaction,
                              const irs::IndexFieldOptions* field_options,
                              const irs::AnnBuildEnv* env)
  -> yaclib::Future<absl::Status> {
  empty_compaction = false;
  SDB_PARK_ONCE_ON_FAILURE("search_compaction_before_selection");
  const irs::CompactionPolicy current_config_policy =
    [&](irs::Compaction& candidates, const irs::IndexReader& index,
        const irs::CompactingSegments& compacting_segments) {
      if (!IsSearchTable() || IsCurrentConfig(field_options)) {
        policy(candidates, index, compacting_segments);
      }
    };
  try {
    // iresearch serializes Compact against refresh/DML internally, so a long
    // merge never blocks the refresh chain.
    const auto res = co_await _writer->CompactAsync(
      current_config_policy, field_options, progress, env);
    if (res.error == irs::CompactionError::Fail) {
      SDB_DEBUG(SEARCH, "compaction of search store ", _id,
                " merged nothing: cancelled, or every document was removed");
      empty_compaction = true;
    } else if (res.error != irs::CompactionError::Busy) {
      empty_compaction = res.size == 0;
    }
  } catch (const std::exception& e) {
    co_return absl::InternalError(
      absl::StrCat("compaction of search store ", _id,
                   " failed: ", duckdb::ErrorData{e}.RawMessage()));
  }
  co_return absl::OkStatus();
}

std::shared_ptr<const irs::IndexFieldOptions> Store::CompactionOptions() {
  if (IsSearchTable()) {
    absl::MutexLock lock{&_layout_mutex};
    if (_layout.compaction_stale) {
      RebuildConfig(_layout.configs, _layout.building_index);
    }
    return _layout.compaction_config;
  }
  // Cloned for this merge: the merge encodes against what it took for its
  // whole length, and an ALTER writes a new configuration rather than mutating
  // this one, while a DROP destroys the bound index but not this clone.
  const auto attached =
    duckdb::AttachedDatabase::TryGetReference(_durability.Database());
  if (!attached) {
    return nullptr;
  }
  auto& catalog = attached->GetCatalog().Cast<catalog::SereneDBCatalog>();
  if (catalog.IsDropped()) {
    return nullptr;
  }
  const auto entry = catalog.FindIn<catalog::InvertedIndexEntry>(nullptr, _id);
  return entry ? entry->Config() : nullptr;
}

absl::Status Store::VacuumRefresh() {
  RefreshResult code = RefreshResult::Undefined;
  if (auto res = RefreshUnsafe(/*wait=*/true, nullptr, code).res; !res.ok()) {
    return res;
  }
  return CleanupUnsafe().res;
}

absl::Status Store::VacuumCompact(duckdb::ClientContext& context,
                                  uint32_t target_segments) {
  const irs::MergeWriter::FlushProgress progress = [&context] {
    return !context.IsInterrupted();
  };
  const auto target = std::max<uint32_t>(1, target_segments);
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
      absl::Status res;
      do {
        const auto field_options = CompactionOptions();
        res =
          irs::GetBlocking(CompactUnsafeAsync(
                             GetSearchEngine().Maintenance().Enter(context),
                             bucket, progress, empty, field_options.get(), env))
            .res;
      } while (absl::IsUnavailable(res));
      context.InterruptCheck();
      status.Update(res);
      merged = merged || !empty;
    }
    if (!status.ok()) {
      return status;
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_search_compact_before_publish");
    if (auto res = RefreshUnsafe(/*wait=*/true, nullptr, code).res; !res.ok()) {
      return res;
    }
    if (!merged) {
      break;
    }
  }
  return CleanupUnsafe().res;
}

StoreStats Store::GetStats() const {
  auto stats = StoreStats::FromReader(GetDirectoryReader());
  stats.numBufferedDocs = _writer ? _writer->BufferedDocs() : 0;
  _maintenance.Fill(stats);
  return stats;
}

void Store::Clear(const irs::SourcePosition& position) {
  _durability.Clear(*_writer, position);
  PublishSnapshot();
}

void Store::ReplaceSegments(
  std::span<const irs::SubReader* const> replaced,
  std::vector<irs::IndexWriter::Replacement> replacements) {
  _durability.ReplaceSegments(*_writer, replaced, std::move(replacements));
  PublishSnapshot();
}

void Store::PrepareCheckpoint(uint64_t iteration) {
  _durability.PrepareCheckpoint(*_writer, iteration);
}

void Store::FinishCheckpoint() {
  if (_durability.FinishCheckpoint(*_writer)) {
    PublishSnapshot();
  }
}

void Store::FinishCheckpointIfIdle() {
  if (_durability.FinishCheckpointIfIdle(*_writer)) {
    PublishSnapshot();
  }
}

template<typename Remove>
void Store::CommitRemovalWith(const Remove& remove) {
  auto trx = GetTransaction();
  remove(trx);
  if (!trx.Commit()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("inverted index ", _id,
                            ": failed to commit a removal: out of memory"));
  }
}

void Store::CommitRemoval(std::shared_ptr<const irs::DocRemoval> removal) {
  CommitRemovalWith([&](auto& trx) { trx.Remove(std::move(removal)); });
}

void Store::CommitRemoval(std::vector<irs::KeyRemoval> removals) {
  CommitRemovalWith([&](auto& trx) {
    for (auto& removal : removals) {
      trx.Remove(std::move(removal));
    }
  });
}

void Store::CommitRemoveAll() {
  CommitRemovalWith([](auto& trx) { trx.RemoveAll(); });
}

std::vector<int64_t> Store::AppendDeleteLog(std::vector<int64_t> rows) {
  absl::MutexLock lock{&_delete_log_mutex};
  if (!_delete_log_open.load(std::memory_order_relaxed)) {
    return rows;
  }
  const auto end = _delete_log_rowid_end.load(std::memory_order_relaxed);
  std::vector<int64_t> logged;
  std::vector<int64_t> native;
  for (const auto row : rows) {
    (row >= end ? native : logged).push_back(row);
  }
  if (!logged.empty()) {
    _delete_log.emplace_back(std::move(logged));
  }
  return native;
}

std::vector<irs::KeyRemoval> Store::TakeDeleteLog() {
  absl::MutexLock lock{&_delete_log_mutex};
  _delete_log_open.store(false, std::memory_order_release);
  return std::exchange(_delete_log, {});
}

void Store::DrainPriorWriters(duckdb::ClientContext& context) {
  _writers.Drain(context);
}

void Store::WatchTruncates() {
  absl::MutexLock lock{&_truncate_mutex};
  _build_truncate = {};
}

void Store::RecordTruncateForBuild(const irs::SourcePosition& position) {
  absl::MutexLock lock{&_truncate_mutex};
  _build_truncate = std::max(_build_truncate, position);
}

std::shared_ptr<const catalog::InvertedIndexConfig> Store::Config() const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return _layout.config;
}

std::shared_ptr<const catalog::InvertedIndexConfig> Store::ScanConfig() const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return _layout.scan_config;
}

bool Store::IsCurrentConfig(const irs::IndexFieldOptions* options) const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return _layout.compaction_config.get() == options;
}

std::shared_ptr<const catalog::InvertedIndexConfig> Store::MergeConfigs(
  std::span<const IndexConfig> configs, std::span<const IndexConfig> retired,
  std::optional<duckdb::idx_t> skip) const {
  std::vector<catalog::InvertedIndexKey> keys;
  catalog::InvertedIndexFields fields;
  const auto add = [&](const IndexConfig& index) {
    for (const auto& [id, field] : index.config->fields) {
      fields.emplace(id, field);
    }
    keys.insert(keys.end(), index.config->keys.begin(),
                index.config->keys.end());
  };
  for (const auto& index : configs) {
    if (index.oid != skip) {
      add(index);
    }
  }
  absl::c_for_each(retired, add);
  auto merged = std::make_shared<catalog::InvertedIndexConfig>(
    std::move(keys), std::move(fields));
  merged->pk = catalog::PkColumnKind::None;
  merged->top_k_scorer = _topk_options;
  merged->row_group_size = _row_group_size;
  merged->declared_compression = _compression;
  merged->table_columns = *_table_columns;
  return merged;
}

template<typename Configs>
void Store::RebuildConfig(Configs&& configs,
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

irs::SegmentReader Store::OpenSegment(std::string_view meta_file) const {
  return _durability.OpenSegment(meta_file, ReaderOptions());
}

void Store::MergeIndexConfig(
  duckdb::idx_t index_oid,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  absl::MutexLock lock{&_layout_mutex};
  RebuildConfig(
    WithConfig(_layout.configs, IndexConfig{index_oid, std::move(config)}),
    _layout.building_index);
}

void Store::ClaimLayout(duckdb::ClientContext& context, std::string_view name,
                        TableChange change) {
  auto* connection = connector::GetSereneDBContextPtr(context);
  if (!connection) {
    SDB_ASSERT(change == TableChange::Layout);
    return;
  }
  if (change == TableChange::Schema &&
      connection->SearchTxn().HasWritesFor(GetId())) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
      ERR_MSG("ALTER TABLE on a search-backed table cannot run in a "
              "transaction that has already written to \"",
              name, "\""));
  }
  connection->ClaimSearchLayout(
    shared_from_this(),
    duckdb::MetaTransaction::Get(context).global_transaction_id, name, change);
}

bool Store::AcquireLayout(duckdb::transaction_t owner, std::string_view name,
                          TableChange change) {
  SDB_ASSERT(owner != kNoLayoutOwner);
  absl::MutexLock lock{&_layout_mutex};
  const bool acquired = _layout.owner != owner;
  if (acquired && _layout.owner != kNoLayoutOwner) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("search table \"", name,
              "\" is being changed by another transaction"),
      ERR_HINT("Retry once that transaction commits or rolls back."));
  }
  _layout.owner = owner;
  if (change == TableChange::Schema) {
    SDB_ASSERT(IsSearchTable());
    if (!_layout.schema_undo) {
      _layout.schema_undo = SchemaUndo{
        .epoch = _schema_epoch.load(),
        .table_columns = *_table_columns,
        .compression = _compression,
      };
    }
    _schema_epoch.fetch_add(1);
  }
  return acquired;
}

void Store::ReleaseLayout(duckdb::transaction_t owner,
                          bool committed) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  SDB_ASSERT(_layout.owner == owner);
  _layout.owner = kNoLayoutOwner;
  auto undo = std::exchange(_layout.schema_undo, std::nullopt);
  if (undo && !committed) {
    _schema_epoch.store(undo->epoch);
    _table_columns = std::move(undo->table_columns);
    _compression = std::move(undo->compression);
    RebuildConfig(_layout.configs, _layout.building_index);
  }
}

void Store::AddTableColumns(const duckdb::ColumnList& columns) {
  absl::MutexLock lock{&_layout_mutex};
  _table_columns->merge(TableColumns(columns));
  _compression.merge(DeclaredCompression(columns));
  RebuildConfig(_layout.configs, _layout.building_index);
}

void Store::SetTableColumns(const duckdb::ColumnList& columns) {
  absl::MutexLock lock{&_layout_mutex};
  _table_columns = TableColumns(columns);
  _compression = DeclaredCompression(columns);
  RebuildConfig(_layout.configs, _layout.building_index);
}

void Store::SyncPublished() {
  if (!_durability.SyncPublished()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("search store ", _id,
              " cannot persist its rows, so its table cannot be altered"));
  }
}

Store::BuildClaim Store::BeginIndexBuild(
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

void Store::FinishIndexBuild(duckdb::idx_t index_oid) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  if (_layout.building_index != index_oid) {
    return;
  }
  _layout.building_index.reset();
  _layout.scan_config = _layout.config;
}

void Store::AbortIndexBuild(duckdb::idx_t index_oid) noexcept {
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

void Store::BuildClaim::Finish() noexcept { Release(/*finished=*/true); }

void Store::BuildClaim::Release(bool finished) noexcept {
  if (!_store) {
    return;
  }
  if (finished) {
    _store->FinishIndexBuild(_index_oid);
  } else {
    _store->AbortIndexBuild(_index_oid);
  }
  _store = nullptr;
}

bool Store::BuildInFlight() const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return static_cast<bool>(_layout.building_index);
}

void Store::RemoveIndexConfig(duckdb::idx_t index_oid) {
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

void Store::ReleaseRetiredConfig(duckdb::idx_t index_oid) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  if (std::erase_if(_layout.retired, [&](const IndexConfig& index) {
        return index.oid == index_oid;
      }) != 0) {
    _layout.compaction_stale = true;
  }
}

bool Store::Replays(duckdb::AttachedDatabase& db) const {
  if (!_writer) {
    return false;
  }
  auto& manager = duckdb::DuckTransactionManager::Get(db);
  const irs::SourcePosition position{manager.GetReplayGeneration(),
                                     manager.GetReplayCommitOffset()};
  SDB_ENSURE(position.offset != 0, "store ", _id,
             ": a write outside a commit and a replay");
  return !(position < _durability.Position());
}

Store::ReplaySession* Store::BeginReplay(duckdb::AttachedDatabase& db) {
  if (!Replays(db)) {
    return nullptr;
  }
  if (!_replay) {
    _replay = std::make_unique<ReplaySession>(db, *this);
  }
  return _replay.get();
}

void Store::ReplayInsert(duckdb::ClientContext& context,
                         duckdb::Catalog& catalog,
                         std::span<const connector::ColumnId> column_ids,
                         duckdb::DataChunk& chunk) {
  auto* session = BeginReplay(catalog.GetAttached());
  if (!session) {
    return;
  }
  if (!session->sink) {
    session->sink = connector::MakeSearchTableInsertSink(
      session->replay.Slot(), *this, catalog, context);
  }
  connector::WriteChunkToSearchSink(*session->sink, chunk, column_ids, _id,
                                    context);
}

void Store::ReplayDeleteRowPositions(
  duckdb::AttachedDatabase& db, std::shared_ptr<const duckdb::data_t> entry,
  std::span<const duckdb::WALFrozenRowPositionGroup> groups) {
  if (auto* session = BeginReplay(db)) {
    session->replay.RemovePositions(std::move(entry), groups);
  }
}

void Store::ReplayRowPositionMaps(
  duckdb::AttachedDatabase& db, std::shared_ptr<const duckdb::data_t> entry,
  uint64_t source, std::span<const duckdb::WALFrozenRowPositionMap> maps) {
  if (auto* session = BeginReplay(db)) {
    session->replay.MapPositions(std::move(entry), source, maps);
  }
}

void Store::ReplayTruncate(duckdb::AttachedDatabase& db) {
  if (auto* session = BeginReplay(db)) {
    session->replay.Truncate();
  }
}

void Store::ReplayAdoptSegments(duckdb::AttachedDatabase& db,
                                std::vector<std::string> segments) {
  if (auto* session = BeginReplay(db)) {
    session->replay.Adopt(segments);
  }
}

bool Store::FinishReplay() {
  if (!_replay) {
    return false;
  }
  _replay->sink.reset();
  _replay->replay.Finish();
  _replay.reset();
  return true;
}

void ThrowIfRefreshFailed(const absl::Status& status,
                          std::string_view index_name) {
  if (!status.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("failed to refresh index '", index_name,
                            "': ", status.message()));
  }
}

}  // namespace sdb::search
