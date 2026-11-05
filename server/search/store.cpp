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
#include <atomic>
#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/table_log_storage.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <exception>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
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
      compression.emplace(
        column.Oid(),
        catalog::DeclaredCodec{.type = column.CompressionType(),
                               .compression_level = column.CompressionLevel()});
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

}  // namespace

struct Store::ReplaySession {
  ReplaySession(duckdb::AttachedDatabase& db, Store& store)
    : replay{db, store} {}

  Replay replay;
  std::unique_ptr<connector::SearchSinkInsertBaseImpl> sink;
};

Store::Store(std::shared_ptr<catalog::DatabaseDirectory> directory,
             uint64_t attachment, bool in_memory, bool read_only,
             duckdb::idx_t db_id, duckdb::idx_t id,
             const catalog::InvertedIndexSettings& options,
             const std::optional<irs::ScorerOptions>& top_k_scorer,
             std::vector<irs::SortKey> sort, bool is_new)
  : _directory{std::move(directory)},
    _attachment{attachment},
    _sort{std::move(sort)},
    _merge_relation{std::make_shared<MergeScheduler::Relation>(id)},
    _id{id},
    _db_id{db_id},
    _in_memory{in_memory},
    _read_only{read_only},
    _is_new{is_new},
    _segment_memory_max{options.segment_memory_max},
    _segment_docs_max{options.segment_docs_max},
    _topk_options{top_k_scorer},
    _durability{db_id, absl::StrCat("inverted index ", id)} {
  SDB_ASSERT(id != 0);
  SetTasksSettings(options);
  if (_topk_options) {
    _topk_scorer = MakeScorer(*_topk_options);
  }
  if (is_new) {
    Open();
  }
}

Store::Store(std::shared_ptr<catalog::DatabaseDirectory> directory,
             uint64_t attachment, bool in_memory, bool read_only,
             duckdb::idx_t db_id, duckdb::idx_t id, bool is_new,
             const catalog::SearchTableOptions& options,
             const duckdb::ColumnList& columns, std::vector<irs::SortKey> sort)
  : _directory{std::move(directory)},
    _attachment{attachment},
    _sort{std::move(sort)},
    _merge_relation{std::make_shared<MergeScheduler::Relation>(id)},
    _id{id},
    _db_id{db_id},
    _in_memory{in_memory},
    _read_only{read_only},
    _is_new{is_new},
    _segment_memory_max{options.segment_memory_max},
    _durability{db_id, absl::StrCat("search table ", id)},
    _table_columns{TableColumns(columns)},
    _row_group_size{options.row_group_size},
    _compression{DeclaredCompression(columns)} {
  if (!options.optimize_top_k.empty()) {
    _topk_options = ParseScorerExpression(nullptr, options.optimize_top_k);
    _topk_scorer = MakeScorer(*_topk_options);
  }
  SetTasksSettings(options);
  RebuildConfig(std::vector<IndexConfig>{}, std::nullopt);
  if (is_new) {
    Open();
  }
}

Store::~Store() {
  _replay.reset();
  _writer.reset();
  _dir.reset();
  if (_dropped.load(std::memory_order_acquire)) {
    catalog::DatabaseDirectory::RemoveStorage(std::move(_directory), _id);
  }
}

StorageDirectory OpenStorageDirectory(
  const catalog::DatabaseDirectory& database, duckdb::idx_t oid, bool is_new,
  bool in_memory, const irs::ResourceManagementOptions& resources) {
  std::optional<std::filesystem::path> path;
  if (!in_memory) {
    path = is_new ? database.CreateStorage(oid) : database.OpenStorage(oid);
  }
  if (!path) {
    return {.directory = std::make_unique<irs::MemoryDirectory>(
              irs::DirectoryAttributes{}, resources),
            .absent = !in_memory};
  }
  return {.directory = std::make_unique<irs::MMapDirectory>(
            *path, irs::DirectoryAttributes{}, resources),
          .on_disk = true};
}

irs::IndexReaderOptions Store::ReaderOptions() const {
  irs::IndexReaderOptions options;
  options.db = &irs::DuckDBEngine::Instance().instance();
  options.scorer = _topk_scorer.get();
  return options;
}

void Store::Open() {
  _delete_log_open = _is_new;
  auto opened = OpenStorageDirectory(*_directory, _id, _is_new, _in_memory,
                                     irs::ResourceManagementOptions{});
  _dir = std::move(opened.directory);
  _absent = opened.absent;
  if (_read_only) {
    return;
  }

  irs::IndexWriterOptions writer_options;
  writer_options.ann_env = &AnnBuildEnv();
  writer_options.segment_memory_max = _segment_memory_max;
  writer_options.segment_docs_max = _segment_docs_max;
  writer_options.sort = _sort;
  writer_options.reader_options = ReaderOptions();
  writer_options.db = writer_options.reader_options.db;
  IndexDurability::ManifestSource manifest;
  if (IsSearchTable()) {
    writer_options.cleanup_on_open = _is_new;
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

  auto [writer, stamp] =
    _durability.OpenWriter(*_dir, Path(), opened.on_disk, _is_new,
                           std::move(writer_options), std::move(manifest));
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
  auto [reader, stamp] = IndexDurability::OpenReader(*_dir, ReaderOptions());
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

void Store::ApplyOptions(const catalog::SearchTableOptions& options) {
  SetTasksSettings(options);
  NudgeCompaction();
}

void Store::SetDeclaredCompression(const duckdb::ColumnList& columns) {
  auto compression = DeclaredCompression(columns);
  absl::MutexLock lock{&_layout_mutex};
  _compression = std::move(compression);
  RebuildConfig(_layout.configs, _layout.building_index);
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
  auto result = _durability.Refresh(
    *_writer, wait, progress, code, [&] { PublishSnapshot(); },
    [&] { PublishSnapshot(); });
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
  auto res = RefreshUnsafe(/*wait=*/true, progress, code).res;
  if (code == RefreshResult::Done) {
    NudgeCompaction();
  }
  return res;
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
  auto result = _durability.Cleanup(*_dir);
  const auto time_ms = MillisSince(begin);
  _maintenance.RecordCleanup(result, time_ms);
  return {std::move(result), time_ms};
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
  auto& db = irs::DuckDBEngine::Instance().instance();
  for (auto& attached : duckdb::DatabaseManager::Get(db).GetDatabases()) {
    if (attached->oid != _db_id) {
      continue;
    }
    const auto entry = attached->GetCatalog()
                         .Cast<catalog::SereneDBCatalog>()
                         .FindIn<catalog::InvertedIndexEntry>(nullptr, _id);
    return entry ? entry->Config() : nullptr;
  }
  return nullptr;
}

absl::Status Store::VacuumRefresh() {
  RefreshResult code = RefreshResult::Undefined;
  if (auto res = RefreshUnsafe(/*wait=*/true, nullptr, code).res; !res.ok()) {
    return res;
  }
  if (code == RefreshResult::Done) {
    NudgeCompaction();
  }
  return CleanupUnsafe().res;
}

StoreStats Store::GetStats() const {
  auto stats = StoreStats::FromReader(GetDirectoryReader());
  stats.numBufferedDocs = _writer ? _writer->BufferedDocs() : 0;
  _maintenance.Fill(stats);
  return stats;
}

yaclib::Future<MergeResult> Store::RunMergeAsync(
  utils::DrainGate::Pass maintenance, irs::IndexWriter::MergeTicket ticket,
  const irs::MergeProgress& progress,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env,
  const irs::PipelineMergeExecutor& executor) {
  MergeResult result;
  if (!maintenance) {
    result.outcome.error = irs::CompactionError::Cancelled;
    result.backup = true;
    co_return result;
  }
  std::atomic_bool backup{false};
  const irs::MergeProgress interruptible = [&progress, &backup] {
    if (IsBackupActive()) {
      backup.store(true, std::memory_order_relaxed);
      return false;
    }
    return progress();
  };
  bool failed = false;
  try {
    co_await _writer->RunMerge(std::move(ticket), field_options, interruptible,
                               env, executor, result.outcome);
  } catch (const std::exception& e) {
    failed = true;
    result.outcome.error = irs::CompactionError::Fail;
    result.error = duckdb::ErrorData{e}.RawMessage();
  }
  if (!failed && !result.outcome) {
    SDB_DEBUG(SEARCH, "merge of relation ", _merge_relation->id,
              " produced nothing: cancelled, or every document was removed");
    if (backup.load(std::memory_order_relaxed)) {
      result.outcome.error = irs::CompactionError::Cancelled;
      result.backup = true;
      co_return result;
    }
  }
  _maintenance.RecordCompaction(
    failed, !result.outcome || result.outcome.size == 0,
    result.outcome.stats.profile.TotalNanos() / 1'000'000);
  co_return result;
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
  SDB_ASSERT(BuildInFlight());
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
      declared.hyperloglog |= options.hyperloglog;
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
  merged->pk = catalog::PkColumnKind::None;
  merged->top_k_scorer = _topk_options;
  merged->row_group_size = _row_group_size;
  merged->declared_compression = std::move(declared_compression);
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

void Store::MergeIndexConfig(
  duckdb::idx_t index_oid,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  absl::MutexLock lock{&_layout_mutex};
  RebuildConfig(
    WithConfig(_layout.configs, IndexConfig{index_oid, std::move(config)}),
    _layout.building_index);
}

void Store::ClaimLayout(duckdb::ClientContext& context, std::string_view name) {
  if (auto* connection = connector::GetSereneDBContextPtr(context)) {
    connection->ClaimSearchLayout(
      shared_from_this(),
      duckdb::MetaTransaction::Get(context).global_transaction_id, name);
  }
}

bool Store::AcquireLayout(duckdb::transaction_t owner, std::string_view name) {
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

void Store::ReleaseLayout(duckdb::transaction_t owner) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  SDB_ASSERT(_layout.owner == owner);
  _layout.owner = kNoLayoutOwner;
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
