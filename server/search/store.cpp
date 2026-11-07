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
#include <array>
#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "catalog/catalog.h"
#include "catalog/record.h"
#include "catalog/transaction.h"
#include "connector/column_id.h"
#include "connector/constraint_keys.h"
#include "connector/search_sink_writer.hpp"
#include "connector/term_dict.h"
#include "pg/connection_context.h"
#include "scheduler/background_scheduler.h"
#include "search/log_entry.h"
#include "search/scorer_options.h"
#include "storage_engine/search_engine.h"
#include "utils/file_utils.h"

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
  for (const auto& column : columns.Physical()) {
    ids.insert(connector::TableColumnId(column));
  }
  return ids;
}

template<typename Config>
std::vector<Config> WithConfig(std::vector<Config> configs, Config config) {
  const auto it = absl::c_lower_bound(
    configs, config.oid,
    [](const Config& other, duckdb::idx_t oid) { return other.oid < oid; });
  if (it != configs.end() && it->oid == config.oid) {
    *it = std::move(config);
  } else {
    configs.insert(it, std::move(config));
  }
  return configs;
}

uint64_t MillisSince(std::chrono::steady_clock::time_point begin) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::steady_clock::now() - begin)
    .count();
}

}  // namespace

Store::Store(std::shared_ptr<catalog::DatabaseDirectory> directory,
             bool in_memory, bool read_only, duckdb::idx_t db_id,
             duckdb::idx_t id, const catalog::InvertedIndexSettings& options,
             const std::optional<irs::ScorerOptions>& top_k_scorer, bool is_new)
  : _id{id},
    _db_id{db_id},
    _directory{std::move(directory)},
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
             bool in_memory, bool read_only, duckdb::idx_t db_id,
             duckdb::idx_t id, bool is_new,
             const catalog::SearchTableOptions& options,
             const duckdb::ColumnList& columns)
  : _id{id},
    _db_id{db_id},
    _directory{std::move(directory)},
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
  _table.reset();
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
  auto opened = OpenStorageDirectory(*_directory, _id, _is_new, _in_memory,
                                     irs::ResourceManagementOptions{});
  _dir = std::move(opened.directory);
  _absent = opened.absent;
  _on_disk = opened.on_disk;
  if (_read_only) {
    return;
  }

  irs::IndexWriterOptions writer_options;
  writer_options.ann_env = &AnnBuildEnv();
  writer_options.segment_memory_max = _segment_memory_max;
  writer_options.segment_docs_max = _segment_docs_max;
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
    _table = std::make_unique<Table>(*_dir, ReaderOptions());
  }
  PublishSnapshot();
}

void Store::EnsureOpen() {
  if (!Opened()) {
    Open();
  }
}

void Store::ClaimColumnOids(const irs::DirectoryReader& reader) const {
  auto& db_manager =
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance());
  const auto claim = [&](irs::field_id id) {
    if (id <= connector::kMaxRealColumnIdValue) {
      db_manager.ClaimOid(id);
    }
  };
  for (const auto& segment : reader) {
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

void Store::SyncSegments() const {
  if (_on_disk) {
    utils::file_utils::SyncDirectory(Path());
  }
}

StoreSnapshotPtr Store::Snapshot() const {
  if (_table) {
    return std::make_shared<StoreSnapshot>(
      _table->View(catalog::Transactions::Get().LastCommit(), {}), nullptr);
  }
  return std::atomic_load(&_snapshot);
}

irs::DirectoryReader Store::BuildSnapshot() {
  SDB_ASSERT(_table);
  CoolAll();
  return _table->ColdView(kAllApplied);
}

void Store::CoolAll() {
  if (_table) {
    CoolRetired(kNoLsn);
  }
}

std::vector<OwnSegment> Store::OpenFlushed(
  std::span<const irs::IndexWriter::FlushedSegment> flushed) const {
  std::vector<OwnSegment> segments;
  segments.reserve(flushed.size());
  for (const auto& entry : flushed) {
    OwnSegment segment;
    segment.segment.filename = entry.filename;
    segment.segment.meta = entry.meta;
    segment.reader = irs::SegmentReaderImpl::Open(*_dir, segment.segment.meta,
                                                  ReaderOptions());
    segments.push_back(std::move(segment));
  }
  return segments;
}

void Store::ReleaseHot(const std::shared_ptr<HotContext>& hot) {
  bool full = false;
  {
    absl::MutexLock lock{&hot->Mutex()};
    full = _segment_memory_max != 0 && hot->Memory() >= _segment_memory_max;
  }
  _table->ReleaseHot(hot, full);
  if (full) {
    ScheduleCooling();
  }
}

void Store::ScheduleCooling() {
  if (!BackgroundScheduler::gInstance ||
      _cooling_scheduled.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  BackgroundScheduler::instance()
    .Run([weak = weak_from_this()] {
      const auto store = weak.lock();
      if (!store) {
        return;
      }
      store->_cooling_scheduled.store(false, std::memory_order_release);
      store->Cool(false);
    })
    .Detach();
}

void Store::CommitStorage(std::string entry,
                          absl::FunctionRef<void(Tick, Tick, Lsn)> apply) {
  std::vector<uint8_t> record;
  if (!_in_memory) {
    duckdb::vector<std::string> entries;
    entries.push_back(std::move(entry));
    record = catalog::SearchRecord(_db_id, _id, entries);
  }
  catalog::Transactions::Get().CommitStorage(record, apply);
}

void Store::CoolOne(const std::shared_ptr<HotContext>& hot) {
  irs::doc_id_t docs = 0;
  for (const auto& unit : _table->State()->units) {
    if (unit->hot == hot) {
      docs = unit->docs;
      break;
    }
  }
  hot->Reader(*ReaderOptions().db, docs);
  OwnSegment written;
  uint64_t id = 0;
  {
    absl::MutexLock lock{&hot->Mutex()};
    id = hot->Id();
    const auto flushed = hot->Trx().FlushAndFsync();
    SDB_ASSERT(flushed.size() <= 1);
    if (!flushed.empty()) {
      const auto& entry = flushed.front();
      written.segment.filename = entry.filename;
      written.segment.meta = entry.meta;
      if (!entry.docs_mask.Empty()) {
        written.segment.meta.docs_mask =
          std::make_shared<const irs::DocumentMask>(
            irs::DocumentMaskBuilder{entry.docs_mask}.Finish());
        irs::index_utils::FlushIndexSegment(*_dir, written.segment, true);
        const std::array<std::string_view, 1> files{written.segment.filename};
        if (!_dir->sync(files)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_IO_ERROR),
            ERR_MSG("failed to sync '", written.segment.filename, "'"));
        }
      }
      written.reader = irs::SegmentReaderImpl::Open(*_dir, written.segment.meta,
                                                    ReaderOptions());
    }
  }
  SyncSegments();
  SDB_PARK_ONCE_ON_FAILURE("pause_search_cooling");
  CommitStorage(
    LogCooled({.id = id, .meta_file = written.segment.filename, .docs = docs}),
    [&](Tick, Tick, Lsn lsn) { _table->Cooled(hot, std::move(written), lsn); });
  absl::MutexLock lock{&hot->Mutex()};
  hot->Trx().Abort();
}

void Store::Cool(bool all) {
  if (!_table) {
    return;
  }
  const auto taken = _table->TakeCoolable(all ? 0 : _segment_memory_max, all);
  for (const auto& hot : taken) {
    try {
      CoolOne(hot);
    } catch (const std::exception& e) {
      SDB_FATAL(SEARCH, "search table ", _id,
                ": could not write the rows it holds in memory: ",
                duckdb::ErrorData{e}.RawMessage());
    }
  }
  if (!taken.empty()) {
    NudgeCompaction();
  }
}

void Store::CoolRetired(Lsn before) {
  _table->Retire(before);
  for (;;) {
    Cool(before == kNoLsn);
    if (!_table->HasRetiredHot()) {
      return;
    }
    _table->AwaitRetiredReleased();
  }
}

void Store::Cover(Lsn target) {
  EnsureOpen();
  if (!_table || _in_memory || _table->NeededFrom() >= target) {
    return;
  }
  CoolRetired(target);
  catalog::Transactions::Get().CommitState(
    [&] {
      duckdb::vector<std::string> entries;
      entries.push_back(LogState(_table->StateUnits()));
      return catalog::SearchRecord(_db_id, _id, entries);
    },
    [&](Lsn lsn) { _table->StateLogged(lsn); });
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

void Store::Commit(const irs::ProgressReportCallback& progress) {
  SDB_ASSERT(_writer && !_table);
  SDB_IF_FAILURE("Search::FailOnCommit") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("debug failure point"));
  }
  _durability.Commit(*_writer, progress);
  PublishSnapshot();
  SDB_IF_FAILURE("Search::CrashAfterCommit") { SDB_IMMEDIATE_ABORT(); }
}

ResultWithTime Store::CleanupUnsafe() {
  const auto maintenance = ClaimMaintenance();
  if (!maintenance) {
    return {absl::OkStatus(), 0};
  }
  const auto begin = std::chrono::steady_clock::now();
  if (_table) {
    _table->Collect(catalog::Transactions::Get().OldestSnapshot());
  }
  auto result = _durability.Cleanup(*_dir);
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
  _merges.fetch_add(1, std::memory_order_relaxed);
  absl::Cleanup counted = [this] {
    if (_merges.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      duckdb::ClientContext::Notify();
    }
  };
  SDB_PARK_ONCE_ON_FAILURE("search_compaction_before_selection");
  if (_table) {
    if (!IsCurrentConfig(field_options)) {
      empty_compaction = true;
      co_return absl::OkStatus();
    }
    try {
      const auto visible = catalog::Transactions::Get().LastCommit();
      auto merged = co_await _writer->MergeAsync(
        policy, _table->SettledView(visible), field_options, progress, env);
      if (!merged.reader) {
        empty_compaction = true;
        co_return absl::OkStatus();
      }
      SDB_PARK_ONCE_ON_FAILURE("pause_search_compact_before_publish");
      std::vector<uint64_t> ids;
      ids.reserve(merged.claim.candidates.size());
      for (const auto* candidate : merged.claim.candidates) {
        ids.push_back(
          irs::utils::downCast<irs::SegmentReader>(*candidate).GetImpl()->Id());
      }
      const std::array<ReplacedSegment, 1> named{
        ReplacedSegment{.id = merged.reader->Id(),
                        .meta_file = merged.segment.filename,
                        .docs = merged.reader->Meta().docs_count}};
      SyncSegments();
      std::vector<OwnSegment> segments;
      segments.push_back(OwnSegment{.segment = std::move(merged.segment),
                                    .reader = std::move(merged.reader)});
      CommitStorage(LogReplace(ids, named, merged.maps),
                    [&](Tick tick, Tick oldest, Lsn lsn) {
                      _table->Replace(visible, tick, oldest, lsn, ids,
                                      std::move(segments),
                                      std::move(merged.maps));
                    });
    } catch (const std::exception& e) {
      co_return absl::InternalError(
        absl::StrCat("compaction of search table ", _id,
                     " failed: ", duckdb::ErrorData{e}.RawMessage()));
    }
    co_return absl::OkStatus();
  }
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
    if (!empty_compaction) {
      Commit();
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
  auto& db = irs::DuckDBEngine::Instance().instance();
  for (auto& attached : duckdb::DatabaseManager::Get(db).GetDatabases()) {
    if (attached->oid != _db_id) {
      continue;
    }
    const catalog::CommittedView view;
    const auto entry =
      attached->GetCatalog()
        .Cast<catalog::SereneDBCatalog>()
        .FindIn<catalog::InvertedIndexEntry>(view.Transaction(db), _id);
    return entry ? entry->Config() : nullptr;
  }
  return nullptr;
}

absl::Status Store::VacuumCompact(duckdb::ClientContext& context,
                                  uint32_t target_segments) {
  const irs::MergeWriter::FlushProgress progress = [&context] {
    return !context.IsInterrupted();
  };
  const auto target = std::max<uint32_t>(1, target_segments);
  GetSearchEngine().Maintenance().Enter(context).reset();
  CoolAll();
  const ManualCompactionSlot slot;
  const irs::AnnBuildEnv* env = slot.AnnEnv();
  for (size_t pass = 0; pass < 8; ++pass) {
    std::vector<std::vector<std::string>> buckets;
    {
      const auto snapshot =
        _table ? _table->SettledView(catalog::Transactions::Get().LastCommit())
               : _writer->GetSnapshot();
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
      bool contended = false;
      const irs::CompactionPolicy bucket =
        [&names, &contended](irs::Compaction& candidates,
                             const irs::IndexReader& reader,
                             const irs::CompactingSegments& busy) {
          for (size_t i = 0; i < reader.size(); ++i) {
            const auto& segment = reader[i];
            const auto& name = segment.Meta().name;
            if (!absl::c_linear_search(names, name)) {
              continue;
            }
            if (busy.contains(name)) {
              contended = true;
            } else {
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
      if (contended) {
        AwaitMerges(context);
        merged = true;
      }
    }
    if (!status.ok()) {
      return status;
    }
    if (!merged) {
      break;
    }
  }
  return CleanupUnsafe().res;
}

void Store::AwaitMerges(duckdb::ClientContext& context) {
  absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
  context.Await([this] { return _merges.load() == 0; });
}

StoreStats Store::GetStats() const {
  StoreStats stats;
  if (_table) {
    const auto tick = catalog::Transactions::Get().LastCommit();
    stats = StoreStats::FromReader(_table->ColdView(tick));
    stats.numFiles += _table->DataFiles(tick);
    stats.numBufferedDocs = _table->BufferedDocs(tick);
  } else {
    stats = StoreStats::FromReader(GetDirectoryReader());
    stats.numBufferedDocs = _writer ? _writer->BufferedDocs() : 0;
  }
  _maintenance.Fill(stats);
  return stats;
}

Tick Store::ReplaceSegments(
  std::span<const irs::SubReader* const> replaced,
  std::vector<irs::IndexWriter::Replacement> replacements) {
  SDB_ASSERT(_table);
  std::vector<uint64_t> ids;
  ids.reserve(replaced.size());
  for (const auto* sub : replaced) {
    ids.push_back(
      irs::utils::downCast<irs::SegmentReader>(*sub).GetImpl()->Id());
  }
  std::vector<OwnSegment> segments;
  std::vector<ReplacedSegment> named;
  std::vector<irs::DocMap> maps;
  for (auto& replacement : replacements) {
    auto opened = _table->OpenSegment(std::string{replacement.meta_file});
    const auto target = opened.reader->Id();
    for (auto& map : replacement.absorbed) {
      map.target = target;
      maps.push_back(std::move(map));
    }
    named.push_back({.id = target,
                     .meta_file = opened.segment.filename,
                     .docs = opened.reader->Meta().docs_count});
    segments.push_back(std::move(opened));
  }
  SyncSegments();
  Tick published = 0;
  CommitStorage(LogReplace(ids, named, maps),
                [&](Tick tick, Tick oldest, Lsn lsn) {
                  _table->Replace(tick, tick, oldest, lsn, ids,
                                  std::move(segments), std::move(maps));
                  published = tick;
                });
  return published;
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

void Store::DrainPriorWriters(duckdb::ClientContext& context) {
  const auto* writes =
    ConnectionContext::Get(context).Transaction().SearchWrites();
  _writers.Drain(context, writes ? writes->Registration(_id) : nullptr);
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
  auto constraint_keys = _constraint_keys;
  const auto add = [&](const IndexConfig& index) {
    for (const auto& [id, field] : index.config->fields) {
      fields.emplace(id, field);
    }
    keys.insert(keys.end(), index.config->keys.begin(),
                index.config->keys.end());
    constraint_keys.insert(constraint_keys.end(),
                           index.config->constraint_keys.begin(),
                           index.config->constraint_keys.end());
  };
  for (const auto& index : configs) {
    if (index.oid != skip) {
      add(index);
    }
  }
  absl::c_for_each(retired, add);
  for (const auto& key : constraint_keys) {
    fields.emplace(key.field,
                   catalog::InvertedIndexField{{.indexed_term_dict = true}});
  }
  auto merged = std::make_shared<catalog::InvertedIndexConfig>(
    std::move(keys), std::move(fields));
  merged->pk = catalog::PkColumnKind::None;
  merged->top_k_scorer = _topk_options;
  merged->row_group_size = _row_group_size;
  merged->declared_compression = _compression;
  merged->table_columns = *_table_columns;
  merged->constraint_keys = std::move(constraint_keys);
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

void Store::ClaimLayout(duckdb::ClientContext& context, std::string_view name,
                        TableChange change) {
  if (Dropped()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("could not serialize access due to concurrent update of \"", name,
              "\""));
  }
  ConnectionContext::Get(context).Transaction().ClaimSearchLayout(
    shared_from_this(), name, change);
}

void Store::ClaimExclusive(duckdb::ClientContext& context,
                           std::string_view name, Exclusive kind) {
  auto& session = ConnectionContext::Get(context).Transaction();
  session.ClaimSearchLayout(shared_from_this(), name, TableChange::Layout);
  session.SearchTxn().ClaimExclusive(shared_from_this(), kind);
  absl::MutexLock lock{&_layout_mutex};
  _layout.exclusive = kind;
}

Store::Exclusive Store::ExclusiveClaim() const {
  absl::MutexLock lock{&_layout_mutex};
  return _layout.owner == kNoLayoutOwner ? Exclusive::None : _layout.exclusive;
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
        .constraint_keys = _constraint_keys,
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
  _layout.exclusive = Exclusive::None;
  auto undo = std::exchange(_layout.schema_undo, std::nullopt);
  if (undo && !committed) {
    _schema_epoch.store(undo->epoch);
    _table_columns = std::move(undo->table_columns);
    _compression = std::move(undo->compression);
    _constraint_keys = std::move(undo->constraint_keys);
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

void Store::SetConstraintKeys(std::vector<catalog::ConstraintKey> keys) {
  absl::MutexLock lock{&_layout_mutex};
  const auto same = [](const catalog::ConstraintKey& lhs,
                       const catalog::ConstraintKey& rhs) {
    return lhs.field == rhs.field && lhs.columns == rhs.columns &&
           lhs.types == rhs.types;
  };
  if (absl::c_equal(keys, _constraint_keys, same)) {
    return;
  }
  _constraint_keys = std::move(keys);
  RebuildConfig(_layout.configs, _layout.building_index);
}

void Store::AddReferencing(std::weak_ptr<const Referencing> referencing) {
  absl::MutexLock lock{&_referencing_mutex};
  std::erase_if(_referencing,
                [](const auto& entry) { return entry.expired(); });
  _referencing.push_back(std::move(referencing));
}

std::vector<std::shared_ptr<const Referencing>> Store::ReferencingKeys() const {
  std::vector<std::shared_ptr<const Referencing>> live;
  absl::MutexLock lock{&_referencing_mutex};
  std::erase_if(_referencing, [&](const auto& entry) {
    auto referencing = entry.lock();
    if (!referencing) {
      return true;
    }
    live.push_back(std::move(referencing));
    return false;
  });
  return live;
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

bool Store::HasIndexes() const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return absl::c_any_of(_layout.configs, [&](const IndexConfig& index) {
    return index.oid != _id;
  });
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

void Store::ReplayHotRows(duckdb::Catalog& catalog,
                          std::vector<connector::ColumnId> column_ids,
                          uint64_t segment, irs::doc_id_t first,
                          duckdb::unique_ptr<duckdb::DataChunk> chunk,
                          Lsn lsn) {
  EnsureOpen();
  if (!_table) {
    return;
  }
  _table->ReplayRows(segment, first, static_cast<irs::doc_id_t>(chunk->size()),
                     lsn);
  _replayed_rows[segment].push_back({.first = first,
                                     .chunk = std::move(chunk),
                                     .column_ids = std::move(column_ids),
                                     .catalog = &catalog});
}

void Store::ReplayAddSegment(const ReplacedSegment& segment, Lsn lsn) {
  EnsureOpen();
  if (_table) {
    _table->ReplayAddSegment(segment, lsn);
  }
}

void Store::ReplayRemoval(const irs::DocRemoval& removal, Lsn lsn) {
  EnsureOpen();
  if (_table) {
    _table->ReplayRemoval(removal, lsn);
  }
}

void Store::ReplayCooled(const ReplacedSegment& segment, Lsn lsn) {
  EnsureOpen();
  if (_table) {
    _replayed_rows.erase(segment.id);
    _table->ReplayCooled(segment, lsn);
  }
}

void Store::ReplayTruncate(Lsn lsn) {
  EnsureOpen();
  if (_table) {
    _table->ReplayTruncate(lsn);
  }
}

void Store::ReplayReplace(std::span<const uint64_t> replaced,
                          std::span<const ReplacedSegment> segments,
                          std::span<const irs::DocMap> maps, Lsn lsn) {
  EnsureOpen();
  if (_table) {
    _table->ReplayReplace(replaced, segments, maps, lsn);
  }
}

void Store::ReplayState(std::span<const StateUnit> units, Lsn lsn) {
  EnsureOpen();
  if (_table) {
    _table->ReplayState(units, lsn);
  }
}

void Store::InvertReplayed(duckdb::ClientContext& context) {
  const auto rows = std::exchange(_replayed_rows, {});
  if (!_table) {
    return;
  }
  for (const auto& [segment, chunks] : rows) {
    if (!_table->ReplayPending(segment)) {
      continue;
    }
    for (const auto& replayed : chunks) {
      _table->ReplayHotRows(
        *_writer, segment, replayed.first,
        [&](irs::IndexWriter::Transaction& trx, HotKeys& keys) {
          const auto sink = connector::MakeSearchTableInsertSink(
            trx, *this, *replayed.catalog, context);
          connector::WriteChunkToSearchSink(*sink, *replayed.chunk,
                                            replayed.column_ids, _id, context);
          const auto& constraints = sink->ConstraintKeys();
          auto& chunk = *replayed.chunk;
          duckdb::Vector encoded{duckdb::LogicalType::BLOB, chunk.size()};
          for (const auto& constraint : constraints) {
            connector::EncodeConstraintKeys(chunk, replayed.column_ids,
                                            constraint, encoded);
            auto values = encoded.Values<duckdb::string_t>();
            for (duckdb::idx_t row = 0; row != chunk.size(); ++row) {
              const auto entry = values[row];
              if (entry.IsValid()) {
                const auto& value = entry.GetValue();
                keys.Add(constraint.field, {value.GetData(), value.GetSize()},
                         static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                                    replayed.first + row));
              }
            }
          }
        });
    }
  }
}

void Store::FinishReplay() {
  EnsureOpen();
  if (!_table) {
    return;
  }
  _writer->ReserveSegmentId(_table->ReplayTop());
  _table->FinishReplay();
  ClaimColumnOids(_table->View(catalog::Transactions::Get().LastCommit(), {}));
}

}  // namespace sdb::search
