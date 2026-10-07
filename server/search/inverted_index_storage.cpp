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

#include <absl/cleanup/cleanup.h>
#include <absl/time/time.h>

#include <chrono>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
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
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serializer.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <memory>
#include <system_error>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "query/transaction.h"
#include "scheduler/background_scheduler.h"
#include "search/scorer_options.h"
#include "search/tick_domain.h"
#include "server/utils/lifecycle.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

duckdb::optional_ptr<duckdb::AttachedDatabase> AttachedDatabaseById(
  duckdb::idx_t id) {
  auto& manager =
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance());
  for (auto& attached : manager.GetDatabases()) {
    if (attached->oid == id) {
      return attached.get();
    }
  }
  return nullptr;
}

constexpr duckdb::field_id_t kFieldTick = 0;
constexpr duckdb::field_id_t kFieldWalGeneration = 1;
constexpr duckdb::field_id_t kFieldWalOffset = 2;
constexpr duckdb::field_id_t kFieldLegacyManifest = 3;
constexpr duckdb::field_id_t kFieldDefinition = 4;
constexpr duckdb::field_id_t kFieldSnapshotId = 5;
constexpr duckdb::field_id_t kFieldListing = 6;
constexpr duckdb::field_id_t kFieldPassFloor = 7;

struct LegacyManifestEntry {
  uint64_t file_id = 0;
  std::string path;
  std::string etag;
  int64_t mtime_micros = 0;
};

struct LegacyManifest {
  irs::containers::NodeHashMap<uint64_t, LegacyManifestEntry> entries;
  int64_t version = 0;
};

}  // namespace

void InvertedIndexStorage::RecordFlushCursor(Tick tick,
                                             WalCursor cursor) noexcept {
  duckdb::lock_guard<duckdb::mutex> lock{_flush_cursors_mutex};
  _flush_cursors.insert_or_assign(tick, cursor);
}

WalCursor InvertedIndexStorage::CursorAtOrBelow(Tick tick) noexcept {
  duckdb::lock_guard<duckdb::mutex> lock{_flush_cursors_mutex};
  auto it = _flush_cursors.upper_bound(tick);
  if (it == _flush_cursors.begin()) {
    return {};
  }
  --it;
  const WalCursor cursor = it->second;
  // Entries strictly below the returned one can never be the highest at/below a
  // future bound for THIS index, so drop them. Safe because the table is
  // per-index: no other index relies on these entries.
  _flush_cursors.erase(_flush_cursors.begin(), it);
  return cursor;
}

std::filesystem::path InvertedIndexStorage::GetPath(duckdb::idx_t db_id,
                                                    duckdb::idx_t schema_id,
                                                    duckdb::idx_t table_id,
                                                    duckdb::idx_t index_id) {
  SDB_ASSERT(db_id != 0);
  auto path = search::GetSearchEngine().GetPersistedPath(db_id);
  if (schema_id != 0) {
    path /= absl::StrCat(schema_id);
  }
  if (table_id != 0) {
    SDB_ASSERT(schema_id != 0);
    path /= absl::StrCat(table_id);
  }
  if (index_id != 0) {
    SDB_ASSERT(table_id != 0);
    path /= absl::StrCat(index_id);
  }
  return path;
}

InvertedIndexStorage::InvertedIndexStorage(
  duckdb::idx_t db_id, duckdb::idx_t schema_id, duckdb::idx_t table_id,
  duckdb::idx_t index_id, const catalog::InvertedIndexSettings& options,
  const std::optional<irs::ScorerOptions>& top_k_scorer, bool is_new)
  : _index_id{index_id}, _db_id{db_id}, _search{GetSearchEngine()} {
  _tasks_settings.refresh_interval_msec = options.refresh_interval_ms;
  _tasks_settings.compaction_interval_msec = options.compaction_interval_ms;
  _tasks_settings.reindex_interval_msec = options.reindex_interval_ms;
  _tasks_settings.cleanup_interval_step = options.cleanup_interval_step;
  _tasks_settings.compaction_max_segments = options.compaction_max_segments;
  _tasks_settings.compaction_max_segments_bytes =
    options.compaction_max_segments_bytes;
  _tasks_settings.compaction_floor_segment_bytes =
    options.compaction_floor_segment_bytes;

  SDB_ASSERT(index_id != 0);
  _path = GetPath(db_id, schema_id, table_id, index_id);
  const auto& path = _path;
  // TODO(mbkkt) maybe we should use create_directories result instead of
  // exists?
  std::error_code ec;
  bool path_exists = std::filesystem::exists(path, ec);
  if (ec) {
    THROW_SQL_ERROR(ERR_MSG("Failed to check existence of path '",
                            path.string(), "' while initializing data store '",
                            _index_id, "': ", ec.message()));
  }
  if (!path_exists) {
    std::filesystem::create_directories(path, ec);
    if (ec) {
      THROW_SQL_ERROR(ERR_MSG("Failed to create directory '", path.string(),
                              "' while initializing data store '", _index_id,
                              "': ", ec.message()));
    }
  }

  const bool reopen = path_exists && !is_new;
  const auto open_mode =
    reopen ? (irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate)
           : irs::OpenMode::kOmCreate;

  // New indexes start at the current tick; existing directories override
  // both values from the persisted segment meta below.
  _recovery_tick = TickDomain::Instance().Current();
  _last_durable_tick = _recovery_tick;
  _delete_log_open = is_new;
  irs::ResourceManagementOptions resource_manager;
  resource_manager.transactions = _writers_memory;
  resource_manager.readers = _readers_memory;
  resource_manager.compactions = _compactions_memory;
  resource_manager.file_descriptors = _file_descriptors_count;
  _dir = std::make_unique<irs::MMapDirectory>(path, irs::DirectoryAttributes{},
                                              resource_manager);
  if (reopen) {
    OpenSourceFiles();
  }

  irs::IndexWriterOptions writer_options;
  writer_options.ann_env = &AnnBuildEnv();
  writer_options.segment_memory_max = options.segment_memory_max;
  writer_options.segment_docs_max = options.segment_docs_max;
#ifdef SDB_DEV
  // Dev safety net: a second IndexWriter opening the same index directory (a
  // lifecycle bug -- two storages/loops on one dir) fails to acquire the lock
  // with a clean error instead of silently corrupting segment files.
  writer_options.lock_repository = true;
#else
  writer_options.lock_repository = false;  // single-process server owns the dir
#endif
  writer_options.db = &irs::DuckDBEngine::Instance().instance();
  writer_options.reader_options.db = writer_options.db;
  // No column/norm options are configured on the writer: the per-column
  // encoding config travels with each operation instead. A write hands its own
  // DDL snapshot's InvertedIndex (SegmentWriter::SetFieldOptions, via the
  // serenedb transaction) and a merge hands the compaction task's snapshot
  // index (CompactUnsafe). The long-lived writer therefore never reaches into
  // the live catalog, so a concurrent DROP can no longer dangle it.

  if (const auto& options = top_k_scorer) {
    _topk_scorer = MakeScorer(*options);
    writer_options.reader_options.scorer = _topk_scorer.get();
  }

  writer_options.meta_payload_writer = [this](uint64_t tick,
                                              duckdb::BinarySerializer& out) {
    if (_phase == Phase::Creating) {
      tick = TickDomain::Instance().Current();
    }
    _last_durable_tick = std::max(_last_durable_tick, tick);
    out.WriteProperty<uint64_t>(kFieldTick, "tick", _last_durable_tick);

    // Durable WAL cursor, stamped consistently with the durable tick we just
    // persisted above. `tick` here is the exact tick this flush made durable
    // (FlushContext::FlushPending's flushed_tick in the Recovering/Creating
    // phase, before_refresh in the Active phase -- both are the highest tick
    // covered by these segments), and _last_durable_tick is its running max.
    // The matching cursor is the highest per-index commit entry at/below that
    // tick. A 0/absent lookup means no recorded commit fell at/below it, so
    // keep the prior durable cursor rather than regressing it to 0. Checkpoint
    // refreshes set _pending_wal_cursor up front (next generation, offset 0)
    // and disable this stamping. Recovery replays only operations at or past
    // the stamped cursor.
    if (_stamp_cursor_from_flush) {
      if (const auto cursor = CursorAtOrBelow(_last_durable_tick);
          cursor.generation != 0 || cursor.offset != 0) {
        _pending_wal_cursor = cursor;
      }
    }
    out.WriteProperty<uint64_t>(kFieldWalGeneration, "wal_generation",
                                _pending_wal_cursor.generation);
    out.WriteProperty<uint64_t>(kFieldWalOffset, "wal_offset",
                                _pending_wal_cursor.offset);
    SDB_IF_FAILURE("legacy_view_index_payload") {
      out.OnOptionalPropertyBegin(kFieldLegacyManifest, "manifest", true);
      irs::utils::WriteTuple(out, LegacyManifest{});
      out.OnOptionalPropertyEnd(true);
      return;
    }
    out.WritePropertyWithDefault<uint64_t>(kFieldDefinition, "definition",
                                           _position.definition);
    out.WritePropertyWithDefault<int64_t>(kFieldSnapshotId, "snapshot_id",
                                          _position.snapshot_id);
    out.WritePropertyWithDefault<uint64_t>(kFieldListing, "listing",
                                           _position.listing);
    out.WritePropertyWithDefault<uint64_t>(kFieldPassFloor, "pass_floor",
                                           _pass_floor);
  };

  uint64_t pass_floor = 0;
  writer_options.meta_payload_reader = [&](duckdb::BinaryDeserializer& in) {
    _recovery_tick = in.ReadProperty<uint64_t>(kFieldTick, "tick");
    _recovery_wal_cursor.generation =
      in.ReadProperty<uint64_t>(kFieldWalGeneration, "wal_generation");
    _recovery_wal_cursor.offset =
      in.ReadProperty<uint64_t>(kFieldWalOffset, "wal_offset");
    const bool has_manifest =
      in.OnOptionalPropertyBegin(kFieldLegacyManifest, "manifest");
    if (has_manifest) {
      LegacyManifest manifest;
      irs::utils::ReadTuple(in, manifest);
    }
    in.OnOptionalPropertyEnd(has_manifest);
    _position.definition =
      in.ReadPropertyWithDefault<uint64_t>(kFieldDefinition, "definition");
    _position.snapshot_id =
      in.ReadPropertyWithDefault<int64_t>(kFieldSnapshotId, "snapshot_id");
    _position.listing =
      in.ReadPropertyWithDefault<uint64_t>(kFieldListing, "listing");
    pass_floor =
      in.ReadPropertyWithDefault<uint64_t>(kFieldPassFloor, "pass_floor");
  };

  SDB_IF_FAILURE("segment_1000_docs_max") {
    writer_options.segment_docs_max = 1000;
  }

  _writer = irs::IndexWriter::Make(*_dir, open_mode, std::move(writer_options));

  if (!reopen) {
    _writer->RefreshCommit();
  }

  if (reopen) {
    _last_durable_tick = _recovery_tick;
    if (pass_floor != 0) {
      _writer->RefreshCommit(
        {.payload_changed = true,
         .drop_segments = irs::SegmentIdRange{.first = pass_floor + 1}});
    }
  }

  auto reader = _writer->GetSnapshot();
  SDB_ASSERT(reader);
  StoreInvertedIndexSnapshot(std::make_shared<InvertedIndexSnapshot>(
    std::move(reader), _position, _files));
}

void RemoveDroppedStorageDir(const std::filesystem::path& path,
                             size_t parent_levels) {
  auto remove = [path, parent_levels] {
    std::error_code ec;
    const auto tombstone = DroppedStoragePath(path);
    std::filesystem::rename(path, tombstone, ec);
    std::filesystem::remove_all(ec ? path : tombstone, ec);
    if (ec) {
      SDB_WARN(GENERAL, "could not remove dropped storage '", path.string(),
               "': ", ec.message());
      return;
    }
    auto parent = path;
    for (size_t level = 0; level < parent_levels; ++level) {
      parent = parent.parent_path();
      if (!std::filesystem::remove(parent, ec) || ec) {
        return;
      }
    }
  };
  if (lifecycle::IsStopping() || BackgroundScheduler::instance().IsStopping()) {
    remove();
    return;
  }
  BackgroundScheduler::instance().Run(std::move(remove)).Detach();
}

InvertedIndexStorage::~InvertedIndexStorage() {
  _pass = {};
  _writer.reset();
  _dir.reset();
  if (_dropped.load(std::memory_order_acquire)) {
    RemoveDroppedStorageDir(_path, 3);
  }
}

void InvertedIndexStorage::ApplyOptions(
  const catalog::InvertedIndexSettings& options) {
  _tasks_settings.refresh_interval_msec = options.refresh_interval_ms;
  _tasks_settings.compaction_interval_msec = options.compaction_interval_ms;
  _tasks_settings.reindex_interval_msec = options.reindex_interval_ms;
  _tasks_settings.cleanup_interval_step = options.cleanup_interval_step;
  _tasks_settings.compaction_max_segments = options.compaction_max_segments;
  _tasks_settings.compaction_max_segments_bytes =
    options.compaction_max_segments_bytes;
  _tasks_settings.compaction_floor_segment_bytes =
    options.compaction_floor_segment_bytes;

  irs::SegmentOptions segment_options;
  segment_options.segment_count_max = 0;
  segment_options.segment_memory_max = options.segment_memory_max;
  segment_options.segment_docs_max = options.segment_docs_max;
  SDB_ASSERT(_writer);
  _writer->Options(segment_options);
}

auto InvertedIndexStorage::ReindexClaim::TryAcquire(
  InvertedIndexStorage& storage) -> ReindexClaim {
  absl::MutexLock lock{&storage._reindex_mutex};
  if (storage._reindex_in_flight || storage._reindex_waiters != 0) {
    return ReindexClaim{nullptr};
  }
  storage._reindex_in_flight = true;
  return ReindexClaim{&storage};
}

auto InvertedIndexStorage::ReindexClaim::Acquire(
  InvertedIndexStorage& storage, absl::FunctionRef<bool()> cancelled,
  absl::Duration poll) -> ReindexClaim {
  absl::MutexLock lock{&storage._reindex_mutex};
  ++storage._reindex_waiters;
  absl::Cleanup leave = [&]() ABSL_NO_THREAD_SAFETY_ANALYSIS noexcept {
    --storage._reindex_waiters;
  };
  while (storage._reindex_in_flight) {
    if (storage._reindex_cv.WaitWithTimeout(&storage._reindex_mutex, poll) &&
        cancelled()) {
      return ReindexClaim{nullptr};
    }
  }
  storage._reindex_in_flight = true;
  return ReindexClaim{&storage};
}

InvertedIndexStorage::ReindexClaim::~ReindexClaim() {
  if (!_storage) {
    return;
  }
  absl::MutexLock lock{&_storage->_reindex_mutex};
  _storage->_reindex_in_flight = false;
  _storage->_reindex_cv.SignalAll();
}

void InvertedIndexStorage::BeginPass() {
  std::lock_guard lock{_refresh_mutex};
  if (_pass.Held()) {
    PublishLocked(_position, UnpublishedPassSegments());
  }
  _pass = _writer->ArmCompactionFloor(irs::FloorArming::Now);
  SDB_ASSERT(_pass.Held());
  _pass_floor = _pass.Floor();
}

void InvertedIndexStorage::PublishDelta(irs::IndexWriter::Transaction removals,
                                        const SourcePosition& position,
                                        SourceFilesUpdate files) {
  std::lock_guard lock{_refresh_mutex};
  SDB_ASSERT(_pass.Held());
  removals.RegisterFlush();
  if (!removals.Commit(
        TickDomain::Instance().Next(removals.GetQueries() + 1))) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("search index '", GetId(),
              "': the removals of a REINDEX pass failed to commit"));
  }
  PublishLocked(position, std::nullopt, files);
}

void InvertedIndexStorage::PublishRebuild(const SourcePosition& position,
                                          SourceFilesUpdate files) {
  std::lock_guard lock{_refresh_mutex};
  SDB_ASSERT(_pass.Held());
  PublishLocked(position, irs::SegmentIdRange{.last = _pass.Floor()}, files);
}

void InvertedIndexStorage::CommitPosition(const SourcePosition& position,
                                          SourceFilesUpdate files) {
  std::lock_guard lock{_refresh_mutex};
  PublishLocked(position, UnpublishedPassSegments(), files);
}

uint64_t InvertedIndexStorage::NextSourceFileId() {
  std::lock_guard lock{_refresh_mutex};
  return _files->NextId();
}

void InvertedIndexStorage::OpenSourceFiles() {
  _files_ref = irs::directory_utils::Reference(*_dir, kSourceFilesName);
  if (!_files_ref) {
    return;
  }
  auto stored = ReadSourceFiles(
    duckdb::FileSystem::GetFileSystem(irs::DuckDBEngine::Instance().instance()),
    (_path / kSourceFilesName).string());
  if (stored.torn) {
    RewriteSourceFilesLocked(std::move(stored.files));
    return;
  }
  _files = std::make_shared<const SourceFiles>(std::move(stored.files));
}

void InvertedIndexStorage::AppendSourceFilesLocked(
  std::span<const SourceFile> added) {
  if (added.empty()) {
    return;
  }
  if (!_files_ref) {
    _files_ref = _dir->attributes().refs().add(kSourceFilesName);
  }
  const auto path = _path / kSourceFilesName;
  AppendSourceFiles(
    duckdb::FileSystem::GetFileSystem(irs::DuckDBEngine::Instance().instance()),
    path.string(), added);
  SDB_IF_FAILURE("crash_torn_source_files_append") {
    std::filesystem::resize_file(path, std::filesystem::file_size(path) - 1);
    SDB_IMMEDIATE_ABORT();
  }
  SDB_IF_FAILURE("crash_after_source_files_append") { SDB_IMMEDIATE_ABORT(); }
  std::vector<SourceFile> files{_files->Files().begin(), _files->Files().end()};
  files.insert(files.end(), added.begin(), added.end());
  _files = std::make_shared<const SourceFiles>(std::move(files));
}

void InvertedIndexStorage::CompactSourceFilesLocked(
  std::span<const uint64_t> live) {
  std::vector<SourceFile> kept;
  kept.reserve(live.size());
  for (const auto id : live) {
    if (const auto* file = _files->Find(id)) {
      kept.push_back(*file);
    }
  }
  if (_files->Size() - kept.size() <= kept.size()) {
    return;
  }
  RewriteSourceFilesLocked(std::move(kept));
}

void InvertedIndexStorage::RewriteSourceFilesLocked(
  std::vector<SourceFile> files) {
  const auto tmp_ref = _dir->attributes().refs().add(kSourceFilesTmpName);
  WriteSourceFiles(
    duckdb::FileSystem::GetFileSystem(irs::DuckDBEngine::Instance().instance()),
    (_path / kSourceFilesTmpName).string(), (_path / kSourceFilesName).string(),
    files);
  if (!_files_ref) {
    _files_ref = _dir->attributes().refs().add(kSourceFilesName);
  }
  _files = std::make_shared<const SourceFiles>(std::move(files));
}

void InvertedIndexStorage::PublishLocked(
  const SourcePosition& position,
  std::optional<irs::SegmentIdRange> drop_segments,
  const SourceFilesUpdate& files) {
  const auto begin = std::chrono::steady_clock::now();
  const auto code = CommitPayloadLocked(position, drop_segments, files);
  _maintenance.RecordCommit(
    absl::OkStatus(), code,
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count());
}

bool InvertedIndexStorage::ReindexInFlight() {
  absl::MutexLock lock{&_reindex_mutex};
  return _reindex_in_flight;
}

std::optional<irs::SegmentIdRange>
InvertedIndexStorage::UnpublishedPassSegments() const {
  if (!_pass.Held()) {
    return std::nullopt;
  }
  return irs::SegmentIdRange{.first = _pass.Floor() + 1};
}

RefreshResult InvertedIndexStorage::CommitPayloadLocked(
  const SourcePosition& position,
  std::optional<irs::SegmentIdRange> drop_segments,
  const SourceFilesUpdate& files) {
  AppendSourceFilesLocked(files.added);
  const bool payload_changed = position != _position || _pass_floor != 0;
  absl::Cleanup restore = [&, previous_position = _position,
                           previous_floor = _pass_floor] {
    _position = previous_position;
    _pass_floor = previous_floor;
  };
  _position = position;
  _pass_floor = 0;
  const auto code = RefreshLocked(
    {.payload_changed = payload_changed, .drop_segments = drop_segments});
  std::move(restore).Cancel();
  _pass = {};
  if (files.live) {
    CompactSourceFilesLocked(*files.live);
  }
  return code;
}

void InvertedIndexStorage::Refresh(
  const irs::ProgressReportCallback& progress) {
  RefreshResult code = RefreshResult::Undefined;
  std::ignore = RefreshUnsafe(/*wait=*/true, progress, code);
}

void InvertedIndexStorage::CheckpointRefresh() {
  RefreshResult code = RefreshResult::Undefined;
  std::ignore = RefreshUnsafe(/*wait=*/true, nullptr, code,
                              /*for_checkpoint=*/true);
}

StoreStats InvertedIndexStorage::UpdateStatsUnsafe(
  InvertedIndexSnapshotPtr inverted_index_snapshot) const {
  auto stats = StoreStats::FromReader(inverted_index_snapshot->reader);
  stats.numBufferedDocs = _writer->BufferedDocs();
  _maintenance.Fill(stats);
  return stats;
}

ResultWithTime InvertedIndexStorage::CleanupUnsafe() {
  auto begin = std::chrono::steady_clock::now();
  auto result = CleanupUnsafeImpl();
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  _maintenance.RecordCleanup(result, time_ms);
  return {std::move(result), time_ms};
}

absl::Status InvertedIndexStorage::CleanupUnsafeImpl() {
  try {
    irs::directory_utils::RemoveAllUnreferenced(*_dir);
  } catch (const std::exception& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while cleaning up Search index '", GetId(),
                   "': ", e.what()));
  } catch (...) {
    return absl::InternalError(absl::StrCat(
      "caught exception while cleaning up Search index '", GetId(), "'"));
  }
  return absl::OkStatus();
}

auto InvertedIndexStorage::CompactUnsafeAsync(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env)
  -> yaclib::Future<ResultWithTime> {
  auto begin = std::chrono::steady_clock::now();
  auto result = co_await CompactUnsafeImpl(policy, progress, empty_compaction,
                                           field_options, env);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  _maintenance.RecordCompaction(result, empty_compaction, time_ms);
  co_return ResultWithTime{std::move(result), time_ms};
}

ResultWithTime InvertedIndexStorage::RefreshUnsafe(
  bool wait, const irs::ProgressReportCallback& progress, RefreshResult& code,
  bool for_checkpoint) {
  auto begin = std::chrono::steady_clock::now();
  auto result = RefreshUnsafeImpl(wait, progress, code, for_checkpoint);
  uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();

  SDB_IF_FAILURE("Search::FailOnCommit") {
    result = absl::InternalError("debug failure point");
  }
  SDB_IF_FAILURE("Search::CrashAfterCommit") { SDB_IMMEDIATE_ABORT(); }

  _maintenance.RecordCommit(result, code, time_ms);
  return {std::move(result), time_ms};
}

auto InvertedIndexStorage::CompactUnsafeImpl(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env)
  -> yaclib::Future<absl::Status> {
  empty_compaction = false;

  try {
    const auto res =
      co_await _writer->CompactAsync(policy, field_options, progress, env);
    if (res.error == irs::CompactionError::Fail) {
      co_return absl::InternalError(absl::StrCat(
        "failure while executing compaction policy on Search index '", GetId(),
        "'"));
    }
    if (res.error == irs::CompactionError::Busy) {
      co_return absl::OkStatus();
    }

    empty_compaction = (res.size == 0);
  } catch (const std::exception& e) {
    co_return absl::InternalError(
      absl::StrCat("caught exception while executing compaction policy "
                   "on Search index '",
                   GetId(), "': ", e.what()));
  } catch (...) {
    co_return absl::InternalError(
      absl::StrCat("caught exception while executing compaction policy "
                   "on Search index '",
                   GetId(), "'"));
  }
  co_return absl::OkStatus();
}

absl::Status InvertedIndexStorage::RefreshUnsafeImpl(
  bool wait, const irs::ProgressReportCallback& progress, RefreshResult& code,
  bool for_checkpoint) {
  code = RefreshResult::NoChanges;

  try {
    std::unique_lock refresh_lock{_refresh_mutex, std::try_to_lock};
    if (!refresh_lock.owns_lock()) {
      if (!wait) {
        SDB_TRACE(SEARCH, "Refresh for Search index '", GetId(),
                  "' is already in progress, skipping");

        code = RefreshResult::InProgress;
        return absl::OkStatus();
      }

      SDB_TRACE(SEARCH, "Refresh for Search index '", GetId(),
                "' is already in progress, waiting");

      refresh_lock.lock();
    }
    if (_pass.Held() && !ReindexInFlight()) {
      code = CommitPayloadLocked(_position, UnpublishedPassSegments(), {});
    } else {
      code = RefreshLocked({.progress = progress}, for_checkpoint);
    }
  } catch (const irs::SqlException& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while refreshing Search index '", GetId(),
                   "': ", e.message()));
  } catch (const std::exception& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while refreshing Search index '", GetId(),
                   "': ", e.what()));
  } catch (...) {
    return absl::InternalError(absl::StrCat(
      "caught exception while refreshing Search index '", GetId(), "'"));
  }
  return absl::OkStatus();
}

RefreshResult InvertedIndexStorage::RefreshLocked(irs::CommitInfo info,
                                                  bool for_checkpoint) {
  const auto before_refresh = TickDomain::Instance().Current();
  SDB_ASSERT(_last_durable_tick <= before_refresh);
  SDB_IF_FAILURE("pause_index_refresh_after_tick") {
    if (info.progress) {
      info.progress("pause_index_refresh_after_tick", 0, 0);
    }
    SDB_WAIT_ON_FAILURE("pause_index_refresh_after_tick");
  }

  // Stamp the EXACT durable WAL cursor consistently with the durable tick
  // this refresh persists. RefreshCommit (below) flushes every staged batch
  // with tick <= info.tick and, inside the meta payload provider, persists
  // _last_durable_tick == the highest tick covered by the flushed segments.
  // The per-index table recorded, per settled CommitSearch BEFORE the batch
  // became flushable and after the store WAL was durable, the WAL end offset
  // of that commit; commits serialize, so the cursor that matches the durable
  // tick is the entry of the highest tick at/below it. The payload provider
  // performs that CursorAtOrBelow(_last_durable_tick) lookup just before
  // persisting (it has the exact durable tick in hand), gated by
  // _stamp_cursor_from_flush.
  //
  // A checkpoint-driven refresh runs the moment before the checkpoint
  // truncates the store WAL and bumps the iteration to
  // GetCheckpointIteration()
  // + 1. The post-checkpoint WAL starts fresh, so stamp that next generation
  // with offset 0: the next boot loads iteration+1 and the cursor generation
  // matches (the live iteration is still N here, but the persisted header
  // will be N+1). The payload provider must NOT overwrite that, so disable
  // the flush-driven stamping.
  _stamp_cursor_from_flush = !for_checkpoint;
  absl::Cleanup stamp_guard = [&]() noexcept {
    _stamp_cursor_from_flush = false;
  };
  if (for_checkpoint) {
    if (auto store = AttachedDatabaseById(_db_id)) {
      const auto next_gen =
        store->GetStorageManager().GetBlockManager().GetCheckpointIteration() +
        1;
      _pending_wal_cursor = WalCursor{next_gen, 0};
    }
  }
  absl::Cleanup refresh_guard = [&, last = _last_durable_tick]() noexcept {
    _last_durable_tick = last;
  };

  info.tick = [&] {
    switch (_phase) {
      case Phase::Creating:
      case Phase::Recovering:
        return irs::writer_limits::kMaxTick;
      case Phase::Active:
        return before_refresh;
    }
  }();
  info.reopen_reader = /* TODO(codeworse) */ false;
  const bool were_changes = _writer->RefreshCommit(info);
  // get new reader
  auto reader = _writer->GetSnapshot();
  SDB_ASSERT(reader);
  std::move(refresh_guard).Cancel();
  if (were_changes) {
    SDB_ASSERT(_phase != Phase::Active || _last_durable_tick == before_refresh);
    SDB_DEBUG(SEARCH, "successful sync of Search index '", GetId(),
              "', segments '", reader->size(), "', docs count '",
              reader->docs_count(), "', live docs count '",
              reader->live_docs_count(), "', last operation tick '",
              _last_durable_tick, "'");
  } else {
    SDB_TRACE(SEARCH, "Refresh for Search index '", GetId(),
              "' is no changes, tick ", before_refresh, "'");
    if (_phase != Phase::Recovering) {
      _last_durable_tick = before_refresh;
    }
  }
  // update reader
  if (_pass_floor == 0) {
    SDB_ASSERT(!were_changes || GetInvertedIndexSnapshot()->reader != reader);
    StoreInvertedIndexSnapshot(std::make_shared<InvertedIndexSnapshot>(
      std::move(reader), _position, _files));
  }
  return were_changes ? RefreshResult::Done : RefreshResult::NoChanges;
}

void InvertedIndexStorage::FinishCreation() {
  std::lock_guard lock{_refresh_mutex};
  _phase = Phase::Active;
}

bool InvertedIndexStorage::AppendDeleteLog(std::vector<int64_t>&& rows) {
  duckdb::lock_guard<duckdb::mutex> lock{_delete_log_mutex};
  if (!_delete_log_open.load(std::memory_order_relaxed)) {
    return false;
  }
  _delete_log.push_back(std::move(rows));
  return true;
}

std::vector<int64_t> InvertedIndexStorage::TakeDeleteLog() {
  std::vector<std::vector<int64_t>> batches;
  {
    duckdb::lock_guard<duckdb::mutex> lock{_delete_log_mutex};
    _delete_log_open.store(false, std::memory_order_release);
    batches = std::exchange(_delete_log, {});
  }
  size_t total = 0;
  for (const auto& batch : batches) {
    total += batch.size();
  }
  std::vector<int64_t> rows;
  rows.reserve(total);
  for (const auto& batch : batches) {
    rows.insert(rows.end(), batch.begin(), batch.end());
  }
  return rows;
}

}  // namespace sdb::search
