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
#include <absl/flags/flag.h>
#include <absl/strings/str_cat.h>

#include <chrono>
#include <duckdb/common/file_system.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <limits>
#include <mutex>
#include <system_error>

#include "basics/duckdb_engine.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "search/task.h"
#include "storage_engine/search_engine.h"

ABSL_FLAG(uint32_t, server_search_table_refresh_interval_ms, 1000,
          "Interval between background commits (RefreshCommit) for search "
          "tables, making inserts searchable. 0 disables background commit "
          "(VACUUM is then the only way to flush); set both this and the "
          "compaction interval to 0 to turn background maintenance off.");
ABSL_FLAG(uint32_t, server_search_table_compaction_interval_ms, 10000,
          "Interval between background segment consolidations for search "
          "tables. 0 disables background consolidation.");
ABSL_FLAG(uint32_t, server_search_table_cleanup_interval_step, 10,
          "Run iresearch directory GC every Nth background commit for search "
          "tables. 0 disables periodic GC (VACUUM still GCs).");

namespace sdb::search {

std::filesystem::path SearchTable::GetPath(ObjectId db_id, ObjectId table_id) {
  SDB_ASSERT(db_id.isSet());
  SDB_ASSERT(table_id.isSet());
  auto path = GetSearchEngine().GetPersistedPath(db_id);
  path /= absl::StrCat(table_id);
  return path;
}

std::filesystem::path SearchTable::GetWalPath(ObjectId db_id) {
  SDB_ASSERT(db_id.isSet());
  auto path = GetSearchEngine().GetPersistedPath(db_id);
  path /= "wal";
  return path;
}

std::filesystem::path SearchTable::GetChunkDir(ObjectId db_id,
                                               ObjectId table_id) {
  SDB_ASSERT(table_id.isSet());
  auto path = GetWalPath(db_id);
  path /= "chunks";
  path /= std::to_string(table_id.id());
  return path;
}

Result SearchTable::DropArtifacts(ObjectId db_id, ObjectId table_id) {
  auto path = GetPath(db_id, table_id);
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    return Result{ERROR_INTERNAL, "Failed to remove search table directory '" +
                                    path.string() + "': " + ec.message()};
  }
  auto chunk_dir = GetChunkDir(db_id, table_id);
  std::filesystem::remove_all(chunk_dir, ec);
  if (ec) {
    return Result{ERROR_INTERNAL,
                  "Failed to remove search table chunk directory '" +
                    chunk_dir.string() + "': " + ec.message()};
  }
  GetSearchEngine().GetDbWal(db_id).DeregisterShard(table_id);
  return {};
}

std::shared_ptr<SearchTable> SearchTable::Create(ObjectId db_id,
                                                 ObjectId table_id,
                                                 bool is_new) {
  return std::make_shared<SearchTable>(db_id, table_id, is_new);
}

SearchTable::SearchTable(ObjectId db_id, ObjectId table_id, bool is_new)
  : _table_id{table_id}, _db_id{db_id}, _is_new{is_new} {
  OpenWriter();

  _maint_settings.refresh_interval_msec =
    absl::GetFlag(FLAGS_server_search_table_refresh_interval_ms);
  _maint_settings.compaction_interval_msec =
    absl::GetFlag(FLAGS_server_search_table_compaction_interval_ms);
  _maint_settings.cleanup_interval_step =
    absl::GetFlag(FLAGS_server_search_table_cleanup_interval_step);
}

SearchTable::~SearchTable() {
  _writer.reset();
  _dir.reset();
}

void SearchTable::OpenWriter() {
  auto path = GetPath(_db_id, GetTableId());

  std::error_code ec;
  bool path_exists = std::filesystem::exists(path, ec);
  if (ec) {
    SDB_THROW(ERROR_INTERNAL, "Failed to check existence of path '",
              path.string(), "' while initializing search table for table ",
              GetTableId().id(), ": ", ec.message());
  }
  if (!path_exists) {
    std::filesystem::create_directories(path, ec);
    if (ec) {
      SDB_THROW(ERROR_INTERNAL, "Failed to create directory '", path.string(),
                "' while initializing search table for table ",
                GetTableId().id(), ": ", ec.message());
    }
  }

  auto codec = irs::formats::Get("1_5simd");
  const auto open_mode =
    path_exists ? (irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate)
                : irs::OpenMode::kOmCreate;

  irs::ResourceManagementOptions resource_manager;
  _dir = std::make_unique<irs::MMapDirectory>(path, irs::DirectoryAttributes{},
                                              resource_manager);

  irs::IndexWriterOptions writer_options;
  writer_options.segment_memory_max = 256 * (size_t{1} << 20);
  // TODO(Dronplane): for now we rely on rocksdb (still present) lock
  // But in future we need own server wide data dir lock.
  writer_options.lock_repository = false;
  writer_options.db = &sdb::DuckDBEngine::Instance().instance();
  writer_options.reader_options.db = writer_options.db;

  writer_options.meta_payload_provider = [this](uint64_t tick,
                                                irs::bstring& out) {
    _last_committed_tick = std::max(_last_committed_tick, tick);
    uint64_t tick_be = absl::big_endian::FromHost(_last_committed_tick);
    out.append(reinterpret_cast<const irs::byte_type*>(&tick_be),
               sizeof(tick_be));
    return true;
  };

  _writer = irs::IndexWriter::Make(*_dir, codec, open_mode, writer_options);

  if (path_exists) {
    // Restore the durable commit tick from the last commit's meta payload.
    auto reader = _writer->GetSnapshot();
    auto payload = irs::GetPayload(reader.Meta().index_meta);
    if (payload.size() >= sizeof(uint64_t)) {
      _last_committed_tick = absl::big_endian::Load64(payload.data());
    }
  }

  _wal = &GetSearchEngine().GetDbWal(_db_id);
  _wal->RegisterShard(GetTableId(), _last_committed_tick);

  if (_is_new) {
    _writer->RefreshCommit();
  }
}

void SearchTable::StartTasks() {
#ifdef SDB_DEV
  const bool already = _tasks_started.exchange(true);
  SDB_ASSERT(!already, "SearchTable::StartTasks called twice for table ",
             GetTableId().id());
#endif
  // Launch this table's refresh + compaction loops on the shared background
  // scheduler -- the same RefreshLoop / CompactionCoordinator that drive
  // inverted indexes, templated on the storage type. A zero refresh/compaction
  // interval makes the respective loop idle-poll until an ALTER enables it.
  // Called only after recovery finalize (StartSearchTableMaintenance) or
  // CREATE/CTAS finalize, so a background commit's WAL GC never races replay.
  GetSearchEngine().StartTasks(shared_from_this());
}

ResultWithTime SearchTable::RefreshUnsafe(
  bool wait, const irs::ProgressReportCallback& /*progress*/,
  RefreshResult& code) {
  const auto begin = std::chrono::steady_clock::now();
  code = RefreshResult::NoChanges;
  Result result;
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
      // Publish staged batches; touch the WAL only when something actually
      // committed, so an idle table doesn't churn ticks + GC every interval.
      if (_writer->RefreshCommit()) {
        _wal->OnShardCommit(GetTableId(), _last_committed_tick);
        code = RefreshResult::Done;
      }
    }
  } catch (const std::exception& e) {
    result = {ERROR_INTERNAL, "refresh failed for search table ",
              GetTableId().id(), ": ", e.what()};
  }
  const uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - begin)
                             .count();
  return {std::move(result), time_ms};
}

ResultWithTime SearchTable::CompactUnsafe(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options) {
  const auto begin = std::chrono::steady_clock::now();
  empty_compaction = false;
  Result result;
  if (!policy) {
    result = {ERROR_BAD_PARAMETER, "unset compaction policy for search table ",
              GetTableId().id()};
  } else {
    try {
      // Lock-free: iresearch serializes Compact against refresh/DML internally,
      // so a long merge never blocks the refresh chain. field_options is the
      // merged segment's per-field encoding config -- generic (nullptr) for a
      // search table for now.
      const auto res =
        _writer->Compact(policy, field_options, nullptr, progress);
      if (!res) {
        result = {ERROR_INTERNAL, "compaction failed for search table ",
                  GetTableId().id()};
      } else {
        empty_compaction = (res.size == 0);  // nothing merged -> idle round
      }
    } catch (const std::exception& e) {
      result = {ERROR_INTERNAL, "consolidation failed for search table ",
                GetTableId().id(), ": ", e.what()};
    }
  }
  const uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - begin)
                             .count();
  return {std::move(result), time_ms};
}

ResultWithTime SearchTable::CleanupUnsafe() {
  const auto begin = std::chrono::steady_clock::now();
  Result result;
  try {
    irs::directory_utils::RemoveAllUnreferenced(*_dir);
  } catch (const std::exception& e) {
    result = {ERROR_INTERNAL, "cleanup failed for search table ",
              GetTableId().id(), ": ", e.what()};
  }
  const uint64_t time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - begin)
                             .count();
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
  CompactUnsafe(kFullMerge, kProgress, empty, /*field_options=*/nullptr);
  if (!empty) {
    RefreshUnsafe(/*wait=*/true, nullptr, code);
  }
  CleanupUnsafe();
}

}  // namespace sdb::search
