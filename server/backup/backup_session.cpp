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

#include "backup/backup_session.h"

#include <absl/cleanup/cleanup.h>
#include <absl/time/clock.h>

#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <span>
#include <utility>

#include "backup/backup_label.h"
#include "backup/commit_gate.h"
#include "backup/pg_control.h"
#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/duckdb_storage_extension.h"
#include "search/store.h"
#include "storage_engine/search_engine.h"

namespace sdb::backup {
namespace {

bool LeftByFailedCheckpoint(duckdb::StorageManager& storage) {
  const auto wal = storage.GetWAL();
  SDB_ASSERT(wal);
  return wal->GetPath() != storage.GetWALPath();
}

uint64_t RetainedRunStart(duckdb::StorageManager& storage, uint64_t end) {
  auto& fs = catalog::StorageFileSystem();
  auto start = end;
  while (start > 0 && fs.FileExists(storage.GetRetainedWALPath(start - 1))) {
    --start;
  }
  return start;
}

uint64_t GenerationOf(const irs::DirectoryReader& reader) {
  return reader ? reader.Meta().index_meta.gen : 0;
}

void CollectStores(catalog::SereneDBCatalog& catalog, duckdb::idx_t database,
                   std::vector<StoreCut>& stores) {
  catalog::ForEachSearchStorage(
    catalog,
    [&](catalog::SearchTableEntry& table) {
      stores.push_back({database, table.oid,
                        GenerationOf(table.Storage()->GetDirectoryReader())});
    },
    [&](catalog::InvertedIndexEntry& index) {
      stores.push_back({database, index.oid,
                        GenerationOf(index.Storage()->GetDirectoryReader())});
    });
}

Cut RecordCut(
  duckdb::ClientContext& context, const std::filesystem::path& backup_dir,
  std::span<const duckdb::shared_ptr<duckdb::AttachedDatabase>> pins,
  pg::ProgressMetrics& progress) {
  Cut cut;
  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForCommits);
  const auto commits = CommitGate().Close(context);
  cut.catalog_link = backup_dir / kCatalogLink;
  cut.catalog_log_size =
    catalog::ClusterOf().SnapshotCatalogLog(cut.catalog_link);
  for (const auto& db : pins) {
    auto& storage = db->GetStorageManager();
    auto& entry = cut.databases.emplace_back();
    entry.oid = db->oid;
    auto commit_lock = storage.GetCommitLock();
    entry.with_failed_checkpoint = LeftByFailedCheckpoint(storage);
    entry.wal_size = storage.GetWALSize();
    entry.checkpoint_iteration =
      storage.GetBlockManager().GetCheckpointIteration();
    entry.retained_from = RetainedRunStart(storage, entry.checkpoint_iteration);
  }
  for (const auto& db : pins) {
    CollectStores(db->GetCatalog().Cast<catalog::SereneDBCatalog>(), db->oid,
                  cut.stores);
  }
  return cut;
}

std::optional<duckdb::Identifier> UserTablespace() {
  auto& cluster = catalog::ClusterOf();
  std::optional<duckdb::Identifier> found;
  cluster.GetCatalogSet(duckdb::CatalogType::TABLESPACE_ENTRY)
    .Scan(cluster.LoginTransaction(), [&](duckdb::CatalogEntry& entry) {
      if (!found && !entry.internal) {
        found = entry.name;
      }
    });
  return found;
}

}  // namespace

BackupSession& BackupSession::Instance() {
  static BackupSession gSession;
  return gSession;
}

StartedBackup BackupSession::Begin(duckdb::ClientContext& context,
                                   BackupOwner owner, std::string_view label,
                                   pg::ProgressMetrics& progress) {
  progress.SetCommand(pg::ProgressCommand::BaseBackup);
  progress.SetPhase(pg::progress_phase::BaseBackup::Initializing);
  absl::MutexLock lock{&_mutex};
  if (_active && _owner.session == owner.session) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("a backup is already in progress in this session"));
  }
  if (_active) {
    const auto holder = _owner.pid;
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("a backup is already in progress"),
      ERR_DETAIL("It is held by the session with PID ", holder, "."));
  }
  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForCheckpoint);
  auto checkpoints = CheckpointGate().Close(context);
  auto pins = catalog::SereneDBDatabases(
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance()));
  std::erase_if(pins,
                [](const auto& db) { return db->GetCatalog().InMemory(); });
  std::vector<duckdb::unique_ptr<duckdb::StorageLockKey>> checkpoint_keys;
  for (auto& db : pins) {
    checkpoint_keys.push_back(
      duckdb::DuckTransactionManager::Get(*db).SharedCheckpointLock());
  }
  std::vector<std::string> with_failed_checkpoint;
  for (auto& db : pins) {
    auto& storage = db->GetStorageManager();
    const bool failed_checkpoint = [&] {
      auto commit_lock = storage.GetCommitLock();
      if (!storage.GetWAL()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
          ERR_MSG("cannot start a backup: database \"",
                  db->GetName().GetIdentifierName(),
                  "\" has no active WAL after a failed checkpoint"),
          ERR_HINT("Restart the server, then retry."));
      }
      return LeftByFailedCheckpoint(storage);
    }();
    if (failed_checkpoint) {
      with_failed_checkpoint.push_back(db->GetName().GetIdentifierName());
      continue;
    }
    for (const auto& storage : connector::InvertedStorages(*db)) {
      SDB_ENSURE(!storage->HasPendingCheckpoint(),
                 "cannot start a backup: inverted index ", storage->GetId(),
                 " in database \"", db->GetName().GetIdentifierName(),
                 "\" has an unfinished checkpoint save");
    }
  }
  const auto& layout =
    catalog::ClusterLayout(catalog::ClusterOf().GetAttached());
  auto backup_dir = layout.EngineDir() / kBackupDir;
  if (!catalog::RemoveTree(backup_dir.string())) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_IO_ERROR),
      ERR_MSG("could not remove directory \"", backup_dir.string(), "\""));
  }
  catalog::CreateDirectory(backup_dir);
  absl::Cleanup remove_backup_dir = [&]() noexcept {
    catalog::RemoveTree(backup_dir);
  };

  catalog::DeferRemovals();
  absl::Cleanup resume_removals = [] { catalog::ResumeRemovals(); };
  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForMaintenance);
  auto maintenance = search::GetSearchEngine().Maintenance().Close(context);
  auto cut = RecordCut(context, backup_dir, pins, progress);
  if (const auto tablespace = UserTablespace()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot start a backup: tablespace \"",
                            tablespace->GetIdentifierName(),
                            "\" keeps its files outside the data directory"),
                    ERR_DETAIL("A backup copies only the data directory."));
  }
  auto& positions = PositionSource::Instance();
  const auto start_lsn = positions.Issue();
  cut.catalog_link = cut.catalog_link.lexically_relative(layout.directory);
  auto label_file = BuildBackupLabel(
    label, start_lsn, positions.SystemIdentifier(), absl::Now(), cut);
  std::move(resume_removals).Cancel();
  std::move(remove_backup_dir).Cancel();

  // Noexcept committing of backup start
  _checkpoints = std::move(checkpoints);
  _maintenance = std::move(maintenance);
  _pins = std::move(pins);
  _checkpoint_keys = std::move(checkpoint_keys);
  _backup_dir = std::move(backup_dir);
  _owner = owner;
  _label_file = std::move(label_file);
  _active = true;
  return StartedBackup{start_lsn, std::move(with_failed_checkpoint)};
}

FinishedBackup BackupSession::Finish(const void* session) {
  absl::MutexLock lock{&_mutex};
  if (!_active || _owner.session != session) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("backup is not in progress"),
                    ERR_HINT("Did you call pg_backup_start()?"));
  }
  FinishedBackup finished{PositionSource::Instance().Issue(),
                          std::move(_label_file)};
  Stop();
  return finished;
}

void BackupSession::AbortIfOwnedBy(const void* session) noexcept {
  absl::MutexLock lock{&_mutex};
  if (_active && _owner.session == session) {
    SDB_WARN(GENERAL, "aborting the backup of the closed session with PID ",
             _owner.pid);
    Stop();
  }
}

void BackupSession::End() noexcept {
#ifdef SDB_DEV
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(!_active);
#endif
}

void BackupSession::Stop() noexcept {
  _maintenance.reset();
  catalog::ResumeRemovals();
  _checkpoints.reset();
  _checkpoint_keys.clear();
  _pins.clear();
  catalog::RemoveTree(_backup_dir);
  _backup_dir.clear();
  _owner = {};
  _label_file.clear();
  _active = false;
}

}  // namespace sdb::backup
