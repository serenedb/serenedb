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

#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <utility>

#include "backup/backup_label.h"
#include "backup/commit_gate.h"
#include "backup/pg_control.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/log.h"
#include "catalog/transaction.h"
#include "search/engine.h"
#include "search/store.h"
#include "search/task.h"

namespace sdb::backup {
namespace {

Cut RecordCut(duckdb::ClientContext& context, const std::string& backup_dir,
              pg::ProgressMetrics& progress) {
  Cut cut;
  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForCommits);
  const auto commits = CommitGate().Close(context);
  auto* log = catalog::Transactions::Get().GetLog();
  SDB_ENSURE(log, "cannot start a backup of a server without a log");
  cut.log_size = log->Link(backup_dir);
  return cut;
}

std::optional<duckdb::Identifier> UserTablespace() {
  std::optional<duckdb::Identifier> found;
  catalog::ClusterOf()
    .Set(duckdb::CatalogType::TABLESPACE_ENTRY)
    .ScanCommitted([&](duckdb::CatalogEntry& entry) {
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

uint64_t BackupSession::Begin(duckdb::ClientContext& context, BackupOwner owner,
                              std::string_view label,
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
  auto& fs = catalog::StorageFileSystem();
  auto backup_dir =
    fs.JoinPath(catalog::ClusterOf().Layout().engine, kBackupDir);
  if (!catalog::Log::Remove(fs, backup_dir)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("could not remove the leftover backup directory '",
                            backup_dir, "'"));
  }
  catalog::CreateDirectory(backup_dir);
  absl::Cleanup remove_backup_dir = [&]() noexcept {
    catalog::Log::Remove(fs, backup_dir);
  };

  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForCheckpoint);
  auto checkpoints = catalog::Transactions::Get().CloseCheckpoints(context);
  catalog::DeferRemovals();
  absl::Cleanup resume_removals = [] { catalog::ResumeRemovals(); };
  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForMaintenance);
  absl::Cleanup resume_cleanups = [] { search::RequestCleanups(); };
  auto maintenance = search::GetSearchEngine().Maintenance().Close(context);
  auto cut = RecordCut(context, backup_dir, progress);
  if (const auto tablespace = UserTablespace()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot start a backup: tablespace \"",
                            tablespace->GetIdentifierName(),
                            "\" keeps its files outside the data directory"),
                    ERR_DETAIL("A backup copies only the data directory."));
  }
  auto& positions = PositionSource::Instance();
  const auto start_lsn = positions.Issue();
  auto label_file = BuildBackupLabel(
    label, start_lsn, positions.SystemIdentifier(), absl::Now(), cut);
  std::move(resume_cleanups).Cancel();
  std::move(resume_removals).Cancel();
  std::move(remove_backup_dir).Cancel();

  // Noexcept committing of backup start
  _checkpoints = std::move(checkpoints);
  _maintenance = std::move(maintenance);
  _backup_dir = std::move(backup_dir);
  _owner = owner;
  _label_file = std::move(label_file);
  _active = true;
  return start_lsn;
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
  search::RequestCleanups();
  _checkpoints.reset();
  catalog::ResumeRemovals();
  catalog::Log::Remove(catalog::StorageFileSystem(), _backup_dir);
  _backup_dir.clear();
  _owner = {};
  _label_file.clear();
  _active = false;
}

}  // namespace sdb::backup
