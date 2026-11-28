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
#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/path.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <utility>

#include "backup/backup_label.h"
#include "backup/commit_gate.h"
#include "backup/pg_control.h"
#include "catalog/boot.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/log.h"
#include "catalog/transaction.h"
#include "search/engine.h"
#include "search/store.h"
#include "search/task.h"

namespace sdb::backup {
namespace {

Cut RecordCut(duckdb::ClientContext& context, const std::string& pin,
              std::string name, pg::ProgressMetrics& progress) {
  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForCommits);
  const auto commits = CommitGate().Close(context);
  auto& transactions = catalog::Transactions::Get();
  auto* log = transactions.GetLog();
  SDB_ENSURE(log, "cannot start a backup of a server without a log");
  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForCheckpoint);
  if (const auto status = transactions.TruncateLog(); !status.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("cannot start a backup: the checkpoint failed: ",
                            status.message()));
  }
  log->Pin(pin);
  return {.snapshot = std::move(name)};
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
  const auto& layout = catalog::ClusterOf().Layout();
  if (duckdb::Path::IsRemote(layout.directory)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot start a backup: the server directory \"",
              layout.directory, "\" is on an object store"),
      ERR_DETAIL("A backup copies the files of a local server directory."));
  }
  if (const auto tablespace = UserTablespace()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot start a backup: tablespace \"",
                            tablespace->GetIdentifierName(),
                            "\" keeps its files outside the server directory"),
                    ERR_DETAIL("A backup copies only the server directory."));
  }
  auto& fs = catalog::StorageFileSystem();
  fs.CreateDirectoryExtended(
    layout.BackupsDir(),
    {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true});
  auto& positions = PositionSource::Instance();
  const auto start_lsn = positions.Issue();
  auto name = absl::StrCat("pg_backup-", start_lsn);
  auto pin = fs.JoinPath(layout.BackupsDir(), name);
  absl::Cleanup remove_pin = [&]() noexcept { fs.TryRemoveFile(pin); };

  catalog::DeferRemovals();
  absl::Cleanup resume_removals = [] { catalog::ResumeRemovals(); };
  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForMaintenance);
  absl::Cleanup resume_cleanups = [] { search::RequestCleanups(); };
  auto maintenance = search::GetSearchEngine().Maintenance().Close(context);
  auto cut = RecordCut(context, pin, std::move(name), progress);
  auto checkpoints = catalog::Transactions::Get().CloseCheckpoints(context);
  auto label_file = BuildBackupLabel(
    label, start_lsn, positions.SystemIdentifier(), absl::Now(), cut);
  std::move(resume_cleanups).Cancel();
  std::move(resume_removals).Cancel();
  std::move(remove_pin).Cancel();

  // Noexcept committing of backup start
  _checkpoints = std::move(checkpoints);
  _maintenance = std::move(maintenance);
  _pin = std::move(pin);
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
  catalog::StorageFileSystem().TryRemoveFile(_pin);
  _pin.clear();
  _owner = {};
  _label_file.clear();
  _active = false;
}

}  // namespace sdb::backup
