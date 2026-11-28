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

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>

#include <duckdb/common/file_system.hpp>
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
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/log.h"
#include "catalog/pins.h"
#include "catalog/transaction.h"

namespace sdb::backup {
namespace {

constexpr std::string_view kReplacingSuffix = ".tmp";

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

void CheckName(std::string_view name) {
  const bool valid =
    !name.empty() && name.front() != '.' &&
    !absl::EndsWith(name, kReplacingSuffix) &&
    !name.starts_with(kSessionPinPrefix) && absl::c_all_of(name, [](char c) {
      return absl::ascii_isalnum(c) || c == '_' || c == '-' || c == '.';
    });
  if (!valid) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_NAME),
      ERR_MSG("invalid backup name \"", name, "\""),
      ERR_DETAIL("A backup name holds letters, digits, '_', '-' and '.', does "
                 "not start with '.', does not end with \"",
                 kReplacingSuffix, "\" and does not start with \"",
                 kSessionPinPrefix, "\"."));
  }
}

CreatedBackup Create(duckdb::ClientContext& context, std::string_view name,
                     std::string_view label, uint64_t start_lsn,
                     pg::ProgressMetrics& progress) {
  if (const auto tablespace = UserTablespace()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot start a backup: tablespace \"",
                            tablespace->GetIdentifierName(),
                            "\" keeps its files outside the server directory"),
                    ERR_DETAIL("A backup copies only the server directory."));
  }
  auto& transactions = catalog::Transactions::Get();
  auto* log = transactions.GetLog();
  SDB_ENSURE(log, "cannot start a backup of a server without a WAL");
  const auto& layout = catalog::ClusterOf().Layout();
  auto& fs = catalog::StorageFileSystem();
  fs.CreateDirectoryExtended(
    layout.BackupsDir(),
    {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true});

  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForCheckpoint);
  const auto checkpoints = transactions.CloseCheckpoints(context);
  auto& pins = catalog::Pins::Get();
  if (pins.Contains(name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("backup \"", name, "\" already exists"));
  }
  progress.SetPhase(pg::progress_phase::BaseBackup::WaitingForCommits);
  const auto commits = CommitGate().Close(context);
  const auto pin = fs.JoinPath(layout.BackupsDir(), name);
  absl::Cleanup drop_pin = [&] {
    fs.TryRemoveFile(pin);
    catalog::RemoveParked(pins.Remove(name));
  };
  const auto status = transactions.TruncateLog(
    checkpoints, [&](std::span<const uint64_t> segments) {
      log->Pin(pin);
      pins.Add(std::string{name}, segments);
    });
  if (!status.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("cannot start a backup: the checkpoint failed: ",
                            status.message()));
  }
  CreatedBackup created{
    .start_lsn = start_lsn,
    .label_file =
      BuildBackupLabel(label, start_lsn, PositionSource::Instance().ServerId(),
                       absl::Now(), {.snapshot = std::string{name}}),
  };
  std::move(drop_pin).Cancel();
  return created;
}

void Drop(std::string_view name) {
  const auto& layout = catalog::ClusterOf().Layout();
  auto& fs = catalog::StorageFileSystem();
  fs.RemoveFile(fs.JoinPath(layout.BackupsDir(), name));
  fs.SyncDirectory(layout.BackupsDir());
  catalog::RemoveParked(catalog::Pins::Get().Remove(name));
}

}  // namespace

CreatedBackup CreateBackup(duckdb::ClientContext& context,
                           std::string_view name,
                           pg::ProgressMetrics& progress) {
  CheckName(name);
  progress.SetCommand(pg::ProgressCommand::BaseBackup);
  progress.SetPhase(pg::progress_phase::BaseBackup::Initializing);
  return Create(context, name, name, PositionSource::Instance().Issue(),
                progress);
}

void DropBackup(std::string_view name) {
  if (!catalog::Pins::Get().Contains(name) ||
      name.starts_with(kSessionPinPrefix)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("backup \"", name, "\" does not exist"));
  }
  Drop(name);
}

std::vector<std::string> Backups() { return catalog::Pins::Get().Names(); }

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
  const auto start_lsn = PositionSource::Instance().Issue();
  auto name = absl::StrCat(kSessionPinPrefix, start_lsn);
  auto created = Create(context, name, label, start_lsn, progress);

  // Noexcept committing of backup start
  _name = std::move(name);
  _owner = owner;
  _label_file = std::move(created.label_file);
  _active = true;
  return created.start_lsn;
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
  try {
    Drop(_name);
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "cannot remove the backup \"", _name, "\": ", e.what());
  }
  _name.clear();
  _owner = {};
  _label_file.clear();
  _active = false;
}

}  // namespace sdb::backup
