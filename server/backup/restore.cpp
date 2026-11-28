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

#include "backup/restore.h"

#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/query_context.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "backup/backup_label.h"
#include "backup/backup_session.h"
#include "backup/pg_control.h"
#include "catalog/boot.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/log.h"

namespace sdb::backup {
namespace {

constexpr std::string_view kRetiredLabelFile = "backup_label.old";
constexpr std::string_view kSubscriptionHoldFile = "subscriptions.hold";

const std::string& ServerDir() {
  return catalog::ClusterOf().Layout().directory;
}

[[noreturn]] void Fail(std::string_view what, std::string_view path,
                       std::string_view why) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG(what, " \"", path, "\": ", why));
}

void RequireDirectory(duckdb::FileSystem& fs, const std::string& path) {
  if (!fs.DirectoryExists(path)) {
    Fail("the backup names a missing directory", path, "it does not exist");
  }
}

void RemoveFiles(duckdb::FileSystem& fs, const std::string& dir) {
  if (!fs.DirectoryExists(dir)) {
    return;
  }
  std::vector<std::string> names;
  fs.ListFiles(
    dir,
    [&](duckdb::OpenFileInfo& info) {
      if (!duckdb::FileSystem::IsDirectory(info)) {
        names.push_back(std::move(info.path));
      }
    },
    nullptr, {.strict = true});
  for (const auto& name : names) {
    fs.RemoveFile(fs.JoinPath(dir, name));
  }
  fs.SyncDirectory(dir);
}

void Restore(const catalog::ServerDirectory& layout,
             const std::string& label_path) {
  auto& fs = catalog::StorageFileSystem();
  const auto& data_dir = layout.directory;
  auto label = ParseBackupLabel(
    irs::ReadWhole<std::string>(duckdb::QueryContext{}, fs, label_path));
  if (!label.ok()) {
    Fail("cannot use", label_path, label.status().message());
  }
  const auto control = ReadPgControl(data_dir);
  if (!control) {
    Fail("the copy has no control file", fs.JoinPath(data_dir, kPgControlFile),
         "copy the whole data directory");
  }
  if (control->system_identifier != label->system_identifier) {
    Fail("the backup belongs to another cluster:", label_path,
         absl::StrCat("its system identifier is ", label->system_identifier,
                      ", the copy's is ", control->system_identifier));
  }
  RequireDirectory(fs, layout.StoreDir());
  const auto pin = fs.JoinPath(layout.BackupsDir(), label->cut.snapshot);
  if (!fs.FileExists(pin)) {
    Fail("the copy has no snapshot of the backup", pin,
         "copy the whole server directory");
  }
  fs.MoveFile(pin, layout.SnapshotPath());
  fs.SyncDirectory(data_dir);
  RemoveFiles(fs, layout.LogDir());
  RemoveFiles(fs, layout.BackupsDir());
  SDB_IF_FAILURE("crash_in_restore_after_log") { SDB_IMMEDIATE_ABORT(); }

  fs.TryRemoveFile(fs.JoinPath(data_dir, "recovery.signal"));
  fs.TryRemoveFile(fs.JoinPath(data_dir, "standby.signal"));
  SDB_IF_FAILURE("crash_in_restore_after_cleanup") { SDB_IMMEDIATE_ABORT(); }
  irs::WriteWhole(fs, fs.JoinPath(data_dir, kSubscriptionHoldFile), {}, true);
  WritePgControl(data_dir, {.system_identifier = control->system_identifier,
                            .checkpoint = label->start_lsn});
  SDB_IF_FAILURE("crash_in_restore_after_control") { SDB_IMMEDIATE_ABORT(); }

  fs.SyncDirectory(layout.StoreDir());
  SDB_IF_FAILURE("crash_in_restore_before_retire") { SDB_IMMEDIATE_ABORT(); }
  fs.MoveFile(label_path, fs.JoinPath(data_dir, kRetiredLabelFile));
  fs.SyncDirectory(data_dir);
  SDB_INFO(STARTUP, "restored the backup described by '", label_path, "'");
}

}  // namespace

void PrepareServerDirectory(const catalog::ServerDirectory& layout) {
  const auto& data_dir = layout.directory;
  auto& fs = catalog::StorageFileSystem();
  try {
    if (const auto label_path = fs.JoinPath(data_dir, kBackupLabelFile);
        fs.FileExists(label_path)) {
      Restore(layout, label_path);
    }
    PositionSource::Instance().Open(data_dir);
  } catch (const std::exception& e) {
    SDB_FATAL(STARTUP, "cannot prepare server directory '", data_dir,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

bool SubscriptionsHeld() {
  auto& fs = catalog::StorageFileSystem();
  return fs.FileExists(fs.JoinPath(ServerDir(), kSubscriptionHoldFile));
}

void ReleaseSubscriptionHold() {
  auto& fs = catalog::StorageFileSystem();
  const auto& dir = ServerDir();
  fs.TryRemoveFile(fs.JoinPath(dir, kSubscriptionHoldFile));
  fs.SyncDirectory(dir);
}

}  // namespace sdb::backup
