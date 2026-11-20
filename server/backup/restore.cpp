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

const std::string& EngineDir() { return catalog::ClusterOf().Layout().engine; }

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

void RemoveLogCopy(duckdb::FileSystem& fs, const std::string& dir) {
  if (!catalog::Log::Remove(fs, dir)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("could not remove \"", dir, "\""));
  }
}

void Restore(const catalog::DataDirectory& layout,
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
  const auto& cut = label->cut;
  RequireDirectory(fs, layout.StoreDir());

  const auto log = layout.LogDir();
  if (const auto link = fs.JoinPath(layout.engine, kBackupDir);
      fs.DirectoryExists(link)) {
    fs.CreateDirectoryExtended(
      log, {.mode = duckdb::CreateDirectoryMode::RECURSIVE});
    catalog::Log::Move(fs, link, log);
    RemoveLogCopy(fs, link);
  }
  catalog::Log::Cut(fs, log, cut.log_size);
  SDB_IF_FAILURE("crash_in_restore_after_log") { SDB_IMMEDIATE_ABORT(); }

  fs.TryRemoveFile(fs.JoinPath(data_dir, "recovery.signal"));
  fs.TryRemoveFile(fs.JoinPath(data_dir, "standby.signal"));
  SDB_IF_FAILURE("crash_in_restore_after_cleanup") { SDB_IMMEDIATE_ABORT(); }
  irs::WriteWhole(fs, fs.JoinPath(layout.engine, kSubscriptionHoldFile), {},
                  true);
  WritePgControl(data_dir, {.system_identifier = control->system_identifier,
                            .checkpoint = label->start_lsn});
  SDB_IF_FAILURE("crash_in_restore_after_control") { SDB_IMMEDIATE_ABORT(); }

  fs.SyncDirectory(layout.StoreDir());
  fs.SyncDirectory(layout.engine);
  SDB_IF_FAILURE("crash_in_restore_before_retire") { SDB_IMMEDIATE_ABORT(); }
  fs.MoveFile(label_path, fs.JoinPath(data_dir, kRetiredLabelFile));
  fs.SyncDirectory(data_dir);
  SDB_INFO(STARTUP, "restored the backup described by '", label_path, "'");
}

}  // namespace

void PrepareDataDirectory(const catalog::DataDirectory& layout) {
  const auto& data_dir = layout.directory;
  auto& fs = catalog::StorageFileSystem();
  try {
    if (const auto label_path = fs.JoinPath(data_dir, kBackupLabelFile);
        fs.FileExists(label_path)) {
      Restore(layout, label_path);
    } else {
      RemoveLogCopy(fs, fs.JoinPath(layout.engine, kBackupDir));
    }
    PositionSource::Instance().Open(data_dir);
  } catch (const std::exception& e) {
    SDB_FATAL(STARTUP, "cannot prepare data directory '", data_dir,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

bool SubscriptionsHeld() {
  auto& fs = catalog::StorageFileSystem();
  return fs.FileExists(fs.JoinPath(EngineDir(), kSubscriptionHoldFile));
}

void ReleaseSubscriptionHold() {
  auto& fs = catalog::StorageFileSystem();
  const auto& engine = EngineDir();
  fs.TryRemoveFile(fs.JoinPath(engine, kSubscriptionHoldFile));
  fs.SyncDirectory(engine);
}

}  // namespace sdb::backup
