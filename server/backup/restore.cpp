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
#include <duckdb/common/path.hpp>
#include <duckdb/common/query_context.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "backup/backup_session.h"
#include "backup/pg_control.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"

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

template<typename Prepare>
auto Fatally(const catalog::ServerDirectory& layout, Prepare&& prepare) {
  try {
    return prepare();
  } catch (const std::exception& e) {
    SDB_FATAL(STARTUP, "cannot prepare server directory '", layout.directory,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

void RemoveSessionPins(duckdb::FileSystem& fs, const std::string& dir) {
  if (!fs.DirectoryExists(dir)) {
    return;
  }
  std::vector<std::string> names;
  fs.ListFiles(dir,
               [&](duckdb::OpenFileInfo& info) {
                 if (!duckdb::FileSystem::IsDirectory(info) &&
                     info.path.starts_with(kSessionPinPrefix)) {
                   names.push_back(std::move(info.path));
                 }
               },
               nullptr, {.strict = true});
  if (names.empty()) {
    return;
  }
  for (const auto& name : names) {
    fs.RemoveFile(fs.JoinPath(dir, name));
  }
  fs.SyncDirectory(dir);
}

void Restore(const catalog::ServerDirectory& layout,
             const PendingRestore& restore) {
  auto& fs = catalog::StorageFileSystem();
  const auto& data_dir = layout.directory;
  const bool remote = layout.Remote();
  catalog::Log::Copy(fs, restore.pin, layout.SnapshotPath());
  catalog::Log::Discard(fs, layout.WalDir(), layout.SnapshotPath());
  SDB_IF_FAILURE("crash_in_restore_after_log") { SDB_IMMEDIATE_ABORT(); }

  if (!remote) {
    fs.TryRemoveFile(fs.JoinPath(data_dir, "recovery.signal"));
    fs.TryRemoveFile(fs.JoinPath(data_dir, "standby.signal"));
  }
  SDB_IF_FAILURE("crash_in_restore_after_cleanup") { SDB_IMMEDIATE_ABORT(); }
  irs::WriteWhole(fs, fs.JoinPath(data_dir, kSubscriptionHoldFile), {}, true);
  if (!remote) {
    WritePgControl(data_dir,
                   {.system_identifier = restore.label.server_id.High(),
                    .checkpoint = restore.label.start_lsn});
  }
  SDB_IF_FAILURE("crash_in_restore_after_control") { SDB_IMMEDIATE_ABORT(); }

  fs.SyncDirectory(layout.StoreDir());
  SDB_IF_FAILURE("crash_in_restore_before_retire") { SDB_IMMEDIATE_ABORT(); }
  const auto retired = fs.JoinPath(data_dir, kRetiredLabelFile);
  if (remote) {
    fs.CopyFile(restore.label_path, retired);
    fs.RemoveFile(restore.label_path);
  } else {
    fs.MoveFile(restore.label_path, retired);
    fs.SyncDirectory(data_dir);
  }
  fs.RemoveFile(restore.pin);
  fs.SyncDirectory(layout.BackupsDir());
  SDB_INFO(STARTUP, "restored the backup described by '", restore.label_path,
           "'");
}

}  // namespace

std::optional<PendingRestore> FindRestore(
  const catalog::ServerDirectory& layout) {
  return Fatally(layout, [&]() -> std::optional<PendingRestore> {
    auto& fs = catalog::StorageFileSystem();
    auto label_path = fs.JoinPath(layout.directory, kBackupLabelFile);
    if (!fs.FileExists(label_path)) {
      return std::nullopt;
    }
    auto label = ParseBackupLabel(
      irs::ReadWhole<std::string>(duckdb::QueryContext{}, fs, label_path));
    if (!label.ok()) {
      Fail("cannot use", label_path, label.status().message());
    }
    if (!layout.Remote()) {
      RequireDirectory(fs, layout.StoreDir());
    }
    auto pin = fs.JoinPath(layout.BackupsDir(), label->cut.snapshot);
    const auto identity = catalog::Log::ReadIdentity(fs, pin);
    if (!identity) {
      Fail("the copy has no snapshot of the backup", pin,
           "copy the whole server directory");
    }
    if (identity->server_id != label->server_id) {
      Fail(
        "the backup belongs to another server:", label_path,
        absl::StrCat("it names the server ", label->server_id.Hex(),
                     ", its snapshot the server ", identity->server_id.Hex()));
    }
    return PendingRestore{
      .label_path = std::move(label_path),
      .label = *std::move(label),
      .pin = std::move(pin),
      .identity = *identity,
    };
  });
}

void PrepareServerDirectory(const catalog::ServerDirectory& layout,
                            const std::optional<PendingRestore>& restore) {
  Fatally(layout, [&] {
    if (restore) {
      Restore(layout, *restore);
    }
    RemoveSessionPins(catalog::StorageFileSystem(), layout.BackupsDir());
  });
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
