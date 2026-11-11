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
#include <filesystem>
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
#include "utils/file_utils.h"

namespace sdb::backup {
namespace {

constexpr std::string_view kRetiredLabelFile = "backup_label.old";
constexpr std::string_view kSubscriptionHoldFile = "subscriptions.hold";

std::filesystem::path SubscriptionHoldPath() {
  return catalog::ClusterOf().Layout().EngineDir() / kSubscriptionHoldFile;
}

[[noreturn]] void Fail(std::string_view what, const std::filesystem::path& path,
                       std::string_view why) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG(what, " \"", path.string(), "\": ", why));
}

bool Exists(const std::filesystem::path& path) {
  std::error_code ec;
  const bool exists = std::filesystem::exists(path, ec);
  if (ec) {
    utils::file_utils::ThrowFileError("check", path.string(), ec);
  }
  return exists;
}

void RequireDirectory(const std::filesystem::path& path) {
  std::error_code ec;
  if (!std::filesystem::is_directory(path, ec)) {
    if (ec && ec != std::errc::no_such_file_or_directory) {
      utils::file_utils::ThrowFileError("check", path.string(), ec);
    }
    Fail("the backup names a missing directory", path, "it does not exist");
  }
}

void Remove(const std::filesystem::path& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    utils::file_utils::ThrowFileError("remove", path.string(), ec);
  }
}

void Restore(const catalog::DataDirectory& layout,
             const std::filesystem::path& label_path) {
  const std::filesystem::path data_dir{layout.directory};
  const auto engine_dir = layout.EngineDir();
  auto label = ParseBackupLabel(utils::file_utils::Slurp(label_path.string()));
  if (!label.ok()) {
    Fail("cannot use", label_path, label.status().message());
  }
  const auto control = ReadPgControl(data_dir);
  if (!control) {
    Fail("the copy has no control file", data_dir / kPgControlFile,
         "copy the whole data directory");
  }
  if (control->system_identifier != label->system_identifier) {
    Fail("the backup belongs to another cluster:", label_path,
         absl::StrCat("its system identifier is ", label->system_identifier,
                      ", the copy's is ", control->system_identifier));
  }
  const auto& cut = label->cut;
  for (const auto& store : cut.stores) {
    RequireDirectory(layout.StorageDir(store.database_oid, store.oid));
  }

  const auto log = layout.LogDir();
  const auto link = data_dir / cut.log_link;
  std::error_code ec;
  if (Exists(link)) {
    std::filesystem::create_directories(log, ec);
    if (ec) {
      utils::file_utils::ThrowFileError("create", log.string(), ec);
    }
    catalog::Log::Move(link, log);
    Remove(link);
  }
  catalog::Log::Cut(log, cut.log_size);
  SDB_IF_FAILURE("crash_in_restore_after_log") { SDB_IMMEDIATE_ABORT(); }

  Remove(data_dir / "recovery.signal");
  Remove(data_dir / "standby.signal");
  Remove(engine_dir / kBackupDir);
  SDB_IF_FAILURE("crash_in_restore_after_cleanup") { SDB_IMMEDIATE_ABORT(); }
  utils::file_utils::Spit((engine_dir / kSubscriptionHoldFile).string(), "",
                          true);
  WritePgControl(data_dir, {.system_identifier = control->system_identifier,
                            .checkpoint = label->start_lsn});
  SDB_IF_FAILURE("crash_in_restore_after_control") { SDB_IMMEDIATE_ABORT(); }

  for (const auto& store : cut.stores) {
    utils::file_utils::SyncDirectory(
      layout.StorageDir(store.database_oid, store.oid));
  }
  utils::file_utils::SyncDirectory(engine_dir);
  SDB_IF_FAILURE("crash_in_restore_before_retire") { SDB_IMMEDIATE_ABORT(); }
  std::filesystem::rename(label_path, data_dir / kRetiredLabelFile, ec);
  if (ec) {
    utils::file_utils::ThrowFileError("retire", label_path.string(), ec);
  }
  utils::file_utils::SyncDirectory(data_dir);
  SDB_INFO(STARTUP, "restored the backup described by '", label_path.string(),
           "'");
}

}  // namespace

void PrepareDataDirectory(const catalog::DataDirectory& layout) {
  const std::filesystem::path data_dir{layout.directory};
  const auto label_path = data_dir / kBackupLabelFile;
  try {
    if (Exists(label_path)) {
      Restore(layout, label_path);
    } else {
      Remove(layout.EngineDir() / kBackupDir);
    }
    PositionSource::Instance().Open(data_dir);
    EnsurePgVersion(data_dir);
  } catch (const std::exception& e) {
    SDB_FATAL(STARTUP, "cannot prepare data directory '", data_dir.string(),
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

bool SubscriptionsHeld() { return Exists(SubscriptionHoldPath()); }

void ReleaseSubscriptionHold() {
  const auto path = SubscriptionHoldPath();
  Remove(path);
  utils::file_utils::SyncDirectory(path.parent_path());
}

}  // namespace sdb::backup
