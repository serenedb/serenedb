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

#include <absl/container/btree_map.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <filesystem>
#include <iresearch/formats/index_meta_reader.hpp>
#include <iresearch/formats/index_meta_writer.hpp>
#include <iresearch/index/index_writer.hpp>
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

void TruncateFile(const std::filesystem::path& path, uint64_t size) {
  const int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
  if (fd < 0) {
    utils::file_utils::ThrowFileError("open", path.string(), errno);
  }
  struct stat st{};
  bool ok = ::fstat(fd, &st) == 0;
  int error = errno;
  if (ok && static_cast<uint64_t>(st.st_size) < size) {
    ::close(fd);
    Fail("the copy is incomplete:", path,
         absl::StrCat("it has ", st.st_size, " bytes, the backup recorded ",
                      size));
  }
  ok = ok && ::ftruncate(fd, static_cast<off_t>(size)) == 0 && ::fsync(fd) == 0;
  error = ok ? 0 : errno;
  ::close(fd);
  if (!ok) {
    utils::file_utils::ThrowFileError("truncate", path.string(), error);
  }
}

template<typename Visit>
void ForEachFile(const std::filesystem::path& directory, Visit&& visit) {
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator{directory, ec}) {
    visit(entry.path(), entry.path().filename().string());
  }
  if (ec) {
    utils::file_utils::ThrowFileError("list", directory.string(), ec);
  }
}

void PruneStore(const std::filesystem::path& directory, uint64_t generation) {
  ForEachFile(directory,
              [&](const std::filesystem::path& path, std::string_view name) {
                if (absl::StartsWith(name, irs::index_meta::kPendingPrefix) ||
                    irs::index_meta::ParseGeneration(name) > generation) {
                  Remove(path);
                }
              });
}

void RemoveStaleLocks(const std::filesystem::path& engine_dir) {
  for (const auto& [_, database] : catalog::OidDirectories(engine_dir)) {
    for (const auto& [_, store] : catalog::OidDirectories(database)) {
      Remove(store / irs::IndexWriter::kWriteLockName);
    }
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
    ForEachFile(link,
                [&](const std::filesystem::path& path, std::string_view name) {
                  if (name != catalog::Log::kSnapshotFile &&
                      !catalog::Log::ChunkBegin(name)) {
                    return;
                  }
                  std::filesystem::rename(path, log / name, ec);
                  if (ec) {
                    utils::file_utils::ThrowFileError("restore the log from",
                                                      path.string(), ec);
                  }
                });
    utils::file_utils::SyncDirectory(log);
    Remove(link);
  }
  absl::btree_map<catalog::Lsn, std::filesystem::path> chunks;
  ForEachFile(log,
              [&](const std::filesystem::path& path, std::string_view name) {
                if (const auto begin = catalog::Log::ChunkBegin(name)) {
                  chunks.emplace(*begin, path);
                }
              });
  if (const auto from = catalog::Log::SnapshotCut(log);
      from && *from < cut.log_size &&
      (chunks.empty() || chunks.begin()->first > *from)) {
    Fail("the copy is incomplete:", log,
         absl::StrCat("the log needs the records from ", *from,
                      ", its first file starts at ",
                      chunks.empty() ? cut.log_size : chunks.begin()->first));
  }
  for (auto it = chunks.begin(); it != chunks.end(); ++it) {
    const auto& [begin, path] = *it;
    const auto next = std::next(it);
    if (begin > cut.log_size) {
      Remove(path);
    } else if (next == chunks.end() || next->first > cut.log_size) {
      TruncateFile(path, cut.log_size - begin);
    } else if (std::filesystem::file_size(path) < next->first - begin) {
      Fail("the copy is incomplete:", path,
           absl::StrCat("it has ", std::filesystem::file_size(path),
                        " bytes, the log needs ", next->first - begin));
    }
  }
  utils::file_utils::SyncDirectory(log);
  SDB_IF_FAILURE("crash_in_restore_after_log") { SDB_IMMEDIATE_ABORT(); }

  for (const auto& store : cut.stores) {
    PruneStore(layout.StorageDir(store.database_oid, store.oid),
               store.generation);
  }
  SDB_IF_FAILURE("crash_in_restore_after_stores") { SDB_IMMEDIATE_ABORT(); }

  RemoveStaleLocks(engine_dir);
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
