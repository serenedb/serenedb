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

#include "catalog/database_directory.h"

#include <absl/algorithm/container.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <ranges>
#include <utility>
#include <vector>

#include "scheduler/background_scheduler.h"
#include "server/utils/lifecycle.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kStorageMarker = ".storage";

constinit std::atomic<uint64_t> gTablespaceRemovals{0};

bool IsRemote(std::string_view path) {
  return irs::StorageKindOf(path) == irs::StorageKind::Remote;
}

bool TryCreateDirectory(const std::filesystem::path& path) {
  auto& fs = StorageFileSystem();
  if (!fs.CreateDirectoryExtended(path.string(), {})) {
    return false;
  }
  fs.SyncDirectory(path.parent_path().string());
  return true;
}

std::string MarkerPath(std::string_view path) {
  return absl::StrCat(path, kStorageMarker);
}

using Removal = absl::AnyInvocable<void()>;

struct RemovalGate {
  std::atomic_bool deferring{false};
  absl::Mutex mutex;
  std::vector<Removal> deferred ABSL_GUARDED_BY(mutex);
};

RemovalGate& Gate() {
  static RemovalGate gGate;
  return gGate;
}

void RemoveObject(const std::string& path) {
  try {
    StorageFileSystem().TryRemoveFile(path);
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "could not remove '", path,
             "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

void RunRemoval(Removal&& removal) {
  if (auto& gate = Gate(); gate.deferring.load()) {
    absl::MutexLock lock{&gate.mutex};
    if (gate.deferring.load()) {
      gate.deferred.push_back(std::move(removal));
      return;
    }
  }
  if (lifecycle::IsStopping() || BackgroundScheduler::instance().IsStopping()) {
    removal();
    return;
  }
  BackgroundScheduler::instance().Run(std::move(removal)).Detach();
}

}  // namespace

void DeferRemovals() noexcept {
  [[maybe_unused]] const bool was_deferring = Gate().deferring.exchange(true);
  SDB_ASSERT(!was_deferring);
}

void ResumeRemovals() {
  std::vector<Removal> deferred;
  {
    auto& gate = Gate();
    absl::MutexLock lock{&gate.mutex};
    gate.deferring.store(false);
    deferred.swap(gate.deferred);
  }
  for (auto& removal : deferred) {
    RunRemoval(std::move(removal));
  }
}

void CreateDirectory(const std::filesystem::path& path) {
  if (!TryCreateDirectory(path)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("could not create directory \"", path.string(),
                            "\": it already exists"));
  }
}

void CreateDirectories(const std::filesystem::path& path) {
  auto& fs = StorageFileSystem();
  std::vector<std::filesystem::path> missing;
  for (auto directory = path;
       !directory.empty() && !fs.DirectoryExists(directory.string());
       directory = directory.parent_path()) {
    missing.push_back(directory);
  }
  for (const auto& directory : missing | std::views::reverse) {
    TryCreateDirectory(directory);
  }
}

bool RemoveTree(const std::string& path) noexcept {
  duckdb::TaskScheduler::BlockingScope blocking;
  try {
    auto& fs = StorageFileSystem();
    SDB_IF_FAILURE("pause_removal_midway") {
      std::vector<std::string> files;
      fs.ListFiles(path,
                   [&](duckdb::OpenFileInfo& info) {
                     files.push_back(std::move(info.path));
                   },
                   nullptr, {.recursive = true});
      if (!files.empty()) {
        fs.RemoveFile(fs.JoinPath(path, *absl::c_min_element(files)));
      }
      SDB_PARK_ONCE_ON_FAILURE("pause_removal_midway");
    }
    fs.RemoveDirectory(path);
    if (IsRemote(path) || !fs.DirectoryExists(path)) {
      return true;
    }
    SDB_WARN(GENERAL, "could not remove '", path, "'");
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "could not remove '", path,
             "': ", duckdb::ErrorData{e}.RawMessage());
  }
  return false;
}

duckdb::FileSystem& StorageFileSystem() {
  return irs::DuckDBEngine::Instance().instance().GetInternalFileSystem();
}

std::optional<duckdb::idx_t> OidFromName(std::string_view name) {
  duckdb::idx_t oid = 0;
  if (!absl::c_all_of(name, absl::ascii_isdigit) ||
      !absl::SimpleAtoi(name, &oid)) {
    return std::nullopt;
  }
  return oid;
}

void CreateStorage(const std::string& path) {
  if (IsRemote(path)) {
    StorageFileSystem()
      .OpenFile(MarkerPath(path),
                duckdb::FileFlags::FILE_FLAGS_WRITE |
                  duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW)
      ->Close();
    return;
  }
  const std::filesystem::path directory{path};
  CreateDirectories(directory.parent_path());
  CreateDirectory(directory);
}

bool StorageExists(const std::string& path) {
  auto& fs = StorageFileSystem();
  return IsRemote(path) ? fs.FileExists(MarkerPath(path))
                        : fs.DirectoryExists(path);
}

void RemoveStorage(StorageLocation location) {
  RunRemoval([location = std::move(location)] {
    RemoveTree(location.path);
    if (IsRemote(location.path)) {
      RemoveObject(MarkerPath(location.path));
    }
  });
}

bool TablespaceRemovalsPending() noexcept {
  return gTablespaceRemovals.load(std::memory_order_relaxed) != 0;
}

TablespaceDirectory::~TablespaceDirectory() {
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  RunRemoval([path = std::move(_path)] {
    SDB_PARK_ONCE_ON_FAILURE("pause_tablespace_removal");
    SDB_IF_FAILURE("tablespace_removal_fails") { return; }
    if (RemoveTree(path)) {
      gTablespaceRemovals.fetch_sub(1, std::memory_order_relaxed);
    }
  });
}

void TablespaceDirectory::MarkDropped() noexcept {
  gTablespaceRemovals.fetch_add(1, std::memory_order_relaxed);
  _dropped.store(true, std::memory_order_release);
}

std::vector<StorageEntry> ListStorages(const std::string& root) {
  std::vector<StorageEntry> found;
  StorageFileSystem().ListFiles(
    root, [&](const std::string& name, bool is_directory) {
      std::string_view digits = name;
      if (!is_directory && !absl::ConsumeSuffix(&digits, kStorageMarker)) {
        return;
      }
      if (const auto oid = OidFromName(digits)) {
        found.push_back({.oid = *oid,
                         .path = absl::StrCat(root, "/", name),
                         .is_directory = is_directory});
      }
    });
  return found;
}

bool HoldsFiles(const StorageEntry& entry) {
  bool holds = false;
  if (entry.is_directory) {
    StorageFileSystem().ListFiles(
      entry.path, [&](const std::string&, bool) { holds = true; });
  }
  return holds;
}

StorageSize StorageBytes(const std::string& directory) {
  StorageSize size;
  StorageFileSystem().ListFiles(
    directory,
    [&](duckdb::OpenFileInfo& info) {
      duckdb::idx_t bytes = 0;
      if (info.extended_info &&
          info.extended_info->TryGetOption("file_size", bytes)) {
        (absl::StrContains(info.path, '/') ? size.nested : size.files) += bytes;
      }
    },
    nullptr, {.recursive = true});
  return size;
}

void RemoveUnowned(std::span<const StorageEntry> entries,
                   const irs::containers::FlatHashSet<duckdb::idx_t>& owned) {
  for (const auto& entry : entries) {
    if (owned.contains(entry.oid)) {
      continue;
    }
    if (entry.is_directory) {
      RemoveTree(entry.path);
    } else {
      RemoveObject(entry.path);
    }
  }
}

DatabaseDirectory::DatabaseDirectory(
  duckdb::idx_t oid, std::filesystem::path path,
  std::shared_ptr<TablespaceDirectory> tablespace)
  : _oid{oid}, _path{std::move(path)}, _tablespace{std::move(tablespace)} {}

DatabaseDirectory::~DatabaseDirectory() {
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  std::vector<std::string> directories;
  const auto add = [&](std::string directory) {
    if (directory != _path.string() &&
        !absl::c_contains(directories, directory)) {
      directories.push_back(std::move(directory));
    }
  };
  if (_tablespace) {
    add(absl::StrCat(_tablespace->Path(), "/", _oid));
  }
  for (const auto& tablespace : _tablespaces) {
    add(absl::StrCat(tablespace, "/", _oid));
  }
  RunRemoval([path = _path.string(), directories = std::move(directories)] {
    RemoveTree(path);
    for (const auto& directory : directories) {
      RemoveTree(directory);
    }
  });
}

std::string DatabaseDirectory::DataFile() const {
  return (_path / kDataFile).string();
}

std::string DatabaseDirectory::StoragePath(
  const TablespaceDirectory* tablespace, duckdb::idx_t oid) const {
  if (!tablespace) {
    return absl::StrCat(_path.string(), "/", oid);
  }
  return absl::StrCat(tablespace->Path(), "/", _oid, "/", oid);
}

void DatabaseDirectory::Create() const {
  CreateDirectories(_path.parent_path());
  CreateDirectory(_path);
}

}  // namespace sdb::catalog
