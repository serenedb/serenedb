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
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/store/directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>
#include <vector>

#include "catalog/cluster.h"
#include "scheduler/background_scheduler.h"
#include "server/utils/lifecycle.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kDataFile = "data.db";
constexpr std::string_view kStorageMarker = ".storage";
constexpr std::string_view kTablespaceMarker = "tablespace";

bool CreateDirectory(const std::filesystem::path& path) {
  auto& fs = StorageFileSystem();
  if (!fs.CreateDirectoryExtended(path.string(), {})) {
    return false;
  }
  fs.SyncDirectory(path.parent_path().string());
  return true;
}

bool CreateMarker(const std::string& marker) {
  auto handle = StorageFileSystem().OpenFile(
    marker, duckdb::FileFlags::FILE_FLAGS_WRITE |
              duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
              duckdb::FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE |
              duckdb::FileFlags::FILE_FLAGS_NULL_IF_EXISTS);
  if (!handle) {
    return false;
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_marker_creation");
  handle->Close();
  return true;
}

void CreateNewDirectory(const std::filesystem::path& path) {
  if (!CreateDirectory(path)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("could not create directory \"", path.string(),
                            "\": it already exists"));
  }
}

void RemoveTree(const std::string& path) {
  duckdb::TaskScheduler::BlockingScope blocking;
  try {
    auto& fs = StorageFileSystem();
    SDB_IF_FAILURE("pause_removal_midway") {
      std::vector<std::string> files;
      fs.ListFilesRecursive(path, [&](duckdb::OpenFileInfo& info) {
        files.push_back(std::move(info.path));
      });
      if (!files.empty()) {
        fs.RemoveFile(fs.JoinPath(path, *absl::c_min_element(files)));
      }
      SDB_PARK_ONCE_ON_FAILURE("pause_removal_midway");
    }
    fs.RemoveDirectory(path);
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "could not remove '", path,
             "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

void RemoveObject(const std::string& path) {
  try {
    StorageFileSystem().TryRemoveFile(path);
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "could not remove '", path,
             "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

template<typename Removal>
void RunRemoval(Removal&& removal) {
  if (lifecycle::IsStopping() || BackgroundScheduler::instance().IsStopping()) {
    removal();
    return;
  }
  BackgroundScheduler::instance().Run(std::forward<Removal>(removal)).Detach();
}

}  // namespace

duckdb::FileSystem& StorageFileSystem() {
  return duckdb::FileSystem::GetFileSystem(
    irs::DuckDBEngine::Instance().instance());
}

std::string EngineDirOf(std::string_view location) {
  return absl::StrCat(location, "/", kEngineDir);
}

std::optional<duckdb::idx_t> OidFromName(std::string_view name) {
  duckdb::idx_t oid = 0;
  if (name.empty() || !absl::c_all_of(name, absl::ascii_isdigit) ||
      !absl::SimpleAtoi(name, &oid)) {
    return std::nullopt;
  }
  return oid;
}

std::vector<StorageEntry> ListStorages(const std::string& root) {
  std::vector<StorageEntry> found;
  irs::containers::FlatHashSet<std::string> seen;
  StorageFileSystem().ListFiles(
    root, [&](const std::string& name, bool is_directory) {
      std::string_view entry = name;
      bool directory = is_directory;
      if (const auto slash = entry.find('/'); slash != std::string_view::npos) {
        entry = entry.substr(0, slash);
        directory = true;
      }
      std::string_view digits = entry;
      if (!directory && !absl::ConsumeSuffix(&digits, kStorageMarker)) {
        return;
      }
      const auto oid = OidFromName(digits);
      if (!oid || !seen.emplace(entry).second) {
        return;
      }
      found.push_back({.oid = *oid,
                       .path = absl::StrCat(root, "/", entry),
                       .is_directory = directory});
    });
  return found;
}

StorageSize StorageBytes(const std::string& directory) {
  StorageSize size;
  StorageFileSystem().ListFilesRecursive(
    directory, [&](duckdb::OpenFileInfo& info) {
      duckdb::idx_t bytes = 0;
      if (info.extended_info &&
          info.extended_info->TryGetOption("file_size", bytes)) {
        (absl::StrContains(info.path, '/') ? size.nested : size.files) += bytes;
      }
    });
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

TablespaceRoot::TablespaceRoot(std::string path)
  : _path{std::move(path)},
    _remote{irs::StorageKindOf(_path) == irs::StorageKind::Remote} {}

TablespaceRoot::~TablespaceRoot() {
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  RunRemoval([path = std::move(_path)] {
    SDB_PARK_ONCE_ON_FAILURE("pause_tablespace_removal");
    RemoveTree(path);
  });
}

void TablespaceRoot::Claim() {
  const auto in_use = [&] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("directory \"", Path(), "\" already in use as a tablespace"),
      ERR_HINT("Remove it if no tablespace uses it; a dropped tablespace "
               "removes it once the transactions that began before the drop "
               "have ended."));
  };
  if (!_remote) {
    const std::filesystem::path path{Path()};
    StorageFileSystem().CreateDirectoriesRecursive(path.parent_path().string());
    if (!CreateDirectory(path)) {
      in_use();
    }
  } else if (!CreateMarker(absl::StrCat(Path(), "/", kTablespaceMarker))) {
    in_use();
  }
}

std::string TablespaceRoot::DatabaseDir(duckdb::idx_t oid) const {
  return absl::StrCat(Path(), "/", oid);
}

StorageDirectory::StorageDirectory(std::shared_ptr<DatabaseDirectory> database,
                                   std::shared_ptr<TablespaceRoot> root,
                                   std::string path)
  : _database{std::move(database)},
    _root{std::move(root)},
    _path{std::move(path)} {}

std::string StorageDirectory::StoragePath(duckdb::idx_t oid) const {
  return absl::StrCat(_path, "/", oid);
}

std::string StorageDirectory::MarkerPath(duckdb::idx_t oid) const {
  return absl::StrCat(_path, "/", oid, kStorageMarker);
}

std::string StorageDirectory::CreateStorage(duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  if (!Remote()) {
    CreateDirectory(_path);
    CreateNewDirectory(path);
    return path;
  }
  if (!CreateMarker(MarkerPath(oid))) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_IO_ERROR),
      ERR_MSG("could not create storage \"", path, "\": it already exists"));
  }
  return path;
}

std::optional<std::string> StorageDirectory::OpenStorage(
  duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  auto& fs = StorageFileSystem();
  if (Remote() ? fs.FileExists(MarkerPath(oid)) : fs.DirectoryExists(path)) {
    return path;
  }
  return std::nullopt;
}

void StorageDirectory::RemoveStorage(duckdb::idx_t oid) const {
  RunRemoval([directory = *this, oid] {
    RemoveTree(directory.StoragePath(oid));
    if (directory.Remote()) {
      RemoveObject(directory.MarkerPath(oid));
    }
  });
}

DatabaseDirectory::DatabaseDirectory(duckdb::idx_t oid,
                                     std::shared_ptr<TablespaceRoot> root,
                                     std::filesystem::path path)
  : _oid{oid}, _root{std::move(root)}, _path{std::move(path)} {}

DatabaseDirectory::~DatabaseDirectory() {
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  std::vector<std::string> directories;
  const auto add = [&](const std::shared_ptr<TablespaceRoot>& root) {
    if (!root) {
      return;
    }
    auto directory = root->DatabaseDir(_oid);
    if (directory != _path.string() &&
        !absl::c_contains(directories, directory)) {
      directories.push_back(std::move(directory));
    }
  };
  add(_root);
  for (const auto& root : _tablespace_roots) {
    add(root);
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

void DatabaseDirectory::Create() const { CreateNewDirectory(_path); }

StorageDirectory DatabaseDirectory::InTablespace(
  duckdb::CatalogTransaction transaction, duckdb::idx_t tablespace) {
  if (!_root) {
    return {shared_from_this(), nullptr, _path.string()};
  }
  auto root =
    tablespace == 0
      ? _root
      : ClusterOf(transaction).TablespaceRootOf(transaction, tablespace);
  auto path = root->DatabaseDir(_oid);
  return {shared_from_this(), std::move(root), std::move(path)};
}

}  // namespace sdb::catalog
