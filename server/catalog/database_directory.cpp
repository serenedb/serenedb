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
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "catalog/cluster.h"
#include "scheduler/background_scheduler.h"
#include "server/utils/lifecycle.h"

namespace sdb::catalog {
namespace {

bool CreateDirectory(const std::filesystem::path& path) {
  if (!StorageFileSystem().CreateDirectoryExtended(path.string(), {})) {
    return false;
  }
  SyncDirectory(path.parent_path());
  return true;
}

bool CreateMarker(const std::string& marker) {
  auto& fs = StorageFileSystem();
  if (fs.FileExists(marker)) {
    return false;
  }
  fs.OpenFile(marker, duckdb::FileFlags::FILE_FLAGS_WRITE |
                        duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW)
    ->Close();
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
    StorageFileSystem().RemoveDirectory(path);
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

void SyncDirectory(const std::filesystem::path& directory) {
  duckdb::FileSystem::GetLocal(irs::DuckDBEngine::Instance().instance())
    .SyncDirectory(directory.string());
}

duckdb::FileSystem& StorageFileSystem() {
  return duckdb::FileSystem::GetFileSystem(
    irs::DuckDBEngine::Instance().instance());
}

bool IsRemote(std::string_view path) {
  return duckdb::Path::FromString(std::string{path}).IsRemote();
}

std::string EngineDirOf(std::string_view location) {
  return absl::StrCat(location, "/", kEngineDir);
}

std::vector<StorageEntry> ListStorages(const std::string& root) {
  std::vector<StorageEntry> found;
  absl::flat_hash_set<std::string> seen;
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
      duckdb::idx_t oid = 0;
      if (digits.empty() || !absl::c_all_of(digits, absl::ascii_isdigit) ||
          !absl::SimpleAtoi(digits, &oid) || !seen.emplace(entry).second) {
        return;
      }
      found.push_back({.oid = oid,
                       .path = absl::StrCat(root, "/", entry),
                       .is_directory = directory});
    });
  return found;
}

void RemoveUnowned(std::span<const StorageEntry> entries,
                   const absl::flat_hash_set<duckdb::idx_t>& owned) {
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
  : _path{std::move(path)}, _remote{IsRemote(_path)} {}

TablespaceRoot::~TablespaceRoot() {
  if (!_removed.load(std::memory_order_acquire)) {
    return;
  }
  RunRemoval([path = std::move(_path)] { RemoveTree(path); });
}

void TablespaceRoot::Claim() {
  const auto in_use = [&] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("directory \"", _path, "\" already in use as a tablespace"));
  };
  if (!_remote) {
    const std::filesystem::path path{_path};
    StorageFileSystem().CreateDirectoriesRecursive(path.parent_path().string());
    if (!CreateDirectory(path)) {
      in_use();
    }
  } else if (!CreateMarker(absl::StrCat(_path, "/", kTablespaceMarker))) {
    in_use();
  }
  _claimed.store(true, std::memory_order_release);
}

std::string TablespaceRoot::DatabaseDir(duckdb::idx_t oid) const {
  return absl::StrCat(_path, "/", oid);
}

std::shared_ptr<void> TablespaceRoot::Pin() {
  {
    absl::MutexLock lock{&_mutex};
    if (_removed.load(std::memory_order_acquire) || !_drop.expired()) {
      return nullptr;
    }
    ++_pins;
  }
  auto self = shared_from_this();
  auto* pinned = self.get();
  return std::shared_ptr<void>{
    pinned, [self = std::move(self)](void*) { self->Unpin(); }};
}

void TablespaceRoot::Unpin() noexcept {
  absl::MutexLock lock{&_mutex};
  --_pins;
}

bool TablespaceRoot::BeginDrop(const std::shared_ptr<void>& drop) {
  absl::MutexLock lock{&_mutex};
  if (_pins != 0 || !_drop.expired() ||
      _removed.load(std::memory_order_acquire)) {
    return false;
  }
  _drop = drop;
  return true;
}

StorageArea::StorageArea(std::shared_ptr<DatabaseDirectory> database,
                         std::shared_ptr<TablespaceRoot> root, std::string path)
  : _database{std::move(database)},
    _root{std::move(root)},
    _path{std::move(path)},
    _remote{IsRemote(_path)} {}

std::string StorageArea::StoragePath(duckdb::idx_t oid) const {
  return absl::StrCat(_path, "/", oid);
}

std::string StorageArea::MarkerPath(duckdb::idx_t oid) const {
  return absl::StrCat(_path, "/", oid, kStorageMarker);
}

std::string StorageArea::CreateStorage(duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  if (!_remote) {
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

std::optional<std::string> StorageArea::OpenStorage(duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  auto& fs = StorageFileSystem();
  if (_remote ? fs.FileExists(MarkerPath(oid)) : fs.DirectoryExists(path)) {
    return path;
  }
  return std::nullopt;
}

void StorageArea::RemoveStorage(std::shared_ptr<StorageArea> area,
                                duckdb::idx_t oid) {
  RunRemoval([area = std::move(area), oid] {
    RemoveTree(area->StoragePath(oid));
    if (area->_remote) {
      RemoveObject(area->MarkerPath(oid));
    }
  });
}

DatabaseDirectory::DatabaseDirectory(duckdb::idx_t oid,
                                     std::shared_ptr<TablespaceRoot> root,
                                     std::filesystem::path path)
  : _oid{oid}, _root{std::move(root)}, _path{std::move(path)} {}

DatabaseDirectory::DatabaseDirectory(std::filesystem::path path)
  : _oid{0}, _path{std::move(path)} {}

DatabaseDirectory::~DatabaseDirectory() {
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  std::vector<std::string> areas;
  const auto add = [&](const std::shared_ptr<TablespaceRoot>& root) {
    if (!root) {
      return;
    }
    auto area = root->DatabaseDir(_oid);
    if (area != _path.string() && !absl::c_contains(areas, area)) {
      areas.push_back(std::move(area));
    }
  };
  add(_root);
  {
    absl::MutexLock lock{&_mutex};
    for (const auto& root : _roots) {
      add(root.lock());
    }
  }
  RunRemoval([path = _path.string(), areas = std::move(areas)] {
    RemoveTree(path);
    for (const auto& area : areas) {
      RemoveTree(area);
    }
  });
}

std::string DatabaseDirectory::DataFile() const {
  return (_path / kDataFile).string();
}

void DatabaseDirectory::Create() const { CreateNewDirectory(_path); }

std::shared_ptr<TablespaceRoot> DatabaseDirectory::RootOf(
  duckdb::idx_t tablespace) const {
  if (tablespace == 0) {
    return _root;
  }
  return ClusterOf().TablespaceRootOf(tablespace);
}

std::shared_ptr<StorageArea> DatabaseDirectory::Area(duckdb::idx_t tablespace) {
  if (!_root) {
    return std::make_shared<StorageArea>(shared_from_this(), nullptr,
                                         _path.string());
  }
  auto root = RootOf(tablespace);
  if (root != _root) {
    absl::MutexLock lock{&_mutex};
    std::erase_if(_roots, [](const auto& known) { return known.expired(); });
    if (!absl::c_any_of(
          _roots, [&](const auto& known) { return known.lock() == root; })) {
      _roots.emplace_back(root);
    }
  }
  auto path = root->DatabaseDir(_oid);
  return std::make_shared<StorageArea>(shared_from_this(), std::move(root),
                                       std::move(path));
}

}  // namespace sdb::catalog
