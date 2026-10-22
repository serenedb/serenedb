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

#include "scheduler/background_scheduler.h"
#include "server/utils/lifecycle.h"

namespace sdb::catalog {
namespace {

duckdb::FileSystem& Files() {
  return duckdb::FileSystem::GetFileSystem(
    irs::DuckDBEngine::Instance().instance());
}

bool CreateDirectory(const std::filesystem::path& path) {
  if (!Files().CreateDirectoryExtended(path.string(), {})) {
    return false;
  }
  SyncDirectory(path.parent_path());
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
    Files().RemoveDirectory(path);
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "could not remove '", path,
             "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

void RemoveObject(const std::string& path) {
  try {
    Files().TryRemoveFile(path);
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

std::vector<StorageEntry> ListStorages(const std::string& root) {
  std::vector<StorageEntry> found;
  absl::flat_hash_set<std::string> seen;
  Files().ListFiles(root, [&](const std::string& name, bool is_directory) {
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

void RemoveUnowned(const std::string& root,
                   const absl::flat_hash_set<duckdb::idx_t>& owned) {
  for (const auto& entry : ListStorages(root)) {
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

DatabaseDirectory::DatabaseDirectory(std::filesystem::path path,
                                     std::string storage_root)
  : _path{std::move(path)},
    _storage_root{std::move(storage_root)},
    _remote{duckdb::Path::FromString(_storage_root).IsRemote()} {}

DatabaseDirectory::~DatabaseDirectory() {
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  RunRemoval([path = _path.string(), storage_root = std::move(_storage_root)] {
    RemoveTree(path);
    if (storage_root != path) {
      RemoveTree(storage_root);
    }
  });
}

std::string DatabaseDirectory::DataFile() const {
  return (_path / kDataFile).string();
}

std::string DatabaseDirectory::StoragePath(duckdb::idx_t oid) const {
  return absl::StrCat(_storage_root, "/", oid);
}

std::string DatabaseDirectory::MarkerPath(duckdb::idx_t oid) const {
  return absl::StrCat(_storage_root, "/", oid, kStorageMarker);
}

void DatabaseDirectory::Create() const { CreateNewDirectory(_path); }

std::string DatabaseDirectory::CreateStorage(duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  if (!_remote) {
    CreateDirectory(_storage_root);
    CreateNewDirectory(path);
    return path;
  }
  auto& fs = Files();
  const auto marker = MarkerPath(oid);
  if (fs.FileExists(marker)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_IO_ERROR),
      ERR_MSG("could not create storage \"", path, "\": it already exists"));
  }
  fs.OpenFile(marker, duckdb::FileFlags::FILE_FLAGS_WRITE |
                        duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW)
    ->Close();
  return path;
}

std::optional<std::string> DatabaseDirectory::OpenStorage(
  duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  auto& fs = Files();
  if (_remote ? fs.FileExists(MarkerPath(oid)) : fs.DirectoryExists(path)) {
    return path;
  }
  return std::nullopt;
}

void DatabaseDirectory::RemoveStorage(
  std::shared_ptr<DatabaseDirectory> directory, duckdb::idx_t oid) {
  RunRemoval([directory = std::move(directory), oid] {
    RemoveTree(directory->StoragePath(oid));
    if (directory->_remote) {
      RemoveObject(directory->MarkerPath(oid));
    }
  });
}

}  // namespace sdb::catalog
