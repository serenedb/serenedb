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
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>
#include <vector>

#include "scheduler/background_scheduler.h"
#include "server/utils/lifecycle.h"

namespace sdb::catalog {
namespace {

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
  if (!StorageFileSystem().CreateDirectoryExtended(
        path.native(),
        {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true})) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("could not create directory \"", path.string(),
                            "\": it already exists"));
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
    if (!fs.DirectoryExists(path)) {
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

StorageDirectory::~StorageDirectory() {
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  RunRemoval([path = std::move(_path), beside = std::move(_beside),
              parent = std::move(_parent)] {
    RemoveTree(path);
    for (const auto& directory : beside) {
      RemoveTree(directory);
    }
  });
}

void StorageDirectory::MarkDropped(std::vector<std::string> beside) noexcept {
  _beside = std::move(beside);
  _dropped.store(true, std::memory_order_release);
}

std::vector<StorageEntry> ListStorages(const std::string& root) {
  std::vector<StorageEntry> found;
  StorageFileSystem().ListFiles(
    root, [&](const std::string& name, bool is_directory) {
      if (!is_directory) {
        return;
      }
      if (const auto oid = OidFromName(name)) {
        found.push_back({.oid = *oid, .path = absl::StrCat(root, "/", name)});
      }
    });
  return found;
}

bool HoldsFiles(const StorageEntry& entry) {
  bool holds = false;
  StorageFileSystem().ListFiles(
    entry.path, [&](const std::string&, bool) { holds = true; });
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
    RemoveTree(entry.path);
  }
}

DatabaseDirectory::DatabaseDirectory(duckdb::idx_t oid,
                                     std::filesystem::path path,
                                     std::string_view tablespace)
  : _oid{oid},
    _path{std::move(path)},
    _tablespace{tablespace},
    _storage{std::make_shared<StorageDirectory>(_path.string())} {}

void DatabaseDirectory::MarkDropped() noexcept {
  std::vector<std::string> beside;
  const auto add = [&](std::string_view tablespace) {
    auto directory = absl::StrCat(tablespace, "/", _oid);
    if (directory != _path.native() && !absl::c_contains(beside, directory)) {
      beside.push_back(std::move(directory));
    }
  };
  if (!_tablespace.empty()) {
    add(_tablespace);
  }
  for (const auto& tablespace : _tablespaces) {
    add(tablespace);
  }
  _storage->MarkDropped(std::move(beside));
}

std::string DatabaseDirectory::DataFile() const {
  return (_path / kDataFile).string();
}

std::unique_ptr<StorageDirectory> DatabaseDirectory::StorageOf(
  std::string_view tablespace, duckdb::idx_t oid) const {
  return std::make_unique<StorageDirectory>(
    tablespace.empty() ? absl::StrCat(_path.native(), "/", oid)
                       : absl::StrCat(tablespace, "/", _oid, "/", oid),
    _storage);
}

}  // namespace sdb::catalog
