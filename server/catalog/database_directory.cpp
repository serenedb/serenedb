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
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>

#include <cerrno>
#include <cstring>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <system_error>
#include <utility>
#include <vector>

#include "scheduler/background_scheduler.h"
#include "server/utils/lifecycle.h"
#include "utils/file_utils.h"

namespace sdb::catalog {
namespace {

using Removal = absl::AnyInvocable<bool()>;

struct RemovalGate {
  std::atomic_bool deferring{false};
  absl::Mutex mutex;
  std::vector<Removal> deferred ABSL_GUARDED_BY(mutex);
};

RemovalGate& Gate() {
  static RemovalGate gGate;
  return gGate;
}

struct StorageWork {
  std::atomic<uint64_t> open{0};
  std::atomic_bool untidy{false};
};

StorageWork& Work() {
  static StorageWork gWork;
  return gWork;
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
    EndStorageWork(removal());
    return;
  }
  BackgroundScheduler::instance()
    .Run(
      [removal = std::move(removal)]() mutable { EndStorageWork(removal()); })
    .Detach();
}

}  // namespace

void BeginStorageWork() noexcept {
  Work().open.fetch_add(1, std::memory_order_relaxed);
}

void EndStorageWork(bool tidy) noexcept {
  auto& work = Work();
  if (!tidy) {
    work.untidy.store(true, std::memory_order_relaxed);
  }
  [[maybe_unused]] const auto open =
    work.open.fetch_sub(1, std::memory_order_release);
  SDB_ASSERT(open != 0);
}

bool StorageTidy() noexcept {
  auto& work = Work();
  return work.open.load(std::memory_order_acquire) == 0 &&
         !work.untidy.load(std::memory_order_relaxed);
}

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
  std::error_code ec;
  if (!std::filesystem::create_directory(path, ec)) {
    utils::file_utils::ThrowFileError("create directory", path.string(),
                                      ec ? ec.value() : EEXIST);
  }
  utils::file_utils::SyncDirectory(path.parent_path());
}

bool RemoveDirectory(const std::filesystem::path& path) {
  std::error_code ec;
  SDB_IF_FAILURE("pause_removal_midway") {
    std::vector<std::filesystem::path> files;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator{path, ec}) {
      if (entry.is_regular_file()) {
        files.push_back(entry.path());
      }
    }
    if (!files.empty()) {
      std::filesystem::remove(*absl::c_min_element(files));
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_removal_midway");
  }
  SDB_IF_FAILURE("storage_removal_fails") { return false; }
  std::filesystem::remove_all(path, ec);
  if (ec) {
    SDB_WARN(GENERAL, "could not remove '", path.string(), "': ", ec.message());
    return false;
  }
  return true;
}

bool RemoveTree(const std::filesystem::path& path) noexcept {
  duckdb::TaskScheduler::BlockingScope blocking;
  return RemoveDirectory(path);
}

DatabaseDirectory::DatabaseDirectory(std::filesystem::path path)
  : _path{std::move(path)} {}

std::filesystem::path DatabaseDirectory::StoragePath(duckdb::idx_t oid) const {
  return _path / absl::StrCat(oid);
}

std::filesystem::path DatabaseDirectory::CreateStorage(
  duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  CreateDirectory(path);
  return path;
}

std::optional<std::filesystem::path> DatabaseDirectory::OpenStorage(
  duckdb::idx_t oid) const {
  auto path = StoragePath(oid);
  std::error_code ec;
  if (std::filesystem::is_directory(path, ec)) {
    return path;
  }
  if (ec && ec != std::errc::no_such_file_or_directory) {
    utils::file_utils::ThrowFileError("open directory", path.string(), ec);
  }
  return std::nullopt;
}

void DatabaseDirectory::RemoveStorage(
  std::shared_ptr<DatabaseDirectory> directory, duckdb::idx_t oid) {
  BeginStorageWork();
  RunRemoval([directory = std::move(directory), oid] {
    return RemoveTree(directory->StoragePath(oid));
  });
}

}  // namespace sdb::catalog
