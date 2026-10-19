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

#include <absl/base/no_destructor.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>

#include <cerrno>
#include <iresearch/utils/assert.hpp>
#include <cstring>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/log.hpp>
#include <system_error>
#include <utility>
#include <vector>

#include "scheduler/background_scheduler.h"
#include "server/utils/lifecycle.h"
#include "utils/file_utils.h"

namespace sdb::catalog {
namespace {

using Removal = absl::AnyInvocable<void()>;

struct RemovalGate {
  std::atomic_bool deferring{false};
  absl::Mutex mutex;
  std::vector<Removal> deferred ABSL_GUARDED_BY(mutex);
};

RemovalGate& Gate() {
  static absl::NoDestructor<RemovalGate> gGate;
  return *gGate;
}

void RemoveTree(const std::filesystem::path& path) {
  duckdb::TaskScheduler::BlockingScope blocking;
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    SDB_WARN(GENERAL, "could not remove '", path.string(), "': ", ec.message());
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
  std::error_code ec;
  if (!std::filesystem::create_directory(path, ec)) {
    utils::file_utils::ThrowFileError("create directory", path.string(),
                                      ec ? ec.value() : EEXIST);
  }
  utils::file_utils::SyncDirectory(path.parent_path());
}

void RemoveTree(const std::filesystem::path& path) noexcept {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    SDB_WARN(GENERAL, "could not remove '", path.string(), "': ", ec.message());
  }
}

DatabaseDirectory::DatabaseDirectory(std::filesystem::path path)
  : _path{std::move(path)} {}

DatabaseDirectory::~DatabaseDirectory() {
  if (_dropped.load(std::memory_order_acquire)) {
    RunRemoval([path = std::move(_path)] { RemoveTree(path); });
  }
}

std::string DatabaseDirectory::DataFile() const {
  return (_path / kDataFile).string();
}

std::filesystem::path DatabaseDirectory::StoragePath(duckdb::idx_t oid) const {
  return _path / absl::StrCat(oid);
}

void DatabaseDirectory::Create() const { CreateDirectory(_path); }

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
  RunRemoval([directory = std::move(directory), oid] {
    RemoveTree(directory->StoragePath(oid));
  });
}

}  // namespace sdb::catalog
