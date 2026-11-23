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

#include <absl/functional/any_invocable.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>
#include <vector>

#include "runtime/runtime.h"
#include "server/utils/lifecycle.h"

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
  auto& runtime = Runtime::Get();
  if (lifecycle::IsStopping() || runtime.IsStopping()) {
    EndStorageWork(removal());
    return;
  }
  runtime
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

void CreateDirectory(const std::string& path) {
  if (!StorageFileSystem().CreateDirectoryExtended(
        path, {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true})) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_IO_ERROR),
      ERR_MSG("could not create directory \"", path, "\": it already exists"));
  }
}

void RemoveTreeOrThrow(const std::string& path) {
  duckdb::TaskScheduler::BlockingScope blocking;
  auto& fs = StorageFileSystem();
  fs.RemoveDirectoryExtended(
    path, {.mode = duckdb::RemoveDirectoryMode::RECURSIVE, .strict = true});
  if (fs.DirectoryExists(path)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_IO_ERROR),
      ERR_MSG("could not remove directory \"", path, "\": it is still there"));
  }
}

bool RemoveTree(const std::string& path) noexcept {
  try {
    RemoveTreeOrThrow(path);
    return true;
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "could not remove '", path,
             "': ", duckdb::ErrorData{e}.RawMessage());
    return false;
  }
}

duckdb::FileSystem& StorageFileSystem() {
  return irs::DuckDBEngine::Instance().instance().GetInternalFileSystem();
}

uint64_t StorageBytes(const std::string& directory) {
  uint64_t size = 0;
  StorageFileSystem().ListFiles(
    directory,
    [&](duckdb::OpenFileInfo& info) {
      duckdb::idx_t bytes = 0;
      if (info.extended_info &&
          info.extended_info->TryGetOption("file_size", bytes)) {
        size += bytes;
      }
    },
    nullptr, {.recursive = true, .strict = true});
  return size;
}

std::string StoreDirOf(std::string_view tablespace_directory) {
  return duckdb::FileSystem::JoinPath(tablespace_directory, kStoreDir);
}

const std::shared_ptr<std::atomic_uint64_t>& SegmentIds() {
  static const auto gIds = std::make_shared<std::atomic_uint64_t>(0);
  return gIds;
}

void RemoveStorage(absl::AnyInvocable<bool()> remove_files) {
  RunRemoval(std::move(remove_files));
}

}  // namespace sdb::catalog
