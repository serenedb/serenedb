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

#include <absl/cleanup/cleanup.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <absl/synchronization/mutex.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>
#include <vector>

#include "backup/pg_control.h"
#include "runtime/runtime.h"
#include "server/utils/lifecycle.h"
#include "server/utils/unique_id.h"

namespace sdb::catalog {
namespace {

using Removal = absl::AnyInvocable<bool()>;

struct StorageWork {
  std::atomic<uint64_t> open{0};
  std::atomic_bool untidy{false};
};

StorageWork& Work() {
  static StorageWork gWork;
  return gWork;
}

struct Ownership {
  absl::Mutex mutex;
  irs::containers::FlatHashSet<std::string> released ABSL_GUARDED_BY(mutex);
  irs::containers::FlatHashMap<std::string, FileLock> locked
    ABSL_GUARDED_BY(mutex);
};

Ownership& Owners() {
  static Ownership gOwners;
  return gOwners;
}

std::string Owner() {
  return std::string{backup::PositionSource::Instance().ServerId().Bytes()};
}

std::string OwnerName(std::string_view content) {
  const auto id = UniqueId::FromBytes(content);
  return id ? absl::StrCat("the server ", id->Hex()) : "an unreadable owner";
}

[[noreturn]] void ThrowFileError(int error, std::string_view what,
                                 std::string_view path) {
  THROW_SQL_ERROR(ERR_CODE(irs::pg::ErrcodeForFileAccess(error)),
                  ERR_MSG("could not ", what, " file \"", path,
                          "\": ", std::strerror(error)));
}

std::string ReadLocalFile(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    ThrowFileError(errno, "open", path);
  }
  absl::Cleanup close = [fd] { ::close(fd); };
  std::string content;
  std::array<char, 64> buffer;
  while (true) {
    const auto read = ::read(fd, buffer.data(), buffer.size());
    if (read < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowFileError(errno, "read", path);
    }
    if (read == 0) {
      return content;
    }
    content.append(buffer.data(), static_cast<size_t>(read));
  }
}

std::string_view LocationOf(std::string_view store) {
  absl::ConsumeSuffix(&store, kStoreDir);
  absl::ConsumeSuffix(&store, "/");
  return store;
}

bool HoldsSegments(duckdb::FileSystem& fs, const std::string& store) {
  bool held = false;
  if (fs.DirectoryExists(store)) {
    fs.ListFiles(store,
                 [&](duckdb::OpenFileInfo& info) {
                   held =
                     held || (!duckdb::FileSystem::IsDirectory(info) &&
                              !info.path.empty() && !info.path.ends_with('/'));
                 },
                 nullptr, {.strict = true});
  }
  return held;
}

void ReleaseIfEmpty(std::string_view location) noexcept {
  auto& owners = Owners();
  absl::MutexLock lock{&owners.mutex};
  const auto it = owners.released.find(location);
  if (it == owners.released.end()) {
    return;
  }
  try {
    auto& fs = StorageFileSystem();
    if (HoldsSegments(fs, StoreDirOf(location))) {
      return;
    }
    const auto owner = fs.JoinPath(*it, kOwnerFile);
    owners.locked.erase(*it);
    if (fs.FileExists(owner) &&
        irs::ReadWhole<std::string>(duckdb::QueryContext{}, fs, owner) ==
          Owner()) {
      fs.RemoveFile(owner);
    }
    owners.released.erase(it);
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "could not release tablespace location '", location,
             "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

void RunRemoval(Removal&& removal) {
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

void RemoveStorage(std::string store, absl::AnyInvocable<bool()> remove_files) {
  RunRemoval([store = std::move(store),
              remove_files = std::move(remove_files)]() mutable {
    const bool tidy = remove_files();
    ReleaseIfEmpty(LocationOf(store));
    return tidy;
  });
}

void ClaimLocation(const std::string& location) {
  auto& owners = Owners();
  absl::MutexLock lock{&owners.mutex};
  owners.released.erase(location);
  auto& fs = StorageFileSystem();
  const auto owner = fs.JoinPath(location, kOwnerFile);
  const auto self = Owner();
  if (!fs.FileExists(owner)) {
    irs::WriteWhole(fs, owner, self, true);
    return;
  }
  const auto named =
    irs::ReadWhole<std::string>(duckdb::QueryContext{}, fs, owner);
  if (named != self) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("tablespace location \"", location,
              "\" belongs to another server"),
      ERR_DETAIL("Its ", kOwnerFile, " file names ", OwnerName(named), "."),
      ERR_HINT(
        "Use another location, or remove this one once no server uses it."));
  }
  if (duckdb::Path::IsRemote(location) || owners.locked.contains(location)) {
    return;
  }
  auto locked = FileLock::Take(owner, FileLock::Mode::Exclusive);
  if (!locked.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("tablespace location \"", location,
                            "\" is in use by another running server"),
                    ERR_DETAIL(locked.status().message()));
  }
  owners.locked.emplace(location, std::move(*locked));
}

LockedOwner LockOwner(const std::string& directory) {
  auto path = duckdb::FileSystem::JoinPath(directory, kOwnerFile);
  const int fd =
    ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    ThrowFileError(errno, "create", path);
  }
  ::close(fd);
  StorageFileSystem().SyncDirectory(directory);
  auto locked = FileLock::Take(path, FileLock::Mode::Exclusive);
  if (!locked.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("directory \"", directory,
                            "\" is in use by another running server"),
                    ERR_DETAIL(locked.status().message()));
  }
  LockedOwner owner{.path = std::move(path), .lock = std::move(*locked)};
  owner.content = ReadLocalFile(owner.path);
  return owner;
}

void WriteOwner(LockedOwner& owner, std::string content) {
  const int fd = ::open(owner.path.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC);
  if (fd < 0) {
    ThrowFileError(errno, "open", owner.path);
  }
  absl::Cleanup close = [fd] { ::close(fd); };
  for (size_t written = 0; written < content.size();) {
    const auto n =
      ::write(fd, content.data() + written, content.size() - written);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowFileError(errno, "write", owner.path);
    }
    written += static_cast<size_t>(n);
  }
  if (::fsync(fd) != 0) {
    ThrowFileError(errno, "sync", owner.path);
  }
  owner.content = std::move(content);
}

void EmptyDirectory(const std::string& directory, std::string_view keep) {
  auto& fs = StorageFileSystem();
  std::vector<std::pair<std::string, bool>> entries;
  fs.ListFiles(directory,
               [&](duckdb::OpenFileInfo& info) {
                 if (info.path != keep) {
                   entries.emplace_back(fs.JoinPath(directory, info.path),
                                        duckdb::FileSystem::IsDirectory(info));
                 }
               },
               nullptr, {.strict = true});
  for (const auto& [path, directory_entry] : entries) {
    if (directory_entry) {
      RemoveTreeOrThrow(path);
    } else {
      fs.RemoveFile(path);
    }
  }
  fs.SyncDirectory(directory);
}

void ReleaseLocation(std::string location) {
  {
    auto& owners = Owners();
    absl::MutexLock lock{&owners.mutex};
    owners.released.insert(location);
  }
  BeginStorageWork();
  RunRemoval([location = std::move(location)] {
    ReleaseIfEmpty(location);
    return true;
  });
}

}  // namespace sdb::catalog
