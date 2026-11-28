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

#pragma once

#include <absl/functional/any_invocable.h>

#include <atomic>
#include <duckdb/common/file_system.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "server/utils/lockfile.h"

namespace sdb::catalog {

inline constexpr std::string_view kStoreDir = "segments";
inline constexpr std::string_view kOwnerFile = "OWNER";

duckdb::FileSystem& StorageFileSystem();
void CreateDirectory(const std::string& path);
void RemoveTreeOrThrow(const std::string& path);
bool RemoveTree(const std::string& path) noexcept;
void BeginStorageWork() noexcept;
void EndStorageWork(bool tidy) noexcept;
bool StorageTidy() noexcept;

uint64_t StorageBytes(const std::string& directory);

std::string StoreDirOf(std::string_view tablespace_directory);
const std::shared_ptr<std::atomic_uint64_t>& SegmentIds();
void RemoveStorage(std::string store, absl::AnyInvocable<bool()> remove_files);

void ClaimLocation(const std::string& location);
void ReleaseLocation(std::string location);

struct LockedOwner {
  std::string path;
  FileLock lock;
  std::string content;
};

LockedOwner LockOwner(const std::string& directory);
void WriteOwner(LockedOwner& owner, std::string content);
void EmptyDirectory(const std::string& directory, std::string_view keep);

class DatabaseDirectory final {
 public:
  explicit DatabaseDirectory(std::string path) : _path{std::move(path)} {}

  const std::string& Path() const noexcept { return _path; }

  void MarkDropped() noexcept {
    _dropped.store(true, std::memory_order_release);
  }
  bool Dropped() const noexcept {
    return _dropped.load(std::memory_order_acquire);
  }

 private:
  std::string _path;
  std::atomic_bool _dropped{false};
};

}  // namespace sdb::catalog
