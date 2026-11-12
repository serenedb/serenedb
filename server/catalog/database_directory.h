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
#include <duckdb/common/typedefs.hpp>
#include <filesystem>
#include <memory>
#include <optional>

namespace sdb::catalog {

void CreateDirectory(const std::filesystem::path& path);
bool RemoveDirectory(const std::filesystem::path& path);
void DeferRemovals() noexcept;
void ResumeRemovals();
void BeginStorageWork() noexcept;
void EndStorageWork(bool tidy) noexcept;
bool StorageTidy() noexcept;

class DatabaseDirectory final {
 public:
  explicit DatabaseDirectory(std::filesystem::path path);

  const std::filesystem::path& Path() const noexcept { return _path; }
  std::filesystem::path StoragePath(duckdb::idx_t oid) const;

  std::filesystem::path CreateStorage(duckdb::idx_t oid) const;
  std::optional<std::filesystem::path> OpenStorage(duckdb::idx_t oid) const;
  static void RemoveStorage(std::shared_ptr<DatabaseDirectory> directory,
                            duckdb::idx_t oid,
                            absl::AnyInvocable<bool()> remove_files);

  void MarkDropped() noexcept {
    _dropped.store(true, std::memory_order_release);
  }
  bool Dropped() const noexcept {
    return _dropped.load(std::memory_order_acquire);
  }

 private:
  std::filesystem::path _path;
  std::atomic_bool _dropped{false};
};

}  // namespace sdb::catalog
