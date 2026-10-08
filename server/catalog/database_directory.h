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

#include <atomic>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace sdb::catalog {

inline constexpr std::string_view kDataFile = "data.db";

void SyncDirectory(const std::filesystem::path& directory);

class DatabaseDirectory final : public duckdb::StorageExtensionInfo {
 public:
  explicit DatabaseDirectory(std::filesystem::path path);
  ~DatabaseDirectory() final;

  const std::filesystem::path& Path() const noexcept { return _path; }
  std::string DataFile() const;
  std::filesystem::path StoragePath(duckdb::idx_t oid) const;

  void Create() const;
  std::filesystem::path CreateStorage(duckdb::idx_t oid) const;
  std::optional<std::filesystem::path> OpenStorage(duckdb::idx_t oid) const;
  static void RemoveStorage(std::shared_ptr<DatabaseDirectory> directory,
                            duckdb::idx_t oid);

  void MarkDropped() noexcept {
    _dropped.store(true, std::memory_order_release);
  }

 private:
  std::filesystem::path _path;
  std::atomic_bool _dropped{false};
};

}  // namespace sdb::catalog
