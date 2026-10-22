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

#include <absl/container/flat_hash_set.h>

#include <atomic>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::catalog {

inline constexpr std::string_view kDataFile = "data.db";
inline constexpr std::string_view kCatalogLog = "catalog.wal";
inline constexpr std::string_view kStorageMarker = ".storage";

void SyncDirectory(const std::filesystem::path& directory);

struct StorageEntry {
  duckdb::idx_t oid = 0;
  std::string path;
  bool is_directory = false;
};

std::vector<StorageEntry> ListStorages(const std::string& root);

void RemoveUnowned(const std::string& root,
                   const absl::flat_hash_set<duckdb::idx_t>& owned);

class DatabaseDirectory final : public duckdb::StorageExtensionInfo {
 public:
  DatabaseDirectory(std::filesystem::path path, std::string storage_root);
  ~DatabaseDirectory() final;

  const std::filesystem::path& Path() const noexcept { return _path; }
  std::string DataFile() const;
  std::string StoragePath(duckdb::idx_t oid) const;

  void Create() const;
  std::string CreateStorage(duckdb::idx_t oid) const;
  std::optional<std::string> OpenStorage(duckdb::idx_t oid) const;
  static void RemoveStorage(std::shared_ptr<DatabaseDirectory> directory,
                            duckdb::idx_t oid);

  void MarkDropped() noexcept {
    _dropped.store(true, std::memory_order_release);
  }

 private:
  std::string MarkerPath(duckdb::idx_t oid) const;

  std::filesystem::path _path;
  std::string _storage_root;
  bool _remote;
  std::atomic_bool _dropped{false};
};

}  // namespace sdb::catalog
