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
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <filesystem>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::catalog {

inline constexpr std::string_view kEngineDir = "engine_v1";
inline constexpr std::string_view kCatalogLog = "catalog.wal";
inline constexpr std::string_view kDataFile = "data.db";

duckdb::FileSystem& StorageFileSystem();
void CreateDirectory(const std::filesystem::path& path);
bool RemoveTree(const std::string& path) noexcept;
// While a backup runs, removals of dropped storages wait for ResumeRemovals.
void DeferRemovals() noexcept;
void ResumeRemovals();
std::optional<duckdb::idx_t> OidFromName(std::string_view name);

struct StorageEntry {
  duckdb::idx_t oid = 0;
  std::string path;
};

std::vector<StorageEntry> ListStorages(const std::string& root);
bool HoldsFiles(const StorageEntry& entry);

struct StorageSize {
  uint64_t files = 0;
  uint64_t nested = 0;
};

StorageSize StorageBytes(const std::string& directory);

void RemoveUnowned(std::span<const StorageEntry> entries,
                   const irs::containers::FlatHashSet<duckdb::idx_t>& owned);

class StorageDirectory final {
 public:
  explicit StorageDirectory(
    std::string path, std::shared_ptr<const StorageDirectory> parent = nullptr)
    : _path{std::move(path)}, _parent{std::move(parent)} {}
  ~StorageDirectory();

  const std::string& Path() const noexcept { return _path; }
  void MarkDropped(std::vector<std::string> beside = {}) noexcept;

 private:
  std::string _path;
  std::shared_ptr<const StorageDirectory> _parent;
  std::vector<std::string> _beside;
  std::atomic_bool _dropped{false};
};

class DatabaseDirectory final : public duckdb::StorageExtensionInfo {
 public:
  DatabaseDirectory(duckdb::idx_t oid, std::filesystem::path path,
                    std::string_view tablespace = {});

  duckdb::idx_t Oid() const noexcept { return _oid; }
  const std::filesystem::path& Path() const noexcept { return _path; }
  std::string_view Tablespace() const noexcept { return _tablespace; }
  std::string DataFile() const;
  std::unique_ptr<StorageDirectory> StorageOf(std::string_view tablespace,
                                              duckdb::idx_t oid) const;

  void SetTablespaces(std::vector<std::string> tablespaces) {
    _tablespaces = std::move(tablespaces);
  }
  void MarkDropped() noexcept;

 private:
  duckdb::idx_t _oid;
  std::filesystem::path _path;
  std::string _tablespace;
  std::shared_ptr<StorageDirectory> _storage;
  std::vector<std::string> _tablespaces;
};

}  // namespace sdb::catalog
