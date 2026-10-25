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

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>

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

duckdb::FileSystem& StorageFileSystem();
std::string EngineDirOf(std::string_view location);

struct StorageEntry {
  duckdb::idx_t oid = 0;
  std::string path;
  bool is_directory = false;
};

std::vector<StorageEntry> ListStorages(const std::string& root);

struct StorageSize {
  uint64_t files = 0;
  uint64_t nested = 0;
};

StorageSize StorageBytes(const std::string& directory);

void RemoveUnowned(std::span<const StorageEntry> entries,
                   const irs::containers::FlatHashSet<duckdb::idx_t>& owned);

class TablespaceRoot final
  : public std::enable_shared_from_this<TablespaceRoot> {
 public:
  explicit TablespaceRoot(std::string path);
  ~TablespaceRoot();

  const std::string& Path() const noexcept { return *_path; }
  std::weak_ptr<const std::string> HeldPath() const noexcept { return _path; }
  bool Remote() const noexcept { return _remote; }
  std::string DatabaseDir(duckdb::idx_t oid) const;

  void Claim();
  [[nodiscard]] std::shared_ptr<void> Pin();
  [[nodiscard]] std::shared_ptr<void> BeginDrop();
  bool Dropped() const;
  void MarkDropped();
  void Abandon();

 private:
  enum class State : uint8_t {
    Live,
    Dropping,
    Dropped,
  };

  mutable absl::Mutex _mutex;
  State _state ABSL_GUARDED_BY(_mutex) = State::Live;
  uint32_t _pins ABSL_GUARDED_BY(_mutex) = 0;
  bool _claimed ABSL_GUARDED_BY(_mutex) = false;
  std::shared_ptr<const std::string> _path;
  bool _remote;
};

class DatabaseDirectory;

class StorageDirectory final {
 public:
  StorageDirectory(std::shared_ptr<DatabaseDirectory> database,
                   std::shared_ptr<TablespaceRoot> root, std::string path);

  std::string StoragePath(duckdb::idx_t oid) const;

  std::string CreateStorage(duckdb::idx_t oid) const;
  std::optional<std::string> OpenStorage(duckdb::idx_t oid) const;
  void RemoveStorage(duckdb::idx_t oid) const;

 private:
  std::string MarkerPath(duckdb::idx_t oid) const;
  bool Remote() const noexcept { return _root && _root->Remote(); }

  std::shared_ptr<DatabaseDirectory> _database;
  std::shared_ptr<TablespaceRoot> _root;
  std::string _path;
};

class DatabaseDirectory final
  : public duckdb::StorageExtensionInfo,
    public std::enable_shared_from_this<DatabaseDirectory> {
 public:
  DatabaseDirectory(duckdb::idx_t oid, std::shared_ptr<TablespaceRoot> root,
                    std::filesystem::path path);
  ~DatabaseDirectory() final;

  duckdb::idx_t Oid() const noexcept { return _oid; }
  const std::filesystem::path& Path() const noexcept { return _path; }
  std::string DataFile() const;

  void Create() const;
  StorageDirectory InTablespace(duckdb::idx_t tablespace);

  void MarkDropped() noexcept {
    _dropped.store(true, std::memory_order_release);
  }

 private:
  std::shared_ptr<TablespaceRoot> RootOf(duckdb::idx_t tablespace) const;

  duckdb::idx_t _oid;
  std::shared_ptr<TablespaceRoot> _root;
  std::filesystem::path _path;
  absl::Mutex _mutex;
  std::vector<std::weak_ptr<TablespaceRoot>> _roots ABSL_GUARDED_BY(_mutex);
  std::atomic_bool _dropped{false};
};

}  // namespace sdb::catalog
