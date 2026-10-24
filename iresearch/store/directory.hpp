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

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/common/memory_file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/ref_counter.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {

enum class StorageKind : uint8_t {
  Local,
  Memory,
  Remote,
};

using IndexFileRefs = RefCounter<std::string>;

using FileRefs = std::vector<IndexFileRefs::ref_t>;

struct FileInfo {
  uint64_t size = 0;
  std::shared_ptr<const bstring> tail;
  std::string etag;
};

class FileInfos {
 public:
  void Remember(std::string_view name, FileInfo info);
  void Listed(std::string_view name, uint64_t size, std::string etag);
  std::optional<FileInfo> Find(std::string_view name) const;
  bool Contains(std::string_view name) const;
  bool Forget(std::string_view name);
  void Rename(std::string_view src, std::string_view dst);

 private:
  mutable absl::Mutex _mutex;
  absl::flat_hash_map<std::string, FileInfo> _infos ABSL_GUARDED_BY(_mutex);
};

struct TrackedFiles {
  std::vector<std::string> files;
  uint64_t bytes = 0;
};

class FileTracker {
 public:
  void Add(std::string_view name) { _files.emplace(name); }
  void AddBytes(uint64_t size) noexcept { _files_size += size; }

  TrackedFiles Flush();
  void Clear() noexcept;

 private:
  absl::flat_hash_set<std::string> _files;
  uint64_t _files_size = 0;
};

class RefTracker {
 public:
  void Keep(IndexFileRefs::ref_t ref);
  void Forget(std::string_view name);
  void Replace(std::string_view src, IndexFileRefs::ref_t dst);

  std::vector<IndexFileRefs::ref_t> GetRefs() const;
  void Clear();

 private:
  using Refs = absl::flat_hash_set<IndexFileRefs::ref_t, IndexFileRefs::Hash,
                                   IndexFileRefs::EqualTo>;

  mutable absl::Mutex _mutex;
  Refs _tracked ABSL_GUARDED_BY(_mutex);
};

class Directory {
 public:
  Directory(
    duckdb::DatabaseInstance& db, std::string_view root,
    const ResourceManagementOptions& rm = ResourceManagementOptions::gDefault);
  Directory(
    duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
    const ResourceManagementOptions& rm = ResourceManagementOptions::gDefault);

  static Directory PrivateMemory(duckdb::DatabaseInstance& db);

  IndexOutput::ptr Create(std::string_view name) const;
  IndexInput::ptr Open(std::string_view name,
                       duckdb::MappedFileAccess access) const;
  IndexInput::ptr ReadWhole(std::string_view name) const;
  bool Has(std::string_view name) const;
  bool Remove(std::string_view name) const;
  void Rename(std::string_view src, std::string_view dst) const;
  void Sync(std::span<const std::string_view> names) const;
  void SyncDirectory() const;
  void Compact(std::string_view name) const;
  void Visit(absl::FunctionRef<void(std::string_view)> visitor) const;
  void ForEach(size_t count, absl::FunctionRef<void(size_t)> visit) const;

  std::optional<FileInfo> Info(std::string_view name) const;
  void Remember(std::string_view name, FileInfo info) const;

  IndexFileRefs::ref_t Reference(std::string_view name) const;
  size_t RemoveUnreferenced() const;
  void RemoveAllUnreferenced() const;

  StorageKind Kind() const noexcept { return _state->kind; }
  const std::string& Root() const noexcept { return _state->root; }
  std::string Path(std::string_view name) const;
  IndexFileRefs& Refs() const noexcept { return _state->refs; }
  const ResourceManagementOptions& ResourceManager() const noexcept {
    return _state->rm;
  }

  Directory Tracking(FileTracker& tracker) const noexcept;
  Directory Tracking(RefTracker& tracker) const noexcept;
  Directory Untracked() const noexcept;

 private:
  struct State {
    ~State();

    duckdb::DatabaseInstance& db;
    duckdb::FileSystem& fs;
    std::string root;
    StorageKind kind;
    ResourceManagementOptions rm;
    std::unique_ptr<duckdb::MemoryFileSystem> heap;
    IndexFileRefs refs;
    FileInfos infos;
  };

  explicit Directory(std::shared_ptr<State> state) noexcept
    : _state{std::move(state)} {}

  bool InBlocks() const noexcept {
    return Kind() == StorageKind::Remote ||
           (Kind() == StorageKind::Memory && !_state->heap);
  }
  bool RemoveFile(std::string_view name) const;
  bool RemoveUnusedFile(std::string_view name) const noexcept;
  size_t RemoveFiles(std::span<const std::string> names) const noexcept;
  IndexInput::ptr OpenMapped(const std::string& path,
                             duckdb::MappedFileAccess access) const;
  IndexInput::ptr OpenBlocks(const std::string& path, std::string_view name,
                             duckdb::MappedFileAccess access) const;
  duckdb::BlockCache& Cache() const;
  duckdb::MemoryFileSystem& MemoryFiles() const;

  std::shared_ptr<State> _state;
  FileTracker* _tracker = nullptr;
  RefTracker* _ref_tracker = nullptr;
};

std::string MemoryIndexRoot();

}  // namespace irs
