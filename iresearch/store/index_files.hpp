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

#include <atomic>
#include <duckdb/common/memory_file_system.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/ref_counter.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {

enum class IOAdvice : uint32_t {
  NORMAL = 0,
  SEQUENTIAL = 1,
  RANDOM = 2,
  READONCE = 4,
  ReadonceSequential = 5,
  ReadonceRandom = 6,
};

ENABLE_BITMASK_ENUM(IOAdvice);

enum class StorageKind : uint8_t {
  Local,
  Memory,
  Remote,
  Linked,
};

struct LinkedFile {
  std::string_view name;
  bytes_view bytes;
};

enum class InputKind : uint8_t {
  Mapped,
  Block,
};

class IndexFileRefs final {
 public:
  using counter_t = RefCounter<std::string>;
  using ref_t = counter_t::ref_t;

  ref_t add(std::string_view key) { return _refs.add(key); }
  bool remove(std::string_view key) { return _refs.remove(key); }

  counter_t& refs() noexcept { return _refs; }

 private:
  counter_t _refs;
};

using FileRefs = std::vector<IndexFileRefs::ref_t>;

struct FileInfo {
  uint64_t size = 0;
  std::shared_ptr<const bstring> tail;
};

class FileInfos {
 public:
  void Remember(std::string_view name, FileInfo info);
  void Listed(std::string_view name, uint64_t size);
  std::optional<FileInfo> Find(std::string_view name) const;
  bool Contains(std::string_view name) const;
  bool Forget(std::string_view name);
  void Rename(std::string_view src, std::string_view dst);

 private:
  mutable absl::Mutex _mutex;
  absl::flat_hash_map<std::string, FileInfo> _infos ABSL_GUARDED_BY(_mutex);
};

class FileTracker {
 public:
  void Add(std::string_view name) { _files.emplace(name); }
  void Erase(std::string_view name) noexcept { _files.erase(name); }
  void Rename(std::string_view src, std::string_view dst);
  void AddBytes(uint64_t size) noexcept { _files_size += size; }

  std::vector<std::string> Flush(uint64_t& files_size);
  void Clear() noexcept;

 private:
  absl::flat_hash_set<std::string> _files;
  uint64_t _files_size = 0;
};

class RefTracker {
 public:
  explicit RefTracker(IndexFileRefs& refs, bool track_open = false) noexcept
    : _refs{&refs}, _track_open{track_open} {}
  RefTracker(RefTracker&& other) noexcept;
  RefTracker& operator=(RefTracker&&) = delete;

  bool TrackOpen() const noexcept { return _track_open; }

  IndexFileRefs::ref_t Add(std::string_view name) { return _refs->add(name); }
  void Forget(std::string_view name) { _refs->remove(name); }
  void Keep(IndexFileRefs::ref_t ref);
  void Remove(std::string_view name);
  void Rename(std::string_view src, std::string_view dst);

  std::vector<IndexFileRefs::ref_t> GetRefs() const;
  void Clear() const;

 private:
  using Refs =
    absl::flat_hash_set<IndexFileRefs::ref_t, IndexFileRefs::counter_t::Hash,
                        IndexFileRefs::counter_t::EqualTo>;

  IndexFileRefs* _refs;
  mutable absl::Mutex _mutex;
  mutable Refs _tracked ABSL_GUARDED_BY(_mutex);
  bool _track_open;
};

class IndexFiles {
 public:
  using Visitor = std::function<bool(std::string_view)>;

  IndexFiles(
    duckdb::DatabaseInstance& db, std::string_view root,
    const ResourceManagementOptions& rm = ResourceManagementOptions::gDefault);
  IndexFiles(
    duckdb::DatabaseInstance& db, duckdb::FileSystem& fs, std::string_view root,
    const ResourceManagementOptions& rm = ResourceManagementOptions::gDefault);
  IndexFiles(
    duckdb::DatabaseInstance& db, std::vector<LinkedFile> files,
    const ResourceManagementOptions& rm = ResourceManagementOptions::gDefault);

  static IndexFiles PrivateMemory(
    duckdb::DatabaseInstance& db,
    const ResourceManagementOptions& rm = ResourceManagementOptions::gDefault);

  IndexOutput::ptr Create(std::string_view name) const;
  IndexInput::ptr Open(std::string_view name, IOAdvice advice) const;
  bool Exists(bool& result, std::string_view name) const noexcept;
  bool Has(std::string_view name) const;
  bool Remove(std::string_view name) const noexcept;
  bool Rename(std::string_view src, std::string_view dst) const noexcept;
  void Sync(std::span<const std::string_view> names) const;
  void SyncDirectory() const;
  bool Visit(const Visitor& visitor) const;
  void ForEach(size_t count, absl::FunctionRef<void(size_t)> visit) const;

  std::optional<FileInfo> Info(std::string_view name) const;
  void Remember(std::string_view name, FileInfo info) const;

  IndexFileRefs::ref_t Reference(std::string_view name) const;
  size_t RemoveUnreferenced(
    const std::function<bool(std::string_view)>& acceptor =
      [](std::string_view) { return true; }) const;
  void RemoveAllUnreferenced() const;

  StorageKind Kind() const noexcept { return _state->kind; }
  const std::string& Root() const noexcept { return _state->root; }
  std::string Path(std::string_view name) const;
  IndexFileRefs& Refs() const noexcept { return _state->refs; }
  const ResourceManagementOptions& ResourceManager() const noexcept {
    return _state->rm;
  }

  IndexFiles Tracking(FileTracker& tracker) const noexcept;
  IndexFiles Tracking(RefTracker& tracker) const noexcept;
  IndexFiles Untracked() const noexcept;
  RefTracker* GetRefTracker() const noexcept { return _ref_tracker; }

 private:
  struct State {
    ~State();

    duckdb::DatabaseInstance& db;
    duckdb::FileSystem& fs;
    std::string root;
    StorageKind kind;
    InputKind input;
    ResourceManagementOptions rm;
    std::vector<LinkedFile> linked;
    std::unique_ptr<duckdb::MemoryFileSystem> heap;
    IndexFileRefs refs;
    std::shared_ptr<FileInfos> infos;
  };

  explicit IndexFiles(std::shared_ptr<State> state) noexcept
    : _state{std::move(state)} {}

  const LinkedFile* FindLinked(std::string_view name) const noexcept;
  bool RemoveFile(std::string_view name) const noexcept;
  size_t RemoveFiles(std::span<const std::string> names) const noexcept;
  IndexInput::ptr OpenMapped(const std::string& path, IOAdvice advice) const;
  IndexInput::ptr OpenBlocks(const std::string& path,
                             std::string_view name) const;
  IndexInput::ptr ReadWhole(const std::string& path,
                            std::string_view name) const;
  duckdb::BlockCache& Cache() const;
  duckdb::MemoryFileSystem& MemoryFiles() const;

  std::shared_ptr<State> _state;
  FileTracker* _tracker = nullptr;
  RefTracker* _ref_tracker = nullptr;
};

std::string MemoryIndexRoot();

}  // namespace irs
