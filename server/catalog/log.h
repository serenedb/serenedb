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
#include <absl/container/btree_map.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::catalog {

using Lsn = uint64_t;

class Log {
 public:
  struct Range {
    Lsn begin = 0;
    Lsn end = 0;
  };

  using CatalogPart = absl::FunctionRef<std::span<uint8_t>(std::span<uint8_t>)>;

  static constexpr uint64_t kChunkBytes = uint64_t{16} << 20;
  static constexpr std::string_view kHistoryFile = "history";

  static std::optional<Lsn> ChunkBegin(std::string_view name);
  static std::string ChunkName(Lsn begin);
  static std::optional<Lsn> HistoryCut(const std::filesystem::path& dir);
  static std::vector<duckdb::unique_ptr<duckdb::FileHandle>> LockChunks(
    duckdb::FileSystem& fs, const std::filesystem::path& dir);
  static void Read(
    duckdb::FileSystem& fs, const std::filesystem::path& dir,
    absl::FunctionRef<void(Lsn, std::span<uint8_t>)> history,
    absl::FunctionRef<void(const Range&, std::span<uint8_t>)> apply);

  Log(duckdb::FileSystem& fs, std::filesystem::path dir);

  void Replay(absl::FunctionRef<void(Lsn, std::span<uint8_t>)> history,
              absl::FunctionRef<void(const Range&, std::span<uint8_t>)> apply);

  Range Reserve(size_t payload_size);
  void Write(const Range& range, std::span<const uint8_t> payload);
  void Sync(Lsn end);

  Lsn Append(std::span<const uint8_t> payload);

  void Truncate(Lsn below, CatalogPart part);

  Lsn Link(const std::filesystem::path& dir);

  Lsn Reserved() const {
    absl::MutexLock lock{&_mutex};
    return _reserved;
  }

  Lsn Durable() const {
    absl::MutexLock lock{&_mutex};
    return _durable;
  }

  Lsn End() const { return Reserved(); }

  Lsn Appended() const {
    absl::MutexLock lock{&_mutex};
    return _reserved - _replayed;
  }

  Lsn Oldest() const noexcept {
    return _oldest.load(std::memory_order_relaxed);
  }
  Lsn NewestChunk() const noexcept {
    return _newest.load(std::memory_order_relaxed);
  }
  uint64_t Size() const { return Reserved() - Oldest(); }

  Lsn Switch();

  bool HasSpare() const noexcept {
    return _has_spare.load(std::memory_order_relaxed);
  }
  void PrepareSpare();

 private:
  struct Chunk {
    Lsn begin = 0;
    duckdb::unique_ptr<duckdb::FileHandle> file;
  };

  bool SyncIdle() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex) {
    return _syncs == 0;
  }
  std::filesystem::path ChunkPath(Lsn begin) const;
  std::shared_ptr<Chunk> OpenChunk(Lsn begin, bool create) const;
  void StartChunk() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void SyncDirectory() const;

  duckdb::FileSystem& _fs;
  std::filesystem::path _dir;
  mutable absl::Mutex _mutex;
  absl::btree_map<Lsn, std::shared_ptr<Chunk>> _chunks ABSL_GUARDED_BY(_mutex);
  Lsn _replayed ABSL_GUARDED_BY(_mutex) = 0;
  Lsn _reserved ABSL_GUARDED_BY(_mutex) = 0;
  Lsn _written ABSL_GUARDED_BY(_mutex) = 0;
  Lsn _durable ABSL_GUARDED_BY(_mutex) = 0;
  absl::btree_map<Lsn, Lsn> _out_of_order ABSL_GUARDED_BY(_mutex);
  Lsn _syncing_to ABSL_GUARDED_BY(_mutex) = 0;
  uint32_t _syncs ABSL_GUARDED_BY(_mutex) = 0;
  bool _parallel_syncs ABSL_GUARDED_BY(_mutex) = false;
  uint64_t _directory_changes ABSL_GUARDED_BY(_mutex) = 0;
  uint64_t _directory_synced ABSL_GUARDED_BY(_mutex) = 0;
  std::shared_ptr<Chunk> _spare ABSL_GUARDED_BY(_mutex);
  std::atomic<bool> _has_spare{false};
  std::atomic<Lsn> _oldest{0};
  std::atomic<Lsn> _newest{0};
  absl::Mutex _files_mutex;
};

}  // namespace sdb::catalog
