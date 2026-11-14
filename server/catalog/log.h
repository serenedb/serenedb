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
#include <span>
#include <string>
#include <vector>

namespace sdb::catalog {

using Lsn = uint64_t;

class Log {
  struct Chunk;

 public:
  struct Range {
    Lsn begin = 0;
    Lsn end = 0;
    uint64_t seq = 0;
    std::shared_ptr<Chunk> chunk;
  };

  using Apply = absl::FunctionRef<void(std::span<uint8_t>)>;
  using CatalogPart = absl::FunctionRef<std::span<uint8_t>(std::span<uint8_t>)>;
  using Compact = absl::FunctionRef<std::vector<std::vector<uint8_t>>(
    std::span<const std::span<uint8_t>>)>;

  static constexpr uint64_t kChunkBytes = uint64_t{16} << 20;

  static std::string ChunkName(uint64_t number);
  static bool Exists(const std::filesystem::path& dir);
  static void Read(duckdb::FileSystem& fs, const std::filesystem::path& dir,
                   Apply apply);
  static void Move(const std::filesystem::path& from,
                   const std::filesystem::path& to);
  static void Cut(const std::filesystem::path& dir, Lsn end);
  static bool Remove(const std::filesystem::path& dir);

  Log(duckdb::FileSystem& fs, std::filesystem::path dir);

  void Replay(Apply apply, CatalogPart part);

  Range Reserve(size_t payload_size);
  void Write(const Range& range, std::span<const uint8_t> payload);
  void Sync(Lsn end);

  Lsn Append(std::span<const uint8_t> payload);

  void WriteSnapshot(Lsn covered, std::span<const std::vector<uint8_t>> records,
                     CatalogPart part, Compact compact);
  void DropCovered();

  Lsn Link(const std::filesystem::path& dir);

  Lsn Covered() const noexcept {
    return _covered.load(std::memory_order_acquire);
  }
  uint64_t SnapshotBytes() const noexcept {
    return _snapshot_bytes.load(std::memory_order_relaxed);
  }
  uint64_t HistoryBytes() const noexcept {
    return _history_bytes.load(std::memory_order_relaxed);
  }

  Lsn End() const {
    absl::MutexLock lock{&_mutex};
    return _reserved;
  }

  Lsn Durable() const noexcept {
    return _durable.load(std::memory_order_acquire);
  }

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
  uint64_t Size() const { return End() - Oldest(); }

  bool ShouldPrepareNext(Lsn end) const noexcept {
    return !_has_next.load(std::memory_order_relaxed) &&
           (!_newest_prepared.load(std::memory_order_relaxed) ||
            end - NewestChunk() > kChunkBytes / 2);
  }
  void PrepareNext();

 private:
  struct Chunk {
    uint64_t number = 0;
    Lsn begin = 0;
    duckdb::unique_ptr<duckdb::FileHandle> file;
  };

  struct Written {
    std::atomic<uint64_t> seq{0};
    std::atomic<Lsn> end{0};
  };

  static constexpr uint64_t kWrittenRing = uint64_t{1} << 16;

  std::filesystem::path ChunkPath(uint64_t number) const;
  duckdb::unique_ptr<duckdb::FileHandle> OpenChunk(uint64_t number,
                                                   bool create) const;
  void StartChunk() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  bool NotPreparing() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex) {
    return !_preparing;
  }
  void SyncDirectory() const;
  void AdvanceWritten() noexcept;
  void SyncTo(Lsn target);

  duckdb::FileSystem& _fs;
  std::filesystem::path _dir;
  mutable absl::Mutex _mutex;
  absl::btree_map<Lsn, std::shared_ptr<Chunk>> _chunks ABSL_GUARDED_BY(_mutex);
  uint64_t _next_number ABSL_GUARDED_BY(_mutex) = 0;
  Lsn _replayed ABSL_GUARDED_BY(_mutex) = 0;
  Lsn _reserved ABSL_GUARDED_BY(_mutex) = 0;
  uint64_t _reserved_seq ABSL_GUARDED_BY(_mutex) = 0;
  std::unique_ptr<Written[]> _written_ring =
    std::make_unique<Written[]>(kWrittenRing);
  std::atomic<uint64_t> _written_seq{0};
  std::atomic<Lsn> _written{0};
  std::atomic<Lsn> _durable{0};
  std::atomic<bool> _syncing{false};
  std::atomic<uint64_t> _syncs{0};
  uint64_t _directory_changes ABSL_GUARDED_BY(_mutex) = 0;
  uint64_t _directory_synced ABSL_GUARDED_BY(_mutex) = 0;
  duckdb::unique_ptr<duckdb::FileHandle> _next ABSL_GUARDED_BY(_mutex);
  bool _preparing ABSL_GUARDED_BY(_mutex) = false;
  std::atomic<bool> _has_next{false};
  std::atomic<Lsn> _oldest{0};
  std::atomic<Lsn> _newest{0};
  std::atomic<bool> _newest_prepared{false};
  std::atomic<Lsn> _covered{0};
  std::atomic<uint64_t> _covered_number{0};
  std::atomic<uint64_t> _snapshot_bytes{0};
  std::atomic<uint64_t> _history_bytes{0};
  absl::Mutex _files_mutex;
  std::vector<std::vector<uint8_t>> _history ABSL_GUARDED_BY(_files_mutex);
};

}  // namespace sdb::catalog
