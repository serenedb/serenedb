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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/write_stream.hpp>
#include <iresearch/utils/group_commit.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "server/utils/message_buffer.h"

namespace sdb::catalog {

using Lsn = uint64_t;

class RecordBuffer final : public duckdb::WriteStream {
 public:
  static constexpr size_t kFrameBytes = 2 * sizeof(uint32_t);

  RecordBuffer();
  RecordBuffer(RecordBuffer&&) noexcept = default;
  RecordBuffer& operator=(RecordBuffer&&) noexcept = default;
  ~RecordBuffer() final;

  [[nodiscard]] duckdb::data_ptr_t Alloc(duckdb::idx_t size) final {
    return _writer->Alloc(size);
  }

  size_t PayloadBytes() const noexcept {
    return _writer->Written() - kFrameBytes;
  }

  void MarkCatalog(size_t begin, size_t size) noexcept {
    _catalog_begin = begin;
    _catalog_size = size;
  }

  size_t CatalogBytes() const noexcept { return _catalog_size; }

  std::vector<uint8_t> CopyCatalog() const {
    std::vector<uint8_t> out;
    out.reserve(_catalog_size);
    const auto end = _catalog_begin + _catalog_size;
    size_t offset = 0;
    ForEachPayload([&](std::span<const uint8_t> part) {
      const auto from = std::max(offset, _catalog_begin);
      const auto to = std::min(offset + part.size(), end);
      if (from < to) {
        out.insert(out.end(), part.begin() + (from - offset),
                   part.begin() + (to - offset));
      }
      offset += part.size();
    });
    return out;
  }

  void SetFrame(std::span<const uint8_t, kFrameBytes> frame) noexcept {
    std::memcpy(_frame, frame.data(), kFrameBytes);
  }

  message::SequenceView Framed() const noexcept { return _buffer->Written(); }

  template<typename Visit>
  void ForEachPayload(const Visit& visit) const {
    size_t skip = kFrameBytes;
    for (const auto part : Framed()) {
      const auto taken = std::min(skip, part.size());
      skip -= taken;
      if (taken != part.size()) {
        visit(std::span{static_cast<const uint8_t*>(part.data()) + taken,
                        part.size() - taken});
      }
    }
  }

 protected:
  void Overflow(duckdb::const_data_ptr_t data, duckdb::idx_t size) final {
    _writer->Write({reinterpret_cast<const char*>(data), size});
  }

 private:
  std::unique_ptr<message::Buffer> _buffer;
  std::unique_ptr<message::Writer> _writer;
  uint8_t* _frame = nullptr;
  size_t _catalog_begin = 0;
  size_t _catalog_size = 0;
};

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

  struct Replayed {
    bool snapshot = false;
    bool clean = false;
  };

  static constexpr uint64_t kChunkBytes = uint64_t{16} << 20;

  static std::string ChunkName(uint64_t number);
  static bool HasSnapshot(duckdb::FileSystem& fs, const std::string& snapshot);
  static void Read(duckdb::FileSystem& fs, const std::string& dir,
                   const std::string& snapshot, Apply apply);
  static void Copy(duckdb::FileSystem& fs, const std::string& from,
                   const std::string& to);

  Log(duckdb::FileSystem& fs, std::string dir, std::string snapshot);

  Replayed Replay(Apply apply, CatalogPart part);

  Range Reserve(size_t payload_size);
  void Write(const Range& range, RecordBuffer& record);
  void Sync(Lsn end);

  Lsn Append(RecordBuffer record);

  void WriteSnapshot(Lsn covered, std::span<const RecordBuffer> records,
                     CatalogPart part, Compact compact, bool clean);
  void DropCovered();
  void Pin(const std::string& to);
  void Clear();

  static constexpr uint64_t kTailBytes = uint64_t{16} << 20;

  uint64_t TailBytes() const noexcept {
    return _tail_bytes.load(std::memory_order_relaxed);
  }
  void FoldTail(Lsn bound, CatalogPart part, Compact compact);

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

  bool HasNext() const noexcept {
    return _has_next.load(std::memory_order_relaxed);
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

  struct TailPart {
    Lsn end = 0;
    std::vector<uint8_t> bytes;
  };

  static constexpr uint64_t kWrittenRing = uint64_t{1} << 16;

  std::string ChunkPath(uint64_t number) const;
  duckdb::unique_ptr<duckdb::FileHandle> OpenChunk(uint64_t number,
                                                   bool create) const;
  void StartChunk() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  bool NotPreparing() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex) {
    return !_preparing;
  }
  void SyncDirectory() const;
  void AdvanceWritten() noexcept;
  void SyncTo(Lsn target);
  void AddTail(Lsn begin, Lsn end, std::vector<uint8_t> bytes);
  std::vector<std::vector<uint8_t>> TakeTail(Lsn bound);

  duckdb::FileSystem& _fs;
  std::string _dir;
  std::string _snapshot_path;
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
  irs::GroupCommit<Lsn> _syncs;
  uint64_t _directory_changes ABSL_GUARDED_BY(_mutex) = 0;
  uint64_t _directory_synced ABSL_GUARDED_BY(_mutex) = 0;
  duckdb::unique_ptr<duckdb::FileHandle> _next ABSL_GUARDED_BY(_mutex);
  bool _preparing ABSL_GUARDED_BY(_mutex) = false;
  std::atomic<bool> _has_next{false};
  std::atomic<Lsn> _oldest{0};
  std::atomic<Lsn> _newest{0};
  std::atomic<Lsn> _covered{0};
  std::atomic<uint64_t> _covered_number{0};
  std::atomic<uint64_t> _snapshot_bytes{0};
  std::atomic<uint64_t> _history_bytes{0};
  absl::Mutex _files_mutex;
  std::vector<std::vector<uint8_t>> _history ABSL_GUARDED_BY(_files_mutex);
  absl::Mutex _tail_mutex;
  absl::btree_map<Lsn, TailPart> _tail ABSL_GUARDED_BY(_tail_mutex);
  std::atomic<uint64_t> _tail_bytes{0};
};

}  // namespace sdb::catalog
