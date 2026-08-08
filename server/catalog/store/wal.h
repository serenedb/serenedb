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

#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace duckdb {

class BufferedFileWriter;
class FileSystem;

}  // namespace duckdb
namespace sdb::catalog {

// Append-only file of checksummed frames ([u64 size][u64 checksum][payload]).
// Append returns once fsync-durable; concurrent appends group commit.
class CatalogWal {
 public:
  struct Stats {
    uint64_t frames = 0;
    uint64_t sync_batches = 0;
    uint64_t appended_bytes = 0;
    uint64_t size_on_disk = 0;
  };

  using FrameVisitor = absl::FunctionRef<void(std::span<const uint8_t>)>;
  using FrameSink = absl::FunctionRef<void(std::span<const uint8_t>)>;

  CatalogWal();
  ~CatalogWal();

  // Replays every complete frame, truncating a torn tail. Fatal on IO errors.
  void Open(std::string_view directory, FrameVisitor replay);

  void Append(std::span<const uint8_t> payload);
  // False before Open finishes replaying and after Close. Deserializing an
  // entry default-constructs objects, which allocates ids, so the id path runs
  // during replay too -- and must not append into a file it is still reading.
  bool Writable() const noexcept {
    return _writable.load(std::memory_order_acquire);
  }

  // Replaces the file with whatever `fill` emits, so a frame appended but not
  // represented in it is dropped -- including one still in flight, whose Append
  // returns success anyway. A concurrent appender must publish into the state
  // `fill` renders from before appending, and hold it across this call.
  void Compact(absl::FunctionRef<void(FrameSink)> fill);

  void Close();

  Stats GetStats() const;

  // Stops at a torn tail without truncating; safe against a live appender.
  static void Scan(std::string_view directory, FrameVisitor visitor);

 private:
  uint64_t ReplayAndTruncate(FrameVisitor replay);
  // Drops _mutex around the fsync, which the analysis cannot model.
  void SyncLocked(uint64_t my_seq) ABSL_NO_THREAD_SAFETY_ANALYSIS;

  std::unique_ptr<duckdb::FileSystem> _fs;
  std::string _path;
  std::string _tmp_path;
  std::string _dir;

  absl::Mutex _mutex;
  std::unique_ptr<duckdb::BufferedFileWriter> _writer ABSL_GUARDED_BY(_mutex);
  uint64_t _written_seq ABSL_GUARDED_BY(_mutex) = 0;
  uint64_t _synced_seq ABSL_GUARDED_BY(_mutex) = 0;
  bool _sync_in_progress ABSL_GUARDED_BY(_mutex) = false;

  std::atomic<bool> _writable = false;
  std::atomic<uint64_t> _frames = 0;
  std::atomic<uint64_t> _sync_batches = 0;
  std::atomic<uint64_t> _appended_bytes = 0;
  std::atomic<uint64_t> _size_on_disk = 0;
};

}  // namespace sdb::catalog
