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

// The catalog's persistent form: one append-only file of checksummed frames
// ([u64 size][u64 checksum][payload]), replayed on open. A torn tail is
// truncated at the last complete frame. Compaction rewrites the file as the
// minimal frame list that replays to the current state (tmp + sync + atomic
// rename + directory fsync); there is no separate snapshot format.
//
// Append returns once the frame is fsync-durable. Concurrent appends group
// commit: frames buffer under the mutex, one caller becomes the sync leader
// for everything written so far, followers wait for the covering sync.
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

  // Creates `directory` if needed, removes a stale compaction temp, replays
  // every complete frame through `replay`, truncates a torn tail, and leaves
  // the file open for appends. Fatal on IO errors.
  void Open(std::string_view directory, FrameVisitor replay);

  void Append(std::span<const uint8_t> payload);

  // `fill` receives a sink and emits the compacted frames in order; the new
  // file atomically replaces the old one. Appends block for the duration.
  void Compact(absl::FunctionRef<void(FrameSink)> fill);

  void Close();

  Stats GetStats() const;

 private:
  uint64_t ReplayAndTruncate(FrameVisitor replay);
  // Runs under _mutex; drops it around the fsync (leader/follower group
  // commit), which the static analysis cannot model.
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

  std::atomic<uint64_t> _frames = 0;
  std::atomic<uint64_t> _sync_batches = 0;
  std::atomic<uint64_t> _appended_bytes = 0;
  std::atomic<uint64_t> _size_on_disk = 0;
};

}  // namespace sdb::catalog
