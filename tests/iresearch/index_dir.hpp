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

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/serializer/buffered_file_writer.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <iresearch/formats/flush_state.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tests {

enum class StorageKind : uint8_t {
  Local,
  Memory,
  Remote,
};

struct CommittedSegment {
  irs::SegmentMeta meta;
  std::shared_ptr<const irs::FileTracker> files;
};

class IndexDir {
 public:
  IndexDir(duckdb::DatabaseInstance& db, std::string_view root);
  IndexDir(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
           std::string_view root);

  std::unique_ptr<duckdb::BufferedFileWriter> Create(
    std::string_view name) const;
  duckdb::shared_ptr<duckdb::InputFile> Open(
    std::string_view name, duckdb::MappedFileAccess access) const;
  irs::bstring ReadWhole(std::string_view name) const;
  bool Has(std::string_view name) const;
  bool Remove(std::string_view name) const;
  void Rename(std::string_view src, std::string_view dst) const;
  void Sync(std::span<const std::string_view> names) const;
  void Compact(std::string_view name) const;
  std::vector<std::string> Files() const;

  duckdb::DatabaseInstance& Db() const noexcept { return _state->db; }
  duckdb::FileSystem& Fs() const noexcept { return _state->fs; }
  const std::string& Root() const noexcept { return _state->root; }
  StorageKind Kind() const noexcept { return _state->kind; }
  const std::shared_ptr<irs::ReleasedSegments>& Released() const noexcept {
    return _state->segments->released;
  }
  irs::FileTracker& Tracker(std::string_view segment) const;
  std::shared_ptr<const irs::FileTracker> Files(
    const irs::SegmentMeta& meta) const;

  std::vector<irs::SegmentMeta> Committed() const;
  std::vector<CommittedSegment> CommittedSegments() const;
  void Publish(std::vector<CommittedSegment> segments) const;
  std::vector<std::string> HeldFiles() const;
  size_t RemoveReleased() const;
  uint64_t TopSegmentId() const;
  void RaiseTopSegmentId(uint64_t id) const;

  IndexDir Over(duckdb::FileSystem& fs) const;
  IndexDir Reopened() const;

 private:
  struct Published {
    irs::ReleasedSegment segment;
    std::weak_ptr<const irs::FileTracker> files;
  };

  struct Segments {
    std::shared_ptr<irs::ReleasedSegments> released =
      std::make_shared<irs::ReleasedSegments>();
    absl::Mutex mutex;
    std::vector<CommittedSegment> committed ABSL_GUARDED_BY(mutex);
    std::vector<Published> published ABSL_GUARDED_BY(mutex);
    uint64_t top_segment_id ABSL_GUARDED_BY(mutex) = 0;
  };

  struct State {
    State(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
          std::string root, StorageKind kind,
          std::shared_ptr<Segments> segments)
      : db{db},
        fs{fs},
        root{std::move(root)},
        kind{kind},
        segments{std::move(segments)} {}
    ~State();

    duckdb::DatabaseInstance& db;
    duckdb::FileSystem& fs;
    std::string root;
    StorageKind kind;
    std::shared_ptr<Segments> segments;
    absl::Mutex trackers_mutex;
    irs::containers::FlatHashMap<std::string, std::unique_ptr<irs::FileTracker>>
      trackers ABSL_GUARDED_BY(trackers_mutex);
  };

  std::shared_ptr<State> _state;
};

}  // namespace tests
