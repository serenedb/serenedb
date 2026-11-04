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

#include <absl/functional/any_invocable.h>
#include <absl/functional/function_ref.h>
#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/main/attached_database.hpp>
#include <filesystem>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "connector/file_manifest.h"
#include "search/maintenance.h"

namespace sdb::search {

// The attached database a store belongs to, or null when it is gone. Looked up
// by oid rather than held, because a store outlives neither the database nor
// its own detach.
duckdb::shared_ptr<duckdb::AttachedDatabase> AttachedDatabaseById(
  duckdb::idx_t id);

struct IndexStamp {
  irs::SourcePosition position;
  std::shared_ptr<const FileManifest> manifest;
  uint64_t checkpoint = 0;

  static IndexStamp Read(duckdb::BinaryDeserializer& in);
};

class IndexDurability {
 public:
  using ManifestSource =
    absl::AnyInvocable<std::shared_ptr<const FileManifest>() const>;

  IndexDurability(duckdb::idx_t db_id, std::string name);

  std::pair<std::shared_ptr<irs::IndexWriter>, IndexStamp> OpenWriter(
    irs::Directory& dir, std::filesystem::path path, bool on_disk, bool is_new,
    irs::IndexWriterOptions options, ManifestSource manifest = nullptr);
  static std::pair<irs::DirectoryReader, IndexStamp> OpenReader(
    irs::Directory& dir, const irs::IndexReaderOptions& options);

  const irs::SourcePosition& Position() const noexcept { return _position; }
  void MarkStaged(const irs::SourcePosition& position);
  std::optional<uint64_t> NeededWAL() const;

  absl::Status Refresh(irs::IndexWriter& writer, bool wait,
                       const irs::ProgressReportCallback& progress,
                       RefreshResult& code, absl::FunctionRef<void()> unchanged,
                       absl::FunctionRef<void()> published);
  void ReplaceSegments(
    irs::IndexWriter& writer, std::span<const std::string_view> replaced,
    std::span<const std::string_view> adopted_metas,
    absl::FunctionRef<bool(irs::IndexWriter::QueryContext::FilterPtr&)>
      removal_provider);
  void Clear(irs::IndexWriter& writer, const irs::SourcePosition& position);
  void PrepareCheckpoint(irs::IndexWriter& writer, uint64_t iteration);
  bool FinishCheckpoint(irs::IndexWriter& writer);
  void FinishCheckpointIfIdle(irs::IndexWriter& writer);
  // The published reader and the position it covers, as one instant. Waits
  // out a refresh in progress to get it.
  std::pair<irs::DirectoryReader, irs::SourcePosition> Snapshot(
    irs::IndexWriter& writer);

  // The published reader and a position it is guaranteed to cover, for a
  // certifier that goes on to read the log from that position. Waits for no
  // refresh, which is the point: Refresh holds its lock across the whole
  // publish, and certification happens on the commit path, where waiting that
  // out would serialize every write to the table behind it.
  //
  // It needs no instant, only a position the reader cannot be behind. Refresh
  // installs the new reader before it advances the published position, so a
  // position read first can only lag the reader taken after it. Lagging is
  // free: the rows in between are in the reader and in the log read, and a key
  // found twice is still found. Leading would leave them in neither.
  std::pair<irs::DirectoryReader, irs::SourcePosition> CertificationSnapshot(
    irs::IndexWriter& writer);

  absl::Status Cleanup(irs::Directory& dir);
  void MarkOutOfSync() noexcept {
    _out_of_sync.store(true, std::memory_order_relaxed);
  }
  bool IsOutOfSync() const noexcept {
    return _out_of_sync.load(std::memory_order_relaxed);
  }

 private:
  void SyncDirectory();
  void SetCreationFloor();
  void MarkPublished();
  bool FinishPendingSave(irs::IndexWriter& writer);
  void AwaitPendingSave(std::unique_lock<absl::Mutex>& lock,
                        irs::IndexWriter& writer);
  void WritePayload(const irs::SourcePosition& position,
                    const FileManifest* manifest,
                    duckdb::BinarySerializer& out);
  void ResolvePendingSave(irs::Directory& dir) const;
  bool CanPersist() const;
  uint64_t RunningCheckpoint(uint64_t generation) const;
  bool CheckpointWritten(uint64_t iteration) const;
  void WaitForCheckpoint() const;

  duckdb::idx_t _db_id;
  std::string _name;
  std::filesystem::path _path;
  bool _on_disk = false;
  std::atomic<bool> _directory_dirty{true};
  std::atomic<bool> _out_of_sync{false};
  absl::Mutex _refresh_mutex;
  irs::SourcePosition _position;
  mutable absl::Mutex _positions_mutex;
  irs::SourcePosition _published ABSL_GUARDED_BY(_positions_mutex);
  irs::SourcePosition _durable ABSL_GUARDED_BY(_positions_mutex);
  irs::SourcePosition _staged ABSL_GUARDED_BY(_positions_mutex);
  uint64_t _checkpoint = 0;
  uint64_t _pending_checkpoint = 0;
};

}  // namespace sdb::search
