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

struct IndexStamp {
  irs::SourcePosition position;
  std::shared_ptr<const FileManifest> manifest;

  static IndexStamp Read(duckdb::BinaryDeserializer& in);
};

struct RefreshGate {
  absl::Mutex& mutex;
  absl::FunctionRef<bool()> open;
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

  absl::Status Refresh(irs::IndexWriter& writer, bool wait,
                       const irs::ProgressReportCallback& progress,
                       RefreshResult& code, absl::FunctionRef<void()> unchanged,
                       absl::FunctionRef<void()> published,
                       const RefreshGate* gate);
  void Clear(irs::IndexWriter& writer, const irs::SourcePosition& position);

  absl::Status Cleanup(irs::Directory& dir);
  void MarkOutOfSync() noexcept {
    _out_of_sync.store(true, std::memory_order_relaxed);
  }
  bool IsOutOfSync() const noexcept {
    return _out_of_sync.load(std::memory_order_relaxed);
  }

 private:
  void SyncDirectory();
  void MarkPublished();
  void WritePayload(const irs::SourcePosition& position,
                    const FileManifest* manifest,
                    duckdb::BinarySerializer& out);
  void RemovePendingSave(irs::Directory& dir) const;
  bool PayloadChanged() const;
  bool CanPersist() const;

  duckdb::idx_t _db_id;
  std::string _name;
  std::filesystem::path _path;
  ManifestSource _manifest;
  std::shared_ptr<const FileManifest> _written_manifest;
  bool _on_disk = false;
  std::atomic<bool> _directory_dirty{true};
  std::atomic<bool> _out_of_sync{false};
  absl::Mutex _refresh_mutex;
  irs::SourcePosition _position;
  mutable absl::Mutex _positions_mutex;
  irs::SourcePosition _published;
  irs::SourcePosition _durable ABSL_GUARDED_BY(_positions_mutex);
  irs::SourcePosition _staged ABSL_GUARDED_BY(_positions_mutex);
};

}  // namespace sdb::search
