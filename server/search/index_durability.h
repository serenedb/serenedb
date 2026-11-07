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
#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/typedefs.hpp>
#include <filesystem>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <string>
#include <utility>

#include "connector/file_manifest.h"

namespace sdb::search {

struct IndexStamp {
  std::shared_ptr<const FileManifest> manifest;

  static IndexStamp Read(duckdb::BinaryDeserializer& in);
};

class IndexDurability {
 public:
  using ManifestSource =
    absl::AnyInvocable<std::shared_ptr<const FileManifest>() const>;

  IndexDurability(duckdb::idx_t db_id, std::string name);

  std::pair<std::shared_ptr<irs::IndexWriter>, IndexStamp> OpenWriter(
    irs::Directory& dir, std::filesystem::path path, bool on_disk, bool is_new,
    bool keeps_meta, irs::IndexWriterOptions options,
    ManifestSource manifest = nullptr);

  void Commit(irs::IndexWriter& writer,
              const irs::ProgressReportCallback& progress);

  absl::Status Cleanup(irs::Directory& dir);

 private:
  void SyncDirectory();
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
  absl::Mutex _commit_mutex;
};

}  // namespace sdb::search
