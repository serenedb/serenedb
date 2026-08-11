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

#include <cstdint>
#include <duckdb/common/open_file_info.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "catalog/inverted_index.h"
#include "connector/view_fast_path.h"

namespace sdb::search {

struct FileManifestEntry {
  uint64_t file_id = 0;
  std::string path;
  // etag for object stores (their mtime is whole-second), mtime for the
  // rest; both empty on the iceberg road (the version diffs it).
  std::string etag;
  int64_t mtime_micros = 0;

  bool operator==(const FileManifestEntry&) const = default;
};

struct FileManifest {
  containers::NodeHashMap<uint64_t, FileManifestEntry> entries;
  // Iceberg: the indexed snapshot id (everything else -- the sequence
  // baseline, the diff -- resolves from the table metadata by it). 0 for
  // the stat regime.
  int64_t version = 0;

  const FileManifestEntry* FindById(uint64_t id) const noexcept {
    const auto it = entries.find(id);
    return it == entries.end() ? nullptr : &it->second;
  }

  void Serialize(irs::bstring& out) const;

  static std::shared_ptr<const FileManifest> Parse(irs::bytes_view tail);

  bool operator==(const FileManifest&) const = default;
};

}  // namespace sdb::search
namespace duckdb {

struct IcebergDeletionVectorData;
struct IcebergMultiFileList;
struct MultiFileBindData;
struct MultiFileColumnDefinition;
class MultiFileList;

}  // namespace duckdb
namespace sdb::catalog {

class Index;
struct Snapshot;

}  // namespace sdb::catalog
namespace sdb::connector {

struct IcebergDeleteState {
  // Newest delete sequence (watermark) for a scope wider than one file.
  // `any` gates "file changed"; `mask_block` is the newest positional
  // delete there -- its rows cannot be attributed to files from metadata,
  // so it forces a rescan instead of a mask.
  struct Watermarks {
    uint64_t any = 0;
    uint64_t mask_block = 0;
  };
  struct EqualityDelete {
    uint64_t seq = 0;
    std::string partition_key;  // empty = global
  };
  // File-scoped deletes: the newest sequence per data file.
  containers::FlatHashMap<std::string, uint64_t> per_file;
  containers::FlatHashMap<std::string, Watermarks> per_partition;
  std::vector<EqualityDelete> equality;
  const duckdb::IcebergMultiFileList* list = nullptr;
  Watermarks global;

  // The delete scopes covering one data file (global reads off the state).
  struct Covering {
    uint64_t file = 0;
    Watermarks partition;
  };
  Covering CoveringFor(const std::string& path) const;

  uint64_t SeqFor(const std::string& path) const;
};

IcebergDeleteState CollectIcebergDeleteState(
  duckdb::IcebergMultiFileList& files);

void ProcessIcebergDeletes(const duckdb::IcebergMultiFileList& list,
                           const duckdb::MultiFileBindData& bind);

void FillFileIdentity(duckdb::ClientContext& context,
                      const duckdb::OpenFileInfo& file,
                      search::FileManifestEntry& entry);

search::FileManifest CaptureManifest(duckdb::ClientContext& context,
                                     duckdb::MultiFileBindData& bind);

// True when `snapshot_id` is the list's pinned snapshot or one of its
// ancestors. False = the indexed snapshot left the table's history
// (expired, rollback, replaced table): deletes may have been UNDONE, which
// no sequence-number comparison can see -- only a rebuild converges.
bool SnapshotIsAncestor(const duckdb::IcebergMultiFileList& list,
                        int64_t snapshot_id);

struct FileDiff {
  int64_t added = 0;
  int64_t changed = 0;
  int64_t removed = 0;
  std::vector<uint64_t> del_files;
  std::vector<search::FileManifestEntry> scan;
  int64_t version = 0;
  bool Empty() const noexcept { return !added && !changed && !removed; }
};

struct Source {
  ViewFastPath fast_path;
  duckdb::unique_ptr<duckdb::FunctionData> bind;
  duckdb::MultiFileList* list = nullptr;
  duckdb::IcebergMultiFileList* iceberg_list = nullptr;
  duckdb::vector<duckdb::OpenFileInfo> files;
  int64_t version = 0;
};

std::optional<Source> ResolveSource(
  duckdb::ClientContext& context, const catalog::Snapshot& snapshot,
  const catalog::Index& index, const catalog::InvertedIndexOptions& options);

class IcebergObserve {
 public:
  // `stored_version` is the manifest's indexed snapshot id; the sequence
  // baseline resolves from the table metadata by it.
  IcebergObserve(duckdb::IcebergMultiFileList& list,
                 const duckdb::MultiFileBindData& bind, int64_t stored_version);

  void Fill(const duckdb::OpenFileInfo&, search::FileManifestEntry&) const {}

  bool Same(const search::FileManifestEntry&,
            const search::FileManifestEntry& live) const {
    return _deletes.SeqFor(live.path) <= _sequence_number;
  }

  struct DeleteMask {
    uint64_t file_id;
    std::vector<int64_t> rows;
    std::vector<std::pair<int32_t, roaring::Roaring>> dv;
  };

  bool TryMask(size_t listing_idx, const search::FileManifestEntry& entry,
               const search::FileManifestEntry& live);

  bool HasNewEquality(const std::string& path) const;

  uint64_t SequenceNumber() const noexcept { return _sequence_number; }

  const duckdb::vector<duckdb::MultiFileColumnDefinition>& GlobalColumns()
    const;

  void EnsureDeletesProcessed() {
    ProcessIcebergDeletes(*_deletes.list, *_bind);
  }

  const IcebergDeleteState& Deletes() const noexcept { return _deletes; }

  bool ExtractMaskRows(size_t listing_idx, DeleteMask& mask);

  std::vector<DeleteMask> del_masks;

  struct EqCovered {
    search::FileManifestEntry live;
    uint64_t file_id;
    size_t listing_idx;
  };
  std::vector<EqCovered> eq_covered;

 private:
  IcebergDeleteState _deletes;
  const duckdb::MultiFileBindData* _bind;
  uint64_t _sequence_number;
};

struct StatObserve {
  duckdb::ClientContext& context;

  void Fill(const duckdb::OpenFileInfo& file,
            search::FileManifestEntry& live) const {
    FillFileIdentity(context, file, live);
  }

  static bool Same(const search::FileManifestEntry& entry,
                   const search::FileManifestEntry& live) {
    return entry.etag == live.etag && entry.mtime_micros == live.mtime_micros;
  }

  static bool TryMask(size_t, const search::FileManifestEntry&,
                      const search::FileManifestEntry&) {
    return false;
  }
};

template<typename Observe>
FileDiff DiffListing(const Source& src, const search::FileManifest& manifest,
                     Observe& observe) {
  containers::FlatHashMap<std::string_view, const search::FileManifestEntry*>
    by_path;
  by_path.reserve(manifest.entries.size());
  for (const auto& [id, e] : manifest.entries) {
    by_path.emplace(e.path, &e);
  }
  containers::FlatHashSet<std::string_view> live_paths;
  live_paths.reserve(src.files.size());

  FileDiff files;
  files.version = src.version;
  for (size_t i = 0; i < src.files.size(); ++i) {
    const auto& file = src.files[i];
    live_paths.emplace(file.path);
    const auto it = by_path.find(file.path);
    search::FileManifestEntry live;
    live.path = file.path;
    // The listing ordinal; BuildNextManifest turns it into the file's id
    // (base + ordinal), which the pass emits as `file_index + base`.
    live.file_id = i;
    observe.Fill(file, live);
    if (it == by_path.end()) {
      ++files.added;
      files.scan.push_back(std::move(live));
      continue;
    }
    const auto& entry = *it->second;
    if (observe.Same(entry, live)) {
      continue;
    }
    ++files.changed;
    if (observe.TryMask(i, entry, live)) {
      continue;
    }
    files.del_files.push_back(entry.file_id);
    files.scan.push_back(std::move(live));
  }
  for (const auto& [id, entry] : manifest.entries) {
    if (!live_paths.contains(entry.path)) {
      ++files.removed;
      files.del_files.push_back(id);
    }
  }
  return files;
}

}  // namespace sdb::connector
