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

// The persisted model keeps its original namespace (sdb::search): the index
// storage holds/parses it as part of its segment state, and the 40+
// search::FileManifest references tree-wide stay valid.
namespace sdb::search {

struct FileManifestEntry {
  uint64_t file_id = 0;
  std::string path;
  uint64_t size = 0;
  int64_t mtime_micros = 0;
  // FileSystem::GetVersionTag: S3 ETag / local {device,inode,size,mtime} pack;
  // empty = source has no versioning.
  std::string version;
  // Max data sequence number among the iceberg delete files applying to
  // this file at index time (0 = none) -- the spec's delete-applicability
  // counter. Lets the delta skip rescanning files whose data AND delete
  // state are unchanged.
  uint64_t delete_seq = 0;
  // The APPLIED iceberg-v3 delete mask (the file's deletion vector), stored
  // exactly like the puffin blob stores it: a 64-bit roaring bitmap split
  // into per-bucket masks -- `high` is the shared upper 32 bits of the dead
  // row positions in the bucket, `bitmap` the portable-serialized 32-bit
  // roaring of their lower halves (opaque bytes; string is this
  // serialization's blob type). A file's next mask is a superset by spec,
  // so the observe diffs it against the stored one and only the newly dead
  // rows touch the term dictionary. Empty = none applied (the first
  // encounter walks the whole set once, then stores it).
  struct V3DeleteMask {
    int32_t high = 0;
    std::string bitmap;

    bool operator==(const V3DeleteMask&) const = default;
  };
  std::vector<V3DeleteMask> v3_delete_masks;

  bool operator==(const FileManifestEntry&) const = default;

  // Stat identity for versionless-source files: size gate, then the
  // strongest identity available -- the version tag (outranks mtime: a
  // re-uploaded identical object moves mtime, not the tag), mtime when the
  // entry carries no tag (filesystems without version tags, or entries
  // predating them).
  bool SameStatAs(const FileManifestEntry& live) const noexcept {
    if (size != live.size) {
      return false;
    }
    if (version.empty()) {
      return mtime_micros == live.mtime_micros;
    }
    return version == live.version;
  }
};

struct FileManifest {
  // Iteration (and so serialization) order is NOT canonical -- absl salts it
  // per container instance. Readers only ever parse it back into a map, and
  // the persistence fixture compares semantically, so nothing may depend on
  // the byte order.
  containers::NodeHashMap<uint64_t, FileManifestEntry> entries;
  // Source-level version pinned at build (iceberg snapshot id); 0 = the
  // files are the whole truth.
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

// Everything that turns a bound view source into FileManifest state: regime
// detection (iceberg vs stat, from the bound list's runtime type),
// per-file fingerprints, the iceberg delete-state ladder, and the listing
// diff. Fingerprints are computed HERE and only here -- the CREATE INDEX
// manifest capture and REFRESH's observe both call this, so they cannot
// drift (a freshly (re)built index must never immediately observe as
// changed). What to DO about the observed state (delta/rebuild planning,
// equality-delete translation, execution) lives with REINDEX.
namespace sdb::connector {

// The delete side of an iceberg snapshot, reduced per data file to the max
// DATA SEQUENCE NUMBER among the delete files that apply to it -- the
// spec's own delete-applicability counter (v2+): sequence numbers are
// snapshot-monotone, so any new applicable delete raises a file's number,
// from delete-manifest METADATA alone. Buckets follow the attribution
// ladder (see CollectIcebergDeleteState); a file's delete version =
// max(mine, partition, global); 0 = no applicable deletes.
struct IcebergDeleteState {
  // Wide (partition/global) buckets track the positional kind separately:
  // an unattributable POSITIONAL delete forces a rescan of the files it may
  // cover, while an equality delete rides the remove-by-query road instead.
  struct Wide {
    uint64_t any = 0;
    uint64_t positional = 0;
  };
  // One entry per equality delete file in the snapshot -- manifest metadata
  // only: seq for the is-it-new test, partition key for the scope test
  // (empty = global). The delete ROWS stay with the reader; the
  // remove-by-query road reads them back parsed (GetEqualityDeletesForFile)
  // when it engages.
  struct EqualityDelete {
    uint64_t seq = 0;
    std::string partition_key;
  };
  // File-pinned delete entries, split by kind: `any` feeds the fingerprint,
  // `non_dv` tracks parquet positional entries separately, and the newest
  // deletion vector carries its manifest-recorded cardinality -- a DV
  // re-registered under a new seq with an unchanged count is provably the
  // same set (supersets only), skippable without fetching the blob.
  struct Pinned {
    uint64_t any = 0;
    uint64_t non_dv = 0;
    uint64_t dv_seq = 0;
    int64_t dv_record_count = -1;
  };

  containers::FlatHashMap<std::string, Pinned> per_file;
  containers::FlatHashMap<std::string, Wide> per_partition;
  std::vector<EqualityDelete> equality;
  const duckdb::IcebergMultiFileList* list = nullptr;
  Wide global;

  struct Components {
    Pinned mine;
    Wide partition;
  };
  Components ComponentsFor(const std::string& path) const;

  uint64_t SeqFor(const std::string& path) const;
};

IcebergDeleteState CollectIcebergDeleteState(
  duckdb::IcebergMultiFileList& files);

// Parse every delete file of the bound list through the reader (idempotent:
// the shared cursor makes later ProcessDeletes calls no-ops). Columns = the
// same selection the multi-file scan itself makes.
void ProcessIcebergDeletes(const duckdb::IcebergMultiFileList& list,
                           const duckdb::MultiFileBindData& bind);

// The reader's parsed deletion vector as manifest chunks (portable roaring
// per 32-bit-high bucket, ascending) -- the shape FileManifestEntry
// persists and the observe diffs against.
std::vector<search::FileManifestEntry::V3DeleteMask> SerializeV3DeleteMasks(
  const duckdb::IcebergDeletionVectorData& data);

// The stat regime's identity: extended_info when the lister produced it for
// free, a filesystem stat otherwise.
void FillFileFingerprint(duckdb::ClientContext& context,
                         const duckdb::OpenFileInfo& file,
                         search::FileManifestEntry& entry);

// The full manifest of a bound source, fingerprinted exactly as the observe
// below computes it: what CREATE INDEX (and every rebuild/delta republish)
// persists. For iceberg sources with deletion vectors the APPLIED sets are
// captured too, so the first refresh diffs them instead of re-walking the
// whole set.
search::FileManifest CaptureManifest(duckdb::ClientContext& context,
                                     duckdb::MultiFileBindData& bind,
                                     int64_t version);

// The listing diff every observable source produces (a single file is a
// listing of one): manifest ids whose docs die (removed files + changed
// files not handled in place) and the files to scan (those changed files +
// added ones), fingerprinted from the live listing; file_id assigned by
// the executor. Changed files handled in place (masks / remove-by-query)
// appear in NEITHER list.
struct FileDiff {
  int64_t added = 0;
  int64_t changed = 0;
  int64_t removed = 0;
  std::vector<uint64_t> del_files;
  std::vector<search::FileManifestEntry> scan;
  // The source version the listing came from (iceberg snapshot pin; 0 =
  // versionless source, always matching the manifest's 0).
  int64_t version = 0;
  bool Empty() const noexcept { return !added && !changed && !removed; }
};

// The observable source behind a view-backed index: the view, its reader
// fast path, and the BOUND reader whose file list is the live listing.
// nullopt = not observable this way (no file-reader fast path) -- the caller
// rebuilds.
struct Source {
  ViewFastPath fast_path;
  duckdb::unique_ptr<duckdb::FunctionData> bind;
  duckdb::MultiFileList* list = nullptr;
  // The bound list downcast: engaged = the iceberg regime (delete-state
  // fingerprints, masks, snapshot pin); null = stat fingerprints.
  duckdb::IcebergMultiFileList* iceberg_list = nullptr;
  duckdb::vector<duckdb::OpenFileInfo> files;
  int64_t version = 0;
};

std::optional<Source> ResolveSource(
  duckdb::ClientContext& context, const catalog::Snapshot& snapshot,
  const catalog::Index& index, const catalog::InvertedIndexOptions& options);

// Everything the iceberg regime adds on top of the common listing walk:
// delete-state sequence numbers for the fingerprints and the delete masks.
// A changed file whose NEW deletes are all position-based and file-pinned
// keeps its docs and file_id -- only its dead rows are masked; the gate is
// per file, so a wide (partition/global) delete elsewhere in the snapshot
// only rescans the files it can actually touch. Anything else rescans
// wholesale.
class IcebergObserve {
 public:
  IcebergObserve(duckdb::IcebergMultiFileList& list,
                 const duckdb::MultiFileBindData& bind);

  void Fill(const duckdb::OpenFileInfo& file,
            search::FileManifestEntry& live) const {
    live.delete_seq = _deletes.SeqFor(file.path);
  }

  static bool Same(const search::FileManifestEntry& entry,
                   const search::FileManifestEntry& live) {
    return entry.delete_seq == live.delete_seq;
  }

  // Byte-unchanged files that only gained position-based deletes keep their
  // docs and file_id -- only the dead rows are masked, nothing is rescanned
  // (counted in `changed`). Positional deletes list their (incremental)
  // rows; a deletion vector's NEW rows (diffed against the applied chunks)
  // list the same way when few, or leapfrog as sorted roaring buckets when
  // many.
  struct DeleteMask {
    uint64_t file_id;
    uint64_t delete_seq;
    std::vector<int64_t> rows;
    std::vector<std::pair<int32_t, roaring::Roaring>> dv_diff;
    // Engaged when a DV was diffed: the full live set, restamped into the
    // manifest entry alongside delete_seq.
    std::optional<std::vector<search::FileManifestEntry::V3DeleteMask>>
      v3_delete_masks;
  };

  // The applied-DV cardinality recorded in the manifest entry's chunks.
  static uint64_t AppliedDvCardinality(const search::FileManifestEntry& entry);

  // Per-file classification of a changed file. A new wide POSITIONAL
  // delete (unattributable positions) forces a rescan; new file-pinned
  // deletes mask their rows in place; new EQUALITY deletes ride the
  // remove-by-query road (eq_covered; the refresher demotes them back to
  // a rescan when translation is refused). Any handled file gets its
  // manifest entry restamped through del_masks, rows or no rows.
  bool TryMask(size_t listing_idx, const search::FileManifestEntry& entry,
               const search::FileManifestEntry& live);

  bool HasNewEquality(const std::string& path, uint64_t entry_seq) const;

  // The same global-column selection ProcessIcebergDeletes uses.
  const duckdb::vector<duckdb::MultiFileColumnDefinition>& GlobalColumns()
    const;

  // The reader materializes every delete file's content -- dead positions
  // keyed by the RECORDED data-file path (parquet and puffin alike) and
  // parsed equality rows keyed by sequence number. The bind's real columns
  // let the equality parquets project; no delete-format parsing here.
  // Idempotent: the reader's shared cursor makes repeat calls no-ops.
  void EnsureDeletesProcessed() {
    ProcessIcebergDeletes(*_deletes.list, *_bind);
  }

  const IcebergDeleteState& Deletes() const noexcept { return _deletes; }

  // Row lists above this ride the leapfrog filter (one forward term
  // iterator, monotone seek_ge per dead row) instead of per-key point
  // lookups; a deletion vector's diff takes the same cut (small diffs
  // expand to rows, big ones leapfrog the roaring buckets). The leapfrog
  // is O(min(dead, file terms)) regardless of file size, so the cut only
  // trades point-lookup overhead against sharing one iterator -- both
  // roads are safe at any scale.
  static constexpr size_t kMaskRangeWalkThreshold = 250000;

  bool ExtractMaskRows(size_t listing_idx,
                       const search::FileManifestEntry& entry,
                       DeleteMask& mask);

  std::vector<DeleteMask> del_masks;

  // Kept files whose seq moved because of new EQUALITY deletes: their dead
  // rows are removed by query (eq_removes, one per group) instead of a
  // rescan; `live` is kept so a refused translation can demote them back
  // to the rescan road.
  struct EqCovered {
    search::FileManifestEntry live;
    uint64_t file_id;
    size_t listing_idx;
    uint64_t old_seq;
  };
  std::vector<EqCovered> eq_covered;
  std::vector<std::shared_ptr<irs::Filter>> eq_removes;

 private:
  IcebergDeleteState _deletes;
  const duckdb::MultiFileBindData* _bind;
};

// The stat regime: identity straight from the filesystem, nothing to mask.
struct StatObserve {
  duckdb::ClientContext& context;

  void Fill(const duckdb::OpenFileInfo& file,
            search::FileManifestEntry& live) const {
    FillFileFingerprint(context, file, live);
  }

  static bool Same(const search::FileManifestEntry& entry,
                   const search::FileManifestEntry& live) {
    return entry.SameStatAs(live);
  }

  static bool TryMask(size_t, const search::FileManifestEntry&,
                      const search::FileManifestEntry&) {
    return false;
  }
};

// The listing walk both regimes share, the regime spliced in at compile
// time: it fills the live identity, compares it against the manifest entry,
// and may mask a changed file's dead rows in place instead of rescanning it.
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
