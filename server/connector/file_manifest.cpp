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

#include "connector/file_manifest.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/main/client_context.hpp>

#include "basics/assert.h"
#include "basics/serializer.h"
#include "catalog/catalog.h"
#include "catalog/pk_spec.h"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/view.h"
#include "core/deletes/iceberg_deletion_vector.hpp"
#include "core/deletes/iceberg_positional_delete.hpp"
#include "planning/iceberg_multi_file_list.hpp"

namespace sdb::search {

void FileManifest::Serialize(irs::bstring& out) const {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  basics::WriteTuple(serializer, *this);
  out.append(stream.GetData(), stream.GetPosition());
}

std::shared_ptr<const FileManifest> FileManifest::Parse(irs::bytes_view tail) {
  duckdb::MemoryStream stream{const_cast<duckdb::data_t*>(tail.data()),
                              tail.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  auto manifest = std::make_shared<FileManifest>();
  basics::ReadTuple(deserializer, *manifest);
  return manifest;
}

}  // namespace sdb::search
namespace sdb::connector {
namespace {

// Per-data-file delete-state fingerprints from delete MANIFEST metadata:
// the classification itself never reads delete file contents (the roads it
// picks -- masks, remove-by-query, the pk scan, rescans -- read what they
// need afterwards). Entries carrying referenced_data_file (mandatory for
// deletion vectors, optional for positional deletes) attribute exactly by
// leaf name (paths remap under allow_moved_paths; iceberg file names are
// UUID-unique); unattributable POSITIONAL entries invalidate every file
// they may cover (a rescan).
std::string_view FileKey(std::string_view path) noexcept {
  const auto slash = path.rfind('/');
  return slash == std::string_view::npos ? path : path.substr(slash + 1);
}

std::string PartitionKey(
  const duckdb::vector<duckdb::IcebergPartitionInfo>& partition) {
  std::string key;
  for (const auto& part : partition) {
    absl::StrAppend(&key, part.field_id, "=", part.value.ToString(), ";");
  }
  return key;
}

// The scan's global columns: the reader-bind schema when the format set
// one (iceberg does, WITH the field-id identifiers), else the bind's
// columns -- the same selection the multi-file scan itself makes.
const duckdb::vector<duckdb::MultiFileColumnDefinition>& GlobalScanColumns(
  const duckdb::MultiFileBindData& bind) {
  return bind.reader_bind.schema.empty() ? bind.columns
                                         : bind.reader_bind.schema;
}

}  // namespace

IcebergDeleteState::Components IcebergDeleteState::ComponentsFor(
  const std::string& path) const {
  Components comps;
  const auto it = per_file.find(FileKey(path));
  if (it != per_file.end()) {
    comps.mine = it->second;
  }
  if (!per_partition.empty()) {
    SDB_ASSERT(list);
    const auto part_it =
      per_partition.find(PartitionKey(list->GetPartitionInfoForDataFile(path)));
    if (part_it != per_partition.end()) {
      comps.partition = part_it->second;
    }
  }
  return comps;
}

uint64_t IcebergDeleteState::SeqFor(const std::string& path) const {
  const auto comps = ComponentsFor(path);
  return std::max({comps.mine.any, comps.partition.any, global.any});
}

void ProcessIcebergDeletes(const duckdb::IcebergMultiFileList& list,
                           const duckdb::MultiFileBindData& bind) {
  const auto& columns = GlobalScanColumns(bind);
  duckdb::vector<duckdb::ColumnIndex> ids;
  ids.reserve(columns.size());
  for (duckdb::idx_t i = 0; i < columns.size(); ++i) {
    ids.emplace_back(i);
  }
  list.ProcessDeletes(columns, ids, {});
}

std::vector<search::FileManifestEntry::V3DeleteMask> SerializeV3DeleteMasks(
  const duckdb::IcebergDeletionVectorData& data) {
  std::vector<search::FileManifestEntry::V3DeleteMask> chunks;
  chunks.reserve(data.bitmaps.size());
  for (const auto& [high, bitmap] : data.bitmaps) {
    auto& chunk = chunks.emplace_back();
    chunk.high = high;
    chunk.bitmap.resize(bitmap.getSizeInBytes(/*portable=*/true));
    bitmap.write(chunk.bitmap.data(), /*portable=*/true);
  }
  absl::c_sort(chunks, [](const auto& lhs, const auto& rhs) {
    return lhs.high < rhs.high;
  });
  return chunks;
}

IcebergDeleteState CollectIcebergDeleteState(
  duckdb::IcebergMultiFileList& iceberg_list) {
  IcebergDeleteState state;
  // Attribution ladder: `referenced_data_file` when the writer stored it
  // (duckdb-iceberg does only for v3), else equal lower/upper bounds of the
  // positional delete's `file_path` column -- the spec-reserved field id, or
  // FILENAME_FIELD_ID in duckdb's own v2 manifests. Entries with neither
  // (equality deletes, foreign writers) scope to their partition when the
  // spec has one -- per the spec a delete file applies only to data files
  // sharing its partition -- and to everything otherwise.
  constexpr int32_t kIcebergFilePathFieldId = 2147483546;
  const auto referenced_file = [&](const duckdb::IcebergDataFile& delete_file,
                                   bool equality) -> std::string {
    if (equality) {
      // Value-scoped: never pinned to one file, whatever metadata it carries.
      return {};
    }
    if (!delete_file.referenced_data_file.empty()) {
      return delete_file.referenced_data_file;
    }
    for (const auto id : {kIcebergFilePathFieldId,
                          duckdb::MultiFileReader::FILENAME_FIELD_ID}) {
      const auto lo = delete_file.lower_bounds.find(id);
      const auto hi = delete_file.upper_bounds.find(id);
      if (lo == delete_file.lower_bounds.end() ||
          hi == delete_file.upper_bounds.end() || lo->second.IsNull() ||
          hi->second.IsNull() || lo->second != hi->second) {
        continue;
      }
      return lo->second.GetValue<std::string>();
    }
    return {};
  };
  const auto bump = [](uint64_t& slot, uint64_t seq) {
    slot = std::max(slot, seq);
  };
  for (const auto& bound : iceberg_list.GetDeleteManifestEntries()) {
    const auto& delete_file = bound.entry->data_file;
    const auto seq = static_cast<uint64_t>(
      bound.entry->GetSequenceNumber(iceberg_list.GetManifestFileForEntry(
        bound, duckdb::IcebergManifestContentType::DELETE)));
    const bool equality =
      delete_file.content ==
      duckdb::IcebergManifestEntryContentType::EQUALITY_DELETES;
    if (auto file = referenced_file(delete_file, equality); !file.empty()) {
      auto& pinned = state.per_file[FileKey(file)];
      bump(pinned.any, seq);
      if (duckdb::StringUtil::CIEquals(delete_file.file_format, "puffin")) {
        if (seq >= pinned.dv_seq) {
          pinned.dv_seq = seq;
          pinned.dv_record_count = delete_file.record_count;
        }
      } else {
        bump(pinned.non_dv, seq);
      }
      continue;
    }
    std::string partition_key;
    IcebergDeleteState::Wide* wide = &state.global;
    if (!delete_file.partition_info.empty()) {
      partition_key = PartitionKey(delete_file.partition_info);
      wide = &state.per_partition[partition_key];
    }
    bump(wide->any, seq);
    if (equality) {
      state.equality.push_back({seq, std::move(partition_key)});
    } else {
      bump(wide->positional, seq);
    }
  }
  state.list = &iceberg_list;
  return state;
}

void FillFileFingerprint(duckdb::ClientContext& context,
                         const duckdb::OpenFileInfo& file,
                         search::FileManifestEntry& entry) {
  // extended_info is duckdb's opportunistic metadata cache: a lister fills it
  // only when its protocol produced the data for free (S3 LIST responses, the
  // local directory walk) and leaves it absent or partial otherwise (single
  // paths, iceberg's cache hints).
  if (const auto& ext = file.extended_info) {
    const auto find = [&](const char* name) -> const duckdb::Value* {
      auto it = ext->options.find(name);
      return it != ext->options.end() && !it->second.IsNull() ? &it->second
                                                              : nullptr;
    };
    const auto* size = find("file_size");
    const auto* mtime = find("last_modified");
    const auto* version = find("etag");
    if (size && mtime && version) {
      entry.size = size->GetValue<uint64_t>();
      entry.mtime_micros = mtime->DefaultCastAs(duckdb::LogicalType::TIMESTAMP)
                             .GetValue<duckdb::timestamp_t>()
                             .value;
      entry.version = duckdb::StringValue::Get(*version);
      return;
    }
  }
  auto& fs = duckdb::FileSystem::GetFileSystem(context);
  auto handle = fs.OpenFile(file.path, duckdb::FileFlags::FILE_FLAGS_READ);
  entry.size = fs.GetFileSize(*handle);
  entry.mtime_micros = fs.GetLastModifiedTime(*handle).value;
  entry.version = fs.GetVersionTag(*handle);
}

search::FileManifest CaptureManifest(duckdb::ClientContext& context,
                                     duckdb::MultiFileBindData& bind,
                                     int64_t version) {
  auto files = bind.file_list->GetAllFiles();
  search::FileManifest manifest;
  manifest.version = version;
  manifest.entries.reserve(files.size());
  auto* iceberg_list =
    dynamic_cast<duckdb::IcebergMultiFileList*>(bind.file_list.get());
  IcebergDeleteState delete_state;
  bool has_dv = false;
  if (iceberg_list) {
    delete_state = CollectIcebergDeleteState(*iceberg_list);
    has_dv = absl::c_any_of(delete_state.per_file, [](const auto& kv) {
      return kv.second.dv_record_count >= 0;
    });
    if (has_dv) {
      // The scan parses these anyway (shared cursor makes it once-only);
      // doing it now lets the manifest capture the APPLIED deletion
      // vectors, so the first refresh diffs them instead of re-walking the
      // whole set.
      ProcessIcebergDeletes(*iceberg_list, bind);
    }
  }
  for (size_t i = 0; i < files.size(); ++i) {
    auto& entry = manifest.entries[i];
    entry.file_id = i;
    entry.path = files[i].path;
    if (iceberg_list) {
      entry.delete_seq = delete_state.SeqFor(entry.path);
      if (has_dv) {
        const auto& recorded =
          iceberg_list->GetManifestEntry(i).entry->data_file.file_path;
        const auto data =
          iceberg_list->GetExistingPositionalDeleteData(recorded);
        if (data && data->type == duckdb::IcebergDeleteType::DELETION_VECTOR) {
          entry.v3_delete_masks = SerializeV3DeleteMasks(
            static_cast<const duckdb::IcebergDeletionVectorData&>(*data));
        }
      }
    } else {
      FillFileFingerprint(context, files[i], entry);
    }
  }
  return manifest;
}

std::optional<Source> ResolveSource(
  duckdb::ClientContext& context, const catalog::Snapshot& snapshot,
  const catalog::Index& index, const catalog::InvertedIndexOptions& options) {
  Source src;
  const auto view =
    snapshot.GetObject<catalog::PgSqlView>(index.GetRelationId());
  if (!view) {
    return std::nullopt;
  }
  auto fp = ResolveViewFastPath(context, *view, options.key_columns);
  if (!fp) {
    return std::nullopt;
  }
  switch (fp->pk_spec) {
    case catalog::PkSpec::FileRowNumber:
    case catalog::PkSpec::FileIndexPlusRowNumber:
    case catalog::PkSpec::FileOffset:
    case catalog::PkSpec::FileIndexPlusOffset:
      break;
    case catalog::PkSpec::DuckDBRowId:
    case catalog::PkSpec::FileIndexPlusDuckDBRowId:
    case catalog::PkSpec::ExternalPostgresCtid:
    case catalog::PkSpec::ExternalColumnKey:
      return std::nullopt;
  }
  if (fp->catalog_ref) {
    // Attached iceberg (the only catalog_ref road with a file pk_spec): the
    // bind below yields the same observable IcebergMultiFileList as the path
    // road. With MAX_TABLE_STALENESS the attach serves table metadata from a
    // cache, and the barrier must never observe a stale table -- force one
    // fresh LoadTable first; the observe bind and the delta pass are then
    // served from the just-primed entry. Without the option every bind loads
    // fresh metadata already.
    auto entry = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
      context,
      duckdb::QualifiedName(duckdb::Identifier{fp->catalog_ref->catalog},
                            duckdb::Identifier{fp->catalog_ref->schema},
                            duckdb::Identifier{fp->catalog_ref->table}),
      duckdb::OnEntryNotFound::RETURN_NULL);
    auto* iceberg_entry = dynamic_cast<duckdb::IcebergTableEntry*>(entry.get());
    if (!iceberg_entry) {
      return std::nullopt;
    }
    auto& ic_catalog =
      iceberg_entry->ParentCatalog().Cast<duckdb::IcebergCatalog>();
    if (ic_catalog.attach_options.max_table_staleness_micros.IsValid()) {
      iceberg_entry->table_info.RefreshFromCatalog(context);
    }
  }
  src.fast_path = std::move(*fp);
  src.bind = BindFastPathSource(context, src.fast_path);
  if (!src.bind) {
    return std::nullopt;
  }
  auto& mfbd = src.bind->Cast<duckdb::MultiFileBindData>();
  if (!mfbd.file_list) {
    return std::nullopt;
  }
  src.list = mfbd.file_list.get();
  src.iceberg_list = dynamic_cast<duckdb::IcebergMultiFileList*>(src.list);
  if (src.iceberg_list) {
    // Resolved from the bound metadata -- no listing enumeration yet: the
    // caller early-exits on an unmoved pin before materializing files.
    src.version = ExtractIcebergSnapshotId(*src.bind);
  }
  return src;
}

IcebergObserve::IcebergObserve(duckdb::IcebergMultiFileList& list,
                               const duckdb::MultiFileBindData& bind)
  : _deletes{CollectIcebergDeleteState(list)}, _bind{&bind} {}

uint64_t IcebergObserve::AppliedDvCardinality(
  const search::FileManifestEntry& entry) {
  uint64_t cardinality = 0;
  for (const auto& chunk : entry.v3_delete_masks) {
    cardinality +=
      roaring::Roaring::readSafe(chunk.bitmap.data(), chunk.bitmap.size())
        .cardinality();
  }
  return cardinality;
}

bool IcebergObserve::TryMask(size_t listing_idx,
                             const search::FileManifestEntry& entry,
                             const search::FileManifestEntry& live) {
  const auto comps = _deletes.ComponentsFor(live.path);
  if (std::max(comps.partition.positional, _deletes.global.positional) >
      entry.delete_seq) {
    return false;
  }
  bool pinned_new = comps.mine.any > entry.delete_seq;
  // A DV re-registered under a new sequence number with an unchanged
  // cardinality is provably the applied set (supersets only): restamp
  // without fetching the blob.
  const bool dv_unchanged =
    pinned_new && comps.mine.non_dv <= entry.delete_seq &&
    comps.mine.dv_record_count >= 0 && !entry.v3_delete_masks.empty() &&
    static_cast<uint64_t>(comps.mine.dv_record_count) ==
      AppliedDvCardinality(entry);
  if (dv_unchanged) {
    pinned_new = false;
  }
  const bool eq_new = HasNewEquality(live.path, entry.delete_seq);
  if (!pinned_new && !eq_new && !dv_unchanged) {
    return false;
  }
  DeleteMask mask{entry.file_id, live.delete_seq};
  if (pinned_new && !ExtractMaskRows(listing_idx, entry, mask)) {
    return false;
  }
  if (eq_new) {
    eq_covered.push_back({live, entry.file_id, listing_idx, entry.delete_seq});
  }
  del_masks.push_back(std::move(mask));
  return true;
}

bool IcebergObserve::HasNewEquality(const std::string& path,
                                    uint64_t entry_seq) const {
  std::optional<std::string> partition_key;
  for (const auto& eq : _deletes.equality) {
    if (eq.seq <= entry_seq) {
      continue;
    }
    if (eq.partition_key.empty()) {
      return true;
    }
    if (!partition_key) {
      partition_key =
        PartitionKey(_deletes.list->GetPartitionInfoForDataFile(path));
    }
    if (eq.partition_key == *partition_key) {
      return true;
    }
  }
  return false;
}

const duckdb::vector<duckdb::MultiFileColumnDefinition>&
IcebergObserve::GlobalColumns() const {
  return GlobalScanColumns(*_bind);
}

bool IcebergObserve::ExtractMaskRows(size_t listing_idx,
                                     const search::FileManifestEntry& entry,
                                     DeleteMask& mask) {
  EnsureDeletesProcessed();
  const auto& recorded =
    _deletes.list->GetManifestEntry(listing_idx).entry->data_file.file_path;
  auto data = _deletes.list->GetExistingPositionalDeleteData(recorded);
  if (!data) {
    return false;
  }
  // One pass straight from the reader's parsed state (hash set / roaring
  // chunks) into the sorted vector the remove query wants.
  switch (data->type) {
    case duckdb::IcebergDeleteType::POSITIONAL_DELETE: {
      // Positional deletes: only rows from delete files NEWER than the
      // manifest entry -- older buckets were applied by an earlier
      // refresh (or by the build itself, which read through the reader).
      const auto& by_seq =
        static_cast<const duckdb::IcebergPositionalDeleteData&>(*data)
          .rows_by_sequence;
      size_t dead = 0;
      for (const auto& [seq, bucket] : by_seq) {
        if (static_cast<uint64_t>(seq) > entry.delete_seq) {
          dead += bucket.size();
        }
      }
      if (dead == 0) {
        return false;
      }
      mask.rows.reserve(dead);
      for (const auto& [seq, bucket] : by_seq) {
        if (static_cast<uint64_t>(seq) > entry.delete_seq) {
          mask.rows.insert(mask.rows.end(), bucket.begin(), bucket.end());
        }
      }
    } break;
    case duckdb::IcebergDeleteType::DELETION_VECTOR: {
      // A deletion vector ACCUMULATES (one superset per file, no
      // provenance): diff it against the manifest entry's applied chunks
      // so only newly dead rows reach the dictionary, and restamp the
      // entry with the full live set for the next diff. An entry with no
      // chunks (fresh build/rescan) diffs against nothing -- the whole
      // set, once.
      const auto& dv =
        static_cast<const duckdb::IcebergDeletionVectorData&>(*data);
      const auto& bitmaps = dv.bitmaps;
      containers::FlatHashMap<int32_t, roaring::Roaring> applied;
      applied.reserve(entry.v3_delete_masks.size());
      for (const auto& chunk : entry.v3_delete_masks) {
        applied.emplace(
          chunk.high,
          roaring::Roaring::readSafe(chunk.bitmap.data(), chunk.bitmap.size()));
      }
      size_t dead = 0;
      for (const auto& [high, bitmap] : bitmaps) {
        auto diff = bitmap;
        if (const auto it = applied.find(high); it != applied.end()) {
          diff -= it->second;
        }
        if (diff.isEmpty()) {
          continue;
        }
        dead += diff.cardinality();
        mask.dv_diff.emplace_back(high, std::move(diff));
      }
      mask.v3_delete_masks = SerializeV3DeleteMasks(dv);
      if (dead == 0) {
        // live == applied (e.g. the DV was rewritten in place): restamp
        // only.
        return true;
      }
      if (dead > kMaskRangeWalkThreshold) {
        absl::c_sort(mask.dv_diff, [](const auto& lhs, const auto& rhs) {
          return lhs.first < rhs.first;
        });
        return true;
      }
      mask.rows.reserve(dead);
      for (const auto& [high, diff] : mask.dv_diff) {
        for (const uint32_t value : diff) {
          mask.rows.push_back((static_cast<int64_t>(high) << 32) | value);
        }
      }
      mask.dv_diff.clear();
    } break;
  }
  if (mask.rows.empty()) {
    return false;
  }
  absl::c_sort(mask.rows);
  // Two delete files can list the same row (CDC re-deletes): the exact
  // road seeks each key once.
  mask.rows.erase(std::unique(mask.rows.begin(), mask.rows.end()),
                  mask.rows.end());
  return true;
}

}  // namespace sdb::connector
