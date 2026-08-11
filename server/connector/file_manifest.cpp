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
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"

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

const duckdb::vector<duckdb::MultiFileColumnDefinition>& GlobalScanColumns(
  const duckdb::MultiFileBindData& bind) {
  return bind.reader_bind.schema.empty() ? bind.columns
                                         : bind.reader_bind.schema;
}

}  // namespace

IcebergDeleteState::Covering IcebergDeleteState::CoveringFor(
  const std::string& path) const {
  Covering covering;
  const auto it = per_file.find(FileKey(path));
  if (it != per_file.end()) {
    covering.file = it->second;
  }
  if (!per_partition.empty()) {
    SDB_ASSERT(list);
    const auto part_it =
      per_partition.find(PartitionKey(list->GetPartitionInfoForDataFile(path)));
    if (part_it != per_partition.end()) {
      covering.partition = part_it->second;
    }
  }
  return covering;
}

uint64_t IcebergDeleteState::SeqFor(const std::string& path) const {
  const auto covering = CoveringFor(path);
  return std::max({covering.file, covering.partition.any, global.any});
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

IcebergDeleteState CollectIcebergDeleteState(
  duckdb::IcebergMultiFileList& iceberg_list) {
  IcebergDeleteState state;
  constexpr int32_t kIcebergFilePathFieldId = 2147483546;
  const auto referenced_file = [&](const duckdb::IcebergDataFile& delete_file,
                                   bool equality) -> std::string {
    if (equality) {
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
      bump(state.per_file[FileKey(file)], seq);
      continue;
    }
    std::string partition_key;
    IcebergDeleteState::Watermarks* scope = &state.global;
    if (!delete_file.partition_info.empty()) {
      partition_key = PartitionKey(delete_file.partition_info);
      scope = &state.per_partition[partition_key];
    }
    bump(scope->any, seq);
    if (equality) {
      state.equality.push_back({seq, std::move(partition_key)});
    } else {
      bump(scope->mask_block, seq);
    }
  }
  state.list = &iceberg_list;
  return state;
}

void FillFileIdentity(duckdb::ClientContext& context,
                      const duckdb::OpenFileInfo& file,
                      search::FileManifestEntry& entry) {
  if (const auto& ext = file.extended_info) {
    const auto find = [&](const char* name) -> const duckdb::Value* {
      auto it = ext->options.find(name);
      return it != ext->options.end() && !it->second.IsNull() ? &it->second
                                                              : nullptr;
    };
    if (const auto* etag = find("etag")) {
      entry.etag = duckdb::StringValue::Get(*etag);
      if (!entry.etag.empty()) {
        return;
      }
    }
    if (const auto* mtime = find("last_modified")) {
      entry.mtime_micros = mtime->DefaultCastAs(duckdb::LogicalType::TIMESTAMP)
                             .GetValue<duckdb::timestamp_t>()
                             .value;
      return;
    }
  }
  auto& fs = duckdb::FileSystem::GetFileSystem(context);
  auto handle = fs.OpenFile(file.path, duckdb::FileFlags::FILE_FLAGS_READ);
  entry.etag = fs.GetVersionTag(*handle);
  if (entry.etag.empty()) {
    entry.mtime_micros = fs.GetLastModifiedTime(*handle).value;
  }
}

search::FileManifest CaptureManifest(duckdb::ClientContext& context,
                                     duckdb::MultiFileBindData& bind) {
  auto files = bind.file_list->GetAllFiles();
  search::FileManifest manifest;
  manifest.entries.reserve(files.size());
  auto* iceberg_list =
    dynamic_cast<duckdb::IcebergMultiFileList*>(bind.file_list.get());
  if (iceberg_list) {
    if (const auto& info = iceberg_list->GetSnapshot(); info.snapshot) {
      manifest.version = info.snapshot->snapshot_id;
    }
  }
  for (size_t i = 0; i < files.size(); ++i) {
    auto& entry = manifest.entries[i];
    entry.file_id = i;
    entry.path = files[i].path;
    if (!iceberg_list) {
      FillFileIdentity(context, files[i], entry);
    }
  }
  return manifest;
}

bool SnapshotIsAncestor(const duckdb::IcebergMultiFileList& list,
                        int64_t snapshot_id) {
  const auto& snapshots = list.GetMetadata().snapshots;
  auto snapshot = list.GetSnapshot().snapshot;
  while (snapshot) {
    if (snapshot->snapshot_id == snapshot_id) {
      return true;
    }
    if (!snapshot->has_parent_snapshot) {
      return false;
    }
    const auto it = snapshots.find(snapshot->parent_snapshot_id);
    snapshot = it == snapshots.end() ? nullptr : &it->second;
  }
  return false;
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
    if (const auto& info = src.iceberg_list->GetSnapshot(); info.snapshot) {
      src.version = info.snapshot->snapshot_id;
    }
  }
  return src;
}

namespace {

// The stored snapshot's sequence number, resolved from the CURRENT table
// metadata (the ancestry gate already guarantees the snapshot is there;
// 0 -- diff everything -- when it is not).
uint64_t SequenceNumberOf(const duckdb::IcebergMultiFileList& list,
                          int64_t snapshot_id) {
  const auto& snapshots = list.GetMetadata().snapshots;
  const auto it = snapshots.find(snapshot_id);
  return it == snapshots.end()
           ? 0
           : static_cast<uint64_t>(it->second.sequence_number);
}

}  // namespace

IcebergObserve::IcebergObserve(duckdb::IcebergMultiFileList& list,
                               const duckdb::MultiFileBindData& bind,
                               int64_t stored_version)
  : _deletes{CollectIcebergDeleteState(list)},
    _bind{&bind},
    _sequence_number{SequenceNumberOf(list, stored_version)} {}

bool IcebergObserve::TryMask(size_t listing_idx,
                             const search::FileManifestEntry& entry,
                             const search::FileManifestEntry& live) {
  const auto covering = _deletes.CoveringFor(live.path);
  if (std::max(covering.partition.mask_block, _deletes.global.mask_block) >
      _sequence_number) {
    return false;
  }
  const bool pinned_new = covering.file > _sequence_number;
  const bool eq_new = HasNewEquality(live.path);
  if (!pinned_new && !eq_new) {
    return false;
  }
  DeleteMask mask{entry.file_id};
  if (pinned_new && !ExtractMaskRows(listing_idx, mask)) {
    return false;
  }
  if (eq_new) {
    eq_covered.push_back({live, entry.file_id, listing_idx});
  }
  del_masks.push_back(std::move(mask));
  return true;
}

bool IcebergObserve::HasNewEquality(const std::string& path) const {
  std::optional<std::string> partition_key;
  return absl::c_any_of(_deletes.equality, [&](const auto& eq) {
    if (eq.seq <= _sequence_number) {
      return false;
    }
    if (eq.partition_key.empty()) {
      return true;
    }
    if (!partition_key) {
      partition_key =
        PartitionKey(_deletes.list->GetPartitionInfoForDataFile(path));
    }
    return eq.partition_key == *partition_key;
  });
}

const duckdb::vector<duckdb::MultiFileColumnDefinition>&
IcebergObserve::GlobalColumns() const {
  return GlobalScanColumns(*_bind);
}

bool IcebergObserve::ExtractMaskRows(size_t listing_idx, DeleteMask& mask) {
  EnsureDeletesProcessed();
  const auto& recorded =
    _deletes.list->GetManifestEntry(listing_idx).entry->data_file.file_path;
  auto data = _deletes.list->GetExistingPositionalDeleteData(recorded);
  if (!data) {
    return false;
  }
  switch (data->type) {
    case duckdb::IcebergDeleteType::POSITIONAL_DELETE: {
      const auto& by_seq =
        static_cast<const duckdb::IcebergPositionalDeleteData&>(*data)
          .rows_by_sequence;
      size_t dead = 0;
      for (const auto& [seq, bucket] : by_seq) {
        if (static_cast<uint64_t>(seq) > _sequence_number) {
          dead += bucket.size();
        }
      }
      if (dead == 0) {
        return false;
      }
      mask.rows.reserve(dead);
      for (const auto& [seq, bucket] : by_seq) {
        if (static_cast<uint64_t>(seq) > _sequence_number) {
          mask.rows.insert(mask.rows.end(), bucket.begin(), bucket.end());
        }
      }
    } break;
    case duckdb::IcebergDeleteType::DELETION_VECTOR: {
      // A DV replaces its predecessor wholesale, and the manifest keeps no
      // copy of what was applied: remove the WHOLE current DV. Rows the
      // index already dropped match nothing -- pure re-pay, never wrong.
      const auto& dv =
        static_cast<const duckdb::IcebergDeletionVectorData&>(*data);
      for (const auto& [high, bitmap] : dv.bitmaps) {
        if (bitmap.isEmpty()) {
          continue;
        }
        mask.dv.emplace_back(high, bitmap);
      }
      absl::c_sort(mask.dv, [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
      });
      return true;
    } break;
  }
  absl::c_sort(mask.rows);
  mask.rows.erase(std::unique(mask.rows.begin(), mask.rows.end()),
                  mask.rows.end());
  return true;
}

}  // namespace sdb::connector
