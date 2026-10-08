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

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/serializer.hpp>

#include "core/deletes/iceberg_deletion_vector.hpp"
#include "core/deletes/iceberg_positional_delete.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"

namespace sdb::search {

void FileManifest::Write(duckdb::BinarySerializer& out) const {
  SDB_IF_FAILURE("manifest_version_only") {
    irs::utils::WriteTuple(out, FileManifest{.version = version});
    return;
  }
  irs::utils::WriteTuple(out, *this);
}

std::shared_ptr<const FileManifest> FileManifest::Read(
  duckdb::BinaryDeserializer& in) {
  auto manifest = std::make_shared<FileManifest>();
  irs::utils::ReadTuple(in, *manifest);
  return manifest;
}

}  // namespace sdb::search
namespace sdb::connector {
namespace {

const duckdb::vector<duckdb::MultiFileColumnDefinition>& GlobalScanColumns(
  const duckdb::MultiFileBindData& bind) {
  return bind.reader_bind.schema.empty() ? bind.columns
                                         : bind.reader_bind.schema;
}

}  // namespace

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

duckdb::vector<duckdb::OpenFileInfo> ListSourceFiles(
  const duckdb::MultiFileList& list) {
  // Files() drives the lazy expansion; GetAllFiles returns only the
  // already-expanded prefix (a fresh iceberg bind stops at two files).
  duckdb::vector<duckdb::OpenFileInfo> files;
  for (const auto& file : list.Files()) {
    files.push_back(file);
  }
  return files;
}

search::FileManifest CaptureManifest(duckdb::ClientContext& context,
                                     duckdb::MultiFileBindData& bind) {
  auto files = ListSourceFiles(*bind.file_list);
  search::FileManifest manifest;
  manifest.entries.reserve(files.size());
  auto* iceberg_list =
    dynamic_cast<duckdb::IcebergMultiFileList*>(bind.file_list.get());
  if (iceberg_list) {
    if (const auto& info = iceberg_list->GetScanPlanner().GetSnapshot();
        info.snapshot) {
      manifest.version = info.snapshot->snapshot_id.value_or(0);
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
  const auto& planner = list.GetScanPlanner();
  const auto& snapshots = planner.GetMetadata().snapshots;
  auto snapshot = planner.GetSnapshot().snapshot;
  while (snapshot) {
    if (snapshot->snapshot_id == snapshot_id) {
      return true;
    }
    if (!snapshot->parent_snapshot_id) {
      return false;
    }
    const auto it = snapshots.find(*snapshot->parent_snapshot_id);
    snapshot = it == snapshots.end() ? nullptr : &it->second;
  }
  return false;
}

namespace {

// The stored snapshot's sequence number, resolved from the CURRENT table
// metadata (the ancestry gate already guarantees the snapshot is there;
// 0 -- diff everything -- when it is not).
uint64_t SequenceNumberOf(const duckdb::IcebergMultiFileList& list,
                          int64_t snapshot_id) {
  const auto& snapshots = list.GetScanPlanner().GetMetadata().snapshots;
  const auto it = snapshots.find(snapshot_id);
  return it == snapshots.end()
           ? 0
           : static_cast<uint64_t>(it->second.sequence_number.value_or(0));
}

}  // namespace

IcebergObserve::IcebergObserve(duckdb::IcebergMultiFileList& list,
                               const duckdb::MultiFileBindData& bind,
                               int64_t stored_version)
  : _list{list},
    _bind{&bind},
    _sequence_number{SequenceNumberOf(list, stored_version)} {}

namespace {

bool ExtractMaskRows(duckdb::IcebergMultiFileList& list,
                     const duckdb::IcebergFileScanTask& task,
                     IcebergObserve::DeleteMask& mask) {
  list.ProcessDeletes(task);
  auto data = list.GetExistingPositionalDeleteData(task.original_file_path);
  if (!data) {
    return true;
  }
  switch (data->type) {
    case duckdb::IcebergDeleteType::POSITIONAL_DELETE: {
      // Like the DV arm: remove the WHOLE current row set. Rows the index
      // already dropped match nothing -- pure re-pay, never wrong.
      const auto* rows =
        static_cast<const duckdb::IcebergPositionalDeleteData&>(*data)
          .invalid_rows.get();
      const auto count = roaring::api::roaring64_bitmap_get_cardinality(rows);
      mask.rows.resize(count);
      roaring::api::roaring64_bitmap_to_uint64_array(
        rows, reinterpret_cast<uint64_t*>(mask.rows.data()));
      return true;
    }
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
    }
  }
  return false;
}

}  // namespace

bool IcebergObserve::Same(const search::FileManifestEntry&,
                          const search::FileManifestEntry& live) const {
  const auto task = _list.GetScanPlanner().GetScanTask(live.file_id);
  return task && absl::c_none_of(task->delete_files, [&](const auto& file) {
           return IsNew(file.sequence_number);
         });
}

bool IcebergObserve::TryMask(size_t listing_idx,
                             const search::FileManifestEntry& entry,
                             const search::FileManifestEntry& live) {
  const auto task = _list.GetScanPlanner().GetScanTask(listing_idx);
  if (!task) {
    return false;
  }
  bool positional_new = false;
  bool equality_new = false;
  for (const auto& file : task->delete_files) {
    if (!IsNew(file.sequence_number)) {
      continue;
    }
    if (file.content ==
        duckdb::IcebergManifestEntryContentType::EQUALITY_DELETES) {
      equality_new = true;
    } else {
      positional_new = true;
    }
  }
  DeleteMask mask{.file_id = entry.file_id};
  if (positional_new && !ExtractMaskRows(_list, *task, mask)) {
    return false;
  }
  if (equality_new) {
    eq_covered.push_back({live, entry.file_id, listing_idx});
  }
  del_masks.push_back(std::move(mask));
  return true;
}

const duckdb::vector<duckdb::MultiFileColumnDefinition>&
IcebergObserve::GlobalColumns() const {
  return GlobalScanColumns(*_bind);
}

}  // namespace sdb::connector
