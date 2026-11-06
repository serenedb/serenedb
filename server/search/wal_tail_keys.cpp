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

#include "search/wal_tail_keys.h"

#include <algorithm>
#include <duckdb/common/file_system.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

namespace sdb::search {
namespace {

// Opens a WAL file for reading, or nothing where it is already gone: a
// generation the span covers but no store needs any more was released while
// this read was being set up, and what it held is published by now.
duckdb::unique_ptr<duckdb::FileHandle> OpenWal(duckdb::FileSystem& fs,
                                               const std::string& path) {
  return fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ |
                             duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
}

}  // namespace

std::vector<WalTailFile> OpenTailFiles(duckdb::AttachedDatabase& db,
                                       const WalTailCursor& from) {
  auto& storage = db.GetStorageManager();
  auto wal = storage.GetWAL();
  if (!wal) {
    return {};
  }
  auto& fs = duckdb::FileSystem::GetFileSystem(db.GetDatabase());
  const auto live = static_cast<uint64_t>(wal->GetCheckpointIteration());
  std::vector<WalTailFile> files;

  // Every generation the span covers but the live one, oldest first. Their
  // files are whole: a retired WAL is never appended to again.
  for (auto generation =
         std::max(from.generation,
                  static_cast<uint64_t>(storage.GetOldestWALGeneration()));
       generation < live; ++generation) {
    auto handle = OpenWal(fs, storage.GetRetainedWALPath(generation));
    if (!handle) {
      continue;
    }
    const auto size = static_cast<uint64_t>(handle->GetFileSize());
    files.push_back({.generation = generation,
                     .handle = std::move(handle),
                     .start = generation == from.generation ? from.offset : 0,
                     .end = size});
  }

  if (auto handle = OpenWal(fs, wal->GetPath())) {
    files.push_back({.generation = live,
                     .handle = std::move(handle),
                     .start = from.generation == live ? from.offset : 0,
                     // A read that ran past the size this lock fixes would
                     // reach a commit that has not decided yet.
                     .end = static_cast<uint64_t>(storage.GetWALSize())});
  }
  return files;
}

WalTailCursor ReadTailKeys(duckdb::AttachedDatabase& db,
                           duckdb::ClientContext& context,
                           const WalTailCursor& from,
                           std::span<WalTailFile> files,
                           WalTailKeyCollector& collector) {
  auto cursor = from;
  for (auto& file : files) {
    if (!file.handle) {
      continue;
    }
    auto read = duckdb::ReadWALTail(db, context, std::move(file.handle),
                                    file.start, file.end, collector);
    if (!read.error.empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                      ERR_MSG("cannot read the WAL of generation ",
                              file.generation, ": ", read.error));
    }
    // A retired WAL decides every batch it holds, so a span moving on to the
    // next generation resumes at its head; only the live WAL can end on a
    // batch that is still being decided.
    cursor = {.generation = file.generation, .offset = read.boundary};
  }
  return cursor;
}

void WalTailKeyCollector::EndBatch(bool committed) {
  if (!committed) {
    _staged.Clear();
    return;
  }
  if (_staged.truncated) {
    // Everything the tail said before this batch is dead, and so is everything
    // the published reader holds; the barrier travels with the result so a
    // probe can drop that tier instead of consulting it.
    _committed.Clear();
    _committed.truncated = true;
  }
  auto& keys = _committed.keys;
  keys.insert(keys.end(), std::make_move_iterator(_staged.keys.begin()),
              std::make_move_iterator(_staged.keys.end()));
  auto& segments = _committed.segments;
  segments.insert(segments.end(),
                  std::make_move_iterator(_staged.segments.begin()),
                  std::make_move_iterator(_staged.segments.end()));
  _committed.removals.Merge(std::move(_staged.removals));
  _staged.Clear();
}

void WalTailKeyCollector::ReplayInsert(duckdb::ClientContext&,
                                       duckdb::DataChunk& chunk) {
  if (_slots.empty()) {
    return;
  }
  connector::EncodeChunkKeys(chunk, _slots, _staged.keys);
}

void WalTailKeyCollector::ReplayTruncate() {
  // Within a batch a TRUNCATE precedes everything else the record holds, so
  // what it kills is what earlier batches staged, not this one's rows.
  _staged.keys.clear();
  _staged.segments.clear();
  _staged.removals.Clear();
  _staged.truncated = true;
}

void WalTailKeyCollector::ReplayAdoptSegments(
  duckdb::vector<std::string> segments) {
  _staged.segments.insert(_staged.segments.end(),
                          std::make_move_iterator(segments.begin()),
                          std::make_move_iterator(segments.end()));
}

void WalTailKeyCollector::ReplayDeleteRowPositions(
  std::shared_ptr<const duckdb::data_t> entry,
  const duckdb::vector<duckdb::WALFrozenRowPositionGroup>& groups) {
  _staged.removals.AddPositions(std::move(entry), groups);
}

void WalTailKeyCollector::ReplayRowPositionMaps(
  std::shared_ptr<const duckdb::data_t> entry, uint64_t source,
  const duckdb::vector<duckdb::WALFrozenRowPositionMap>& maps) {
  _staged.removals.AddMaps(std::move(entry), source, maps);
}

}  // namespace sdb::search
