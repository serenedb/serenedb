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

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <iresearch/utils/assert.hpp>
#include <utility>

#include "search/index_durability.h"
#include "search/search_table.h"

namespace sdb::search {

void WalTailKeyCollector::EndBatch(bool committed) {
  if (!committed) {
    _staged.Clear();
    return;
  }
  if (_staged.truncated) {
    // Everything the tail said before this batch is dead, and so is everything
    // the published reader holds: a tail entry always outranks the reader.
    _committed.Clear();
    _committed.truncated = true;
  }
  _committed.keys.insert(_committed.keys.end(),
                         std::make_move_iterator(_staged.keys.begin()),
                         std::make_move_iterator(_staged.keys.end()));
  _committed.segments.insert(_committed.segments.end(),
                             std::make_move_iterator(_staged.segments.begin()),
                             std::make_move_iterator(_staged.segments.end()));
  _committed.removed.insert(_committed.removed.end(), _staged.removed.begin(),
                            _staged.removed.end());
  _staged.Clear();
}

void WalTailKeyCollector::ReplayInsert(duckdb::ClientContext&,
                                       duckdb::DataChunk& chunk,
                                       duckdb::optional_idx) {
  if (_slots.empty()) {
    return;
  }
  connector::EncodeChunkKeys(chunk, _slots, _staged.keys);
}

void WalTailKeyCollector::ReplayDelete(duckdb::ClientContext&,
                                       duckdb::DataChunk& chunk) {
  SDB_ASSERT(chunk.ColumnCount() == 1);
  for (duckdb::idx_t row = 0; row != chunk.size(); ++row) {
    const auto rowid = chunk.GetValue(0, row).GetValue<int64_t>();
    AppendRowRanges(_staged.removed, std::span{&rowid, 1});
  }
}

void WalTailKeyCollector::ReplayDeleteRanges(
  duckdb::ClientContext&, const duckdb::vector<duckdb::row_t>& starts,
  const duckdb::vector<duckdb::idx_t>& counts) {
  SDB_ASSERT(starts.size() == counts.size());
  for (size_t i = 0; i != starts.size(); ++i) {
    _staged.removed.emplace_back(starts[i],
                                 starts[i] + static_cast<int64_t>(counts[i]));
  }
}

void WalTailKeyCollector::ReplayTruncate(duckdb::ClientContext&) {
  // Within the batch too: rows this transaction wrote before its TRUNCATE are
  // as dead as the ones before it.
  _staged.Clear();
  _staged.truncated = true;
}

void WalTailKeyCollector::ReplayAdoptSegments(
  duckdb::ClientContext&, duckdb::vector<std::string> segments) {
  _staged.segments.insert(_staged.segments.end(),
                          std::make_move_iterator(segments.begin()),
                          std::make_move_iterator(segments.end()));
}

duckdb::shared_ptr<duckdb::AttachedDatabase> ResolveTailDatabase(
  const SearchTable& shard) {
  return AttachedDatabaseById(shard.GetDatabaseId());
}

namespace {

// One generation's file, already open, with the range of it the tail covers.
struct OpenGeneration {
  uint64_t generation = 0;
  duckdb::unique_ptr<duckdb::FileHandle> handle;
  duckdb::idx_t start = 0;
  duckdb::idx_t stop = 0;
};

struct TailFiles {
  std::vector<OpenGeneration> files;
  irs::SourcePosition begin;
  std::string error;
};

TailFiles OpenTailFiles(duckdb::AttachedDatabase& store,
                        const irs::SourcePosition& from);

}  // namespace

WalTail ReadUnpublishedTail(
  SearchTable& shard, duckdb::AttachedDatabase& store,
  duckdb::ClientContext& context, const irs::SourcePosition& from,
  std::span<const connector::primary_key::KeySlot> slots,
  duckdb::optional_ptr<duckdb::unique_lock<duckdb::mutex>> commit_lock) {
  WalTail tail;
  tail.boundary = from;

  if (!store.HasStorageManager()) {
    tail.error = "database has no storage";
    return tail;
  }
  auto& storage = store.GetStorageManager();

  // Neither of these can change under a live database, so they are answered
  // before reaching for the lock.
  //
  // `GetWALSize()` is only a byte offset into the WAL file because
  // `FlushMarker` sets it from `writer->GetFileSize()` after pushing the batch
  // to the OS. For an in-memory database and under recovery_mode
  // 'NO_WAL_WRITES' nothing sets it from a file at all: every commit adds its
  // undo buffer's *estimated* size instead, so the number is a changeset
  // volume and seeking to it would be meaningless. The condition is the one
  // DuckDB adds the estimate under.
  if (storage.InMemory() ||
      store.GetRecoveryMode() == duckdb::RecoveryMode::NO_WAL_WRITES) {
    tail.error = "the WAL size of this database is an estimate, not an offset";
    return tail;
  }
  if (store.IsReadOnly()) {
    tail.error = "a read-only database keeps no WAL to read";
    return tail;
  }

  TailFiles opened;
  {
    duckdb::unique_lock<duckdb::mutex> owned;
    if (!commit_lock) {
      owned = storage.GetCommitLock();
    }
    opened = OpenTailFiles(store, from);
  }
  if (!opened.error.empty()) {
    tail.error = std::move(opened.error);
    return tail;
  }
  tail.boundary = opened.begin;

  WalTailKeyCollector collector{shard.GetTableId(), slots};
  for (auto& file : opened.files) {
    const auto read = duckdb::ReadWALTail(
      store, context, std::move(file.handle), file.start, file.stop, collector);
    if (!read.error.empty()) {
      tail.error = read.error;
      return tail;
    }
    tail.boundary = {.generation = file.generation, .offset = read.boundary};
    if (read.stopped_undecided) {
      tail.stopped_undecided = true;
      break;
    }
  }

  tail.keys = collector.Keys();
  for (const auto& name : tail.keys.segments) {
    auto reader = shard.OpenNamedSegment(name);
    if (!reader) {
      tail.lost = true;
      continue;
    }
    tail.segments.push_back(std::move(reader));
  }
  return tail;
}

namespace {

TailFiles OpenTailFiles(duckdb::AttachedDatabase& store,
                        const irs::SourcePosition& from) {
  TailFiles opened;
  opened.begin = from;
  auto& storage = store.GetStorageManager();

  // A checkpoint does leave the database with no WAL for a moment, twice --
  // `WALStartCheckpoint` closes the main WAL before instantiating the
  // checkpoint one, `WALFinishCheckpoint` closes that one before moving it
  // over the main path -- but both windows are inside the lock held here, so
  // what this sees is an answer and not a glimpse of one.
  auto wal = storage.GetWAL();
  if (!wal) {
    opened.error = "database has no WAL";
    return opened;
  }

  // While the active WAL is the checkpoint WAL, one generation is named
  // neither way: `WALStartCheckpoint` creates `.wal.checkpoint` already
  // carrying the next iteration, and the generation it supersedes stays at the
  // main path until `WALFinishCheckpoint` renames it to `.wal.retained.<n>`.
  //
  // A checkpoint that FAILED after starting leaves exactly the same layout,
  // for good: commits keep going into the checkpoint WAL and every later
  // checkpoint refuses until a restart. So there is nothing to gain by telling
  // the two apart -- and no way to, without the checkpoint lock the in-lock
  // pass must not take. Mapping the name is what both need, and a read during
  // an ordinary checkpoint has to work anyway: checkpoints are frequent, and
  // failing every primary-key commit that overlaps one is not an option.
  const bool checkpointing = wal->GetPath() != storage.GetWALPath();

  // The live WAL's generation and its size move together, at a checkpoint and
  // nowhere else. Both move under the lock held here, so they can be read one
  // after the other and still describe the same file.
  const uint64_t current = wal->GetCheckpointIteration();
  const duckdb::idx_t end = storage.GetWALSize();

  if (from.generation > current ||
      (from.generation == current && from.offset >= end)) {
    return opened;  // nothing committed since
  }

  // `from` can name a generation retention has already released, and on a
  // healthy database: once the index is caught up it asks for no WAL at all,
  // so a checkpoint frees everything, and the published position it was caught
  // up at stays where it is until the next publish. A commit landing after
  // that leaves a tail to read whose start is older than anything kept.
  //
  // Reading from the oldest kept generation instead is not a loss. A
  // generation is only released once nothing unpublished needed it, the
  // published position never goes backwards, and the reader this position came
  // with was taken with it -- so what the released generations held is in that
  // reader, which the probe consults anyway.
  const irs::SourcePosition begin =
    from.generation < storage.GetOldestWALGeneration()
      ? irs::SourcePosition{.generation = storage.GetOldestWALGeneration(),
                            .offset = 0}
      : from;
  opened.begin = begin;

  auto& fs = duckdb::FileSystem::Get(store);

  // A tail that straddles a checkpoint spans several files: the retained WAL
  // it starts in, read from `from.offset`, then whole retained WALs, then the
  // live one up to `end`.
  for (auto generation = begin.generation; generation <= current;
       ++generation) {
    const bool live = generation == current;
    // The live one by the WAL's own path, never by the main path they only
    // happen to share when no checkpoint is running. The generation under it
    // is at the main path while one is, because the rename has not happened.
    std::string path;
    if (live) {
      path = wal->GetPath();
    } else if (checkpointing && generation + 1 == current) {
      path = storage.GetWALPath();
    } else {
      path = storage.GetRetainedWALPath(generation);
    }

    // The live generation is bounded by the flushed size and not by the file,
    // which can hold a batch still being written past it -- and, right after a
    // checkpoint that nothing committed during, is not there at all, because
    // the new WAL only creates its file on first write. Asking its size first
    // would turn that into a failure to open.
    const auto start = generation == begin.generation ? begin.offset : 0;
    if (live && start >= end) {
      continue;
    }

    auto handle = fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ);
    if (!handle) {
      opened.error = "WAL " + path + " is missing";
      return opened;
    }
    const auto stop =
      live ? end : static_cast<duckdb::idx_t>(handle->GetFileSize());
    if (start >= stop) {
      continue;
    }
    opened.files.push_back({.generation = generation,
                            .handle = std::move(handle),
                            .start = start,
                            .stop = stop});
  }
  return opened;
}

}  // namespace
}  // namespace sdb::search
