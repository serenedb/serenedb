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

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <span>

namespace duckdb {
class FileSystem;
class BufferedFileWriter;
class DataChunk;
class ColumnDataCollection;
class MemoryStream;
}  // namespace duckdb

namespace sdb::search {

// Self-contained, per-shard write-ahead log for a search-backed table. See
// WAL_DESIGN.md for the full design; this is the standalone primitive
// (`SearchShardWal`), decoupled from the catalog and from any DuckDB
// DatabaseInstance -- it only needs a duckdb::FileSystem.
//
// Layout under `<wal_dir>` (= GetWalPath(db, schema, table)). Names are
// PostgreSQL-style fixed-width 16-hex, so lexicographic order == numeric order:
//   <016x first_tick>.swal        central commit segments (append-only); the name
//                                 is the tick of the segment's FIRST record (the
//                                 PG-LSN analog), so first ticks are unique and
//                                 monotonic across segments.
//   chunks/<016x seg_id>.swchunk  per-bulk-thread chunk files (referenced by a record)
//
// A commit appends ONE central record carrying a monotonic `tick`:
//   * INLINE    -- small inserts: the rows are serialised into the record.
//   * REFERENCE -- bulk inserts: the record lists `seg_id`s of chunk files that
//                  hold the rows (streamed in parallel during Sink).
// Frame (reused from DuckDB's WAL): [u64 size][u64 checksum][payload], where
// payload begins with [u64 tick] at a fixed offset (readable without
// deserialising the rest -- recovery skip + future PITR bound).
//
// Identifiers are opaque uint64_t here (the operator/recovery maps
// catalog::Column::Id <-> uint64_t).
class SearchShardWal {
 public:
  // One streamed chunk file for a single bulk sink thread. The thread Append()s
  // DataChunks during Sink and Finish()es (flush + fsync) at Combine; the
  // resulting seg_id goes into the transaction's REFERENCE commit record.
  class ChunkWriter {
   public:
    ChunkWriter(uint64_t seg_id,
                std::unique_ptr<duckdb::BufferedFileWriter> writer);
    ChunkWriter(ChunkWriter&&) noexcept;
    ChunkWriter& operator=(ChunkWriter&&) noexcept;
    ChunkWriter(const ChunkWriter&) = delete;
    ChunkWriter& operator=(const ChunkWriter&) = delete;
    ~ChunkWriter();

    uint64_t SegId() const noexcept { return _seg_id; }

    // Serialise + append one chunk (buffered; no fsync). Streaming: heap stays
    // bounded to ~one chunk + the writer's 4 KB buffer.
    void Append(duckdb::DataChunk& chunk);

    // Flush the buffer to the OS and fsync. Call once, at Combine, before the
    // transaction's central record is committed (WAL_DESIGN.md §9).
    void Finish();

   private:
    uint64_t _seg_id;
    std::unique_ptr<duckdb::BufferedFileWriter> _writer;
    // Reused across Append() calls: Rewind() resets the position but keeps the
    // backing buffer, so only the first chunk pays the grow-and-copy; same-width
    // chunks then serialise in place -- no per-chunk allocation (WAL_DESIGN.md
    // §13).
    std::unique_ptr<duckdb::MemoryStream> _stream;
  };

  // Opaque column-id list carried in a record (catalog::Column::Id::id()).
  using ColumnIds = std::span<const uint64_t>;

  // Inline commit (small insert): `data`'s rows are serialised into the record.
  struct InlineCommit {
    ColumnIds column_ids;
    const duckdb::ColumnDataCollection& data;
  };
  // Reference commit (bulk insert): the record points at already-fsynced chunk
  // files (one per sink thread).
  struct ReferenceCommit {
    ColumnIds column_ids;
    std::span<const uint64_t> seg_ids;
  };

  // Per replayed chunk during Recover(): the record's tick + column ids and one
  // DataChunk to re-feed into iresearch. INLINE records yield their CDC's
  // chunks; REFERENCE records yield each referenced chunk file's chunks.
  using ReplayCallback = std::function<void(
    uint64_t tick, ColumnIds column_ids, duckdb::DataChunk& chunk)>;

  // `fs` must outlive this object (e.g. a process-wide LocalFileSystem, or
  // duckdb::FileSystem::CreateLocal() owned by the caller). `wal_dir` is the
  // shard's GetWalPath; it (and chunks/) is created on demand.
  SearchShardWal(duckdb::FileSystem& fs, std::filesystem::path wal_dir);
  ~SearchShardWal();

  SearchShardWal(const SearchShardWal&) = delete;
  SearchShardWal& operator=(const SearchShardWal&) = delete;

  // Recovery (shard open): replay every central record with tick >
  // `committed_tick` through `cb` (INLINE rows + REFERENCE chunk-file rows),
  // then seed the tick / seg-id / segment-seq counters from durable state
  // (WAL_DESIGN.md §11). Idempotent records with tick <= committed_tick are
  // skipped without deserialising. Must run before any NewChunkWriter /
  // AppendCommit. Returns the highest tick seen (0 if none).
  uint64_t Recover(uint64_t committed_tick, const ReplayCallback& cb);

  // Bulk sink thread: allocate a unique seg_id and open its chunk file.
  ChunkWriter NewChunkWriter();

  // Commit (under the per-shard append mutex): assign the next tick, append the
  // record to the active central segment, and fsync. Returns the assigned tick
  // (pass it to iresearch_trx.Commit(tick)). tick assignment + append + fsync
  // are one critical section so file order == tick order.
  uint64_t AppendCommit(const InlineCommit& rec);
  uint64_t AppendCommit(const ReferenceCommit& rec);

  // After a background RefreshCommit advanced the durable iresearch tick to
  // `committed_tick`: seal the active central segment, start a new one, and
  // delete fully-consumed segments + their referenced chunk files
  // (WAL_DESIGN.md §10).
  void OnRefreshCommit(uint64_t committed_tick);

 private:
  duckdb::FileSystem& _fs;
  std::filesystem::path _wal_dir;
  std::filesystem::path _chunks_dir;

  std::mutex _append_mu;        // guards _tick + the active-segment append
  uint64_t _tick = 0;           // last assigned tick; ++ under _append_mu
  std::atomic<uint64_t> _seg_id{0};  // last allocated chunk seg_id (uniqueness)
  // Active central segment writer; null until the first commit after open/roll.
  // Its file is named by the tick of its first record (no separate seq counter).
  std::unique_ptr<duckdb::BufferedFileWriter> _active;

  // Open `_active` named <016x first_tick>.swal if not already open.
  void EnsureActiveSegmentLocked(uint64_t first_tick);
  // Frame `payload` as [u64 size][u64 checksum][payload] (payload starts with
  // the tick), append to the active segment, and fsync (the commit point).
  void WriteFrameLocked(const uint8_t* payload, uint64_t payload_size);
};

}  // namespace sdb::search
