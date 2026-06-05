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
#include <vector>

#include "basics/containers/flat_hash_map.h"

namespace duckdb {

class FileSystem;
class BufferedFileWriter;
class DataChunk;
class ColumnDataCollection;
class MemoryStream;

}  // namespace duckdb
namespace sdb::search {

// Self-contained, rocksdb-free write-ahead log for a database's search-backed
// tables (`StorageKind::kSearch`). See WAL_DESIGN.md for the full design; this
// is the standalone primitive, decoupled from the catalog and from any DuckDB
// DatabaseInstance -- it only needs a duckdb::FileSystem.
//
// **One instance per database** (owned by the search engine, keyed by db_id): a
// single central commit log shared by all of the database's search shards, so a
// transaction touching several search tables commits all-or-nothing behind one
// fsync (multi-shard atomicity, WAL_DESIGN.md §2/§9). Chunk files stay
// per-shard.
//
// Layout under `<wal_dir>` (= GetWalPath(db_id)). Names are PostgreSQL-style
// fixed-width 16-hex, so lexicographic order == numeric order:
//   <016x first_tick>.swal                       central commit segments
//                                                 (append-only); the name is
//                                                 the tick of the segment's
//                                                 FIRST record (the PG-LSN
//                                                 analog).
//   chunks/<schema_id>/<table_id>/<016x seg_id>.swchunk   per-shard bulk chunk
//                                                 files (referenced by a
//                                                 record).
//
// A commit appends ONE central record carrying a monotonic `tick` and a list of
// per-shard sections -- one for each search table the transaction wrote:
//   record = [u64 tick][u32 shard_count][ section x shard_count ]
//   section = [u64 schema_id][u64 table_id][u8 kind][body]
//     INLINE    -- small inserts: the rows are serialised into the section.
//     REFERENCE -- bulk inserts: the section lists `seg_id`s of chunk files
//     that
//                  hold the rows (streamed in parallel during Sink).
// Frame (reused from DuckDB's WAL): [u64 size][u64 checksum][payload], where
// payload begins with [u64 tick] at a fixed offset (readable without
// deserialising the rest -- recovery skip + future PITR bound).
//
// Identifiers (schema_id, table_id, column ids) are opaque uint64_t here (the
// operator/recovery maps catalog ObjectIds <-> uint64_t via `.id()`).
class SearchDbWal {
 public:
  // One streamed chunk file for a single bulk sink thread. The thread Append()s
  // DataChunks during Sink and Finish()es (flush + fsync) at Combine; the
  // resulting seg_id goes into the transaction's REFERENCE section.
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

    // Serialise + append one chunk with its generated-PK base `pk_base` (0 for
    // explicit-PK shards) for replay PK reconstruction (WAL_DESIGN.md §5.6);
    // buffered, no fsync. zstd-1 with raw fallback.
    void Append(duckdb::DataChunk& chunk, uint64_t pk_base);

    // Flush the buffer to the OS and fsync. Call once, at Combine, before the
    // transaction's central record is committed (WAL_DESIGN.md §9).
    void Finish();

   private:
    uint64_t _seg_id;
    std::unique_ptr<duckdb::BufferedFileWriter> _writer;
    // Reused across Append() calls (Rewind keeps the backing buffer).
    std::unique_ptr<duckdb::MemoryStream> _stream;
    // Reused zstd output buffer (grows to the high-water compressed size).
    std::vector<uint8_t> _comp;
  };

  // Opaque column-id list carried in a section (catalog::Column::Id::id()).
  using ColumnIds = std::span<const uint64_t>;

  // One transaction's contribution for a single search shard. Exactly one of
  // `inline_data` (small INSERT, rows serialised into the section) or `seg_ids`
  // (bulk INSERT, references already-fsynced chunk files) is populated.
  struct ShardSection {
    uint64_t
      schema_id;  // locates chunks/<schema_id>/<table_id>/ + recovery dispatch
    uint64_t table_id;
    ColumnIds column_ids;
    const duckdb::ColumnDataCollection* inline_data = nullptr;  // INLINE
    std::span<const uint64_t> seg_ids;                          // REFERENCE
    // INLINE only: per-chunk generated-PK base, aligned to inline_data's chunks
    // (0 for explicit-PK). Recorded for replay PK reconstruction (§5.6).
    std::span<const uint64_t> inline_pk_bases;
  };

  // Per replayed chunk during Recover(): the record's tick, the section's
  // (schema_id, table_id), the chunk's generated-PK base (0 for explicit-PK,
  // §5.6), the column ids, and one DataChunk to re-feed into that shard's
  // iresearch writer.
  using ReplayCallback = std::function<void(
    uint64_t tick, uint64_t schema_id, uint64_t table_id, uint64_t pk_base,
    ColumnIds column_ids, duckdb::DataChunk& chunk)>;

  // Recovery skip hooks: a section is replayed only if its destination shard
  // still exists in the catalog AND has not already published this tick.
  using ShardExistsFn =
    std::function<bool(uint64_t schema_id, uint64_t table_id)>;
  using ShardCommittedFn = std::function<uint64_t(uint64_t table_id)>;

  // Default central-segment seal threshold (PostgreSQL's WAL segment size, ==
  // DuckDB's checkpoint_wal_size): roll the active segment once
  // `active_segment_bytes + outstanding_chunk_bytes` exceeds it. Chunk bytes
  // are counted so a bulk insert (tiny central record, GB of chunks) rolls
  // promptly (WAL_DESIGN.md §10.2).
  static constexpr uint64_t kDefaultSealThreshold = 16 * 1024 * 1024;

  // `fs` must outlive this object. `wal_dir` is the database's GetWalPath; it
  // (and the per-shard chunks/ subtrees) is created on demand. `seal_threshold`
  // is the size cap above; tests pass a small value to force rolls. The
  // constructor seeds the engine-global tick from the max on-disk record tick.
  SearchDbWal(duckdb::FileSystem& fs, std::filesystem::path wal_dir,
              uint64_t seal_threshold = kDefaultSealThreshold);
  ~SearchDbWal();

  SearchDbWal(const SearchDbWal&) = delete;
  SearchDbWal& operator=(const SearchDbWal&) = delete;

  // --- tick ---------------------------------------------------------------

  // Lock-free read of the current engine-global tick. A shard captures this
  // BEFORE its RefreshCommit, then OnShardCommit(table_id, captured) afterwards
  // (WAL_DESIGN.md §10.3). The next AppendCommit assigns strictly greater
  // ticks.
  uint64_t CurrentTick() const noexcept {
    return _tick.load(std::memory_order_relaxed);
  }

  // --- min-tick flush-subscription (GC) -----------------------------------

  // Register a shard on open with its last durable iresearch tick, and bump the
  // engine tick to at least it (so the tick line continues past every shard's
  // committed tick -- iresearch monotonicity). Idempotent.
  void RegisterShard(uint64_t table_id, uint64_t committed_tick);

  // A shard published up to `committed_tick` (its RefreshCommit, e.g. at
  // VACUUM): advance its subscription entry and run a GC sweep. Bumps the
  // engine tick too.
  void OnShardCommit(uint64_t table_id, uint64_t committed_tick);

  // A shard is being dropped: remove its subscription entry so its frozen tick
  // can't pin GC (WAL_DESIGN.md §10.3). Its still-unconsumed central sections
  // become orphans (skipped on recovery via the catalog, GC'd with their
  // segment). Triggers a GC sweep.
  void DeregisterShard(uint64_t table_id);

  // --- commit -------------------------------------------------------------

  // Allocate a unique seg_id for (schema_id, table_id) and open its chunk-file
  // writer at chunks/<schema_id>/<table_id>/<016x seg_id>.swchunk.
  ChunkWriter NewChunkWriter(uint64_t schema_id, uint64_t table_id);

  // Commit (under the central append mutex): assign the next tick, append ONE
  // record covering all `sections`, and fsync. Returns the assigned tick (pass
  // it to each section's iresearch_trx.Commit(tick)). tick assignment + append
  // + fsync are one critical section so file order == tick order.
  uint64_t AppendCommit(std::span<const ShardSection> sections);

  // --- recovery (WAL_DESIGN.md §11) ---------------------------------------

  // Single engine-wide pass over the central log in tick order. For each record
  // and each of its shard sections: skip if the shard no longer exists
  // (`exists_of`) or has already published the tick (`tick <= committed_of`),
  // else replay its rows through `cb`. Orphan chunk files (unreferenced by any
  // surviving section) are swept. Seeds the engine tick + per-shard seg-id
  // counters from durable state. Returns the highest record tick seen.
  uint64_t Recover(const ShardExistsFn& exists_of,
                   const ShardCommittedFn& committed_of,
                   const ReplayCallback& cb);

 private:
  duckdb::FileSystem& _fs;
  std::filesystem::path _wal_dir;
  std::filesystem::path _chunks_root;  // _wal_dir / "chunks"

  const uint64_t _seal_threshold;

  std::mutex
    _append_mu;  // serialises tick assignment + the active-segment append
  std::atomic<uint64_t> _tick{
    0};  // engine-global; assigned under _append_mu, read lock-free
  // Active central segment writer; null until the first commit after open/roll.
  // Named by the tick of its first record.
  std::unique_ptr<duckdb::BufferedFileWriter> _active;
  uint64_t _active_first_tick =
    0;  // 0 when no active segment (GC reads under _append_mu)

  // Per-(table) chunk seg-id counter; lazily seeded by scanning the table's
  // chunk dir on first use. Guarded by _seg_mu.
  std::mutex _seg_mu;
  containers::FlatHashMap<uint64_t /*table_id*/, uint64_t> _seg_ids;

  // min-tick flush-subscription: each shard's last durable tick. GC reclaims
  // sealed segments whose whole range <= min over this map. Guarded by _sub_mu.
  std::mutex _sub_mu;
  containers::FlatHashMap<uint64_t /*table_id*/, uint64_t> _committed;

  void EnsureActiveSegmentLocked(uint64_t first_tick);
  void WriteFrameLocked(const uint8_t* payload, uint64_t payload_size);
  std::filesystem::path ChunkDir(uint64_t schema_id, uint64_t table_id) const;
  uint64_t MinCommittedTick();  // min over _committed (0 if empty)
  void RunGc();                 // delete sealed segments whose range <= min
};

}  // namespace sdb::search
