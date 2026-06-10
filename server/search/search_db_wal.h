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
#include "catalog/identifiers/object_id.h"

namespace duckdb {

class FileSystem;
class BufferedFileWriter;
class DataChunk;
class ColumnDataCollection;
class MemoryStream;

}  // namespace duckdb
namespace sdb::search {

// Self-contained, rocksdb-free write-ahead log for a database's search-backed
// tables (`StorageKind::kSearch`). One instance per database (owned by the
// search engine, keyed by db_id): a single central commit log shared by all of
// the database's search shards, so a transaction touching several search tables
// commits all-or-nothing behind one fsync. Chunk files stay per-shard.
//
// Layout under `<wal_dir>`. Names are fixed-width 16-hex, so lexicographic
// order == numeric order:
//   <016x first_tick>.swal                       central commit segments
//                                                 (append-only); the name is
//                                                 the tick of the segment's
//                                                 FIRST record.
//   chunks/<table_id>/<016x seg_id>.swchunk      per-shard bulk chunk files
//                                                 (referenced by a record).
//
// A commit appends ONE central record carrying a monotonic `tick` and a list of
// per-shard sections -- one for each search table the transaction wrote. Each
// section is a shard header plus an ordered op manifest:
//   record  = [u64 tick][u32 shard_count][ section x shard_count ]
//   section = [u64 table_id][u32 op_count][ op x op_count ]
//   op      = [u8 kind][kind body]
//     INLINE    -- small inserts: the rows are serialised into the op.
//     REFERENCE -- bulk inserts: the op lists `seg_id`s of chunk files that
//     hold
//                  the rows (streamed in parallel during Sink).
// The column layout is NOT recorded -- replay rebuilds it from the catalog
// (recovery runs after the catalog loads). Insert-only today.
// Frame: [u64 size][u64 checksum][payload], where payload begins with
// [u64 tick] at a fixed offset (readable without deserialising the rest).
//
// The public API identifies tables by catalog `ObjectId`; only the on-disk
// record + chunk path store the raw `.id()` (the wire format is a plain u64).
class SearchDbWal {
 public:
  // One streamed chunk file for a single bulk sink thread. Append()s DataChunks
  // during Sink and Finish()es (flush + fsync); the resulting seg_id goes into
  // the transaction's REFERENCE section.
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
    // explicit-PK shards) for replay PK reconstruction; buffered, no fsync.
    // zstd-1 with raw fallback.
    void Append(duckdb::DataChunk& chunk, uint64_t pk_base);

    // Flush the buffer to the OS and fsync. Call once, before the transaction's
    // central record is committed.
    void Finish();

   private:
    uint64_t _seg_id;
    std::unique_ptr<duckdb::BufferedFileWriter> _writer;
    // Reused across Append() calls (Rewind keeps the backing buffer).
    std::unique_ptr<duckdb::MemoryStream> _stream;
    // Reused zstd output buffer (grows to the high-water compressed size).
    std::vector<uint8_t> _comp;
  };

  // One inserted Sink chunk's generated-PK run: `count` rows keyed
  // [base, base+count). Recorded per Sink chunk -- NOT per inline_data Chunk:
  // ColumnDataCollection coalesces partial appends, so its Chunks() boundaries
  // don't line up with the Sink chunks the bases are keyed to. base is 0 for
  // explicit-PK.
  struct InlinePk {
    uint64_t base;
    uint64_t count;
  };

  // One op in a shard section's ordered manifest.
  struct Op {
    // INLINE only: one entry per inserted Sink chunk, in append order.
    const duckdb::ColumnDataCollection* inline_data = nullptr;
    std::span<const InlinePk> inline_pks;
    // REFERENCE
    std::span<const uint64_t> seg_ids;
  };

  // One transaction's contribution for a single search shard
  struct ShardSection {
    ObjectId table_id;
    std::span<const Op> ops;
  };

  using ReplayCallback =
    std::function<void(uint64_t tick, ObjectId table_id, uint64_t pk_base,
                       duckdb::DataChunk& chunk)>;

  using ShardExistsFn = std::function<bool(ObjectId table_id)>;
  using ShardCommittedFn = std::function<uint64_t(ObjectId table_id)>;

  // Default central-segment seal threshold (16MB as common standart like
  // postgres or duckdb)
  static constexpr uint64_t kDefaultSealThreshold = 16 * 1024 * 1024;

  SearchDbWal(duckdb::FileSystem& fs, std::filesystem::path wal_dir,
              uint64_t seal_threshold = kDefaultSealThreshold);
  ~SearchDbWal();

  SearchDbWal(const SearchDbWal&) = delete;
  SearchDbWal& operator=(const SearchDbWal&) = delete;

  uint64_t CurrentTick() const noexcept {
    return _tick.load(std::memory_order_relaxed);
  }

  void RegisterShard(ObjectId table_id, uint64_t committed_tick);
  void OnShardCommit(ObjectId table_id, uint64_t committed_tick);
  void DeregisterShard(ObjectId table_id);

  ChunkWriter NewChunkWriter(ObjectId table_id);
  uint64_t AppendCommit(std::span<const ShardSection> sections);
  uint64_t Recover(const ShardExistsFn& exists_of,
                   const ShardCommittedFn& committed_of,
                   const ReplayCallback& cb);

 private:
  duckdb::FileSystem& _fs;
  std::filesystem::path _wal_dir;
  std::filesystem::path _chunks_root;

  const uint64_t _seal_threshold;

  std::mutex _append_mu;
  std::atomic<uint64_t> _tick{0};
  std::unique_ptr<duckdb::BufferedFileWriter> _active;
  uint64_t _active_first_tick = 0;
  uint64_t _active_chunk_bytes = 0;

  std::mutex _seg_mu;
  containers::FlatHashMap<uint64_t, uint64_t> _seg_ids;

  std::mutex _sub_mu;
  containers::FlatHashMap<uint64_t, uint64_t> _committed;

  void EnsureActiveSegmentLocked(uint64_t first_tick);
  void WriteFrameLocked(const uint8_t* payload, uint64_t payload_size);
  std::filesystem::path ChunkDir(uint64_t table_id) const;
  uint64_t MinCommittedTick();
  void RunGc();
};

// Re-slice an inline collection by its recorded per-Sink-chunk `segments`,
// invoking `emit(slice, base)` once per segment with that chunk's rows + base.
void VisitInlineSegments(
  const duckdb::ColumnDataCollection& cdc,
  std::span<const SearchDbWal::InlinePk> segments,
  const std::function<void(duckdb::DataChunk&, uint64_t base)>& emit);

}  // namespace sdb::search
