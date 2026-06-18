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

#include <absl/functional/any_invocable.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/zstd_context.hpp"
#include "catalog/identifiers/object_id.h"

namespace duckdb {

class FileSystem;
class BufferedFileWriter;
class DataChunk;
class ColumnDataCollection;
class MemoryStream;

}  // namespace duckdb
namespace sdb::search {

class SearchDbWal {
 public:
  class PendingChunk {
   public:
    PendingChunk() = default;
    PendingChunk(uint64_t seg_id, std::filesystem::path path);
    PendingChunk(PendingChunk&&) noexcept;
    PendingChunk& operator=(PendingChunk&&) noexcept;
    PendingChunk(const PendingChunk&) = delete;
    PendingChunk& operator=(const PendingChunk&) = delete;
    ~PendingChunk();

    uint64_t SegId() const noexcept { return _seg_id; }

    void MarkCommitted() noexcept { _committed = true; }

   private:
    void ReclaimIfUncommitted() noexcept;

    uint64_t _seg_id = 0;
    std::filesystem::path _path;
    bool _committed = false;
  };

  class ChunkWriter {
   public:
    ChunkWriter(PendingChunk pending,
                std::unique_ptr<duckdb::BufferedFileWriter> writer);
    ChunkWriter(ChunkWriter&&) noexcept;
    ChunkWriter& operator=(ChunkWriter&&) noexcept;
    ChunkWriter(const ChunkWriter&) = delete;
    ChunkWriter& operator=(const ChunkWriter&) = delete;
    ~ChunkWriter();

    uint64_t SegId() const noexcept { return _pending.SegId(); }

    // Serialise + append one chunk with its generated-PK base `pk_base` (0 for
    // explicit-PK shards) for replay PK reconstruction; buffered, no fsync.
    // zstd-1 with raw fallback.
    void Append(duckdb::DataChunk& chunk, uint64_t pk_base);

    PendingChunk Finish();

   private:
    PendingChunk _pending;
    std::unique_ptr<duckdb::BufferedFileWriter> _writer;
    // Reused across Append() calls (Rewind keeps the backing buffer).
    std::unique_ptr<duckdb::MemoryStream> _stream;
    // Reused zstd context: created once, reset per Append (ZSTD_compressCCtx).
    basics::ZstdCCtxPtr _cctx;
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

  struct Op {
    // INLINE only: one entry per inserted Sink chunk, in append order.
    const duckdb::ColumnDataCollection* inline_data = nullptr;
    std::span<const InlinePk> inline_pks;
    // REFERENCE: the bulk chunk files this op points at.
    std::span<PendingChunk> reference_chunks;
    // DELETE: the encoded PK byte strings to remove (iresearch PK terms).
    std::span<const std::string> delete_pks;

    bool truncate = false;
  };

  // One transaction's contribution for a single search shard
  struct ShardSection {
    ObjectId table_id;
    std::span<const Op> ops;
  };

  using ReplayCallback =
    absl::AnyInvocable<void(uint64_t tick, ObjectId table_id, uint64_t pk_base,
                            duckdb::DataChunk& chunk) const>;

  // Invoked once per DELETE op, in manifest order, with the encoded PK byte
  // strings to remove (views into the record buffer, valid for the call only).
  using DeleteReplayCallback =
    absl::AnyInvocable<void(uint64_t tick, ObjectId table_id,
                            std::span<const std::string_view> pks) const>;

  using TruncateReplayCallback =
    absl::AnyInvocable<void(uint64_t tick, ObjectId table_id) const>;

  using ShardExistsFn = absl::AnyInvocable<bool(ObjectId table_id) const>;
  using ShardCommittedFn =
    absl::AnyInvocable<uint64_t(ObjectId table_id) const>;

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
  // Reserves `tick_span` consecutive ticks under the append lock and writes one
  // record at the top of that band; returns the record tick (== base +
  // tick_span). Once the record is fsynced, marks every REFERENCE op's chunks
  // committed -- they are now durably referenced and must outlive the txn.
  uint64_t AppendCommit(std::span<const ShardSection> sections,
                        uint64_t tick_span);
  uint64_t Recover(const ShardExistsFn& exists_of,
                   const ShardCommittedFn& committed_of,
                   const ReplayCallback& insert_cb,
                   const DeleteReplayCallback& delete_cb,
                   const TruncateReplayCallback& truncate_cb);

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
  const absl::AnyInvocable<void(duckdb::DataChunk&, uint64_t base) const>&
    emit);

}  // namespace sdb::search
