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
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "iresearch/index/index_meta.hpp"

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
  // One inserted Sink chunk's generated-PK run: `count` rows keyed
  // [base, base+count). Recorded per Sink chunk -- NOT per inline_data Chunk:
  // ColumnDataCollection coalesces partial appends, so its Chunks() boundaries
  // don't line up with the Sink chunks the bases are keyed to. base is 0 for
  // explicit-PK.
  struct InlinePk {
    uint64_t base;
    uint64_t count;
  };

  // One iresearch segment a transaction flushed and fsynced (via
  // Transaction::FlushAndFsync) before its commit record was written. The
  // segment files belong to the index, so the WAL records only enough to reopen
  // them and the rows are never written twice.
  //
  // The meta is iresearch's own struct, not a copy of its fields, so the record
  // cannot drift from what AdoptSegment needs. Two of its fields are not on the
  // wire: `docs_mask` is always null (its only producer is a rollback masking
  // documents an earlier transaction committed into a shared segment, and an
  // exclusive segment has none), so `live_docs_count` always equals docs_count
  // and is restored from it.
  //
  // No tick is recorded either. A segment's ordering against the deletes around
  // it is carried by its position in the op manifest (§5.4), and replay has to
  // translate that into a tick in ITS OWN space: removals replayed into one
  // transaction are rebased to `commit_tick - queries + k`, which has nothing
  // to do with the tick bands the record was written under. Recording a tick
  // from the write side would invite using it directly, which masks the wrong
  // segments -- see RunSearchTableRecovery.
  struct SegmentRef {
    irs::SegmentMeta meta;
  };

  struct Op {
    // INLINE only: one entry per inserted Sink chunk, in append order.
    const duckdb::ColumnDataCollection* inline_data = nullptr;
    std::span<const InlinePk> inline_pks;
    // SEGMENT: iresearch segments already flushed + fsynced for this op.
    std::span<const SegmentRef> segments;
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

  // Invoked once per recorded segment, in manifest order. `tick` is the
  // record's own tick, for the caller's high-water mark -- NOT the tick to
  // adopt at; that one lives in the replay transaction's tick space (see
  // SegmentRef).
  using AdoptReplayCallback = absl::AnyInvocable<void(
    uint64_t tick, ObjectId table_id, irs::SegmentMeta&& meta) const>;

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

  // Reserves `tick_span` consecutive ticks under the append lock and writes one
  // record at the top of that band; returns the record tick (== base +
  // tick_span).
  uint64_t AppendCommit(std::span<const ShardSection> sections,
                        uint64_t tick_span);
  uint64_t Recover(const ShardExistsFn& exists_of,
                   const ShardCommittedFn& committed_of,
                   const ReplayCallback& insert_cb,
                   const DeleteReplayCallback& delete_cb,
                   const TruncateReplayCallback& truncate_cb,
                   const AdoptReplayCallback& adopt_cb);

 private:
  duckdb::FileSystem& _fs;
  std::filesystem::path _wal_dir;

  const uint64_t _seal_threshold;

  absl::Mutex _append_mu;
  std::atomic<uint64_t> _tick{0};
  std::unique_ptr<duckdb::BufferedFileWriter> _active;
  uint64_t _active_first_tick = 0;

  absl::Mutex _sub_mu;
  containers::FlatHashMap<uint64_t, uint64_t> _committed;

  void EnsureActiveSegmentLocked(uint64_t first_tick);
  void WriteFrameLocked(const uint8_t* payload, uint64_t payload_size);
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
