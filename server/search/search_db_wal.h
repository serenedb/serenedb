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
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

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
  // One inserted Sink chunk's generated-PK run: `count` rows keyed
  // [base, base+count), base 0 for explicit-PK. Per Sink chunk, NOT per
  // inline_data Chunk -- ColumnDataCollection coalesces partial appends, so its
  // Chunks() boundaries don't line up.
  struct InlinePk {
    uint64_t base;
    uint64_t count;
  };

  // One iresearch segment flushed and fsynced before the commit record was
  // written, so its rows are never written twice. The same pair iresearch's own
  // index meta keeps per segment (index_meta_writer.hpp): the meta file holds
  // every other field behind its own checksum. Ordering against the deletes
  // around it comes from the op manifest, so no tick is recorded.
  struct SegmentRef {
    std::string meta_file;
    std::string codec;
  };

  // One entry of a shard section. The record stores them in issue order, so
  // position IS the ordering -- no watermark needed. Rows name a band range of
  // the section's collection, which lets one buffer back several entries.
  struct Entry {
    enum class Kind : uint8_t {
      kRows = 0,
      kDelete = 1,
      kTruncate = 2,
      kSegments = 3,
    };

    Kind kind = Kind::kRows;
    uint32_t first_band = 0;
    uint32_t last_band = 0;
    std::span<const int64_t> delete_rows;
    std::span<const SegmentRef> segments;
  };

  // One transaction's contribution for a single search shard: the rows it
  // buffered, plus the entries that put them in order with everything else.
  struct ShardSection {
    ObjectId table_id;
    const duckdb::ColumnDataCollection* inline_data = nullptr;
    std::span<const InlinePk> inline_pks;
    std::span<const Entry> entries;
  };

  using ReplayCallback =
    absl::AnyInvocable<void(uint64_t tick, ObjectId table_id, uint64_t pk_base,
                            duckdb::DataChunk& chunk) const>;

  // Invoked once per DELETE op, in record order, with the rowids to remove
  // (a view into the record buffer, valid for the call only).
  using DeleteReplayCallback = absl::AnyInvocable<void(
    uint64_t tick, ObjectId table_id, std::span<const int64_t> rows) const>;

  // Invoked once per recorded segment, in manifest order. `tick` is the
  // record's own, for the caller's high-water mark -- the tick to adopt at
  // lives in the replay transaction's space (see RunSearchTableRecovery).
  using AdoptReplayCallback = absl::AnyInvocable<void(
    uint64_t tick, ObjectId table_id, const SegmentRef& ref) const>;

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
  irs::containers::FlatHashMap<uint64_t, uint64_t> _committed;

  void EnsureActiveSegmentLocked(uint64_t first_tick);
  void WriteFrameLocked(const uint8_t* payload, uint64_t payload_size);
  uint64_t MinCommittedTick();
  void RunGc();
};

// Re-slice an inline collection by its recorded per-Sink-chunk bands, invoking
// `emit(slice, base)` once per band with that chunk's rows + rowid base. The
// ranged overload emits only bands [first_band, last_band), skipping the rows
// the earlier bands hold -- what lets one collection back several record
// entries.
void VisitInlineSegments(
  const duckdb::ColumnDataCollection& cdc,
  std::span<const SearchDbWal::InlinePk> segments, size_t first_band,
  size_t last_band,
  const absl::AnyInvocable<void(duckdb::DataChunk&, uint64_t base) const>&
    emit);

inline void VisitInlineSegments(
  const duckdb::ColumnDataCollection& cdc,
  std::span<const SearchDbWal::InlinePk> segments,
  const absl::AnyInvocable<void(duckdb::DataChunk&, uint64_t base) const>&
    emit) {
  VisitInlineSegments(cdc, segments, 0, segments.size(), emit);
}

}  // namespace sdb::search
