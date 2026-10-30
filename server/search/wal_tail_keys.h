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

#include <cstdint>
#include <duckdb/common/mutex.hpp>
#include <duckdb/storage/table/table_log_storage.hpp>
#include <iresearch/formats/index_meta_writer.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <span>
#include <string>
#include <vector>

#include "connector/primary_key.h"
#include "connector/search_key_unique.h"
#include "search/search_table_changes.h"
#include "search/search_unpublished.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::search {

class SearchTable;

// What one search table's committed-but-unpublished WAL tail says about its
// keys: the same three things a probe needs, plus the barrier a TRUNCATE
// leaves. `truncate_tick` is the offset of the last TRUNCATE in the tail;
// everything the tail said before it, and everything the published reader
// holds, is dead.
struct WalTailKeys {
  std::vector<connector::KeyTerms> keys;
  std::vector<std::string> segments;
  std::vector<RowRange> removed;
  bool truncated = false;

  void Clear() {
    keys.clear();
    segments.clear();
    removed.clear();
    truncated = false;
  }

  bool Empty() const noexcept {
    return keys.empty() && segments.empty() && removed.empty() && !truncated;
  }
};

// Collects `WalTailKeys` for one table out of a WAL tail read. A batch's
// entries arrive before its transaction's fate is known, so they are staged and
// only folded in when the reader confirms the batch committed.
class WalTailKeyCollector final : public duckdb::TableLogStorage,
                                  public duckdb::TableLogStorageResolver {
 public:
  WalTailKeyCollector(duckdb::idx_t table_oid,
                      std::span<const connector::primary_key::KeySlot> slots)
    : _table_oid{table_oid}, _slots{slots.begin(), slots.end()} {}

  const WalTailKeys& Keys() const noexcept { return _committed; }

  duckdb::optional_ptr<duckdb::TableLogStorage> Resolve(
    duckdb::idx_t table_oid) final {
    return table_oid == _table_oid ? this : nullptr;
  }

  void EndBatch(bool committed) final;

  void ReplayInsert(duckdb::ClientContext& context, duckdb::DataChunk& chunk,
                    duckdb::optional_idx row_start) final;
  void ReplayDelete(duckdb::ClientContext& context,
                    duckdb::DataChunk& chunk) final;
  void ReplayDeleteRanges(duckdb::ClientContext& context,
                          const duckdb::vector<duckdb::row_t>& starts,
                          const duckdb::vector<duckdb::idx_t>& counts) final;
  void ReplayTruncate(duckdb::ClientContext& context) final;
  void ReplayAdoptSegments(duckdb::ClientContext& context,
                           duckdb::vector<std::string> segments) final;

 private:
  duckdb::idx_t _table_oid;
  std::vector<connector::primary_key::KeySlot> _slots;
  WalTailKeys _staged;
  WalTailKeys _committed;
};

// One read of a shard's committed-but-unpublished tail, owning everything a
// probe over it borrows.
struct WalTail {
  WalTailKeys keys;
  // Readers over the segments the tail adopted, opened here because the probe
  // reads their term dictionaries. A segment the tail names but that is gone
  // leaves nothing here and sets `lost`.
  std::vector<irs::SegmentReader> segments;
  // Where the next read resumes. Composes: a later pass starting here sees
  // exactly what this one did not.
  irs::SourcePosition boundary;
  // The read stopped on a batch whose transaction had not decided yet, so the
  // tail is a prefix of what is committed. A pass that cannot tolerate that
  // has to wait for the decision or fall back.
  bool stopped_undecided = false;
  // A named segment could not be opened, so the tail is incomplete in a way
  // that cannot be made up for: the rows live on in whatever the compaction
  // merged them into, which only a newer reader lists.
  bool lost = false;
  // The tail could not be read at all. A caller must refuse to certify on
  // this, never treat it as an empty tail: an unread tail and a tail with
  // nothing in it are the same value and opposite answers.
  std::string error;
};

// The database a shard's WAL belongs to. Resolved by scanning every attached
// database under the database manager's global lock, which is why a caller
// holding DuckDB's commit lock must not do it: that would order commit_lock
// above databases_lock, and DETACH -- which becomes a DROP that commits --
// plausibly runs the two the other way round. Resolve once, before any lock,
// and hand the result to every pass.
duckdb::shared_ptr<duckdb::AttachedDatabase> ResolveTailDatabase(
  const SearchTable& shard);

// Reads `shard`'s WAL from `from` to `store`'s current WAL end, following the
// retained-WAL chain when the two straddle a checkpoint.
//
// `from` is a position the caller already answered for -- the published
// position for a first pass, a previous read's `boundary` for a later one.
// Nothing has to be held to keep that range readable: a generation retention
// has released held only published rows, and the clamp inside says why.
//
// `commit_lock` is the caller's, when it has one; otherwise this takes the
// database's commit lock itself for as long as it takes to resolve the files
// and open them. That lock is what makes a WAL path mean one file: every swap
// of the live WAL, every rename and delete of a retained one, and the
// retention floor all move under it and nowhere else. Holding it across the
// reads would serialize commits behind them, so only the opening is under it,
// and the handles carry the files through whatever the next checkpoint does to
// their names.
//
// The checkpoint lock is deliberately not taken: it is held for a whole
// checkpoint, so waiting for it would put every commit behind the checkpoint
// body that DuckDB runs commits concurrently with. Nor is it enough to assume
// the caller's transaction holds it -- an INSERT takes the vacuum lock, and
// reaches for the checkpoint lock only if something in the statement touched a
// sequence.
WalTail ReadUnpublishedTail(
  SearchTable& shard, duckdb::AttachedDatabase& store,
  duckdb::ClientContext& context, const irs::SourcePosition& from,
  std::span<const connector::primary_key::KeySlot> slots,
  duckdb::optional_ptr<duckdb::unique_lock<duckdb::mutex>> commit_lock);

// The WAL's side of the seam: a tail as the probe wants it. Borrows from
// `tail`, which has to outlive the probe.
inline UnpublishedTier Flatten(const WalTail& tail) {
  UnpublishedTier tier;
  tier.segments.reserve(tail.segments.size());
  for (const auto& segment : tail.segments) {
    tier.segments.push_back(&segment);
  }
  tier.keys.reserve(tail.keys.keys.size());
  for (const auto& key : tail.keys.keys) {
    tier.keys.push_back(&key);
  }
  tier.removed = tail.keys.removed;
  return tier;
}

}  // namespace sdb::search
