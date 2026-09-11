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

#include "connector/search_table_backfill.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/numbers.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/system-compiler.h"
#include "catalog/duckdb_primary_key.h"
#include "catalog/table_options.h"
#include "connector/full_scanner.h"
#include "connector/search_sink_writer.hpp"
#include "pg/errcodes.h"
#include "pg/progress_registry.h"
#include "pg/sql_exception_macro.h"
#include "search/search_db_wal.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

// Segment names are "_<decimal id>" (irs::FileName(uint64_t)).
bool SegmentIdOf(std::string_view name, uint64_t& id) {
  return name.size() > 1 && name.front() == '_' &&
         absl::SimpleAtoi(name.substr(1), &id);
}

// How long to back off when the floor cannot be armed because a compaction is
// mid-flight. Compactions finish, so this only paces the retry.
constexpr absl::Duration kArmRetry = absl::Milliseconds(50);

struct RowSource {
  std::vector<ColumnstoreProjection> projections;
  duckdb::idx_t rowid_slot = 0;
  duckdb::DataChunk chunk;
  ColFilterStateCache filter_states;
  duckdb::SelectionVector live{STANDARD_VECTOR_SIZE};
};

// In place: RowSource owns a DataChunk and a selection vector, neither copyable.
void InitRowSource(duckdb::ClientContext& context,
                   const SearchBackfillTarget& target, RowSource& source) {
  source.projections.reserve(target.column_ids.size() + 1);
  duckdb::vector<duckdb::LogicalType> types = target.column_types;
  for (size_t i = 0; i < target.column_ids.size(); ++i) {
    source.projections.push_back(ColumnstoreProjection{
      .output_slot = i,
      .column_id = static_cast<irs::field_id>(target.column_ids[i])});
  }
  // The rowid is a stored column like any other (kPKFieldId is kGeneratedPKId
  // by definition), read here so each rebuilt row keeps its identity.
  source.rowid_slot = target.column_ids.size();
  source.projections.push_back(
    ColumnstoreProjection{.output_slot = source.rowid_slot,
                          .column_id = catalog::term_dict::kPKFieldId});
  types.push_back(duckdb::LogicalType::BIGINT);
  source.chunk.Initialize(duckdb::Allocator::Get(context), types);
}

// Feeds every live row of `sub` into `sink`. A segment with deletes is scanned
// in full and sliced down to the unmasked rows, which keeps this off the scan
// operator's hit-batching machinery; the mask is per doc id, and doc ids are
// row + doc_limits::min().
uint64_t FeedSegment(duckdb::ClientContext& context, const irs::SubReader& sub,
                     RowSource& source, SearchSinkInsertBaseImpl& sink,
                     const SearchBackfillTarget& target) {
  const auto* col_reader = sub.GetColReader();
  SDB_ENSURE(col_reader != nullptr,
             "search-table build: segment has no columnstore");
  FullScanner scanner{*col_reader, source.projections, {}, &context,
                      source.filter_states};
  const auto* mask = sub.docs_mask();
  if (mask != nullptr && mask->empty()) {
    mask = nullptr;
  }
  const uint64_t docs = sub.Meta().docs_count;
  uint64_t fed = 0;
  for (uint64_t row = 0; row < docs; row += STANDARD_VECTOR_SIZE) {
    const auto take =
      static_cast<duckdb::idx_t>(std::min<uint64_t>(STANDARD_VECTOR_SIZE, docs - row));
    auto& chunk = source.chunk;
    chunk.Reset();
    const auto produced = scanner.Scan(row, take, chunk);
    SDB_ASSERT(produced == take, "unfiltered scan produced fewer rows");
    chunk.SetCardinality(produced);
    if (mask != nullptr) {
      duckdb::idx_t keep = 0;
      for (duckdb::idx_t i = 0; i < produced; ++i) {
        const auto doc =
          static_cast<irs::doc_id_t>(row + i + irs::doc_limits::min());
        if (!mask->contains(doc)) {
          source.live.set_index(keep++, i);
        }
      }
      if (keep == 0) {
        continue;
      }
      if (keep != produced) {
        chunk.Slice(source.live, keep);
      }
    }
    WriteRebuiltChunkToSearchSink(sink, chunk, target.column_ids,
                                  source.rowid_slot, target.table_id, context);
    fed += chunk.size();
  }
  return fed;
}

void Publish(search::SearchTable& shard) {
  search::RefreshResult code = search::RefreshResult::Undefined;
  const auto result = shard.RefreshUnsafe(/*wait=*/true, nullptr, code);
  if (!result.res.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("search-table build: publish failed: ",
                            result.res.message()));
  }
}

// Reissues the rowids deleted since the log was last drained, so a row a
// rebuilt segment carries from before its delete does not come back. Safe to
// over-apply: a rowid is never reused (Pillar A), so a removal for one already
// gone matches nothing.
void ReissueDeletes(search::SearchTable& shard, std::vector<int64_t> rowids) {
  if (rowids.empty()) {
    return;
  }
  // Sorted rowids encode to sorted terms, so the remove filter walks each
  // segment's term dictionary sequentially.
  absl::c_sort(rowids);
  auto trx = shard.GetTransaction();
  SearchSinkDeleteBaseImpl remover{trx};
  remover.InitImpl(rowids.size());
  std::string key;
  for (const auto rowid : rowids) {
    key.clear();
    catalog::duckdb_primary_key::AppendGenerated(key,
                                                 static_cast<uint64_t>(rowid));
    remover.DeleteRowImpl(key);
  }
  remover.FinishImpl();
  trx.RegisterFlush();
  // Every row these could name was committed at or below the WAL's current
  // tick, and a removal reaches docs at or below its own.
  const auto tick = shard.Wal().CurrentTick();
  if (!trx.Commit(tick)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("search-table build: failed to reissue deletes"));
  }
  Publish(shard);
}

// Rewrites one group of stale segments into fresh ones and swaps them in.
void RebuildGroup(duckdb::ClientContext& context,
                  const SearchBackfillTarget& target, RowSource& source,
                  std::span<const irs::SubReader* const> group,
                  pg::ProgressMetrics* progress) {
  auto& shard = *target.shard;
  // Exclusive: FlushAndFsync hands back exactly this build's segments, and
  // they must not share one with a concurrent writer.
  auto trx = shard.GetTransaction(/*exclusive_segment=*/true);
  auto sink = MakeSearchTableInsertSink(trx, shard, context);

  std::vector<std::string_view> replaced;
  replaced.reserve(group.size());
  for (const auto* sub : group) {
    replaced.push_back(sub->Meta().name);
    const auto fed = FeedSegment(context, *sub, source, *sink, target);
    if (progress != nullptr) {
      pg::ProgressMetrics::Add(progress->tuples_processed,
                               static_cast<int64_t>(fed));
    }
    if (context.IsInterrupted()) {
      trx.Abort();
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_QUERY_CANCELED),
                      ERR_MSG("canceled while rebuilding search table ",
                              target.table_id.id()));
    }
  }

  const auto flushed = trx.FlushAndFsync();
  std::vector<std::string_view> adopted;
  adopted.reserve(flushed.size());
  for (const auto& segment : flushed) {
    adopted.push_back(segment.filename);
  }
  // Every replacement may be empty (a group whose rows were all deleted), so
  // the codec comes from the writer rather than from the flushed set.
  const auto& codec = shard.Codec();

  // Parks the build with this group read and flushed but not yet swapped in --
  // the window every mid-build delete has to survive, and the only one where
  // the delete-log rather than the adopt tick is what saves the row.
  SDB_WAIT_ON_FAILURE("pause_search_backfill_before_swap");

  // Reference the replacements (ReplaceSegments does) BEFORE aborting the
  // transaction: Abort releases the transaction's own file refs, and the
  // refresh loop's cleanup would then be free to unlink them.
  const bool swapped = shard.ReplaceSegments(replaced, adopted, codec,
                                             irs::writer_limits::kMinTick);
  trx.Abort();
  if (!swapped) {
    // A source going missing is tolerated (it means everything in it was
    // deleted), so what is left here is a codec or meta-file failure.
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("search-table build: failed to swap in the rebuilt segments on "
              "table ",
              target.table_id.id()));
  }
  Publish(shard);
  // Only now: the adopted segments are live, so the reissued removals hit
  // them. Deletes landing from here on reach them through the normal path.
  ReissueDeletes(shard, shard.TakeDeleteLog());
}

}  // namespace

void RunSearchTableBackfill(duckdb::ClientContext& context,
                            const SearchBackfillTarget& target,
                            pg::ProgressMetrics* progress) {
  SDB_ASSERT(target.shard);
  auto& shard = *target.shard;

  search::SearchTable::BuildClaim claim{shard};
  if (!claim.Claimed()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("an index build is already running on search "
                            "table ",
                            target.table_id.id()));
  }
  const auto cancelled = [&] { return context.IsInterrupted(); };

  // The config is already published (CreateIndexImpl). Open the log first so no
  // committed delete falls between the floor and the first drain, then arm the
  // floor -- after the publish, never before (§3.2).
  shard.OpenDeleteLog();
  absl::Cleanup close_log = [&shard] { shard.CloseDeleteLog(); };

  irs::IndexWriter::CompactionFloorGuard floor;
  for (;;) {
    floor = shard.ArmCompactionFloor();
    if (floor.Held()) {
      break;
    }
    if (cancelled()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_QUERY_CANCELED),
                      ERR_MSG("canceled while waiting for compaction on "
                              "search table ",
                              target.table_id.id()));
    }
    absl::SleepFor(kArmRetry);
  }

  // Now nothing can still commit a pre-config segment below the floor.
  shard.DrainPriorWriters(cancelled);

  RowSource source;
  InitRowSource(context, target, source);
  bool counted = false;
  for (;;) {
    auto reader = shard.GetDirectoryReader();
    std::vector<const irs::SubReader*> stale;
    uint64_t live = 0;
    for (const auto& sub : reader) {
      uint64_t id = 0;
      if (SegmentIdOf(sub.Meta().name, id) && id <= floor.Floor() &&
          sub.live_docs_count() != 0) {
        stale.push_back(&sub);
        live += sub.live_docs_count();
      }
    }
    if (stale.empty()) {
      break;
    }
    if (progress != nullptr && !counted) {
      // Exact, unlike the transactional path's planner estimate.
      pg::ProgressMetrics::Set(progress->tuples_total,
                               static_cast<int64_t>(live));
      counted = true;
    }
    // Groups by byte budget: peak disk is ~2x one group, and each publish is
    // proportional to one group rather than the table. A zero budget is "no
    // limit" -- one group, one swap, the whole table's worth of peak disk.
    const bool unlimited = target.group_bytes == 0;
    size_t i = 0;
    while (i < stale.size()) {
      std::vector<const irs::SubReader*> group;
      uint64_t bytes = 0;
      do {
        group.push_back(stale[i]);
        bytes += stale[i]->Meta().byte_size;
        ++i;
      } while (i < stale.size() && (unlimited || bytes < target.group_bytes));
      RebuildGroup(context, target, source, group, progress);
      SDB_IF_FAILURE("crash_after_search_backfill_group") {
        SDB_IMMEDIATE_ABORT();
      }
    }
    // A straddler that committed after the drain shows up on the next pass as
    // a sub-floor segment; loop until none do.
  }
  // Nothing below the floor is left to reissue against, but a delete that
  // landed after the last group's drain is in the log; it already reached the
  // adopted segments through the normal path, so dropping it is safe.
}

}  // namespace sdb::connector
