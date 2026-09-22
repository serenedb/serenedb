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
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "connector/duckdb_client_state.h"
#include "connector/full_scanner.h"
#include "connector/primary_key.h"
#include "connector/search_sink_writer.hpp"
#include "connector/term_dict.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
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
  std::vector<irs::ColumnstoreProjection> projections;
  duckdb::idx_t rowid_slot = 0;
  duckdb::DataChunk chunk;
  irs::ColFilterStateCache filter_states;
  duckdb::SelectionVector live{STANDARD_VECTOR_SIZE};
};

// In place: RowSource owns a DataChunk and a selection vector, neither
// copyable.
void InitRowSource(duckdb::ClientContext& context,
                   const SearchBackfillTarget& target, RowSource& source) {
  source.projections.reserve(target.column_ids.size() + 1);
  duckdb::vector<duckdb::LogicalType> types = target.column_types;
  for (size_t i = 0; i < target.column_ids.size(); ++i) {
    source.projections.push_back(irs::ColumnstoreProjection{
      .output_slot = i, .column_id = target.column_ids[i]});
  }
  // The rowid is a stored column like any other (kPKFieldId is kGeneratedPKId
  // by definition), read here so each rebuilt row keeps its identity.
  source.rowid_slot = target.column_ids.size();
  source.projections.push_back(irs::ColumnstoreProjection{
    .output_slot = source.rowid_slot, .column_id = term_dict::kPKFieldId});
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
  SDB_ENSURE(col_reader, "search-table build: segment has no columnstore");
  FullScanner scanner{
    *col_reader, source.projections, {}, &context, source.filter_states};
  const auto* mask = sub.docs_mask();
  if (mask && mask->empty()) {
    mask = nullptr;
  }
  const uint64_t docs = sub.Meta().docs_count;
  uint64_t fed = 0;
  for (uint64_t row = 0; row < docs; row += STANDARD_VECTOR_SIZE) {
    const auto take = std::min<uint64_t>(STANDARD_VECTOR_SIZE, docs - row);
    auto& chunk = source.chunk;
    chunk.Reset();
    const auto produced = scanner.Scan(row, take, chunk);
    SDB_ASSERT(produced == take, "unfiltered scan produced fewer rows");
    chunk.SetCardinality(produced);
    if (mask) {
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
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("search-table build: publish failed: ", result.res.message()));
  }
}

void StageDeletes(irs::IndexWriter::Transaction& trx,
                  std::vector<int64_t> rowids) {
  if (rowids.empty()) {
    return;
  }
  // Sorted rowids encode to sorted terms, so the remove filter walks each
  // segment's term dictionary sequentially.
  absl::c_sort(rowids);
  SearchSinkDeleteBaseImpl remover{trx};
  remover.InitImpl(rowids.size());
  std::string key;
  for (const auto rowid : rowids) {
    key.clear();
    primary_key::AppendGenerated(key, static_cast<uint64_t>(rowid));
    remover.DeleteRowImpl(key);
  }
  remover.FinishImpl();
  // Deliberately not RegisterFlush. Registering would also bind the removals to
  // whatever context is current here rather than the one the imports go to.
}

struct Slice {
  RowSource source;
  irs::IndexWriter::Transaction trx;
  std::unique_ptr<SearchSinkInsertBaseImpl> sink;
  std::vector<const irs::SubReader*> segments;
  // Views into `trx`, which outlives them: it is aborted after the swap.
  std::vector<std::string_view> adopted;
};

// By bytes, not by segment count: sizes are uneven, and a group is only as
// fast as its heaviest slice.
std::vector<std::vector<const irs::SubReader*>> BalanceSlices(
  std::span<const irs::SubReader* const> group, size_t slices) {
  std::vector<const irs::SubReader*> by_size{group.begin(), group.end()};
  absl::c_sort(by_size, [](const irs::SubReader* l, const irs::SubReader* r) {
    return l->Meta().byte_size > r->Meta().byte_size;
  });
  std::vector<std::vector<const irs::SubReader*>> out(slices);
  std::vector<uint64_t> load(slices, 0);
  for (const auto* sub : by_size) {
    const auto lightest = static_cast<size_t>(
      std::distance(load.begin(), absl::c_min_element(load)));
    out[lightest].push_back(sub);
    load[lightest] += sub->Meta().byte_size;
  }
  return out;
}

struct FeedSliceTask final : duckdb::BaseExecutorTask {
  FeedSliceTask(duckdb::TaskExecutor& executor_in,
                duckdb::ClientContext& context_in,
                const SearchBackfillTarget& target_in, Slice& slice_in,
                pg::ProgressMetrics* progress_in)
    : BaseExecutorTask{executor_in},
      context{context_in},
      target{target_in},
      slice{slice_in},
      progress{progress_in} {}

  void ExecuteTask() final {
    for (const auto* sub : slice.segments) {
      const auto fed =
        FeedSegment(context, *sub, slice.source, *slice.sink, target);
      if (progress) {
        pg::ProgressMetrics::Add(progress->tuples_processed,
                                 static_cast<int64_t>(fed));
      }
      if (context.IsInterrupted()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_QUERY_CANCELED),
          ERR_MSG("canceled while rebuilding search table ", target.table_id));
      }
    }
    // On the worker, like SereneDBSearchInsert::Combine: serialising this tail
    // costs more than the feeding it follows.
    for (const auto& segment : slice.trx.FlushAndFsync()) {
      slice.adopted.push_back(segment.filename);
    }
  }

  std::string TaskType() const final { return "SearchBackfillSlice"; }

  duckdb::ClientContext& context;
  const SearchBackfillTarget& target;
  Slice& slice;
  pg::ProgressMetrics* progress;
};

// Rewrites one group of stale segments into fresh ones and swaps them in.
void RebuildGroup(duckdb::ClientContext& context,
                  const SearchBackfillTarget& target,
                  std::span<const irs::SubReader* const> group,
                  pg::ProgressMetrics* progress) {
  auto& shard = *target.shard;
  const auto slice_count = std::max<size_t>(
    1, std::min<size_t>(
         group.size(),
         duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads()));
  auto assignment = BalanceSlices(group, slice_count);

  // Built here, never on a worker: the sink factory reads the catalog and
  // InitRowSource takes the context allocator.
  std::vector<std::unique_ptr<Slice>> slices;
  slices.reserve(slice_count);
  for (size_t k = 0; k < slice_count; ++k) {
    auto slice = std::make_unique<Slice>();
    slice->segments = std::move(assignment[k]);
    // Exclusive: FlushAndFsync hands back exactly this slice's segments, and
    // they must not share one with a concurrent writer.
    slice->trx = shard.GetTransaction(/*exclusive_segment=*/true);
    slice->sink =
      MakeSearchTableInsertSink(slice->trx, shard, *target.catalog, context);
    InitRowSource(context, target, slice->source);
    slices.push_back(std::move(slice));
  }

  const auto abort_all = [&slices] {
    for (auto& slice : slices) {
      slice->trx.Abort();
    }
  };
  // Abort is idempotent: this covers every throwing path, and is a no-op once
  // the swap below has released them in order.
  absl::Cleanup abort_slices = abort_all;

  {
    duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(context)};
    for (auto& slice : slices) {
      if (slice->segments.empty()) {
        continue;
      }
      executor.ScheduleTask(duckdb::make_uniq<FeedSliceTask>(
        executor, context, target, *slice, progress));
    }
    executor.WorkOnTasks();
  }

  std::vector<std::string_view> replaced;
  replaced.reserve(group.size());
  for (const auto* sub : group) {
    replaced.push_back(sub->Meta().name);
  }
  std::vector<std::string_view> adopted;
  for (auto& slice : slices) {
    adopted.insert(adopted.end(), slice->adopted.begin(), slice->adopted.end());
  }
  // Every replacement may be empty (a group whose rows were all deleted), so
  // the codec comes from the writer rather than from the flushed set.
  const auto& codec = shard.Codec();

  // Parks the build with this group read and flushed but not yet swapped in --
  // the window every mid-build delete has to survive, and the only one where
  // the delete-log rather than the adopt tick is what saves the row.
  SDB_WAIT_ON_FAILURE("pause_search_backfill_before_swap");

  auto deletes = shard.GetTransaction();
  absl::Cleanup abort_deletes = [&deletes] { deletes.Abort(); };

  // Drain and swap under one hold of the delete log, so no removal can be
  // lost while we are swapping
  const bool swapped =
    shard.SwapWithDrainedDeletes([&](std::vector<int64_t> rowids) {
      StageDeletes(deletes, std::move(rowids));
      return shard.ReplaceSegments(replaced, adopted, codec, &deletes,
                                   shard.Wal().CurrentTick());
    });
  // Need explicit call here so on Publish we don't have pending transactions.
  abort_all();
  if (!swapped) {
    // A source going missing is tolerated (it means everything in it was
    // deleted), so what is left here is a codec or meta-file failure.
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("search-table build: failed to swap in the rebuilt segments on "
              "table ",
              target.table_id));
  }
  Publish(shard);
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
                            target.table_id));
  }
  const auto cancelled = [&] { return context.IsInterrupted(); };

  // The config is already published (CreateIndexImpl). Open the log first so no
  // committed delete falls between the floor and the first drain, then arm the
  // floor -- after the publish, never before (§3.2).
  // The statement pinned a reader of this shard at bind time and only drops it
  // at a statement boundary, so every segment it saw would outlive the build
  // and none of the ones this rewrites could be reclaimed while it runs. The
  // plan here produces no rows (LogicalEmptyResult), so nothing reads it. A
  // frozen view keeps its pin and pays the disk instead.
  GetSereneDBContext(context).TryDropSearchReader(target.table_id);

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
                              target.table_id));
    }
    absl::SleepFor(kArmRetry);
  }

  // Now nothing can still commit a pre-config segment below the floor.
  shard.DrainPriorWriters(cancelled);

  // Groups by byte budget: peak disk is ~2x one group, and each publish is
  // proportional to one group rather than the table. A zero budget is "no
  // limit" -- one group, one swap, the whole table's worth of peak disk.
  const bool unlimited = target.group_bytes == 0;
  bool counted = false;
  // One group per pass, and the reader that named it dies with the pass: it
  // references every segment it can see, so holding it across groups would
  // keep the ones already swapped out unreclaimable and make the budget above
  // bound nothing. A straddler that committed after the drain shows up on a
  // later pass as a sub-floor segment; loop until none do.
  for (;;) {
    auto reader = shard.GetDirectoryReader();
    std::vector<const irs::SubReader*> group;
    uint64_t live = 0;
    uint64_t bytes = 0;
    for (const auto& sub : reader) {
      uint64_t id = 0;
      if (!SegmentIdOf(sub.Meta().name, id) || id > floor.Floor() ||
          sub.live_docs_count() == 0) {
        continue;
      }
      live += sub.live_docs_count();
      if (unlimited || bytes < target.group_bytes) {
        group.push_back(&sub);
        bytes += sub.Meta().byte_size;
      }
    }
    if (group.empty()) {
      break;
    }
    if (progress && !counted) {
      // Exact, unlike the transactional path's planner estimate.
      pg::ProgressMetrics::Set(progress->tuples_total,
                               static_cast<int64_t>(live));
      counted = true;
    }
    RebuildGroup(context, target, group, progress);
    SDB_IF_FAILURE("crash_after_search_backfill_group") {
      SDB_IMMEDIATE_ABORT();
    }
  }
  // Nothing below the floor is left to reissue against, but a delete that
  // landed after the last group's drain is in the log; it already reached the
  // adopted segments through the normal path, so dropping it is safe.
}

}  // namespace sdb::connector
