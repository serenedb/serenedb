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
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/docs_mask/docs_mask.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/transaction.h"
#include "connector/full_scanner.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "query/config_variable_names.h"
#include "search/store.h"
#include "search/writes.h"

namespace sdb::connector {
namespace {

// How long to back off when the floor cannot be armed because a compaction is
// mid-flight. Compactions finish, so this only paces the retry.
constexpr absl::Duration kArmRetry = absl::Milliseconds(50);

struct RowSource {
  std::vector<irs::ColumnstoreProjection> projections;
  duckdb::DataChunk chunk;
  irs::ColFilterStateCache filter_states;
  duckdb::SelectionVector live{STANDARD_VECTOR_SIZE +
                               irs::doc_limits::kDocsSlack};
  std::vector<ColumnId> write_ids;
  std::optional<duckdb::ExpressionExecutor> executor;
  duckdb::DataChunk added;
  duckdb::DataChunk written;
};

// In place: RowSource owns a DataChunk and a selection vector, neither
// copyable.
void InitRowSource(duckdb::ClientContext& context,
                   const SearchBackfillTarget& target, RowSource& source) {
  source.projections.reserve(target.column_ids.size());
  for (size_t i = 0; i < target.column_ids.size(); ++i) {
    source.projections.emplace_back(irs::ColumnstoreProjection{
      .output_slot = i, .column_id = target.column_ids[i]});
  }
  auto& allocator = duckdb::Allocator::Get(context);
  source.chunk.Initialize(allocator, target.column_types);
  source.write_ids = target.column_ids;
  if (target.added.empty()) {
    return;
  }
  source.write_ids.insert(source.write_ids.end(), target.added_ids.begin(),
                          target.added_ids.end());
  source.executor.emplace(context);
  auto types = target.column_types;
  duckdb::vector<duckdb::LogicalType> added_types;
  for (const auto& expression : target.added) {
    source.executor->AddExpression(*expression);
    added_types.push_back(expression->GetReturnType());
    types.push_back(expression->GetReturnType());
  }
  source.added.Initialize(allocator, added_types);
  source.written.InitializeEmpty(types);
}

duckdb::DataChunk& Rewritten(RowSource& source, duckdb::DataChunk& chunk) {
  if (!source.executor) {
    return chunk;
  }
  source.added.Reset();
  source.executor->Execute(chunk, source.added);
  const auto read = chunk.ColumnCount();
  for (duckdb::idx_t i = 0; i < read; ++i) {
    source.written.data[i].Reference(chunk.data[i]);
  }
  for (duckdb::idx_t i = 0; i < source.added.ColumnCount(); ++i) {
    source.written.data[read + i].Reference(source.added.data[i]);
  }
  source.written.SetCardinality(chunk.size());
  return source.written;
}

// Feeds every live row of `sub` into `sink`. A segment with deletes is scanned
// in full and sliced down to the unmasked rows, which keeps this off the scan
// operator's hit-batching machinery.
uint64_t FeedSegment(duckdb::ClientContext& context, const irs::SubReader& sub,
                     RowSource& source, SearchSinkInsertBaseImpl& sink,
                     const SearchBackfillTarget& target) {
  uint64_t fed = 0;
  ScanLiveRows(context, sub, source.projections, source.filter_states,
               source.live, source.chunk, [&](duckdb::DataChunk& chunk) {
                 WriteChunkToSearchSink(sink, Rewritten(source, chunk),
                                        source.write_ids, target.table_id,
                                        context);
                 fed += chunk.size();
                 return true;
               });
  return fed;
}

struct Slice {
  RowSource source;
  irs::IndexWriter::Transaction trx;
  std::unique_ptr<SearchSinkInsertBaseImpl> sink;
  std::vector<const irs::SubReader*> segments;
  std::span<const irs::IndexWriter::FlushedSegment> flushed;
};

void AppendReplacements(const Slice& slice,
                        std::vector<irs::IndexWriter::Replacement>& out) {
  const auto first = out.size();
  for (const auto& flushed : slice.flushed) {
    out.push_back({.meta_file = flushed.filename});
  }
  size_t i = 0;
  uint64_t used = 0;
  for (const auto* sub : slice.segments) {
    const auto& segment =
      *irs::utils::downCast<irs::SegmentReader>(*sub).GetImpl();
    const uint64_t live = sub->live_docs_count();
    for (uint64_t rank = 0; rank < live;) {
      SDB_ENSURE(i < slice.flushed.size(),
                 "search-table build: fed more rows than its segments hold");
      const uint64_t docs = slice.flushed[i].meta.docs_count;
      const auto take = std::min(live - rank, docs - used);
      out[first + i].absorbed.push_back({
        .source = segment.Id(),
        .mask = segment.SharedDocsMask(),
        .rank_begin = static_cast<irs::doc_id_t>(rank),
        .rank_end = static_cast<irs::doc_id_t>(rank + take),
        .base = static_cast<irs::doc_id_t>(irs::doc_limits::min() + used),
      });
      rank += take;
      used += take;
      if (used == docs) {
        ++i;
        used = 0;
      }
    }
  }
  SDB_ENSURE(i == slice.flushed.size(),
             "search-table build: fed fewer rows than its segments hold");
}

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

  duckdb::TaskExecutionResult ExecuteTaskStep() final {
    if (next < slice.segments.size()) {
      const auto fed = FeedSegment(context, *slice.segments[next++],
                                   slice.source, *slice.sink, target);
      if (progress) {
        pg::ProgressMetrics::Add(progress->tuples_processed,
                                 static_cast<int64_t>(fed));
      }
      if (context.IsInterrupted()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_QUERY_CANCELED),
          ERR_MSG("canceled while rebuilding search table ", target.table_id));
      }
      return duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
    }
    SDB_WAIT_ON_FAILURE("pause_search_backfill_fed");
    // On the worker, like SereneDBSearchInsert::Combine: serialising this tail
    // costs more than the feeding it follows.
    slice.flushed = slice.trx.FlushAndFsync();
    return duckdb::TaskExecutionResult::TASK_FINISHED;
  }

  std::string TaskType() const final { return "SearchBackfillSlice"; }

  duckdb::ClientContext& context;
  const SearchBackfillTarget& target;
  Slice& slice;
  pg::ProgressMetrics* progress;
  size_t next = 0;
};

// Rewrites one group of stale segments into fresh ones and swaps them in.
search::Tick RebuildGroup(duckdb::ClientContext& context,
                          const SearchBackfillTarget& target,
                          std::span<const irs::SubReader* const> group,
                          pg::ProgressMetrics* progress) {
  auto& shard = *target.shard;
  const auto slice_count = std::max<size_t>(
    1, std::min<size_t>(group.size(),
                        duckdb::TaskScheduler::QueryThreads(context)));
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
    duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(context),
                                  context.GetSchedulerAccount()};
    for (auto& slice : slices) {
      if (slice->segments.empty()) {
        continue;
      }
      executor.ScheduleTask(duckdb::make_uniq<FeedSliceTask>(
        executor, context, target, *slice, progress));
    }
    executor.WorkOnTasks();
  }

  // Parks the build with this group read and flushed but not yet swapped in --
  // the window every mid-build delete has to survive, and the one where the
  // swap carries it through the absorbed doc maps.
  SDB_WAIT_ON_FAILURE("pause_search_backfill_before_swap");

  std::vector<irs::IndexWriter::Replacement> adopted;
  for (const auto& slice : slices) {
    AppendReplacements(*slice, adopted);
  }
  search::Tick published = 0;
  try {
    published = shard.ReplaceSegments(group, std::move(adopted));
  } catch (const irs::SqlException& e) {
    THROW_SQL_ERROR(
      ERR_CODE(e.error().errcode),
      ERR_MSG("search-table build: failed to swap in the rebuilt segments on "
              "table ",
              target.table_id, ": ", e.message()));
  } catch (const std::exception& e) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("search-table build: failed to swap in the rebuilt segments on "
              "table ",
              target.table_id, ": ", duckdb::ErrorData{e}.RawMessage()));
  }
  abort_all();
  SDB_PARK_ONCE_ON_FAILURE("pause_search_backfill_before_publish");
  shard.CoolAll();
  return published;
}

}  // namespace

uint64_t SearchBackfillGroupBytes(duckdb::ClientContext& context) {
  duckdb::Value group_bytes;
  if (context.TryGetCurrentSetting(
        duckdb::Identifier{kSearchBackfillGroupBytesSetting}, group_bytes) &&
      !group_bytes.IsNull()) {
    return group_bytes.GetValue<uint64_t>();
  }
  return uint64_t{1} << 30;
}

void RunSearchTableBackfill(duckdb::ClientContext& context,
                            const SearchBackfillTarget& target,
                            pg::ProgressMetrics* progress) {
  SDB_ASSERT(target.shard);
  auto& shard = *target.shard;

  // The statement pinned a reader of this shard at bind time and only drops it
  // at a statement boundary, so every segment it saw would outlive the build
  // and none of the ones this rewrites could be reclaimed while it runs. The
  // plan here produces no rows (LogicalEmptyResult), so nothing reads it. A
  // frozen view keeps its pin and pays the disk instead.
  ConnectionContext::Get(context).TryDropSearchReader(target.catalog->GetOid(),
                                                      target.table_id);

  shard.DrainPriorWriters(context);

  irs::IndexWriter::CompactionFloorGuard floor;
  for (;;) {
    shard.CoolAll();
    floor = shard.ArmCompactionFloor();
    if (floor.Held()) {
      break;
    }
    if (context.IsInterrupted()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_QUERY_CANCELED),
                      ERR_MSG("canceled while waiting for compaction on "
                              "search table ",
                              target.table_id));
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_search_backfill_floor_retry");
    absl::SleepFor(kArmRetry);
  }

  // Groups by byte budget: peak disk is ~2x one group, and each publish is
  // proportional to one group rather than the table. A zero budget is "no
  // limit" -- one group, one swap, the whole table's worth of peak disk.
  const bool unlimited = target.group_bytes == 0;
  bool counted = false;
  search::Tick rebuilt = 0;
  // One group per pass, and the reader that named it dies with the pass: it
  // references every segment it can see, so holding it across groups would
  // keep the ones already swapped out unreclaimable and make the budget above
  // bound nothing. A straddler that committed after the drain shows up on a
  // later pass as a sub-floor segment; loop until none do.
  for (;;) {
    const auto reader = shard.BuildSnapshot();
    std::vector<const irs::SubReader*> group;
    uint64_t live = 0;
    uint64_t bytes = 0;
    for (const auto& sub : reader) {
      const auto& segment = irs::utils::downCast<irs::SegmentReader>(sub);
      if (segment.GetImpl()->Id() > floor.Floor()) {
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
    rebuilt = RebuildGroup(context, target, group, progress);
    SDB_IF_FAILURE("crash_after_search_backfill_group") {
      SDB_IMMEDIATE_ABORT();
    }
  }
  RewriteOwnRows(context, target);
  if (!target.added.empty()) {
    auto& connection = ConnectionContext::Get(context);
    auto& transaction = connection.Transaction();
    transaction.SearchTxn().Rebuilt(
      target.shard, rebuilt,
      connection.IsStableSnapshot() &&
        shard.GetTable().LastWrite() > transaction.Snapshot());
  }
}

void RewriteOwnRows(duckdb::ClientContext& context,
                    const SearchBackfillTarget& target) {
  auto* writes = ConnectionContext::Get(context).Transaction().SearchWrites();
  if (!writes) {
    return;
  }
  RowSource source;
  InitRowSource(context, target, source);
  writes->RewriteOwn(
    context, target.shard, *target.catalog,
    [&](const irs::SubReader& sub, SearchSinkInsertBaseImpl& sink) {
      FeedSegment(context, sub, source, sink, target);
    });
}

}  // namespace sdb::connector
