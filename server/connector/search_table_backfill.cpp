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
#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/docs_mask/docs_mask.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/error_mapping.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <iterator>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/transaction.h"
#include "connector/constraint_keys.h"
#include "connector/full_scanner.h"
#include "connector/index_expression.hpp"
#include "connector/search_sink_rows.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "query/config_variable_names.h"
#include "search/store.h"
#include "search/writes.h"

namespace sdb::connector {
namespace {

struct RowSource {
  std::vector<irs::ColumnstoreProjection> projections;
  duckdb::DataChunk chunk;
  irs::ColFilterStateCache filter_states;
  duckdb::SelectionVector live{STANDARD_VECTOR_SIZE +
                               irs::doc_limits::kDocsSlack};
  std::vector<ColumnId> write_ids;
  std::vector<irs::field_id> copied;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions;
  std::optional<duckdb::ExpressionExecutor> executor;
  duckdb::DataChunk added;
  duckdb::DataChunk written;
};

struct ColumnPlan {
  absl::flat_hash_set<ColumnId> scanned;
  absl::flat_hash_set<ColumnId> copied;
  std::vector<ColumnId> carried;
};

ColumnPlan AddedReads(const SearchBackfillTarget& target) {
  ColumnPlan plan;
  for (const auto& expression : target.added) {
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundReferenceExpression>(
      *expression, [&](const duckdb::BoundReferenceExpression& reference) {
        plan.scanned.insert(target.column_ids[reference.Index()]);
      });
  }
  return plan;
}

ColumnPlan PlanColumns(const SearchSinkInsertBaseImpl& sink,
                       const catalog::InvertedIndexConfig& config,
                       const SearchBackfillTarget& target) {
  auto plan = AddedReads(target);
  for (const auto column : target.column_ids) {
    if (!config.AnnFields(column).empty() ||
        config.GetColumnOptions(column).distinct != irs::DistinctStats::None) {
      plan.scanned.insert(column);
      continue;
    }
    plan.copied.insert(column);
    if (!sink.TermFieldsForColumn(column).empty()) {
      plan.scanned.insert(column);
    }
  }
  for (const auto column : config.table_columns) {
    if (!absl::c_linear_search(target.column_ids, column) &&
        !absl::c_linear_search(target.added_ids, column)) {
      plan.carried.push_back(column);
    }
  }
  std::vector<KeyRead> expression_reads;
  for (const auto& expression : sink.IndexedExpressions()) {
    CollectReads(*expression.normalized_expr, expression_reads);
  }
  for (const auto& key : sink.ConstraintKeys()) {
    plan.scanned.insert(key.Key().columns.begin(), key.Key().columns.end());
    absl::c_move(key.ExpressionReads(), std::back_inserter(expression_reads));
  }
  for (const auto& read : expression_reads) {
    plan.scanned.insert(read.column);
  }
  return plan;
}

// In place: RowSource owns a DataChunk and a selection vector, neither
// copyable.
void InitRowSource(duckdb::ClientContext& context,
                   const SearchBackfillTarget& target, RowSource& source,
                   const ColumnPlan* plan = nullptr) {
  duckdb::vector<duckdb::LogicalType> types;
  std::vector<duckdb::idx_t> slots(target.column_ids.size(),
                                   duckdb::DConstants::INVALID_INDEX);
  for (size_t i = 0; i < target.column_ids.size(); ++i) {
    const auto column = target.column_ids[i];
    if (plan && plan->copied.contains(column)) {
      source.copied.push_back(column);
    }
    if (plan && !plan->scanned.contains(column)) {
      continue;
    }
    slots[i] = types.size();
    source.projections.emplace_back(irs::ColumnstoreProjection{
      .output_slot = types.size(), .column_id = column});
    source.write_ids.push_back(column);
    types.push_back(target.column_types[i]);
  }
  if (plan) {
    absl::c_copy(plan->carried, std::back_inserter(source.copied));
  }
  auto& allocator = duckdb::Allocator::Get(context);
  source.chunk.Initialize(allocator, types);
  if (target.added.empty()) {
    return;
  }
  source.write_ids.insert(source.write_ids.end(), target.added_ids.begin(),
                          target.added_ids.end());
  source.executor.emplace(context);
  duckdb::vector<duckdb::LogicalType> added_types;
  for (const auto& added : target.added) {
    auto& expression = source.expressions.emplace_back(added->Copy());
    duckdb::ExpressionIterator::VisitExpressionMutable<
      duckdb::BoundReferenceExpression>(
      expression, [&](duckdb::BoundReferenceExpression& reference, auto&) {
        SDB_ASSERT(slots[reference.Index()] !=
                   duckdb::DConstants::INVALID_INDEX);
        reference.IndexMutable() = slots[reference.Index()];
      });
    source.executor->AddExpression(*expression);
    added_types.push_back(expression->GetReturnType());
    types.push_back(expression->GetReturnType());
  }
  source.added.Initialize(allocator, added_types);
  source.written.InitializeEmpty(types);
}

void CopyLiveRows(irs::IndexWriter::Transaction& trx,
                  const irs::ColReader& from,
                  std::span<const irs::field_id> columns, uint64_t row,
                  uint64_t count, std::span<const duckdb::sel_t> live) {
  if (live.empty()) {
    trx.CopyRows(from, columns, row, count);
    return;
  }
  for (size_t i = 0; i < live.size();) {
    auto j = i + 1;
    while (j < live.size() && live[j] == live[j - 1] + 1) {
      ++j;
    }
    trx.CopyRows(from, columns, row + live[i], j - i);
    i = j;
  }
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

// Feeds every live row of `sub` in [begin, end) into `sink`. A range with
// deletes is scanned in full and sliced down to the unmasked rows, which keeps
// this off the scan operator's hit-batching machinery.
uint64_t FeedSegment(duckdb::ClientContext& context, const irs::SubReader& sub,
                     uint64_t begin, uint64_t end, RowSource& source,
                     SearchSinkInsertBaseImpl& sink,
                     const SearchBackfillTarget& target,
                     irs::IndexWriter::Transaction* trx = nullptr) {
  uint64_t fed = 0;
  ScanLiveRows(context, sub, begin, end, source.projections,
               source.filter_states, source.live, source.chunk,
               [&](duckdb::DataChunk& chunk, uint64_t row,
                   std::span<const duckdb::sel_t> live) {
                 WriteChunkToSearchSink(sink, Rewritten(source, chunk),
                                        source.write_ids, target.table_id,
                                        context, source.copied);
                 if (!source.copied.empty()) {
                   SDB_ASSERT(trx);
                   CopyLiveRows(*trx, *sub.GetColReader(), source.copied, row,
                                chunk.size(), live);
                 }
                 fed += chunk.size();
                 return true;
               });
  return fed;
}

struct Piece {
  const irs::SubReader* segment = nullptr;
  uint64_t begin = 0;
  uint64_t end = 0;
};

struct Slice {
  RowSource source;
  irs::IndexWriter::Transaction trx;
  std::unique_ptr<SearchSinkInsertBaseImpl> sink;
  std::vector<Piece> pieces;
  std::span<const irs::IndexWriter::FlushedSegment> flushed;
};

void AppendReplacements(const Slice& slice,
                        std::vector<irs::IndexWriter::Replacement>& out) {
  const auto first = out.size();
  for (const auto& flushed : slice.flushed) {
    out.push_back({.meta = flushed.meta, .reader = flushed.reader});
  }
  size_t i = 0;
  uint64_t used = 0;
  for (const auto& piece : slice.pieces) {
    const auto& segment =
      *irs::utils::downCast<irs::SegmentReader>(*piece.segment).GetImpl();
    const auto* mask = piece.segment->docs_mask();
    const auto removed = [&](uint64_t from, uint64_t to) -> uint64_t {
      return mask ? mask->CountInRange(
                      static_cast<irs::doc_id_t>(irs::doc_limits::min() + from),
                      static_cast<irs::doc_id_t>(irs::doc_limits::min() + to))
                  : 0;
    };
    const uint64_t begin = piece.begin - removed(0, piece.begin);
    const uint64_t live =
      begin + (piece.end - piece.begin) - removed(piece.begin, piece.end);
    for (uint64_t rank = begin; rank < live;) {
      SDB_ENSURE(i < slice.flushed.size(),
                 "index build: fed more rows than its segments hold");
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
             "index build: fed fewer rows than its segments hold");
}

bool StartsBlock(const irs::ColumnReader& column, uint64_t row) {
  return row >= column.RowCount() || column.InMemory() ||
         column.DataBlocks().empty() || column.Locate(row).begin == row;
}

uint64_t BlockBoundaryNear(const irs::ColReader& columns,
                           std::span<const irs::field_id> copied, uint64_t row,
                           uint64_t docs) {
  const irs::ColumnReader* coarsest = nullptr;
  for (const auto id : copied) {
    const auto* column = columns.Column(id);
    if (column && !column->InMemory() && !column->DataBlocks().empty() &&
        row < column->RowCount() &&
        (!coarsest ||
         column->DataBlocks().size() < coarsest->DataBlocks().size())) {
      coarsest = column;
    }
  }
  if (!coarsest) {
    return row;
  }
  const auto common = [&](uint64_t at) {
    return absl::c_all_of(copied, [&](irs::field_id id) {
      const auto* column = columns.Column(id);
      return !column || StartsBlock(*column, at);
    });
  };
  const auto window = coarsest->Locate(row);
  auto down = window.begin;
  auto up = std::min(window.end, docs);
  for (;;) {
    const bool lower = row - down <= up - row;
    if (const auto at = lower ? down : up; common(at)) {
      return at;
    }
    if (lower) {
      down = coarsest->Locate(down - 1).begin;
    } else {
      up = up < coarsest->RowCount() ? std::min(coarsest->Locate(up).end, docs)
                                     : docs;
    }
  }
}

// By rows, not by segment count: a group is only as fast as its heaviest
// slice, and one big segment would otherwise leave every other thread idle.
std::vector<std::vector<Piece>> CutPieces(
  std::span<const irs::SubReader* const> group, size_t slices,
  std::span<const irs::field_id> copied) {
  uint64_t total = 0;
  for (const auto* sub : group) {
    total += sub->Meta().docs_count;
  }
  std::vector<uint64_t> cuts;
  cuts.reserve(slices - 1);
  uint64_t start = 0;
  for (const auto* sub : group) {
    const uint64_t docs = sub->Meta().docs_count;
    for (auto target = (cuts.size() + 1) * total / slices;
         cuts.size() + 1 < slices && target < start + docs;
         target = (cuts.size() + 1) * total / slices) {
      cuts.push_back(start + BlockBoundaryNear(*sub->GetColReader(), copied,
                                               target - start, docs));
    }
    start += docs;
  }
  cuts.resize(slices - 1, total);
  std::vector<std::vector<Piece>> out(slices);
  size_t slice = 0;
  start = 0;
  for (const auto* sub : group) {
    const uint64_t docs = sub->Meta().docs_count;
    for (uint64_t row = 0; row < docs;) {
      while (slice < cuts.size() && cuts[slice] <= start + row) {
        ++slice;
      }
      const auto end =
        slice < cuts.size() ? std::min(docs, cuts[slice] - start) : docs;
      out[slice].push_back({.segment = sub, .begin = row, .end = end});
      row = end;
    }
    start += docs;
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
    if (next < slice.pieces.size()) {
      const auto& piece = slice.pieces[next++];
      const auto fed =
        FeedSegment(context, *piece.segment, piece.begin, piece.end,
                    slice.source, *slice.sink, target, &slice.trx);
      if (progress) {
        progress->AddTuples(static_cast<int64_t>(fed));
        SDB_WAIT_ON_FAILURE("pause_create_index_mid_build");
      }
      context.InterruptCheck();
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
std::shared_ptr<const search::TableState> RebuildGroup(
  duckdb::ClientContext& context, const SearchBackfillTarget& target,
  std::span<const irs::SubReader* const> group, pg::ProgressMetrics* progress) {
  auto& shard = *target.shard;
  uint64_t rows = 0;
  for (const auto* sub : group) {
    rows += sub->Meta().docs_count;
  }
  const uint64_t slice_rows = std::max<uint64_t>(1, shard.RowGroupSize());
  const auto slice_count = static_cast<size_t>(
    std::clamp<uint64_t>((rows + slice_rows - 1) / slice_rows, 1,
                         duckdb::TaskScheduler::QueryThreads(context)));

  // Built here, never on a worker: the sink factory reads the catalog and
  // InitRowSource takes the context allocator.
  std::vector<std::unique_ptr<Slice>> slices;
  slices.reserve(slice_count);
  std::optional<ColumnPlan> plan;
  for (size_t k = 0; k < slice_count; ++k) {
    auto slice = std::make_unique<Slice>();
    slice->trx = shard.GetTransaction();
    slice->sink =
      MakeSearchTableInsertSink(slice->trx, shard, *target.catalog, context)
        .sink;
    if (!plan) {
      plan = PlanColumns(*slice->sink, *shard.Config(), target);
    }
    InitRowSource(context, target, slice->source, &*plan);
    slices.push_back(std::move(slice));
  }
  auto assignment =
    CutPieces(group, slice_count, slices.front()->source.copied);
  for (size_t k = 0; k < slice_count; ++k) {
    slices[k]->pieces = std::move(assignment[k]);
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
      if (slice->pieces.empty()) {
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
  std::shared_ptr<const search::TableState> published;
  try {
    published = shard.ReplaceSegments(group, std::move(adopted));
  } catch (const std::exception& e) {
    irs::pg::ThrowWithContext(
      absl::StrCat(
        "index build: failed to swap in the rebuilt segments of table ",
        target.table_id),
      e);
  }
  abort_all();
  SDB_PARK_ONCE_ON_FAILURE("pause_search_backfill_before_publish");
  shard.WriteOut();
  return published;
}

constexpr size_t kMaxAttachments = 8;

struct AttachJob {
  const irs::SubReader* segment = nullptr;
  std::shared_ptr<const irs::SegmentReaderImpl> rows;
  uint64_t begin = 0;
  uint64_t end = 0;
  bool index = false;
  RowSource source;
  irs::IndexWriter::Transaction trx;
  std::unique_ptr<SearchSinkInsertBaseImpl> sink;
  std::span<const irs::IndexWriter::FlushedSegment> flushed;
};

bool LacksFields(const SearchSinkInsertBaseImpl& sink,
                 const SearchBackfillTarget& target,
                 const irs::SubReader& segment) {
  const auto lacks = [&](irs::field_id field) {
    return segment.fields(field).empty();
  };
  const auto column_lacks = [&](ColumnId column) {
    return absl::c_any_of(sink.TermFieldsForColumn(column), lacks);
  };
  return absl::c_any_of(target.column_ids, column_lacks) ||
         absl::c_any_of(target.added_ids, column_lacks) ||
         absl::c_any_of(sink.IndexedExpressions(),
                        [&](const auto& expression) {
                          return lacks(expression.field_id);
                        }) ||
         absl::c_any_of(sink.ConstraintKeys(), [&](const auto& key) {
           return lacks(key.Key().field);
         });
}

struct FeedAttachTask final : duckdb::BaseExecutorTask {
  FeedAttachTask(duckdb::TaskExecutor& executor_in,
                 duckdb::ClientContext& context_in,
                 const SearchBackfillTarget& target_in, AttachJob& job_in,
                 pg::ProgressMetrics* progress_in)
    : BaseExecutorTask{executor_in},
      context{context_in},
      target{target_in},
      job{job_in},
      progress{progress_in} {}

  void ExecuteTask() final {
    uint64_t fed = 0;
    auto& source = job.source;
    ScanLiveRows(
      context, *job.rows, job.begin, job.end, source.projections,
      source.filter_states, source.live, source.chunk,
      [&](duckdb::DataChunk& chunk, uint64_t, std::span<const duckdb::sel_t>) {
        WriteChunkToSearchSink(*job.sink, Rewritten(source, chunk),
                               source.write_ids, target.table_id, context, {},
                               job.rows.get());
        fed += chunk.size();
        return true;
      });
    if (progress) {
      progress->AddTuples(static_cast<int64_t>(fed));
    }
    context.InterruptCheck();
    job.flushed = job.trx.FlushAndFsync();
  }

  std::string TaskType() const final { return "SearchBackfillAttach"; }

  duckdb::ClientContext& context;
  const SearchBackfillTarget& target;
  AttachJob& job;
  pg::ProgressMetrics* progress;
};

bool Attachable(const SearchBackfillTarget& target,
                const catalog::InvertedIndexConfig& config) {
  return absl::c_none_of(target.column_ids, [&](ColumnId column) {
    const auto options = config.GetColumnOptions(column);
    const auto declared = config.declared_compression.find(column);
    return !config.AnnFields(column).empty() ||
           options.distinct != irs::DistinctStats::None ||
           options.compression != (declared == config.declared_compression.end()
                                     ? duckdb::CompressionType::COMPRESSION_AUTO
                                     : declared->second);
  });
}

std::shared_ptr<const search::TableState> AttachGroup(
  duckdb::ClientContext& context, const SearchBackfillTarget& target,
  std::span<const irs::SubReader* const> group, pg::ProgressMetrics* progress) {
  auto& shard = *target.shard;
  std::vector<std::unique_ptr<AttachJob>> jobs;
  std::optional<ColumnPlan> indexed;
  const auto added = AddedReads(target);
  const uint64_t threads = duckdb::TaskScheduler::QueryThreads(context);
  const uint64_t row_group = std::max<uint64_t>(1, shard.RowGroupSize());
  for (const auto* segment : group) {
    auto rows = irs::utils::downCast<irs::SegmentReader>(*segment)
                  .GetImpl()
                  ->WithDocsMask(nullptr);
    const uint64_t docs = rows->docs_count();
    uint64_t step = docs;
    bool index = false;
    for (uint64_t begin = 0; begin < docs; begin += step) {
      auto& job = *jobs.emplace_back(std::make_unique<AttachJob>());
      job.segment = segment;
      job.rows = rows;
      job.begin = begin;
      job.trx = shard.GetTransaction();
      job.sink =
        MakeSearchTableInsertSink(job.trx, shard, *target.catalog, context)
          .sink;
      if (begin == 0) {
        index = LacksFields(*job.sink, target, *rows);
        if (index) {
          const auto slices = std::clamp<uint64_t>(
            (docs + row_group - 1) / row_group, 1, threads);
          step = ((docs + slices - 1) / slices + row_group - 1) / row_group *
                 row_group;
        }
      }
      job.end = std::min(docs, begin + step);
      job.index = index;
      if (job.index && !indexed) {
        indexed = PlanColumns(*job.sink, *shard.Config(), target);
        indexed->copied.clear();
        indexed->carried.clear();
      }
      InitRowSource(context, target, job.source,
                    job.index ? &*indexed : &added);
    }
  }
  absl::Cleanup abort_jobs = [&] {
    for (auto& job : jobs) {
      job->trx.Abort();
    }
  };
  {
    duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(context),
                                  context.GetSchedulerAccount()};
    for (auto& job : jobs) {
      executor.ScheduleTask(duckdb::make_uniq<FeedAttachTask>(
        executor, context, target, *job, progress));
    }
    executor.WorkOnTasks();
  }
  SDB_WAIT_ON_FAILURE("pause_search_backfill_before_swap");
  std::vector<search::Table::Attachment> attachments;
  attachments.reserve(group.size());
  std::vector<const irs::IndexWriter::FlushedSegment*> parts;
  for (size_t first = 0; first < jobs.size();) {
    const auto& rows = *jobs[first]->rows;
    parts.clear();
    auto last = first;
    for (; last < jobs.size() && jobs[last]->segment == jobs[first]->segment;
         ++last) {
      for (const auto& part : jobs[last]->flushed) {
        parts.push_back(&part);
      }
    }
    first = last;
    if (parts.empty()) {
      continue;
    }
    auto joined = shard.JoinSegments(parts);
    if (joined.meta.kinds == 0) {
      continue;
    }
    SDB_ENSURE(joined.meta.docs_count == rows.docs_count(),
               "index build: segment ", rows.Id(), " holds ", rows.docs_count(),
               " rows, its attachment ", joined.meta.docs_count);
    uint64_t id = 0;
    SDB_ENSURE(irs::ParseSegmentId(joined.meta.name, id),
               "index build: attachment '", joined.meta.name,
               "' has no segment id");
    attachments.push_back({
      .segment = rows.Id(),
      .files = {.id = id, .kinds = joined.meta.kinds},
      .reader = std::move(joined.reader),
    });
  }
  std::shared_ptr<const search::TableState> published;
  try {
    published = shard.AttachSegments(attachments);
  } catch (const std::exception& e) {
    irs::pg::ThrowWithContext(
      absl::StrCat("index build: failed to attach the new fields of table ",
                   target.table_id),
      e);
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_search_backfill_before_publish");
  return published;
}

void AddLiveRows(irs::DocRemovalBuilder& positions, size_t index, uint64_t row,
                 uint64_t count, std::span<const duckdb::sel_t> live) {
  const auto add = [&](uint64_t first, uint64_t end) {
    positions.AddRange(
      static_cast<uint32_t>(index),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + first),
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + end));
  };
  if (live.empty()) {
    add(row, row + count);
    return;
  }
  for (size_t i = 0; i < live.size();) {
    auto j = i + 1;
    while (j < live.size() && live[j] == live[j - 1] + 1) {
      ++j;
    }
    add(row + live[i], row + live[j - 1] + 1);
    i = j;
  }
}

void RewriteIndexedPatches(duckdb::ClientContext& context,
                           const SearchBackfillTarget& target) {
  auto& writes = ConnectionContext::Get(context).Transaction().SearchTxn();
  SearchSinkTable table{.store = target.shard,
                        .catalog = target.catalog,
                        .table_id = target.table_id,
                        .column_ids = target.column_ids,
                        .types = target.column_types,
                        .buffer_bytes = target.shard->InlineRowsMax()};
  SearchSinkRows rows;
  irs::DocRemovalBuilder positions;
  std::optional<RowSource> source;
  writes.TakeIndexedPatches(
    context, *target.shard,
    [&](const irs::DirectoryReader& reader, size_t index,
        const irs::DocumentMask& docs) {
      if (!source) {
        InitRowSource(context, target, source.emplace());
      }
      irs::DocumentMask::Cursor cursor{docs};
      std::array<irs::MaskRun, 64> runs;
      while (const auto count = cursor.ReadRuns(runs)) {
        for (const auto& run : std::span{runs}.first(count)) {
          ScanLiveRows(context, reader[index], run.min - irs::doc_limits::min(),
                       run.max + 1 - irs::doc_limits::min(),
                       source->projections, source->filter_states, source->live,
                       source->chunk,
                       [&](duckdb::DataChunk& chunk, uint64_t row,
                           std::span<const duckdb::sel_t> live) {
                         rows.Append(context, table, chunk);
                         AddLiveRows(positions, index, row, chunk.size(), live);
                         return true;
                       });
        }
      }
    });
  if (positions.Empty()) {
    return;
  }
  rows.Flush();
  writes.AddRemoval(context, target.shard, std::move(positions));
  rows.HandOver(context, table, writes);
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

  if (target.added.empty()) {
    RewriteIndexedPatches(context, target);
  }
  shard.WriteOut();
  const auto floor = shard.ArmCompactionFloor(context);

  // Groups by byte budget: peak disk is ~2x one group, and each publish is
  // proportional to one group rather than the table. A zero budget is "no
  // limit" -- one group, one swap, the whole table's worth of peak disk.
  const bool unlimited = target.group_bytes == 0;
  bool counted = false;
  auto& connection = ConnectionContext::Get(context);
  auto& transaction = connection.Transaction();
  const auto newer = [&] {
    return connection.IsStableSnapshot() &&
           shard.GetTable().LastWrite() > transaction.Snapshot();
  };
  const bool adopt = !target.added.empty() || !newer();
  const auto config = shard.Config();
  const bool attachable = Attachable(target, *config);
  const uint64_t small = shard.InlineRowsMax();
  const auto lacks_key = [&](const irs::SubReader& sub) {
    return absl::c_any_of(config->constraint_keys, [&](const auto& key) {
      return sub.fields(key.field).empty();
    });
  };
  absl::flat_hash_set<uint64_t> attached;
  std::shared_ptr<const search::TableState> rebuilt;
  // One group per pass, and the reader that named it dies with the pass: it
  // references every segment it can see, so holding it across groups would
  // keep the ones already swapped out unreclaimable and make the budget above
  // bound nothing. A straddler that committed after the version bump shows up
  // on a later pass as a sub-floor segment; loop until none do.
  for (;;) {
    const auto reader = shard.BuildSnapshot();
    const auto state = shard.GetTable().State();
    const auto attachments = [&](uint64_t id) {
      const auto it = absl::c_find_if(
        state->units, [&](const auto& unit) { return unit->id == id; });
      return it == state->units.end() ? kMaxAttachments
                                      : (*it)->meta.attachments.size();
    };
    std::vector<const irs::SubReader*> group;
    std::vector<const irs::SubReader*> attach;
    uint64_t live = 0;
    uint64_t bytes = 0;
    for (const auto& sub : reader) {
      const auto id =
        irs::utils::downCast<irs::SegmentReader>(sub).GetImpl()->Id();
      if (id > floor.Floor() || attached.contains(id)) {
        continue;
      }
      live += sub.live_docs_count();
      if (!unlimited && bytes >= target.group_bytes) {
        continue;
      }
      bytes += sub.Meta().byte_size;
      if (attachable && sub.Meta().byte_size >= small &&
          attachments(id) < kMaxAttachments && !lacks_key(sub)) {
        attach.push_back(&sub);
      } else {
        group.push_back(&sub);
      }
    }
    if (group.empty() && attach.empty()) {
      break;
    }
    if (progress && !counted) {
      // Exact, unlike the transactional path's planner estimate.
      pg::ProgressMetrics::Set(progress->tuples_total,
                               static_cast<int64_t>(live));
      counted = true;
    }
    if (adopt && !rebuilt) {
      transaction.ReleaseVersions(target.shard);
    }
    if (!attach.empty()) {
      rebuilt = AttachGroup(context, target, attach, progress);
      for (const auto* sub : attach) {
        attached.insert(
          irs::utils::downCast<irs::SegmentReader>(*sub).GetImpl()->Id());
      }
    }
    if (!group.empty()) {
      rebuilt = RebuildGroup(context, target, group, progress);
    }
    SDB_IF_FAILURE("crash_after_search_backfill_group") {
      SDB_IMMEDIATE_ABORT();
    }
  }
  RewriteOwnRows(context, target);
  if (adopt && rebuilt) {
    transaction.SearchTxn().Rebuilt(target.shard, std::move(rebuilt), newer());
  }
}

void RewriteOwnRows(duckdb::ClientContext& context,
                    const SearchBackfillTarget& target) {
  RowSource source;
  InitRowSource(context, target, source);
  ConnectionContext::Get(context).Transaction().SearchTxn().RewriteOwn(
    context, target.shard, *target.catalog,
    [&](const irs::SubReader& sub, SearchSinkInsertBaseImpl& sink) {
      FeedSegment(context, sub, 0, sub.Meta().docs_count, source, sink, target);
    });
}

}  // namespace sdb::connector
