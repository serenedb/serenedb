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

#include "iresearch/index/merge/pipeline_merge_executor.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <atomic>
#include <duckdb/common/arena_linked_list.hpp>
#include <duckdb/common/enums/set_scope.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/mutex.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/operator/order/physical_presorted_merge.hpp>
#include <duckdb/execution/operator/set/physical_union.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_profiler.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>
#include <duckdb/planner/logical_plan_verifier.hpp>
#include <memory>
#include <span>
#include <string>
#include <utility>

#include "iresearch/index/merge/logical_presorted_merge.hpp"
#include "iresearch/index/merge/merge_schedule.hpp"
#include "iresearch/index/merge/term_ranges.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/misc.hpp"

namespace irs {
namespace {

using Guard = duckdb::annotated_lock_guard<duckdb::annotated_mutex>;

struct MergeOrderSinkState final : duckdb::LocalSinkState {
  duckdb::vector<duckdb::PresortedRun> runs;
};

struct PostingsMergeGlobalState final : duckdb::GlobalSinkState {
  explicit PostingsMergeGlobalState(size_t parts) noexcept
    : parts{parts}, left{parts} {}

  size_t parts;
  std::atomic<size_t> left;
};

struct PostingsMergeSinkState final : duckdb::LocalSinkState {
  std::unique_ptr<TermRangeWriter> writer;
  uint64_t partition = 0;
  duckdb::vector<duckdb::PresortedRun> runs;
};

class MergeUnitsSourceState final : public duckdb::GlobalSourceState {
 public:
  MergeUnitsSourceState(std::shared_ptr<MergeSchedule> schedule,
                        duckdb::idx_t threads)
    : _schedule{std::move(schedule)}, _threads{threads} {
    _schedule->SetUnits(this);
  }

  ~MergeUnitsSourceState() final { _schedule->SetUnits(nullptr); }

  duckdb::idx_t MaxThreads() final { return _threads; }

 private:
  std::shared_ptr<MergeSchedule> _schedule;
  duckdb::idx_t _threads;
};

void ReadRuns(duckdb::DataChunk& chunk,
              duckdb::vector<duckdb::PresortedRun>& runs) {
  const auto* inputs = duckdb::FlatVector::GetData<uint64_t>(chunk.data[1]);
  const auto* begins = duckdb::FlatVector::GetData<uint64_t>(chunk.data[2]);
  const auto* counts = duckdb::FlatVector::GetData<uint64_t>(chunk.data[3]);
  runs.clear();
  for (duckdb::idx_t i = 0; i != chunk.size(); ++i) {
    runs.push_back(
      {.input = inputs[i], .begin = begins[i], .count = counts[i]});
  }
}

class PhysicalMergeOrder final : public duckdb::PhysicalOperator {
 public:
  PhysicalMergeOrder(duckdb::PhysicalPlan& physical_plan, MergePlan& plan)
    : duckdb::PhysicalOperator(physical_plan,
                               duckdb::PhysicalOperatorType::EXTENSION,
                               {duckdb::LogicalType::BIGINT}, 0),
      _plan{plan} {}

  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return true; }
  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext&) const final {
    return duckdb::make_uniq<MergeOrderSinkState>();
  }
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final {
    auto& state = input.local_state.Cast<MergeOrderSinkState>();
    const auto* partitions =
      duckdb::FlatVector::GetData<uint64_t>(chunk.data[0]);
    ReadRuns(chunk, state.runs);
    SDB_ASSERT(absl::c_all_of(
      std::span{partitions, chunk.size()},
      [&](uint64_t partition) { return partition == partitions[0]; }));
    if (!_plan.ExpandOrder(partitions[0], state.runs)) {
      context.client.Interrupt();
    }
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  bool IsSource() const final { return true; }
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext&, duckdb::DataChunk&,
    duckdb::OperatorSourceInput&) const final {
    return duckdb::SourceResultType::FINISHED;
  }

  duckdb::string GetName() const final { return "MERGE_ORDER"; }

 private:
  MergePlan& _plan;
};

class PhysicalPostingsMerge final : public duckdb::PhysicalOperator {
 public:
  PhysicalPostingsMerge(duckdb::PhysicalPlan& physical_plan,
                        TermRangeMerge& merge,
                        std::shared_ptr<MergeSchedule> schedule)
    : duckdb::PhysicalOperator(physical_plan,
                               duckdb::PhysicalOperatorType::EXTENSION,
                               {duckdb::LogicalType::BIGINT}, 0),
      _merge{merge},
      _schedule{std::move(schedule)} {}

  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return true; }
  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext&) const final {
    return duckdb::make_uniq<PostingsMergeGlobalState>(_merge.PartsLeft());
  }
  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext&) const final {
    return duckdb::make_uniq<PostingsMergeSinkState>();
  }
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final {
    if (chunk.size() == 0) {
      return duckdb::SinkResultType::NEED_MORE_INPUT;
    }
    auto& state = input.local_state.Cast<PostingsMergeSinkState>();
    const auto* partitions =
      duckdb::FlatVector::GetData<uint64_t>(chunk.data[0]);
    if (state.writer && state.partition != partitions[0]) {
      FinishPartition(input.global_state, state);
    }
    if (!state.writer) {
      state.partition = partitions[0];
      state.writer = _merge.BeginPart(state.partition);
    }
    ReadRuns(chunk, state.runs);
    SDB_ASSERT(absl::c_all_of(
      std::span{partitions, chunk.size()},
      [&](uint64_t partition) { return partition == state.partition; }));
    if (!state.writer->Consume(state.runs)) {
      context.client.Interrupt();
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_postings_merge_sink");
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  duckdb::SinkCombineResultType Combine(
    duckdb::ExecutionContext&,
    duckdb::OperatorSinkCombineInput& input) const final {
    FinishPartition(input.global_state, input.local_state);
    return duckdb::SinkCombineResultType::FINISHED;
  }
  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext&,
    duckdb::OperatorSinkFinalizeInput& input) const final {
    SDB_PARK_ONCE_ON_FAILURE("pause_postings_merge_finalize");
    if (input.global_state.Cast<PostingsMergeGlobalState>().parts == 0) {
      _schedule->RangeDone();
    }
    return duckdb::SinkFinalizeType::READY;
  }

  bool IsSource() const final { return true; }
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext&, duckdb::DataChunk&,
    duckdb::OperatorSourceInput&) const final {
    return duckdb::SourceResultType::FINISHED;
  }

  duckdb::string GetName() const final { return "POSTINGS_MERGE"; }
  duckdb::InsertionOrderPreservingMap<duckdb::string> ParamsToString()
    const final {
    duckdb::InsertionOrderPreservingMap<duckdb::string> result;
    result["Field"] = std::to_string(_merge.Field());
    result["Parts"] = std::to_string(_merge.PartCount());
    return result;
  }

 private:
  void FinishPartition(duckdb::GlobalSinkState& global,
                       duckdb::LocalSinkState& local) const {
    auto& state = local.Cast<PostingsMergeSinkState>();
    if (!state.writer) {
      return;
    }
    _merge.FinishPart(state.partition, *state.writer);
    state.writer.reset();
    if (global.Cast<PostingsMergeGlobalState>().left.fetch_sub(1) == 1) {
      _schedule->RangeDone();
      return;
    }
    _schedule->PartDone();
  }

  TermRangeMerge& _merge;
  std::shared_ptr<MergeSchedule> _schedule;
};

class PhysicalMergeUnits final : public duckdb::PhysicalOperator {
 public:
  PhysicalMergeUnits(duckdb::PhysicalPlan& physical_plan,
                     std::shared_ptr<MergeSchedule> schedule,
                     duckdb::idx_t threads)
    : duckdb::PhysicalOperator(physical_plan,
                               duckdb::PhysicalOperatorType::EXTENSION,
                               {duckdb::LogicalType::BIGINT}, 0),
      _schedule{std::move(schedule)},
      _threads{threads} {}

  bool IsSource() const final { return true; }
  bool ParallelSource() const final { return true; }
  duckdb::OrderPreservationType SourceOrder() const final {
    return duckdb::OrderPreservationType::NO_ORDER;
  }
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext&) const final {
    return duckdb::make_uniq<MergeUnitsSourceState>(_schedule, _threads);
  }
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk&,
    duckdb::OperatorSourceInput& input) const final {
    auto& state = input.global_state.Cast<MergeUnitsSourceState>();
    auto unit = MergeSchedule::kNoUnit;
    {
      Guard guard{state.lock};
      if (_schedule->Done()) {
        return duckdb::SourceResultType::FINISHED;
      }
      if (_schedule->MayClaim(_threads)) {
        unit = _schedule->Claim();
      }
      if (unit == MergeSchedule::kNoUnit) {
        return state.BlockSource(input.interrupt_state);
      }
    }
    if (!_schedule->Run(unit)) {
      context.client.Interrupt();
      SDB_PARK_ONCE_ON_FAILURE("pause_merge_unit_failed");
    }
    Guard guard{state.lock};
    state.UnblockTasks();
    return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
  }

  duckdb::string GetName() const final { return "MERGE_UNITS"; }

 private:
  std::shared_ptr<MergeSchedule> _schedule;
  duckdb::idx_t _threads;
};

class LogicalMergeStep : public duckdb::LogicalExtensionOperator {
 public:
  explicit LogicalMergeStep(duckdb::TableIndex table_index)
    : _table_index{table_index} {}

  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final {
    return GenerateColumnBindings(_table_index, 1);
  }
  duckdb::vector<duckdb::TableIndex> GetTableIndex() const final {
    return {_table_index};
  }
  bool RequireOptimizer() const final { return false; }
  bool SupportSerialization() const final { return false; }

 protected:
  void ResolveTypes() final { types = {duckdb::LogicalType::BIGINT}; }

 private:
  duckdb::TableIndex _table_index;
};

class LogicalMergeOrder final : public LogicalMergeStep {
 public:
  explicit LogicalMergeOrder(MergePlan& plan)
    : LogicalMergeStep{duckdb::TableIndex(1)}, _plan{plan} {
    children.push_back(
      duckdb::make_uniq<LogicalPresortedMerge>(plan.KeyMerge()));
  }

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) final {
    auto& runs = planner.CreatePlan(*children[0]);
    auto& order = planner.Make<PhysicalMergeOrder>(_plan);
    order.children.push_back(runs);
    return order;
  }
  duckdb::string GetName() const final { return "MERGE_ORDER"; }

 private:
  MergePlan& _plan;
};

class LogicalPostingsMerge final : public LogicalMergeStep {
 public:
  LogicalPostingsMerge(TermRangeMerge& merge, duckdb::TableIndex table_index,
                       std::shared_ptr<MergeSchedule> schedule)
    : LogicalMergeStep{table_index},
      _merge{merge},
      _schedule{std::move(schedule)} {
    children.push_back(duckdb::make_uniq<LogicalPresortedMerge>(
      merge.KeyMerge(), duckdb::TableIndex(table_index.index + 1)));
  }

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) final {
    auto& runs = planner.CreatePlan(*children[0]);
    auto& merge = planner.Make<PhysicalPostingsMerge>(_merge, _schedule);
    merge.children.push_back(runs);
    return merge;
  }
  duckdb::string GetName() const final { return "POSTINGS_MERGE"; }

 private:
  TermRangeMerge& _merge;
  std::shared_ptr<MergeSchedule> _schedule;
};

class LogicalMergeUnits final : public LogicalMergeStep {
 public:
  LogicalMergeUnits(std::shared_ptr<MergeSchedule> schedule,
                    duckdb::idx_t threads,
                    std::span<const std::unique_ptr<TermRangeMerge>> ranges)
    : LogicalMergeStep{duckdb::TableIndex(0)},
      _schedule{std::move(schedule)},
      _threads{threads} {
    for (size_t i = 0; i != ranges.size(); ++i) {
      children.push_back(duckdb::make_uniq<LogicalPostingsMerge>(
        *ranges[i], duckdb::TableIndex(1 + 2 * i), _schedule));
    }
  }

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) final {
    auto& units = planner.Make<PhysicalMergeUnits>(_schedule, _threads);
    if (children.empty()) {
      return units;
    }
    duckdb::ArenaLinkedList<duckdb::reference<duckdb::PhysicalOperator>> plans{
      planner.ArenaRef()};
    for (auto& child : children) {
      plans.push_back(planner.CreatePlan(*child));
    }
    plans.push_back(units);
    return planner.Make<duckdb::PhysicalUnion>(types, plans, 0, true);
  }
  duckdb::string GetName() const final { return "MERGE_UNITS"; }

 private:
  std::shared_ptr<MergeSchedule> _schedule;
  duckdb::idx_t _threads;
};

void TakeProfile(duckdb::ClientContext& context,
                 PipelineMergeExecutor::Profiles& profiles) {
  duckdb::QueryProfiler::Get(context).GetRootUnderLock(
    [&](duckdb::optional_ptr<duckdb::ProfilingNode> node) {
      while (node &&
             node->GetOperatorMetrics().operator_type ==
               duckdb::PhysicalOperatorType::RESULT_COLLECTOR &&
             node->GetChildCount() == 1) {
        node = node->GetChild(0);
      }
      if (!node) {
        return;
      }
      auto& taken =
        profiles.emplace_back(duckdb::make_uniq<duckdb::ProfilingNode>());
      taken->GetOperatorMetrics() = node->GetOperatorMetrics();
      taken->children = std::move(node->children);
    });
}

bool Execute(duckdb::Connection& connection,
             duckdb::unique_ptr<duckdb::LogicalOperator> root,
             PipelineMergeExecutor::Profiles* profiles) {
  root->ResolveOperatorTypes();
  const auto result = connection.Query(
    duckdb::make_uniq<duckdb::LogicalPlanStatement>(std::move(root)));
  if (profiles) {
    TakeProfile(*connection.context, *profiles);
  }
  if (result->HasError() &&
      result->GetErrorType() != duckdb::ExceptionType::INTERRUPT) {
    result->ThrowError();
  }
  return !result->HasError();
}

}  // namespace

LogicalPresortedMerge::LogicalPresortedMerge(
  duckdb::shared_ptr<duckdb::PresortedMerge> merge,
  duckdb::TableIndex table_index)
  : _merge{std::move(merge)}, _table_index{table_index} {}

void LogicalPresortedMerge::ResolveTypes() {
  types = duckdb::PhysicalPresortedMerge::RunTypes();
}

duckdb::vector<duckdb::ColumnBinding>
LogicalPresortedMerge::GetColumnBindings() {
  return GenerateColumnBindings(
    _table_index, duckdb::PhysicalPresortedMerge::RunTypes().size());
}

duckdb::vector<duckdb::TableIndex> LogicalPresortedMerge::GetTableIndex()
  const {
  return {_table_index};
}

duckdb::string LogicalPresortedMerge::GetName() const {
  return "PRESORTED_MERGE";
}

duckdb::PhysicalOperator& LogicalPresortedMerge::CreatePlan(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) {
  return planner.Make<duckdb::PhysicalPresortedMerge>(_merge, 0);
}

bool PipelineMergeExecutor::Run(MergePlan& plan) const {
  SDB_IF_FAILURE("irs::PipelineMergeExecutor::Run") { return false; }
  const auto threads = std::max<size_t>(_threads, 1);
  duckdb::Connection connection{_db};
  auto& context = *connection.context;
  context.task_scheduler = _scheduler;
  context.SetSchedulerAccount(
    duckdb::TaskScheduler::GetScheduler(context).CreateAccount(
      duckdb::SchedulerClass::FOREGROUND));
  context.config.threads = threads;
  duckdb::Settings::Set<duckdb::MaxExecutionTimeSetting>(
    context, duckdb::SetScope::SESSION, duckdb::Value::BIGINT(0));
  duckdb::Settings::Set<duckdb::PreserveInsertionOrderSetting>(
    context, duckdb::SetScope::SESSION, duckdb::Value::BOOLEAN(false));
  if (_profiles) {
    duckdb::EnableProfilingSetting::SetLocal(context,
                                             duckdb::Value{"no_output"});
  }
  if (plan.OrderPending()) {
    if (!Execute(connection, duckdb::make_uniq<LogicalMergeOrder>(plan),
                 _profiles)) {
      return false;
    }
    plan.FinishOrder();
  }
  const auto schedule = std::make_shared<MergeSchedule>(plan);
  auto units =
    duckdb::make_uniq<LogicalMergeUnits>(schedule, threads, plan.TermRanges());
  SDB_ASSERT(!duckdb::LogicalPlanVerifier::VerifyAlways(*units).HasError());
  const bool completed = Execute(connection, std::move(units), _profiles);
  return schedule->Result() && completed;
}

bool MergeSegments(Directory& dir, MergeDocMap& doc_map, SegmentMeta& segment,
                   const SegmentWriterOptions& options,
                   const PipelineMergeExecutor& executor,
                   const MergeProgress& progress, const AnnBuildEnv* env,
                   MergeProfile* profile) {
  SDB_ASSERT(options.db);
  bool merged = false;
  Finally invalidate = [&merged, &segment]() noexcept {
    if (!merged) [[unlikely]] {
      segment.files.clear();
      static_cast<SegmentInfo&>(segment) = SegmentInfo{};
    }
  };
  MergePlan plan{dir,          doc_map,
                 segment,      options.scorer,
                 *options.db,  options.field_options,
                 progress,     env,
                 options.sort, options.hnsw_bases};
  merged = plan.Prepare(executor.Threads(), executor.Scheduler()) &&
           executor.Run(plan);
  if (profile) {
    *profile = plan.Profile();
  }
  return merged;
}

}  // namespace irs
