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

#include "search/compaction/pipeline_merge_operators.h"

#include <atomic>
#include <duckdb/common/arena_linked_list.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/mutex.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/operator/order/physical_presorted_merge.hpp>
#include <duckdb/execution/operator/set/physical_union.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/index/merge/merge_schedule.hpp>
#include <iresearch/utils/assert.hpp>
#include <string>
#include <utility>

namespace sdb::search {
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
  std::unique_ptr<irs::TermRangeWriter> writer;
  uint64_t partition = 0;
  duckdb::vector<duckdb::PresortedRun> runs;
};

struct MergeUnitsSourceState final : duckdb::GlobalSourceState {
  MergeUnitsSourceState(std::shared_ptr<MergeUnitRun> run,
                        duckdb::idx_t threads)
    : run{std::move(run)}, threads{threads} {
    this->run->SetWaker([this] {
      Guard guard{lock};
      UnblockTasks();
    });
  }

  ~MergeUnitsSourceState() final { run->SetWaker(nullptr); }

  duckdb::idx_t MaxThreads() final { return threads; }

  std::shared_ptr<MergeUnitRun> run;
  duckdb::idx_t threads;
};

}  // namespace

LogicalPresortedMerge::LogicalPresortedMerge(
  duckdb::shared_ptr<duckdb::PresortedMerge> merge,
  duckdb::TableIndex table_index)
  : duckdb::LogicalExtensionOperator(),
    _merge{std::move(merge)},
    _table_index{table_index} {}

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

LogicalMergeOrder::LogicalMergeOrder(irs::MergePlan& plan)
  : duckdb::LogicalExtensionOperator(), _plan{plan} {
  children.push_back(duckdb::make_uniq<LogicalPresortedMerge>(plan.KeyMerge()));
}

void LogicalMergeOrder::ResolveTypes() {
  types = {duckdb::LogicalType::BIGINT};
}

duckdb::vector<duckdb::ColumnBinding> LogicalMergeOrder::GetColumnBindings() {
  return GenerateColumnBindings(duckdb::TableIndex(1), 1);
}

duckdb::vector<duckdb::TableIndex> LogicalMergeOrder::GetTableIndex() const {
  return {duckdb::TableIndex(1)};
}

duckdb::string LogicalMergeOrder::GetName() const { return "MERGE_ORDER"; }

duckdb::PhysicalOperator& LogicalMergeOrder::CreatePlan(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) {
  auto& runs = planner.CreatePlan(*children[0]);
  auto& order = planner.Make<PhysicalMergeOrder>(_plan);
  order.children.push_back(runs);
  return order;
}

LogicalPostingsMerge::LogicalPostingsMerge(irs::TermRangeMerge& merge,
                                           duckdb::TableIndex table_index,
                                           std::shared_ptr<MergeUnitRun> run)
  : duckdb::LogicalExtensionOperator(),
    _merge{merge},
    _table_index{table_index},
    _run{std::move(run)} {
  children.push_back(duckdb::make_uniq<LogicalPresortedMerge>(
    merge.KeyMerge(), duckdb::TableIndex(table_index.index + 1)));
}

void LogicalPostingsMerge::ResolveTypes() {
  types = {duckdb::LogicalType::BIGINT};
}

duckdb::vector<duckdb::ColumnBinding>
LogicalPostingsMerge::GetColumnBindings() {
  return GenerateColumnBindings(_table_index, 1);
}

duckdb::vector<duckdb::TableIndex> LogicalPostingsMerge::GetTableIndex() const {
  return {_table_index};
}

duckdb::string LogicalPostingsMerge::GetName() const {
  return "POSTINGS_MERGE";
}

duckdb::PhysicalOperator& LogicalPostingsMerge::CreatePlan(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) {
  auto& runs = planner.CreatePlan(*children[0]);
  auto& merge = planner.Make<PhysicalPostingsMerge>(_merge, _run);
  merge.children.push_back(runs);
  return merge;
}

LogicalMergeUnits::LogicalMergeUnits(
  std::shared_ptr<MergeUnitRun> run, duckdb::idx_t threads,
  std::span<const std::unique_ptr<irs::TermRangeMerge>> ranges)
  : duckdb::LogicalExtensionOperator(),
    _run{std::move(run)},
    _threads{threads} {
  for (size_t i = 0; i != ranges.size(); ++i) {
    children.push_back(duckdb::make_uniq<LogicalPostingsMerge>(
      *ranges[i], duckdb::TableIndex(1 + 2 * i), _run));
  }
}

void LogicalMergeUnits::ResolveTypes() {
  types = {duckdb::LogicalType::BIGINT};
}

duckdb::vector<duckdb::ColumnBinding> LogicalMergeUnits::GetColumnBindings() {
  return GenerateColumnBindings(duckdb::TableIndex(0), 1);
}

duckdb::vector<duckdb::TableIndex> LogicalMergeUnits::GetTableIndex() const {
  return {duckdb::TableIndex(0)};
}

duckdb::string LogicalMergeUnits::GetName() const { return "MERGE_UNITS"; }

duckdb::PhysicalOperator& LogicalMergeUnits::CreatePlan(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) {
  auto& units = planner.Make<PhysicalMergeUnits>(_run, _threads);
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

PhysicalMergeOrder::PhysicalMergeOrder(duckdb::PhysicalPlan& physical_plan,
                                       irs::MergePlan& plan)
  : duckdb::PhysicalOperator(physical_plan,
                             duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT}, 0),
    _plan{plan} {}

duckdb::unique_ptr<duckdb::LocalSinkState>
PhysicalMergeOrder::GetLocalSinkState(duckdb::ExecutionContext&) const {
  return duckdb::make_uniq<MergeOrderSinkState>();
}

duckdb::SinkResultType PhysicalMergeOrder::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& state = input.local_state.Cast<MergeOrderSinkState>();
  const auto* partitions = duckdb::FlatVector::GetData<uint64_t>(chunk.data[0]);
  const auto* inputs = duckdb::FlatVector::GetData<uint64_t>(chunk.data[1]);
  const auto* begins = duckdb::FlatVector::GetData<uint64_t>(chunk.data[2]);
  const auto* counts = duckdb::FlatVector::GetData<uint64_t>(chunk.data[3]);
  state.runs.clear();
  for (duckdb::idx_t i = 0; i != chunk.size(); ++i) {
    SDB_ASSERT(partitions[i] == partitions[0]);
    state.runs.push_back(
      {.input = inputs[i], .begin = begins[i], .count = counts[i]});
  }
  if (!_plan.ExpandOrder(partitions[0], state.runs)) {
    context.client.Interrupt();
  }
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SourceResultType PhysicalMergeOrder::GetDataInternal(
  duckdb::ExecutionContext&, duckdb::DataChunk&,
  duckdb::OperatorSourceInput&) const {
  return duckdb::SourceResultType::FINISHED;
}

duckdb::string PhysicalMergeOrder::GetName() const { return "MERGE_ORDER"; }

PhysicalPostingsMerge::PhysicalPostingsMerge(
  duckdb::PhysicalPlan& physical_plan, irs::TermRangeMerge& merge,
  std::shared_ptr<MergeUnitRun> run)
  : duckdb::PhysicalOperator(physical_plan,
                             duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT}, 0),
    _merge{merge},
    _run{std::move(run)} {}

void PhysicalPostingsMerge::FinishPartition(
  duckdb::GlobalSinkState& global, duckdb::LocalSinkState& local) const {
  auto& state = local.Cast<PostingsMergeSinkState>();
  if (!state.writer) {
    return;
  }
  _merge.FinishPart(state.partition, *state.writer);
  state.writer.reset();
  if (global.Cast<PostingsMergeGlobalState>().left.fetch_sub(1) == 1) {
    _run->RangeDone();
    return;
  }
  _run->Wake();
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
PhysicalPostingsMerge::GetGlobalSinkState(duckdb::ClientContext&) const {
  return duckdb::make_uniq<PostingsMergeGlobalState>(_merge.PartsLeft());
}

duckdb::unique_ptr<duckdb::LocalSinkState>
PhysicalPostingsMerge::GetLocalSinkState(duckdb::ExecutionContext&) const {
  return duckdb::make_uniq<PostingsMergeSinkState>();
}

duckdb::SinkResultType PhysicalPostingsMerge::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  if (chunk.size() == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  auto& state = input.local_state.Cast<PostingsMergeSinkState>();
  const auto* partitions = duckdb::FlatVector::GetData<uint64_t>(chunk.data[0]);
  const auto* inputs = duckdb::FlatVector::GetData<uint64_t>(chunk.data[1]);
  const auto* begins = duckdb::FlatVector::GetData<uint64_t>(chunk.data[2]);
  const auto* counts = duckdb::FlatVector::GetData<uint64_t>(chunk.data[3]);
  if (state.writer && state.partition != partitions[0]) {
    FinishPartition(input.global_state, state);
  }
  if (!state.writer) {
    state.partition = partitions[0];
    state.writer = _merge.BeginPart(state.partition);
  }
  state.runs.clear();
  for (duckdb::idx_t i = 0; i != chunk.size(); ++i) {
    SDB_ASSERT(partitions[i] == state.partition);
    state.runs.push_back(
      {.input = inputs[i], .begin = begins[i], .count = counts[i]});
  }
  if (!state.writer->Consume(state.runs)) {
    context.client.Interrupt();
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_postings_merge_sink");
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType PhysicalPostingsMerge::Combine(
  duckdb::ExecutionContext&, duckdb::OperatorSinkCombineInput& input) const {
  FinishPartition(input.global_state, input.local_state);
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType PhysicalPostingsMerge::Finalize(
  duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext&,
  duckdb::OperatorSinkFinalizeInput& input) const {
  SDB_PARK_ONCE_ON_FAILURE("pause_postings_merge_finalize");
  if (input.global_state.Cast<PostingsMergeGlobalState>().parts == 0) {
    _run->RangeDone();
  }
  return duckdb::SinkFinalizeType::READY;
}

duckdb::SourceResultType PhysicalPostingsMerge::GetDataInternal(
  duckdb::ExecutionContext&, duckdb::DataChunk&,
  duckdb::OperatorSourceInput&) const {
  return duckdb::SourceResultType::FINISHED;
}

duckdb::string PhysicalPostingsMerge::GetName() const {
  return "POSTINGS_MERGE";
}

duckdb::InsertionOrderPreservingMap<duckdb::string>
PhysicalPostingsMerge::ParamsToString() const {
  duckdb::InsertionOrderPreservingMap<duckdb::string> result;
  result["Field"] = std::to_string(_merge.Field());
  result["Parts"] = std::to_string(_merge.PartCount());
  return result;
}

PhysicalMergeUnits::PhysicalMergeUnits(duckdb::PhysicalPlan& physical_plan,
                                       std::shared_ptr<MergeUnitRun> run,
                                       duckdb::idx_t threads)
  : duckdb::PhysicalOperator(physical_plan,
                             duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT}, 0),
    _run{std::move(run)},
    _threads{threads} {}

duckdb::unique_ptr<duckdb::GlobalSourceState>
PhysicalMergeUnits::GetGlobalSourceState(duckdb::ClientContext&) const {
  return duckdb::make_uniq<MergeUnitsSourceState>(_run, _threads);
}

duckdb::SourceResultType PhysicalMergeUnits::GetDataInternal(
  duckdb::ExecutionContext&, duckdb::DataChunk&,
  duckdb::OperatorSourceInput& input) const {
  auto& state = input.global_state.Cast<MergeUnitsSourceState>();
  auto unit = irs::MergeSchedule::kNoUnit;
  {
    Guard guard{state.lock};
    if (_run->schedule.Done()) {
      return duckdb::SourceResultType::FINISHED;
    }
    if (_run->MayClaim(_threads)) {
      unit = _run->schedule.Claim();
    }
    if (unit == irs::MergeSchedule::kNoUnit) {
      return state.BlockSource(input.interrupt_state);
    }
  }
  _run->Run(unit);
  Guard guard{state.lock};
  state.UnblockTasks();
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

duckdb::string PhysicalMergeUnits::GetName() const { return "MERGE_UNITS"; }

}  // namespace sdb::search
