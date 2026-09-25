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

#include "connector/functions/ai/ai_operator.h"

#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <atomic>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/types/batched_data_collection.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "connector/functions/ai/common.h"
#include "query/config.h"

namespace sdb::connector::ai {
namespace {

constinit SettingRef gConcurrency{"sdb_ai_max_concurrent_requests"};

enum class EvaluateOrder : uint8_t {
  Unordered,
  Serial,
  Batch,
};

struct Morsel {
  duckdb::idx_t chunk;
  duckdb::idx_t offset;
  duckdb::idx_t count;
};

class EvaluateGlobalState final : public duckdb::GlobalSinkState {
 public:
  EvaluateGlobalState(duckdb::ClientContext& context,
                      const duckdb::vector<duckdb::LogicalType>& types)
    : batches{context, types,
              duckdb::ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR} {}

  absl::Mutex mutex;
  duckdb::BatchedDataCollection batches ABSL_GUARDED_BY(mutex);
  duckdb::unique_ptr<duckdb::ColumnDataCollection> rows;
};

class EvaluateLocalState final : public duckdb::LocalSinkState {
 public:
  duckdb::unique_ptr<duckdb::BatchedDataCollection> batches;
  duckdb::unique_ptr<duckdb::ColumnDataCollection> rows;
  duckdb::ColumnDataAppendState append;
};

class EvaluateSourceState final : public duckdb::GlobalSourceState {
 public:
  duckdb::idx_t MaxThreads() final { return max_threads; }

  std::vector<Morsel> morsels;
  std::atomic_size_t next = 0;
  duckdb::idx_t max_threads = 1;
};

struct Call {
  Call(duckdb::ExecutionContext& context,
       const duckdb::BoundFunctionExpression& expr)
    : bind{expr.BindInfo()->Cast<AIFunctionData>()},
      requester{context.client, bind.GetEndpoint()},
      executor{context.client, expr.GetChildren()} {
    duckdb::vector<duckdb::LogicalType> types;
    for (const auto& child : expr.GetChildren()) {
      types.push_back(child->GetReturnType());
    }
    args.Initialize(context.client, types);
  }

  const AIFunctionData& bind;
  Requester requester;
  duckdb::ExpressionExecutor executor;
  duckdb::DataChunk args;
};

class EvaluateLocalSource final : public duckdb::LocalSourceState {
 public:
  std::vector<std::unique_ptr<Call>> calls;
  duckdb::DataChunk rows;
  duckdb::idx_t batch_index = 0;
};

class PhysicalAIEvaluate final : public duckdb::PhysicalOperator {
 public:
  PhysicalAIEvaluate(
    duckdb::PhysicalPlan& plan, duckdb::vector<duckdb::LogicalType> types,
    duckdb::vector<duckdb::LogicalType> input_types,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> calls,
    duckdb::idx_t estimated_cardinality, EvaluateOrder order)
    : PhysicalOperator{plan, duckdb::PhysicalOperatorType::EXTENSION,
                       std::move(types), estimated_cardinality},
      _input_types{std::move(input_types)},
      _calls{std::move(calls)},
      _order{order} {}

  std::string GetName() const final { return "AI_EVALUATE"; }

  duckdb::InsertionOrderPreservingMap<std::string> ParamsToString()
    const final {
    duckdb::InsertionOrderPreservingMap<std::string> result;
    std::string calls;
    for (const auto& call : _calls) {
      if (!calls.empty()) {
        calls += "\n";
      }
      calls += call->Cast<duckdb::BoundFunctionExpression>()
                 .Function()
                 .GetName()
                 .GetIdentifierName();
    }
    result["Calls"] = calls;
    SetEstimatedCardinality(result, estimated_cardinality);
    return result;
  }

  bool IsSink() const final { return true; }

  bool ParallelSink() const final { return _order != EvaluateOrder::Serial; }

  bool SinkOrderDependent() const final {
    return _order == EvaluateOrder::Serial;
  }

  duckdb::OperatorPartitionInfo RequiredPartitionInfo() const final {
    return _order == EvaluateOrder::Batch
             ? duckdb::OperatorPartitionInfo::BatchIndex()
             : duckdb::OperatorPartitionInfo::NoPartitionInfo();
  }

  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const final {
    return duckdb::make_uniq<EvaluateGlobalState>(context, _input_types);
  }

  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext& context) const final;

  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final;

  duckdb::SinkCombineResultType Combine(
    duckdb::ExecutionContext& context,
    duckdb::OperatorSinkCombineInput& input) const final;

  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline& pipeline, duckdb::Event& event,
    duckdb::ClientContext& context,
    duckdb::OperatorSinkFinalizeInput& input) const final;

  bool IsSource() const final { return true; }

  bool ParallelSource() const final { return true; }

  bool SupportsPartitioning(
    const duckdb::OperatorPartitionInfo& partition_info) const final {
    return !partition_info.RequiresPartitionColumns();
  }

  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;

  duckdb::unique_ptr<duckdb::LocalSourceState> GetLocalSourceState(
    duckdb::ExecutionContext& context,
    duckdb::GlobalSourceState& gstate) const final;

  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;

  duckdb::OperatorPartitionData GetPartitionData(
    duckdb::ExecutionContext&, duckdb::DataChunk&, duckdb::GlobalSourceState&,
    duckdb::LocalSourceState& lstate,
    const duckdb::OperatorPartitionInfo&) const final {
    return duckdb::OperatorPartitionData{
      lstate.Cast<EvaluateLocalSource>().batch_index};
  }

 private:
  duckdb::vector<duckdb::LogicalType> _input_types;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> _calls;
  EvaluateOrder _order;
};

duckdb::unique_ptr<duckdb::LocalSinkState>
PhysicalAIEvaluate::GetLocalSinkState(duckdb::ExecutionContext& context) const {
  auto state = duckdb::make_uniq<EvaluateLocalState>();
  if (_order == EvaluateOrder::Batch) {
    state->batches = duckdb::make_uniq<duckdb::BatchedDataCollection>(
      context.client, _input_types,
      duckdb::ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
  } else {
    state->rows = duckdb::make_uniq<duckdb::ColumnDataCollection>(
      context.client, _input_types);
    state->rows->InitializeAppend(state->append);
  }
  return state;
}

duckdb::SinkResultType PhysicalAIEvaluate::Sink(
  duckdb::ExecutionContext&, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& state = input.local_state.Cast<EvaluateLocalState>();
  if (state.batches) {
    state.batches->Append(chunk, state.partition_info.batch_index.GetIndex());
  } else {
    state.rows->Append(state.append, chunk);
  }
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType PhysicalAIEvaluate::Combine(
  duckdb::ExecutionContext&, duckdb::OperatorSinkCombineInput& input) const {
  auto& gstate = input.global_state.Cast<EvaluateGlobalState>();
  auto& state = input.local_state.Cast<EvaluateLocalState>();
  absl::MutexLock lock{&gstate.mutex};
  if (state.batches) {
    gstate.batches.Merge(*state.batches);
  } else if (state.rows->Count() != 0) {
    if (gstate.rows) {
      gstate.rows->Combine(*state.rows);
    } else {
      gstate.rows = std::move(state.rows);
    }
  }
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType PhysicalAIEvaluate::Finalize(
  duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext&,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<EvaluateGlobalState>();
  if (_order == EvaluateOrder::Batch) {
    absl::MutexLock lock{&gstate.mutex};
    gstate.rows = gstate.batches.FetchCollection();
  }
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
PhysicalAIEvaluate::GetGlobalSourceState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<EvaluateSourceState>();
  const auto& rows = sink_state->Cast<EvaluateGlobalState>().rows;
  if (!rows || rows->Count() == 0) {
    return state;
  }
  const auto threads = std::clamp<duckdb::idx_t>(
    gConcurrency.Int(context), 1,
    std::max<duckdb::idx_t>(
      1, duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads()));
  duckdb::idx_t size = (rows->Count() + threads - 1) / threads;
  for (const auto& call : _calls) {
    size = std::max<duckdb::idx_t>(size,
                                   call->Cast<duckdb::BoundFunctionExpression>()
                                     .BindInfo()
                                     ->Cast<AIFunctionData>()
                                     .BatchSize());
  }
  duckdb::DataChunk chunk;
  rows->InitializeScanChunk(chunk);
  for (duckdb::idx_t c = 0; c < rows->ChunkCount(); c++) {
    rows->FetchChunk(c, chunk);
    for (duckdb::idx_t offset = 0; offset < chunk.size(); offset += size) {
      state->morsels.push_back(
        {c, offset, std::min(size, chunk.size() - offset)});
    }
  }
  state->max_threads = std::min<duckdb::idx_t>(state->morsels.size(), threads);
  return state;
}

duckdb::unique_ptr<duckdb::LocalSourceState>
PhysicalAIEvaluate::GetLocalSourceState(duckdb::ExecutionContext& context,
                                        duckdb::GlobalSourceState&) const {
  auto state = duckdb::make_uniq<EvaluateLocalSource>();
  for (const auto& call : _calls) {
    state->calls.push_back(std::make_unique<Call>(
      context, call->Cast<duckdb::BoundFunctionExpression>()));
  }
  state->rows.Initialize(context.client, _input_types);
  return state;
}

duckdb::SourceResultType PhysicalAIEvaluate::GetDataInternal(
  duckdb::ExecutionContext&, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<EvaluateSourceState>();
  auto& state = input.local_state.Cast<EvaluateLocalSource>();
  const auto m = source.next.fetch_add(1, std::memory_order_relaxed);
  if (m >= source.morsels.size()) {
    return duckdb::SourceResultType::FINISHED;
  }
  const auto& morsel = source.morsels[m];
  state.batch_index = m;
  sink_state->Cast<EvaluateGlobalState>().rows->FetchChunk(morsel.chunk,
                                                           state.rows);
  chunk.Slice(state.rows, morsel.offset, morsel.offset + morsel.count);
  chunk.SetChildCardinality(morsel.count);
  for (size_t c = 0; c != state.calls.size(); ++c) {
    auto& call = *state.calls[c];
    call.args.Reset();
    call.executor.Execute(chunk, call.args);
    call.bind.Evaluate(call.requester, call.args,
                       chunk.data[_input_types.size() + c]);
  }
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace

LogicalAIEvaluate::LogicalAIEvaluate(
  duckdb::TableIndex table_index,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> calls)
  : LogicalExtensionOperator{std::move(calls)}, _table_index{table_index} {}

duckdb::vector<duckdb::ColumnBinding> LogicalAIEvaluate::GetColumnBindings() {
  auto bindings = children[0]->GetColumnBindings();
  for (auto index : duckdb::ProjectionIndex::GetIndexes(expressions.size())) {
    bindings.emplace_back(_table_index, index);
  }
  return bindings;
}

duckdb::vector<duckdb::TableIndex> LogicalAIEvaluate::GetTableIndex() const {
  return {_table_index};
}

void LogicalAIEvaluate::ResolveTypes() {
  types = children[0]->types;
  for (const auto& expr : expressions) {
    types.push_back(expr->GetReturnType());
  }
}

std::string LogicalAIEvaluate::GetName() const { return "AI_EVALUATE"; }

void LogicalAIEvaluate::Serialize(duckdb::Serializer&) const {
  throw duckdb::NotImplementedException(
    "AI_EVALUATE does not support serialization");
}

duckdb::PhysicalOperator& LogicalAIEvaluate::CreatePlan(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner) {
  auto& child = planner.CreatePlan(*children[0]);
  auto order = EvaluateOrder::Unordered;
  if (duckdb::PhysicalPlanGenerator::PreserveInsertionOrder(context, child)) {
    order = duckdb::PhysicalPlanGenerator::UseBatchIndex(context, child)
              ? EvaluateOrder::Batch
              : EvaluateOrder::Serial;
  }
  auto& op =
    planner.Make<PhysicalAIEvaluate>(types, child.types, std::move(expressions),
                                     EstimateCardinality(context), order);
  op.children.push_back(child);
  return op;
}

}  // namespace sdb::connector::ai
