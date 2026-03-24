////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "query/query.h"

#include <absl/algorithm/container.h>
#include <axiom/logical_plan/PlanPrinter.h>
#include <axiom/optimizer/DerivedTablePrinter.h>
#include <axiom/optimizer/Optimization.h>
#include <axiom/optimizer/Plan.h>
#include <axiom/optimizer/RelationOpPrinter.h>
#include <axiom/optimizer/Schema.h>
#include <axiom/optimizer/ToVelox.h>
#include <axiom/optimizer/VeloxHistory.h>
#include <velox/common/memory/HashStringAllocator.h>
#include <velox/expression/Expr.h>
#include <velox/vector/VariantToVector.h>

#include "catalog/table.h"
#include "connector/serenedb_connector.hpp"
#include "pg/sql_resolver.h"
#include "query/cursor.h"
#include "query/explain_executor.h"
#include "query/show_executor.h"
#include "query/velox_executor.h"

namespace sdb::query {
namespace {

class NoopHistory final : public axiom::optimizer::History {
 public:
  std::optional<axiom::optimizer::Cost> findCost(
    axiom::optimizer::RelationOp&) final {
    return std::nullopt;
  }

  void recordCost(const axiom::optimizer::RelationOp&,
                  axiom::optimizer::Cost) final {}

  bool setLeafSelectivity(axiom::optimizer::BaseTable&,
                          const velox::RowTypePtr&) {
    return false;
  }

  void recordJoinSample(std::string_view, float, float) final {}

  std::pair<float, float> sampleJoin(axiom::optimizer::JoinEdge*) final {
    return {1.0f, 1.0f};
  }

  void recordLeafSelectivity(std::string_view, float, bool) final {}

  folly::dynamic serialize() final { return {}; }

  void update(folly::dynamic&) final {}
};

}  // namespace

std::unique_ptr<Query> Query::CreateQuery(
  const axiom::logical_plan::LogicalPlanNodePtr& root,
  const QueryContext& query_ctx) {
  std::unique_ptr<Query> query{new Query{root, query_ctx}};
  query->SetExecutor(std::make_unique<VeloxExecutor>());
  return query;
}

std::unique_ptr<Query> Query::CreateExplain(
  const axiom::logical_plan::LogicalPlanNodePtr& root,
  const QueryContext& query_ctx) {
  std::unique_ptr<Query> query{new Query{root, query_ctx}};
  query->SetExecutor(std::make_unique<ExplainExecutor>());
  return query;
}

std::unique_ptr<Query> Query::CreateDDL(std::unique_ptr<Executor> executor,
                                        const QueryContext& query_ctx) {
  auto query =
    std::unique_ptr<Query>(new Query{velox::RowTypePtr{}, query_ctx});
  query->SetExecutor(std::move(executor));
  return query;
}

std::unique_ptr<Query> Query::CreateShow(std::string_view show_variable,
                                         const QueryContext& query_ctx) {
  auto query = std::unique_ptr<Query>(new Query{
    velox::ROW({std::string{show_variable}}, {velox::VARCHAR()}),
    query_ctx,
  });
  query->SetExecutor(std::make_unique<ShowExecutor>());
  return query;
}

std::unique_ptr<Query> Query::CreateShowAll(const QueryContext& query_ctx) {
  auto query = std::unique_ptr<Query>(new Query{
    velox::ROW({"name", "value", "description"},
               {velox::VARCHAR(), velox::VARCHAR(), velox::VARCHAR()}),
    query_ctx,
  });
  query->SetExecutor(std::make_unique<ShowAllExecutor>());
  return query;
}

std::unique_ptr<Query> Query::CreatePipeline(
  const axiom::logical_plan::LogicalPlanNodePtr& root,
  const QueryContext& query_ctx,
  std::vector<std::unique_ptr<Executor>> executors,
  absl::AnyInvocable<void()> on_error) {
  return std::unique_ptr<Query>(
    new Query{root, query_ctx, std::move(executors), std::move(on_error)});
}

Query::Query(const axiom::logical_plan::LogicalPlanNodePtr& root,
             const QueryContext& query_ctx)
  : _query_ctx{query_ctx}, _logical_plan{root} {
  CompileQuery();
}

Query::Query(const axiom::logical_plan::LogicalPlanNodePtr& root,
             const QueryContext& query_ctx,
             std::vector<std::unique_ptr<Executor>> executors,
             absl::AnyInvocable<void()> on_error)
  : _query_ctx{query_ctx},
    _logical_plan{root},
    _output_type{query_ctx.command_type.Has(CommandType::Explain)
                   ? velox::ROW({"QUERY PLAN"}, {velox::VARCHAR()})
                   : root->outputType()},
    _on_error{std::move(on_error)} {
  SetExecutors(std::move(executors));
}

void Query::SetExecutor(std::unique_ptr<Executor> executor) {
  std::vector<std::unique_ptr<Executor>> executors;
  executors.emplace_back(std::move(executor));
  SetExecutors(std::move(executors));
}

void Query::SetExecutors(std::vector<std::unique_ptr<Executor>> executors) {
  SDB_ASSERT(_executors.empty());
  _executors = std::move(executors);

  if (_query_ctx.explain_params.Has(ExplainWith::Analyze)) {
    for (auto& executor : _executors) {
      if (auto* velox = dynamic_cast<VeloxExecutor*>(executor.get())) {
        velox->IgnoreOutput() = true;
      }
    }
    _executors.emplace_back(std::make_unique<ExplainExecutor>());
  }

  for (auto& executor : _executors) {
    executor->Init(*this);
  }
}

void Query::CompileQuery() {
  const bool only_explain =
    !_query_ctx.command_type.HasAnyNot(CommandType::Explain);

  const bool needs_initial_query_graph =
    _query_ctx.explain_params.Has(ExplainWith::InitialQueryGraph);
  const bool needs_final_query_graph =
    _query_ctx.explain_params.Has(ExplainWith::FinalQueryGraph);
  const bool needs_physical =
    _query_ctx.explain_params.Has(ExplainWith::Physical);
  const bool needs_execution =
    _query_ctx.explain_params.Has(ExplainWith::Execution);

  irs::Finally set_explain_output_type = [&] noexcept {
    if (_query_ctx.command_type.Has(CommandType::Explain)) {
      _output_type = velox::ROW({"QUERY PLAN"}, {velox::VARCHAR()});
    }
    // _output_type could be nullptr in case of error.
  };

  if (only_explain && !needs_initial_query_graph && !needs_final_query_graph &&
      !needs_physical && !needs_execution) {
    return;
  }

  velox::HashStringAllocator query_graph_allocator{
    _query_ctx.query_memory_pool.get()};
  axiom::optimizer::QueryGraphContext query_graph_context{
    query_graph_allocator};
  axiom::optimizer::queryCtx() = &query_graph_context;
  irs::Finally reset_optimizer_context = [&] noexcept {
    axiom::optimizer::queryCtx() = nullptr;
  };

  NoopHistory history;
  velox::exec::SimpleExpressionEvaluator evaluator{
    _query_ctx.velox_query_ctx.get(), _query_ctx.query_memory_pool.get()};
  // TODO(mbkkt) add options in config
  const auto& config = basics::downCast<Config>(
    *_query_ctx.velox_query_ctx->queryConfig().config());
  axiom::optimizer::OptimizerOptions optimizer_options{
    .parallelProjectWidth = 1,

    // TODO(mbkkt) Should be true?
    .pushdownSubfields = false,

    // TODO(mbkkt) Can be true to avoid support maps in postgres frontend?
    .allMapsAsStruct = false,

    // TODO(mbkkt) Use advanced statistics somehow?
    .sampleJoins = false,
    .sampleFilters = false,

    // TODO(mbkkt) Maybe disable?
    .enableReducingExistences = false,

    .joinOrder =
      config.Get<VariableType::JoinOrderAlgorithm>("join_order_algorithm"),

    // TODO(mbkkt) single option about aggregation?
    .alwaysPlanPartialAggregation = false,
    .alwaysPlanSingleAggregation = false,

    // TODO(mbkkt) single option about limit?
    .alwaysPushdownLimit = false,
    .alwaysPullupLimit = false,
    .planBestThroughput = false,

    // TODO(mbkkt) maybe enable it for prepared statements?
    .enableSubqueryConstantFolding = false,

    // TODO(mbkkt) disable for now as it's broken
    .enableIndexLookupJoin = false,

    .lazyOptimizeGraph = true,
  };
  axiom::runner::MultiFragmentPlan::Options runner_options{
    .numWorkers = 1,
    .numDrivers =
      std::max<int>(config.Get<VariableType::U32>("execution_threads"), 1),
  };
  axiom::Session session{""};
  std::shared_ptr<axiom::Session> session_ptr{std::shared_ptr<axiom::Session>{},
                                              &session};
  axiom::optimizer::Optimization optimization{
    std::move(session_ptr),
    *_logical_plan,
    history,
    _query_ctx.velox_query_ctx,
    evaluator,
    optimizer_options,
    runner_options,
  };

  if (needs_initial_query_graph) {
    _initial_query_graph_plan =
      axiom::optimizer::DerivedTablePrinter::toText(*optimization.graph());
    if (only_explain && !needs_final_query_graph && !needs_physical &&
        !needs_execution) {
      return;
    }
  }

  optimization.optimizeGraph();

  if (needs_final_query_graph) {
    _final_query_graph_plan =
      axiom::optimizer::DerivedTablePrinter::toText(*optimization.graph());
    if (only_explain && !needs_physical && !needs_execution) {
      return;
    }
  }

  auto* best = optimization.bestPlan();

  if (needs_physical) {
    if (_query_ctx.explain_params.Has(ExplainWith::Oneline)) {
      _physical_plan =
        axiom::optimizer::RelationOpPrinter::toOneline(*best->op);
    } else {
      const bool include_cost =
        _query_ctx.explain_params.Has(ExplainWith::Cost);
      _physical_plan = axiom::optimizer::RelationOpPrinter::toText(
        *best->op, {.includeCost = include_cost});
    }
    if (only_explain && !needs_execution) {
      return;
    }
  }
  // This is not really good for prepared statements, but it works for now
  auto result = optimization.toVeloxPlan(best->op);
  _execution_plan = std::move(result.plan);
  _finish_write = std::move(result.finishWrite);

  if (!_query_ctx.command_type.Has(CommandType::Explain)) {
    SDB_ASSERT(_execution_plan);
    const auto& fragments = _execution_plan->fragments();
    SDB_ASSERT(!fragments.empty());
    const auto& gather_fragment = fragments.back().fragment.planNode;
    SDB_ASSERT(gather_fragment);
    _output_type = gather_fragment->outputType();
  }
}

std::string Query::GetLogicalPlanText() const {
  SDB_ASSERT(_logical_plan);
  return axiom::logical_plan::PlanPrinter::toText(*_logical_plan);
}

Query::Query(velox::RowTypePtr output_type, const QueryContext& query_ctx)
  : _query_ctx{query_ctx}, _output_type{std::move(output_type)} {}

std::string Query::GetExecutionPlan() const {
  SDB_ASSERT(_execution_plan);
  return _execution_plan->toString(true);
}

std::unique_ptr<Cursor> Query::MakeCursor(UserTask&& user_task) {
  std::unique_ptr<Cursor> ptr;
  ptr.reset(new Cursor{std::move(user_task), *this});
  _finish_write = {};
  return ptr;
}

void Query::MakeRunner() {
  _runner = Runner{_execution_plan, std::move(_finish_write),
                   _query_ctx.velox_query_ctx,
                   std::make_shared<axiom::runner::ConnectorSplitSourceFactory>(
                     axiom::connector::SplitOptions{}),
                   _query_ctx.query_memory_pool};
}

template<typename StringType>
velox::RowVectorPtr Query::BuildBatchImpl(
  std::span<const std::vector<StringType>> columns) const {
  SDB_ASSERT(_output_type->isRow());
  SDB_ASSERT(absl::c_all_of(_output_type->children(), [](const auto& ptr) {
    return ptr == velox::VARCHAR();
  }));
  auto* pool = _query_ctx.query_memory_pool.get();
  std::vector<velox::VectorPtr> vectors;
  vectors.reserve(columns.size());
  size_t batch_rows = 0;
  for (size_t i = 0; i < columns.size(); ++i) {
    auto vector =
      velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        _output_type->children()[i], columns[i].size(), pool);
    for (size_t j = 0; j < columns[i].size(); ++j) {
      vector->set(j, velox::StringView(columns[i][j]));
    }
    batch_rows = std::max(batch_rows, columns[i].size());
    vectors.push_back(std::move(vector));
  }
  return std::make_shared<velox::RowVector>(pool, _output_type, nullptr,
                                            batch_rows, std::move(vectors));
}

velox::RowVectorPtr Query::BuildBatch(
  std::span<const std::vector<std::string>> columns) const {
  return BuildBatchImpl(columns);
}

velox::RowVectorPtr Query::BuildBatch(
  std::span<const std::vector<std::string_view>> columns) const {
  return BuildBatchImpl(columns);
}

}  // namespace sdb::query
