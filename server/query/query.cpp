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
  return std::unique_ptr<Query>(new Query{root, query_ctx});
}

std::unique_ptr<Query> Query::CreateExternal(
  std::unique_ptr<ExternalExecutor> executor, const QueryContext& query_ctx) {
  return std::unique_ptr<Query>(new Query{std::move(executor), query_ctx});
}

std::unique_ptr<Query> Query::CreateShow(std::string_view show_variable,
                                         const QueryContext& query_ctx) {
  return std::unique_ptr<Query>(new Query{
    velox::ROW({std::string{show_variable}}, {velox::VARCHAR()}),
    query_ctx,
  });
}

std::unique_ptr<Query> Query::CreateShowAll(const QueryContext& query_ctx) {
  return std::unique_ptr<Query>(new Query{
    velox::ROW({"name", "value", "description"},
               {velox::VARCHAR(), velox::VARCHAR(), velox::VARCHAR()}),
    query_ctx,
  });
}

Query::Query(const axiom::logical_plan::LogicalPlanNodePtr& root,
             const QueryContext& query_ctx)
  : _query_ctx{query_ctx}, _logical_plan{root} {
  CompileQuery();

  if (_query_ctx.command_type.Has(CommandType::Explain)) {
    _output_type = velox::ROW({"QUERY PLAN"}, {velox::VARCHAR()});
  } else {
    SDB_ASSERT(_execution_plan);
    const auto& fragments = _execution_plan->fragments();
    SDB_ASSERT(!fragments.empty());
    const auto& gather_fragment = fragments.back().fragment.planNode;
    SDB_ASSERT(gather_fragment);
    _output_type = gather_fragment->outputType();
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

    .syntacticJoinOrder = false,

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
    .numDrivers = 1,
  };
  axiom::Session session{"",
                         _query_ctx.velox_query_ctx->queryConfig().config()};
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
}

Query::Query(std::unique_ptr<ExternalExecutor> executor,
             const QueryContext& query_ctx)
  : _query_ctx{query_ctx}, _executor{std::move(executor)} {}

std::string Query::GetLogicalPlan() const {
  return axiom::logical_plan::PlanPrinter::toText(*_logical_plan);
}

Query::Query(velox::RowTypePtr output_type, const QueryContext& query_ctx)
  : _query_ctx{query_ctx}, _output_type{std::move(output_type)} {}

std::string Query::GetExecutionPlan() const {
  return _execution_plan->toString(true);
}

ExternalExecutor& Query::GetExternalExecutor() const {
  SDB_ASSERT(_executor);
  return *_executor;
}

std::unique_ptr<Cursor> Query::MakeCursor(
  std::function<void()>&& user_task) const {
  std::unique_ptr<Cursor> ptr;
  ptr.reset(new Cursor{std::move(user_task), *this});
  return ptr;
}

Runner Query::MakeRunner() const {
  return Runner{_execution_plan, std::move(_finish_write),
                _query_ctx.velox_query_ctx,
                std::make_shared<axiom::runner::ConnectorSplitSourceFactory>(
                  axiom::connector::SplitOptions{}),
                _query_ctx.query_memory_pool};
}
}  // namespace sdb::query
