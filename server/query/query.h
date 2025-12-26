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

#pragma once

#include <absl/functional/any_invocable.h>
#include <axiom/logical_plan/LogicalPlanNode.h>
#include <axiom/runner/MultiFragmentPlan.h>
#include <velox/common/memory/MemoryPool.h>
#include <velox/core/PlanNode.h>
#include <velox/core/QueryCtx.h>
#include <velox/exec/Task.h>

#include <memory>

#include "basics/fwd.h"
#include "query/context.h"
#include "query/external_executor.h"
#include "query/runner.h"

namespace sdb::query {

class Cursor;

class Query {
 public:
  static std::unique_ptr<Query> CreateQuery(
    const axiom::logical_plan::LogicalPlanNodePtr& root,
    const QueryContext& query_ctx);

  static std::unique_ptr<Query> CreateExternal(
    std::unique_ptr<ExternalExecutor> executor, const QueryContext& query_ctx);

  static std::unique_ptr<Query> CreateShow(std::string_view show_variable,
                                           const QueryContext& query_ctx);

  static std::unique_ptr<Query> CreateShowAll(const QueryContext& query_ctx);

  velox::RowTypePtr GetOutputType() const { return _output_type; }
  const QueryContext& GetContext() const { return _query_ctx; }
  std::string GetLogicalPlan() const;
  const auto& GetInitialQueryGraphPlan() const {
    return _initial_query_graph_plan;
  }
  const auto& GetFinalQueryGraphPlan() const { return _final_query_graph_plan; }
  const auto& GetPhysicalPlan() const { return _physical_plan; }
  std::string GetExecutionPlan() const;
  ExternalExecutor& GetExternalExecutor() const;

  bool HasExternal() const { return _executor != nullptr; }

  bool IsDML() const {
    return _logical_plan &&
           _logical_plan->is(axiom::logical_plan::NodeKind::kTableWrite);
  }

  bool IsDataQuery() const { return _logical_plan != nullptr; }

  std::unique_ptr<Cursor> MakeCursor(std::function<void()>&& user_task) const;

  Runner MakeRunner() const;

 private:
  // use for CreateQuery
  Query(const axiom::logical_plan::LogicalPlanNodePtr& root,
        const QueryContext& query_ctx);

  // use for CreateExternal
  Query(std::unique_ptr<ExternalExecutor> executor,
        const QueryContext& query_ctx);

  // use for CreateShow and CreateShowAll
  Query(velox::RowTypePtr output_type, const QueryContext& query_ctx);

  void CompileQuery();

  QueryContext _query_ctx;
  mutable axiom::runner::FinishWrite _finish_write;
  axiom::runner::MultiFragmentPlanPtr _execution_plan;
  axiom::logical_plan::LogicalPlanNodePtr _logical_plan;
  velox::RowTypePtr _output_type;
  std::unique_ptr<ExternalExecutor> _executor;

  std::string _initial_query_graph_plan;
  std::string _final_query_graph_plan;
  std::string _physical_plan;
};

using QueryPtr = std::unique_ptr<Query>;

}  // namespace sdb::query
