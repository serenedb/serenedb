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

#include "search/compaction/pipeline_merge_executor.h"

#include <algorithm>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_profiler.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>
#include <memory>

#include "connector/segment_rewrite/logical_merge_order.h"
#include "connector/segment_rewrite/logical_merge_units.h"
#include "search/compaction/merge_unit_run.h"

namespace sdb::search {
namespace {

void Check(const duckdb::QueryResult& result) {
  if (result.HasError()) {
    result.ThrowError();
  }
}

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

bool PipelineMergeExecutor::Run(irs::MergePlan& plan) {
  const auto threads = std::max<size_t>(_threads, 1);
  duckdb::Connection connection{_db};
  auto& context = *connection.context;
  context.task_scheduler = _scheduler;
  context.config.threads = threads;
  Check(*connection.Query("SET SESSION max_execution_time = 0"));
  Check(*connection.Query("SET SESSION preserve_insertion_order = false"));
  if (_profiles) {
    Check(*connection.Query("SET SESSION enable_profiling = 'no_output'"));
  }
  if (plan.OrderPending()) {
    if (!Execute(connection,
                 duckdb::make_uniq<connector::LogicalMergeOrder>(plan),
                 _profiles)) {
      return false;
    }
    plan.FinishOrder();
  }
  const auto run = std::make_shared<MergeUnitRun>(plan, plan.Started(),
                                                  plan.PostingsPending());
  run->SetInterrupt([&context] { context.Interrupt(); });
  const bool completed =
    Execute(connection,
            duckdb::make_uniq<connector::LogicalMergeUnits>(run, threads,
                                                            plan.TermRanges()),
            _profiles);
  run->SetInterrupt(nullptr);
  return run->Finish() && completed;
}

}  // namespace sdb::search
