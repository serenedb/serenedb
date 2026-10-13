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
#include <duckdb/parser/statement/logical_plan_statement.hpp>
#include <memory>

#include "connector/segment_rewrite/logical_merge_units.h"
#include "search/compaction/merge_unit_run.h"

namespace sdb::search {

bool PipelineMergeExecutor::Run(irs::MergePlan& plan) {
  const auto run = std::make_shared<MergeUnitRun>(plan);
  const auto threads = std::max<size_t>(_threads, 1);
  duckdb::Connection connection{_db};
  auto& context = *connection.context;
  context.task_scheduler = _scheduler;
  context.config.threads = threads;
  connection.Query("SET max_execution_time = 0");
  auto root = duckdb::make_uniq<connector::LogicalMergeUnits>(run, threads);
  root->ResolveOperatorTypes();
  auto result = connection.Query(
    duckdb::make_uniq<duckdb::LogicalPlanStatement>(std::move(root)));
  if (result->HasError()) {
    result->ThrowError();
  }
  return run->Finish();
}

}  // namespace sdb::search
