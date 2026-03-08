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

#include "query/explain_executor.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_split.h>

#include <yaclib/async/make.hpp>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "query/query.h"
#include "query/velox_executor.h"
#include "utils.h"

namespace sdb::query {
namespace {

std::string ProcessPlan(std::string plan, bool clean_column_names) {
  if (clean_column_names) {
    plan = CleanColumnNames(std::move(plan));
  }
  return plan;
}

}  // namespace

ExplainExecutor::ExplainExecutor(VeloxExecutor* velox) : _velox{velox} {}

void ExplainExecutor::SetQuery(Query& query) { _query = &query; }

yaclib::Future<> ExplainExecutor::Execute(velox::RowVectorPtr& batch) {
  if (!_result) {
    _result = BuildExplainBatch();
    batch = std::move(*_result);
    return yaclib::MakeFuture();
  }
  return {};
}

velox::RowVectorPtr ExplainExecutor::BuildExplainBatch() {
  const auto& query_ctx = _query->GetContext();
  const bool clean_column_names =
    !query_ctx.explain_params.Has(ExplainWith::Registers);

  std::vector<std::string> data;

  auto post_process_plan = [&](std::string_view plan) {
    for (auto line : absl::StrSplit(plan, '\n')) {
      line = basics::string_utils::RTrim(line);
      if (!line.empty()) {
        data.emplace_back(line);
      }
    }
  };

  if (query_ctx.explain_params.Has(ExplainWith::Logical)) {
    data.emplace_back("LOGICAL PLAN:");
    post_process_plan(
      ProcessPlan(_query->GetLogicalPlan(), clean_column_names));
  }

  if (query_ctx.explain_params.Has(ExplainWith::InitialQueryGraph)) {
    data.emplace_back("INITIAL QUERY GRAPH:");
    post_process_plan(
      ProcessPlan(_query->GetInitialQueryGraphPlan(), clean_column_names));
  }

  if (query_ctx.explain_params.Has(ExplainWith::FinalQueryGraph)) {
    data.emplace_back("FINAL QUERY GRAPH:");
    post_process_plan(
      ProcessPlan(_query->GetFinalQueryGraphPlan(), clean_column_names));
  }

  if (query_ctx.explain_params.Has(ExplainWith::Physical)) {
    data.emplace_back("PHYSICAL PLAN:");
    post_process_plan(
      ProcessPlan(_query->GetPhysicalPlan(), clean_column_names));
  }

  if (query_ctx.explain_params.Has(ExplainWith::Execution)) {
    if (query_ctx.explain_params.Has(ExplainWith::Stats)) {
      SDB_ASSERT(_velox);
      data.emplace_back("EXECUTION PLAN WITH STATS:");
      post_process_plan(ProcessPlan(_velox->GetRunner().PrintPlanWithStats(),
                                    clean_column_names));
    } else {
      data.emplace_back("EXECUTION PLAN:");
      post_process_plan(
        ProcessPlan(_query->GetExecutionPlan(), clean_column_names));
    }
  }

  // Build the VARCHAR batch
  auto output_type = _query->GetOutputType();
  auto* pool = _query->GetContext().query_memory_pool.get();
  auto vector = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
    output_type->children()[0], data.size(), pool);
  for (size_t j = 0; j < data.size(); ++j) {
    vector->set(j, velox::StringView(data[j]));
  }
  return std::make_shared<velox::RowVector>(
    pool, output_type, nullptr, data.size(),
    std::vector<velox::VectorPtr>{std::move(vector)});
}

}  // namespace sdb::query
