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

#include <absl/strings/str_split.h>

#include <yaclib/async/make.hpp>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "query/query.h"
#include "utils.h"

namespace sdb::query {

yaclib::Future<> ExplainExecutor::Execute(velox::RowVectorPtr& batch) {
  if (!_query) {  // was fired
    return {};
  }
  batch = BuildExplainBatch();
  _query = nullptr;  // set fired
  return {};
}

velox::RowVectorPtr ExplainExecutor::BuildExplainBatch() {
  const auto& query_ctx = _query->GetContext();
  const bool clean_column_names =
    !query_ctx.explain_params.Has(ExplainWith::Registers);

  std::vector<std::string> plans;
  static constexpr size_t kMaxPlanCount = 5;
  plans.reserve(kMaxPlanCount);
  std::vector<std::string_view> data;

  auto process_plan = [&](std::string plan) {
    if (clean_column_names) {
      plan = CleanColumnNames(std::move(plan));
    }
    SDB_ASSERT(plans.size() < kMaxPlanCount);
    auto& stored = plans.emplace_back(std::move(plan));
    for (auto line : absl::StrSplit(stored, '\n')) {
      line = basics::string_utils::RTrim(line);
      if (!line.empty()) {
        data.emplace_back(line);
      }
    }
  };

  if (query_ctx.explain_params.Has(ExplainWith::Logical)) {
    data.emplace_back("LOGICAL PLAN:");
    process_plan(_query->GetLogicalPlanText());
  }

  if (query_ctx.explain_params.Has(ExplainWith::InitialQueryGraph)) {
    data.emplace_back("INITIAL QUERY GRAPH:");
    process_plan(_query->GetInitialQueryGraphPlan());
  }

  if (query_ctx.explain_params.Has(ExplainWith::FinalQueryGraph)) {
    data.emplace_back("FINAL QUERY GRAPH:");
    process_plan(_query->GetFinalQueryGraphPlan());
  }

  if (query_ctx.explain_params.Has(ExplainWith::Physical)) {
    data.emplace_back("PHYSICAL PLAN:");
    process_plan(_query->GetPhysicalPlan());
  }

  if (query_ctx.explain_params.Has(ExplainWith::Execution)) {
    if (query_ctx.explain_params.Has(ExplainWith::Stats)) {
      data.emplace_back("EXECUTION PLAN WITH STATS:");
      process_plan(_query->GetRunner().PrintPlanWithStats());
    } else {
      data.emplace_back("EXECUTION PLAN:");
      process_plan(_query->GetExecutionPlan());
    }
  }

  return _query->BuildBatch({data});
  ;
}

}  // namespace sdb::query
