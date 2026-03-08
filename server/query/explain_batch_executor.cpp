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

#include "query/explain_batch_executor.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_split.h>

#include <yaclib/async/make.hpp>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "query/query.h"
#include "query/velox_batch_executor.h"
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

ExplainBatchExecutor::ExplainBatchExecutor(
  std::unique_ptr<VeloxBatchExecutor> inner)
  : _inner{std::move(inner)} {}

void ExplainBatchExecutor::SetQuery(Query& query) {
  _query = &query;
  if (_inner) {
    _inner->SetQuery(query);
  }
}

yaclib::Future<velox::RowVectorPtr> ExplainBatchExecutor::Execute() {
  SDB_ASSERT(_query);

  if (_result) {
    return yaclib::MakeFuture<velox::RowVectorPtr>(std::move(_result));
  }

  // If we have an inner executor (EXPLAIN ANALYZE), run Velox first
  if (_inner && !_velox_done) {
    auto f = _inner->Execute();
    if (!f.Valid()) {
      _velox_done = true;
    } else if (f.Ready()) {
      auto batch = std::move(f).Touch().Ok();
      if (batch) {
        // Velox still producing — discard batch, call again
        return yaclib::MakeFuture<velox::RowVectorPtr>(nullptr);
      }
      // Velox done
      _velox_done = true;
    } else {
      // Velox waiting — propagate the future
      return std::move(f).ThenInline(
        [](velox::RowVectorPtr&&) -> velox::RowVectorPtr {
          // Discard batch, cursor will call Execute() again
          return nullptr;
        });
    }
  }

  // No inner executor, or Velox done — produce explain output
  _result = BuildExplainBatch();
  return yaclib::MakeFuture<velox::RowVectorPtr>(std::move(_result));
}

void ExplainBatchExecutor::RequestCancel() {
  if (_inner) {
    _inner->RequestCancel();
  }
}

velox::RowVectorPtr ExplainBatchExecutor::BuildExplainBatch() {
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
      SDB_ASSERT(_inner);
      data.emplace_back("EXECUTION PLAN WITH STATS:");
      post_process_plan(ProcessPlan(_inner->GetRunner().PrintPlanWithStats(),
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
