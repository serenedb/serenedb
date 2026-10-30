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

#include "connector/segment_rewrite/logical_segment_rewrite.h"

#include <duckdb/execution/physical_plan_generator.hpp>
#include <utility>

#include "connector/segment_rewrite/physical_segment_rewrite.h"

namespace sdb::connector {

LogicalSegmentRewrite::LogicalSegmentRewrite(
  duckdb::idx_t bind_index, std::shared_ptr<SegmentRewriteJob> job)
  : duckdb::LogicalExtensionOperator(),
    _bind_index{bind_index},
    _job{std::move(job)} {
  estimated_cardinality = _job->plan.merges.size();
}

void LogicalSegmentRewrite::ResolveTypes() {
  const auto column_types = SegmentRewriteColumnTypes();
  types.assign(column_types.begin(), column_types.end());
}

duckdb::vector<duckdb::ColumnBinding>
LogicalSegmentRewrite::GetColumnBindings() {
  return GenerateColumnBindings(duckdb::TableIndex(_bind_index),
                                SegmentRewriteColumnTypes().size());
}

duckdb::vector<duckdb::TableIndex> LogicalSegmentRewrite::GetTableIndex()
  const {
  return {duckdb::TableIndex(_bind_index)};
}

duckdb::string LogicalSegmentRewrite::GetName() const {
  return "SEGMENT_REWRITE";
}

duckdb::InsertionOrderPreservingMap<duckdb::string>
LogicalSegmentRewrite::ParamsToString() const {
  return SegmentRewriteParams(*_job);
}

duckdb::PhysicalOperator& LogicalSegmentRewrite::CreatePlan(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) {
  return planner.Make<PhysicalSegmentRewrite>(_job, estimated_cardinality);
}

}  // namespace sdb::connector
