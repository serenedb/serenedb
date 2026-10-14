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

#include "connector/segment_rewrite/logical_merge_order.h"

#include <duckdb/execution/physical_plan_generator.hpp>
#include <iresearch/index/merge/merge_plan.hpp>

#include "connector/segment_rewrite/logical_presorted_merge.h"
#include "connector/segment_rewrite/physical_merge_order.h"

namespace sdb::connector {

LogicalMergeOrder::LogicalMergeOrder(irs::MergePlan& plan)
  : duckdb::LogicalExtensionOperator(), _plan{plan} {
  children.push_back(duckdb::make_uniq<LogicalPresortedMerge>(plan.KeyMerge()));
}

void LogicalMergeOrder::ResolveTypes() {
  types = {duckdb::LogicalType::BIGINT};
}

duckdb::vector<duckdb::ColumnBinding> LogicalMergeOrder::GetColumnBindings() {
  return GenerateColumnBindings(duckdb::TableIndex(1), 1);
}

duckdb::vector<duckdb::TableIndex> LogicalMergeOrder::GetTableIndex() const {
  return {duckdb::TableIndex(1)};
}

duckdb::string LogicalMergeOrder::GetName() const { return "MERGE_ORDER"; }

duckdb::PhysicalOperator& LogicalMergeOrder::CreatePlan(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) {
  auto& runs = planner.CreatePlan(*children[0]);
  auto& order = planner.Make<PhysicalMergeOrder>(_plan);
  order.children.push_back(runs);
  return order;
}

}  // namespace sdb::connector
