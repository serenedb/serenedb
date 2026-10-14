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

#include "connector/segment_rewrite/logical_presorted_merge.h"

#include <duckdb/execution/operator/order/physical_presorted_merge.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <utility>

namespace sdb::connector {

LogicalPresortedMerge::LogicalPresortedMerge(
  duckdb::shared_ptr<duckdb::PresortedMerge> merge)
  : duckdb::LogicalExtensionOperator(), _merge{std::move(merge)} {}

void LogicalPresortedMerge::ResolveTypes() {
  types = duckdb::PhysicalPresortedMerge::RunTypes();
}

duckdb::vector<duckdb::ColumnBinding>
LogicalPresortedMerge::GetColumnBindings() {
  return GenerateColumnBindings(
    duckdb::TableIndex(0), duckdb::PhysicalPresortedMerge::RunTypes().size());
}

duckdb::vector<duckdb::TableIndex> LogicalPresortedMerge::GetTableIndex()
  const {
  return {duckdb::TableIndex(0)};
}

duckdb::string LogicalPresortedMerge::GetName() const {
  return "PRESORTED_MERGE";
}

duckdb::PhysicalOperator& LogicalPresortedMerge::CreatePlan(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) {
  return planner.Make<duckdb::PhysicalPresortedMerge>(_merge, 0);
}

}  // namespace sdb::connector
