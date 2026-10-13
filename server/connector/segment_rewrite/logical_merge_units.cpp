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

#include "connector/segment_rewrite/logical_merge_units.h"

#include <duckdb/execution/physical_plan_generator.hpp>
#include <utility>

#include "connector/segment_rewrite/physical_merge_units.h"

namespace sdb::connector {

LogicalMergeUnits::LogicalMergeUnits(std::shared_ptr<search::MergeUnitRun> run,
                                     duckdb::idx_t threads)
  : duckdb::LogicalExtensionOperator(),
    _run{std::move(run)},
    _threads{threads} {}

void LogicalMergeUnits::ResolveTypes() {
  types = {duckdb::LogicalType::BIGINT};
}

duckdb::vector<duckdb::ColumnBinding> LogicalMergeUnits::GetColumnBindings() {
  return GenerateColumnBindings(duckdb::TableIndex(0), 1);
}

duckdb::vector<duckdb::TableIndex> LogicalMergeUnits::GetTableIndex() const {
  return {duckdb::TableIndex(0)};
}

duckdb::string LogicalMergeUnits::GetName() const { return "MERGE_UNITS"; }

duckdb::PhysicalOperator& LogicalMergeUnits::CreatePlan(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) {
  return planner.Make<PhysicalMergeUnits>(_run, _threads);
}

}  // namespace sdb::connector
