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

#include "connector/segment_rewrite/logical_postings_merge.h"

#include <duckdb/execution/physical_plan_generator.hpp>
#include <iresearch/index/merge/term_ranges.hpp>

#include "connector/segment_rewrite/logical_presorted_merge.h"
#include "connector/segment_rewrite/physical_postings_merge.h"

namespace sdb::connector {

LogicalPostingsMerge::LogicalPostingsMerge(irs::TermRangeMerge& merge)
  : duckdb::LogicalExtensionOperator(), _merge{merge} {
  children.push_back(
    duckdb::make_uniq<LogicalPresortedMerge>(merge.KeyMerge()));
}

void LogicalPostingsMerge::ResolveTypes() {
  types = {duckdb::LogicalType::BIGINT};
}

duckdb::vector<duckdb::ColumnBinding>
LogicalPostingsMerge::GetColumnBindings() {
  return GenerateColumnBindings(duckdb::TableIndex(1), 1);
}

duckdb::vector<duckdb::TableIndex> LogicalPostingsMerge::GetTableIndex() const {
  return {duckdb::TableIndex(1)};
}

duckdb::string LogicalPostingsMerge::GetName() const {
  return "POSTINGS_MERGE";
}

duckdb::PhysicalOperator& LogicalPostingsMerge::CreatePlan(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) {
  auto& runs = planner.CreatePlan(*children[0]);
  auto& merge = planner.Make<PhysicalPostingsMerge>(_merge);
  merge.children.push_back(runs);
  return merge;
}

}  // namespace sdb::connector
