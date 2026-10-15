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

#include <atomic>
#include <duckdb/common/arena_linked_list.hpp>
#include <duckdb/execution/operator/set/physical_union.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <utility>

#include "connector/segment_rewrite/logical_postings_merge.h"
#include "connector/segment_rewrite/physical_merge_units.h"
#include "search/compaction/merge_unit_run.h"

namespace sdb::connector {

LogicalMergeUnits::LogicalMergeUnits(
  std::shared_ptr<search::MergeUnitRun> run, duckdb::idx_t threads,
  std::span<const std::unique_ptr<irs::TermRangeMerge>> ranges)
  : duckdb::LogicalExtensionOperator(),
    _run{std::move(run)},
    _threads{threads} {
  if (ranges.empty()) {
    return;
  }
  auto left = std::make_shared<std::atomic<size_t>>(ranges.size());
  for (size_t i = 0; i != ranges.size(); ++i) {
    children.push_back(duckdb::make_uniq<LogicalPostingsMerge>(
      *ranges[i], duckdb::TableIndex(1 + 2 * i), [run = _run] { run->Wake(); },
      [run = _run, left] {
        if (left->fetch_sub(1) == 1) {
          run->ReleasePostings(run->plan.FinishPostings());
        }
      }));
  }
}

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
  auto& units = planner.Make<PhysicalMergeUnits>(_run, _threads);
  if (children.empty()) {
    return units;
  }
  duckdb::ArenaLinkedList<duckdb::reference<duckdb::PhysicalOperator>> plans{
    planner.ArenaRef()};
  for (auto& child : children) {
    plans.push_back(planner.CreatePlan(*child));
  }
  plans.push_back(units);
  return planner.Make<duckdb::PhysicalUnion>(types, plans, 0, true);
}

}  // namespace sdb::connector
