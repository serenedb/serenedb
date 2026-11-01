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

#include "index/merge/support/parallel_merge.hpp"

#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <span>

namespace tests::merge {
namespace {

bool ExpandRuns(const duckdb::PresortedMerge& merge, auto&& expand) {
  duckdb::vector<duckdb::PresortedRun> runs;
  for (duckdb::idx_t partition = 0; partition != merge.PartitionCount();
       ++partition) {
    const auto state = merge.BeginMerge(partition);
    while (!state->Done()) {
      runs.clear();
      duckdb::optional_idx unsorted;
      if (!merge.Merge(
            *state, runs, STANDARD_VECTOR_SIZE, [] { return true; },
            unsorted) ||
          !expand(partition, std::span<const duckdb::PresortedRun>{runs})) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace

irs::PipelineMergeExecutor MakeExecutor(size_t threads) {
  return irs::PipelineMergeExecutor{irs::DuckDBEngine::Instance().instance(),
                                    nullptr, threads};
}

bool RunPlan(irs::MergePlan& plan, size_t threads) {
  return MakeExecutor(threads).Run(plan);
}

bool RunOrder(irs::MergePlan& plan) {
  if (!ExpandRuns(*plan.KeyMerge(), [&](size_t partition, auto runs) {
        return plan.ExpandOrder(partition, runs);
      })) {
    return false;
  }
  plan.FinishOrder();
  return true;
}

bool ExpandOrder(irs::KeyOrder& order) {
  return ExpandRuns(*order.KeyMerge(), [&](size_t partition, auto runs) {
    order.Expand(partition, runs);
    return true;
  });
}

ShuffledClaims::ShuffledClaims(bool shuffled)
  : _armed{shuffled &&
           duckdb::FaultPoints::Arm("irs::MergeSchedule::Shuffle")} {}

ShuffledClaims::~ShuffledClaims() {
  if (_armed) {
    duckdb::FaultPoints::Disarm("irs::MergeSchedule::Shuffle");
  }
}

}  // namespace tests::merge
