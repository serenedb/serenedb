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

#include "connector/segment_rewrite/physical_merge_order.h"

#include <duckdb/common/sorting/presorted_merge.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/utils/assert.hpp>

namespace sdb::connector {
namespace {

struct MergeOrderSinkState final : duckdb::LocalSinkState {
  duckdb::vector<duckdb::PresortedRun> runs;
};

}  // namespace

PhysicalMergeOrder::PhysicalMergeOrder(duckdb::PhysicalPlan& physical_plan,
                                       irs::MergePlan& plan)
  : duckdb::PhysicalOperator(physical_plan,
                             duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT}, 0),
    _plan{plan} {}

duckdb::unique_ptr<duckdb::LocalSinkState>
PhysicalMergeOrder::GetLocalSinkState(duckdb::ExecutionContext&) const {
  return duckdb::make_uniq<MergeOrderSinkState>();
}

duckdb::SinkResultType PhysicalMergeOrder::Sink(
  duckdb::ExecutionContext&, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& state = input.local_state.Cast<MergeOrderSinkState>();
  const auto* partitions = duckdb::FlatVector::GetData<uint64_t>(chunk.data[0]);
  const auto* inputs = duckdb::FlatVector::GetData<uint64_t>(chunk.data[1]);
  const auto* begins = duckdb::FlatVector::GetData<uint64_t>(chunk.data[2]);
  const auto* counts = duckdb::FlatVector::GetData<uint64_t>(chunk.data[3]);
  state.runs.clear();
  for (duckdb::idx_t i = 0; i != chunk.size(); ++i) {
    SDB_ASSERT(partitions[i] == partitions[0]);
    state.runs.push_back(
      {.input = inputs[i], .begin = begins[i], .count = counts[i]});
  }
  _plan.ExpandOrder(partitions[0], state.runs);
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SourceResultType PhysicalMergeOrder::GetDataInternal(
  duckdb::ExecutionContext&, duckdb::DataChunk&,
  duckdb::OperatorSourceInput&) const {
  return duckdb::SourceResultType::FINISHED;
}

duckdb::string PhysicalMergeOrder::GetName() const { return "MERGE_ORDER"; }

}  // namespace sdb::connector
