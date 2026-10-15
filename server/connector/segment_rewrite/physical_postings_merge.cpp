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

#include "connector/segment_rewrite/physical_postings_merge.h"

#include <duckdb/common/sorting/presorted_merge.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/index/merge/term_ranges.hpp>
#include <iresearch/utils/assert.hpp>
#include <memory>
#include <string>

namespace sdb::connector {
namespace {

struct PostingsMergeSinkState final : duckdb::LocalSinkState {
  std::unique_ptr<irs::TermRangeWriter> writer;
  uint64_t partition = 0;
  duckdb::vector<duckdb::PresortedRun> runs;
};

void FinishPartition(irs::TermRangeMerge& merge,
                     PostingsMergeSinkState& state) {
  if (!state.writer) {
    return;
  }
  merge.FinishPart(state.partition, *state.writer);
  state.writer.reset();
}

}  // namespace

PhysicalPostingsMerge::PhysicalPostingsMerge(
  duckdb::PhysicalPlan& physical_plan, irs::TermRangeMerge& merge)
  : duckdb::PhysicalOperator(physical_plan,
                             duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT}, 0),
    _merge{merge} {}

duckdb::unique_ptr<duckdb::LocalSinkState>
PhysicalPostingsMerge::GetLocalSinkState(duckdb::ExecutionContext&) const {
  return duckdb::make_uniq<PostingsMergeSinkState>();
}

duckdb::SinkResultType PhysicalPostingsMerge::Sink(
  duckdb::ExecutionContext&, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  if (chunk.size() == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  auto& state = input.local_state.Cast<PostingsMergeSinkState>();
  const auto* partitions = duckdb::FlatVector::GetData<uint64_t>(chunk.data[0]);
  const auto* inputs = duckdb::FlatVector::GetData<uint64_t>(chunk.data[1]);
  const auto* begins = duckdb::FlatVector::GetData<uint64_t>(chunk.data[2]);
  const auto* counts = duckdb::FlatVector::GetData<uint64_t>(chunk.data[3]);
  if (state.writer && state.partition != partitions[0]) {
    FinishPartition(_merge, state);
  }
  if (!state.writer) {
    state.partition = partitions[0];
    state.writer = _merge.BeginPart(state.partition);
  }
  state.runs.clear();
  for (duckdb::idx_t i = 0; i != chunk.size(); ++i) {
    SDB_ASSERT(partitions[i] == state.partition);
    state.runs.push_back(
      {.input = inputs[i], .begin = begins[i], .count = counts[i]});
  }
  state.writer->Consume(state.runs);
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType PhysicalPostingsMerge::Combine(
  duckdb::ExecutionContext&, duckdb::OperatorSinkCombineInput& input) const {
  FinishPartition(_merge, input.local_state.Cast<PostingsMergeSinkState>());
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SourceResultType PhysicalPostingsMerge::GetDataInternal(
  duckdb::ExecutionContext&, duckdb::DataChunk&,
  duckdb::OperatorSourceInput&) const {
  return duckdb::SourceResultType::FINISHED;
}

duckdb::string PhysicalPostingsMerge::GetName() const {
  return "POSTINGS_MERGE";
}

duckdb::InsertionOrderPreservingMap<duckdb::string>
PhysicalPostingsMerge::ParamsToString() const {
  duckdb::InsertionOrderPreservingMap<duckdb::string> result;
  result["Field"] = std::to_string(_merge.Field());
  result["Parts"] = std::to_string(_merge.PartCount());
  return result;
}

}  // namespace sdb::connector
