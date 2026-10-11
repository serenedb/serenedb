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

#include "connector/segment_rewrite/physical_segment_rewrite.h"

#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_config.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/query_profiler.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <string>
#include <utility>
#include <vector>

namespace sdb::connector {
namespace {

struct SegmentRewriteSourceState final : duckdb::GlobalSourceState {
  search::CompactionOptions options;
  bool pinned = false;
  size_t next = 0;
  bool finished = false;
  std::vector<ExecutedMerge> executed;

  duckdb::idx_t MaxThreads() final { return 1; }
};

double Millis(uint64_t nanos) { return static_cast<double>(nanos) / 1e6; }

void AppendRow(duckdb::DataChunk& chunk, size_t number,
               const ExecutedMerge& merge) {
  const auto& stats = merge.stats;
  duckdb::vector<duckdb::Value> inputs;
  inputs.reserve(stats.inputs.size());
  for (const auto& input : stats.inputs) {
    inputs.emplace_back(input);
  }
  size_t column = 0;
  auto append = [&](duckdb::Value value) {
    chunk.data[column++].Append(std::move(value));
  };
  append(duckdb::Value::BIGINT(static_cast<int64_t>(number)));
  append(duckdb::Value{std::string{search::MergeOutcomeName(merge.outcome)}});
  append(duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(inputs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.input_docs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.input_live_docs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.input_bytes)));
  append(stats.output.empty() ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                              : duckdb::Value{stats.output});
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.output_docs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.output_bytes)));
  append(duckdb::Value::DOUBLE(Millis(stats.profile.TotalNanos())));
  for (size_t step = 0; step < irs::kMergeStepCount; ++step) {
    append(duckdb::Value::DOUBLE(
      Millis(stats.profile.Nanos(static_cast<irs::MergeStep>(step)))));
  }
  append(merge.error.empty() ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                             : duckdb::Value{merge.error});
}

bool DeterministicRendering(duckdb::ClientContext& context) {
  const auto& settings =
    duckdb::ClientConfig::GetConfig(context).profiling_renderer_settings;
  for (const auto& [name, value] : settings) {
    if (duckdb::StringUtil::Lower(name) == "deterministic") {
      return value.DefaultCastAs(duckdb::LogicalType::BOOLEAN).GetValue<bool>();
    }
  }
  return false;
}

}  // namespace

PhysicalSegmentRewrite::PhysicalSegmentRewrite(
  duckdb::PhysicalPlan& physical_plan, std::shared_ptr<SegmentRewriteJob> job,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(
      physical_plan, duckdb::PhysicalOperatorType::EXTENSION,
      [] {
        const auto types = SegmentRewriteColumnTypes();
        return duckdb::vector<duckdb::LogicalType>(types.begin(), types.end());
      }(),
      estimated_cardinality),
    _job{std::move(job)} {}

duckdb::unique_ptr<duckdb::GlobalSourceState>
PhysicalSegmentRewrite::GetGlobalSourceState(duckdb::ClientContext&) const {
  return duckdb::make_uniq<SegmentRewriteSourceState>();
}

duckdb::SourceResultType PhysicalSegmentRewrite::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& state = input.global_state.Cast<SegmentRewriteSourceState>();
  const auto& merges = _job->plan.merges;
  if (state.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  context.client.InterruptCheck();
  if (!state.pinned) {
    state.options = _job->target.PinOptions();
    state.pinned = true;
  }
  if (state.next < merges.size()) {
    const auto& planned = merges[state.next];
    std::vector<std::string> names;
    names.reserve(planned.inputs.size());
    for (const auto& segment : planned.inputs) {
      names.emplace_back(segment.name);
    }
    auto& client = context.client;
    const irs::MergeWriter::FlushProgress progress = [&client] {
      return !client.IsInterrupted();
    };
    auto run = _job->target.Compact(search::ExplicitPolicy(names), progress,
                                    state.options.field_options,
                                    search::MergeOrigin::Sql);
    ExecutedMerge executed{
      .outcome = run.error.empty() ? search::OutcomeOf(run.result)
                                   : search::MergeOutcome::Failed,
      .stats = std::move(run.result.stats),
      .error = std::move(run.error),
    };
    if (executed.stats.inputs.empty()) {
      executed.stats.inputs = std::move(names);
    }
    ++state.next;
    AppendRow(chunk, state.next, executed);
    state.executed.emplace_back(std::move(executed));
    if (state.next < merges.size()) {
      return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
    }
  }
  state.finished = true;
  if (!state.executed.empty()) {
    _job->target.Publish();
  }
  auto& profiler = context.thread.profiler;
  if (profiler.IsEnabled()) {
    auto params = ParamsToString();
    AppendSegmentRewriteActuals(state.executed,
                                DeterministicRendering(context.client), params);
    duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> info;
    for (const auto& [key, value] : params) {
      info[key] = value;
    }
    profiler.GetOperatorMetrics(*this).SetExtraInfo(std::move(info));
  }
  return duckdb::SourceResultType::FINISHED;
}

duckdb::string PhysicalSegmentRewrite::GetName() const {
  return "SEGMENT_REWRITE";
}

duckdb::InsertionOrderPreservingMap<duckdb::string>
PhysicalSegmentRewrite::ParamsToString() const {
  return SegmentRewriteParams(*_job);
}

}  // namespace sdb::connector
