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

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_config.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/query_profiler.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <iresearch/index/merge/pipeline_merge_executor.hpp>
#include <iresearch/utils/enum_name.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "connector/query_deadline.h"

namespace sdb::connector {
namespace {

struct SegmentRewriteSourceState final : duckdb::GlobalSourceState {
  explicit SegmentRewriteSourceState(duckdb::idx_t threads) noexcept
    : threads{threads} {}

  duckdb::idx_t threads;
  std::unique_ptr<ManualCompaction> compaction;
  std::vector<search::MergeResult> executed;
  irs::PipelineMergeExecutor::Profiles pipelines;
};

double Millis(uint64_t nanos) { return static_cast<double>(nanos) / 1e6; }

std::string OutcomeName(const search::MergeResult& merge) {
  return std::string{irs::SnakeCaseName(search::OutcomeOf(merge.outcome))};
}

duckdb::unique_ptr<duckdb::ProfilingNode> MergeNode(
  size_t number, const search::MergeResult& merge,
  irs::PipelineMergeExecutor::Profiles queries) {
  const auto& stats = merge.outcome.stats;
  auto node = duckdb::make_uniq<duckdb::ProfilingNode>();
  auto& metrics = node->GetOperatorMetrics();
  metrics.name = absl::StrCat("MERGE ", number);
  metrics.operator_type = duckdb::PhysicalOperatorType::EXTENSION;
  metrics.time = static_cast<double>(stats.profile.TotalNanos()) / 1e9;
  metrics.AddExtraInfo("Outcome", OutcomeName(merge));
  metrics.AddExtraInfo("Inputs", std::to_string(stats.inputs.size()));
  node->children = std::move(queries);
  return node;
}

void Adopt(duckdb::ProfilingNode& node, duckdb::idx_t depth,
           const MergeTarget& target) {
  node.depth = depth;
  auto& metrics = node.GetOperatorMetrics();
  const auto& info = metrics.GetExtraInfo();
  if (const auto field = info.find("Field");
      field != info.end() && !field->second.IsStructured()) {
    irs::field_id id = 0;
    if (absl::SimpleAtoi(field->second.Scalar(), &id)) {
      auto named = info;
      named["Field"] = target.ColumnName(id);
      metrics.SetExtraInfo(std::move(named));
    }
  }
  for (auto& child : node.children) {
    Adopt(*child, depth + 1, target);
  }
}

void GraftPipelines(duckdb::ClientContext& context,
                    const duckdb::PhysicalOperator& op,
                    const MergeTarget& target,
                    irs::PipelineMergeExecutor::Profiles& merges) {
  auto& profiler = duckdb::QueryProfiler::Get(context);
  profiler.GetRootUnderLock([&](duckdb::optional_ptr<duckdb::ProfilingNode>) {
    const auto& tree = profiler.GetTreeMap();
    const auto entry = tree.find(op);
    if (entry == tree.end()) {
      return;
    }
    auto& node = entry->second.get();
    for (auto& merge : merges) {
      Adopt(*merge, node.depth + 1, target);
      node.AddChild(std::move(merge));
    }
  });
  merges.clear();
}

void AppendRow(duckdb::DataChunk& chunk, size_t number,
               const search::MergeResult& merge) {
  const auto& stats = merge.outcome.stats;
  const auto error = merge.res.message();
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
  append(duckdb::Value{OutcomeName(merge)});
  append(duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(inputs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.input_docs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.input_live_docs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.input_bytes)));
  append(stats.output.empty() ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                              : duckdb::Value{stats.output});
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.output_docs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.output_bytes)));
  append(duckdb::Value::DOUBLE(Millis(stats.profile.TotalNanos())));
  append(error.empty() ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                       : duckdb::Value{std::string{error}});
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
PhysicalSegmentRewrite::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SegmentRewriteSourceState>(
    duckdb::TaskScheduler::QueryThreads(context));
}

duckdb::SourceResultType PhysicalSegmentRewrite::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& state = input.global_state.Cast<SegmentRewriteSourceState>();
  auto& client = context.client;
  client.InterruptCheck();
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(client);
  if (!state.compaction) {
    state.compaction = std::make_unique<ManualCompaction>(
      _job->target, _job->request, scheduler);
  }
  const irs::MergeProgress progress = [&client] {
    return StatementRunning(client);
  };
  const bool profiled = context.thread.profiler.IsEnabled();
  irs::PipelineMergeExecutor::Profiles queries;
  const irs::PipelineMergeExecutor executor{
    *client.db, &scheduler, state.threads, profiled ? &queries : nullptr};
  search::MergeResult executed;
  if (state.compaction->Next(progress, search::MergeOrigin::Sql, executor,
                             executed)) {
    client.InterruptCheck();
    const auto number = state.executed.size() + 1;
    AppendRow(chunk, number, executed);
    if (profiled) {
      state.pipelines.emplace_back(
        MergeNode(number, executed, std::move(queries)));
    }
    state.executed.emplace_back(std::move(executed));
    SDB_PARK_ONCE_ON_FAILURE("pause_segment_rewrite_between_merges");
    return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
  }
  auto& profiler = context.thread.profiler;
  if (profiler.IsEnabled()) {
    auto params = ParamsToString();
    AppendSegmentRewriteActuals(state.executed, DeterministicRendering(client),
                                params);
    duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> info;
    for (const auto& [key, value] : params) {
      info[key] = value;
    }
    profiler.GetOperatorMetrics(*this).SetExtraInfo(info);
    GraftPipelines(client, *this, _job->target, state.pipelines);
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
