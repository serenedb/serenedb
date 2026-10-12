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

#include <chrono>
#include <duckdb/common/mutex.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_config.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/query_profiler.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "connector/query_deadline.h"
#include "search/compaction/merge_scheduler.h"
#include "search/compaction/merge_unit_run.h"

namespace sdb::connector {
namespace {

using Guard = duckdb::annotated_lock_guard<duckdb::annotated_mutex>;

struct SegmentRewriteSourceState final : duckdb::GlobalSourceState {
  explicit SegmentRewriteSourceState(duckdb::idx_t threads) noexcept
    : threads{threads} {}

  duckdb::idx_t MaxThreads() final { return threads; }

  duckdb::idx_t threads;
  search::CompactionOptions options;
  search::MergeScheduler::Lease lease;
  bool pinned = false;
  size_t next = 0;
  std::vector<ExecutedMerge> executed;
  bool driver_taken = false;
  bool finished = false;
  std::shared_ptr<search::MergeUnitRun> run;
  std::optional<duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>>
    actuals;
};

struct SegmentRewriteLocalState final : duckdb::LocalSourceState {
  bool driver = false;
};

void RunAndWake(SegmentRewriteSourceState& state, search::MergeUnitRun& run,
                size_t unit) {
  run.Run(unit);
  Guard guard{state.lock};
  state.UnblockTasks();
}

class SegmentRewriteExecutor final : public irs::MergeExecutor {
 public:
  explicit SegmentRewriteExecutor(SegmentRewriteSourceState& state) noexcept
    : _state{state} {}

  bool Run(irs::MergePlan& plan) final {
    const auto run = std::make_shared<search::MergeUnitRun>(plan);
    {
      Guard guard{_state.lock};
      _state.run = run;
      _state.UnblockTasks();
    }
    while (const auto unit = run->Next()) {
      RunAndWake(_state, *run, *unit);
    }
    {
      Guard guard{_state.lock};
      _state.run.reset();
    }
    return run->Finish();
  }

 private:
  SegmentRewriteSourceState& _state;
};

duckdb::SourceResultType Help(const duckdb::PhysicalOperator& op,
                              duckdb::ExecutionContext& context,
                              SegmentRewriteSourceState& state,
                              const duckdb::InterruptState& interrupt) {
  std::shared_ptr<search::MergeUnitRun> run;
  std::optional<size_t> unit;
  {
    Guard guard{state.lock};
    if (state.finished) {
      if (state.actuals) {
        context.thread.profiler.GetOperatorMetrics(op).SetExtraInfo(
          *state.actuals);
      }
      return duckdb::SourceResultType::FINISHED;
    }
    if (state.run) {
      run = state.run;
      unit = run->schedule.Claim();
    }
    if (!unit) {
      return state.BlockSource(interrupt);
    }
  }
  RunAndWake(state, *run, *unit);
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

void FinishJob(SegmentRewriteSourceState& state) {
  Guard guard{state.lock};
  state.finished = true;
  state.PreventBlocking();
  state.UnblockTasks();
}

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
PhysicalSegmentRewrite::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SegmentRewriteSourceState>(
    duckdb::TaskScheduler::QueryThreads(context));
}

duckdb::unique_ptr<duckdb::LocalSourceState>
PhysicalSegmentRewrite::GetLocalSourceState(duckdb::ExecutionContext&,
                                            duckdb::GlobalSourceState&) const {
  return duckdb::make_uniq<SegmentRewriteLocalState>();
}

duckdb::SourceResultType PhysicalSegmentRewrite::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& state = input.global_state.Cast<SegmentRewriteSourceState>();
  auto& local = input.local_state.Cast<SegmentRewriteLocalState>();
  if (!local.driver) {
    {
      Guard guard{state.lock};
      if (!state.driver_taken) {
        state.driver_taken = true;
        local.driver = true;
      }
    }
    if (!local.driver) {
      return Help(*this, context, state, input.interrupt_state);
    }
  }
  const auto& merges = _job->plan.merges;
  try {
    context.client.InterruptCheck();
    context.client.DeadlineCheck();
    if (!state.pinned) {
      state.options = _job->target.PinOptions();
      state.pinned = true;
    }
    if (state.next < merges.size()) {
      if (!state.lease) {
        state.lease = _job->target.TakeOver();
      }
      const auto& planned = merges[state.next];
      std::vector<std::string> names;
      names.reserve(planned.inputs.size());
      for (const auto& segment : planned.inputs) {
        names.emplace_back(segment.name);
      }
      auto& client = context.client;
      const irs::MergeWriter::FlushProgress progress = [&client] {
        return StatementRunning(client);
      };
      SegmentRewriteExecutor executor{state};
      auto run = _job->target.Compact(search::ExplicitPolicy(names), progress,
                                      state.options.field_options,
                                      search::MergeOrigin::Sql, &executor);
      context.client.InterruptCheck();
      context.client.DeadlineCheck();
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
    if (!state.executed.empty()) {
      _job->target.Publish();
    }
    auto& profiler = context.thread.profiler;
    if (profiler.IsEnabled()) {
      auto params = ParamsToString();
      AppendSegmentRewriteActuals(
        state.executed, DeterministicRendering(context.client), params);
      duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> info;
      for (const auto& [key, value] : params) {
        info[key] = value;
      }
      profiler.GetOperatorMetrics(*this).SetExtraInfo(info);
      Guard guard{state.lock};
      state.actuals = std::move(info);
    }
  } catch (...) {
    FinishJob(state);
    throw;
  }
  FinishJob(state);
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
