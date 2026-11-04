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

#include "connector/functions/sdb_maintenance.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_config.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/main/profiler/profiling_node.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/storage/statistics/node_statistics.hpp>
#include <iresearch/index/merge/merge_profile.hpp>
#include <iresearch/index/merge/pipeline_merge_executor.hpp>
#include <iresearch/utils/enum_name.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/functions/function_values.h"
#include "connector/merge_target.h"
#include "search/compaction/merge_planner.h"
#include "search/compaction/merge_registry.h"

namespace sdb::connector {
namespace {

constexpr size_t kListedNames = 8;

struct CompactBindData final : duckdb::TableFunctionData {
  explicit CompactBindData(MergeTarget target, MergeRequest request,
                           search::MergeRoundPlan plan)
    : target{std::move(target)},
      request{std::move(request)},
      plan{std::move(plan)} {}

  bool SupportStatementCache() const final { return false; }

  MergeTarget target;
  MergeRequest request;
  search::MergeRoundPlan plan;
};

struct CompactState final : duckdb::GlobalTableFunctionState {
  CompactState(duckdb::ClientContext& context, const CompactBindData& data)
    : executor{*context.db, duckdb::TaskScheduler::QueryThreads(context)},
      compaction{data.target, data.request, context} {}

  const irs::PipelineMergeExecutor executor;
  ManualCompaction compaction;
  size_t merges = 0;
  std::array<size_t, magic_enum::enum_count<search::MergeOutcome>()> outcomes{};
  uint64_t written = 0;
  uint64_t total_nanos = 0;
  irs::MergeProfile profile;
};

MergeTarget ResolveWritable(duckdb::ClientContext& context,
                            std::string_view relation,
                            std::string_view function) {
  auto target = MergeTarget::Resolve(context, relation);
  if (target.ReadOnly()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                    ERR_MSG("cannot run ", function, " on database \"",
                            target.Database(), "\": it is attached read-only"));
  }
  return target;
}

std::vector<std::string> ReadSegments(
  const duckdb::TableFunctionBindInput& input) {
  std::vector<std::string> segments;
  const auto& list = NamedParameter(input, "segments");
  if (list.IsNull()) {
    return segments;
  }
  for (const auto& value : duckdb::ListValue::GetChildren(list)) {
    if (!value.IsNull()) {
      segments.emplace_back(value.GetValue<std::string>());
    }
  }
  return segments;
}

search::MergeMode ReadMode(const duckdb::TableFunctionBindInput& input) {
  const auto& value = NamedParameter(input, "mode");
  if (value.IsNull()) {
    return search::MergeMode::Background;
  }
  const auto name = value.GetValue<std::string>();
  const auto mode = magic_enum::enum_cast<search::MergeMode>(
    name, [](char given, char enumerator) noexcept {
      return given == absl::ascii_tolower(enumerator);
    });
  if (mode) {
    return *mode;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("unknown sdb_compact mode \"", name,
            "\": expected background, compact, optimize or segments"));
}

uint32_t ReadTargetSegments(const duckdb::TableFunctionBindInput& input) {
  const auto& target = NamedParameter(input, "target_segments");
  if (target.IsNull()) {
    return 0;
  }
  const auto value = target.GetValue<int64_t>();
  if (value < 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("sdb_compact target_segments must be positive"));
  }
  return static_cast<uint32_t>(
    std::min<int64_t>(value, std::numeric_limits<uint32_t>::max()));
}

void AddResultColumns(duckdb::vector<duckdb::LogicalType>& types,
                      duckdb::vector<duckdb::Identifier>& names) {
  const auto add = [&](std::string_view name, duckdb::LogicalType type) {
    names.emplace_back(name);
    types.emplace_back(std::move(type));
  };
  add("merge", duckdb::LogicalType::BIGINT);
  add("outcome", duckdb::LogicalType::VARCHAR);
  add("inputs", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));
  add("input_docs", duckdb::LogicalType::BIGINT);
  add("input_live_docs", duckdb::LogicalType::BIGINT);
  add("input_bytes", duckdb::LogicalType::BIGINT);
  add("output", duckdb::LogicalType::VARCHAR);
  add("output_docs", duckdb::LogicalType::BIGINT);
  add("output_bytes", duckdb::LogicalType::BIGINT);
  add("total_ms", duckdb::LogicalType::DOUBLE);
  for (const auto name : search::kMergeStepColumns) {
    add(name, duckdb::LogicalType::DOUBLE);
  }
  add("error", duckdb::LogicalType::VARCHAR);
}

double Millis(uint64_t nanos) { return static_cast<double>(nanos) / 1e6; }

std::string Duration(uint64_t nanos) {
  return absl::StrFormat("%.1f ms", Millis(nanos));
}

std::string InputNames(std::span<const search::PlannedSegment> inputs) {
  auto names =
    absl::StrJoin(inputs.first(std::min(inputs.size(), kListedNames)), ", ",
                  [](std::string* out, const search::PlannedSegment& input) {
                    out->append(input.name);
                  });
  if (inputs.size() > kListedNames) {
    absl::StrAppend(&names, ", +", inputs.size() - kListedNames, " more");
  }
  return names;
}

void DescribeMerge(size_t number, const search::PlannedMerge& merge,
                   duckdb::InsertionOrderPreservingMap<std::string>& params) {
  const auto key = absl::StrCat("Merge ", number);
  const auto inputs = merge.inputs.size();
  params[key] = absl::StrCat(inputs, inputs == 1 ? " segment" : " segments",
                             " -> 1, ", merge.reason);
  params[absl::StrCat(key, " Inputs")] = InputNames(merge.inputs);
  params[absl::StrCat(key, " Docs")] =
    absl::StrCat(merge.docs, " (", merge.live_docs, " live)");
  params[absl::StrCat(key, " Bytes")] = absl::StrCat(
    duckdb::StringUtil::BytesToHumanReadableString(merge.bytes), " -> est ",
    duckdb::StringUtil::BytesToHumanReadableString(
      merge.EstimatedOutputBytes()));
}

duckdb::InsertionOrderPreservingMap<std::string> ExplainParams(
  const CompactBindData& data) {
  const auto& plan = data.plan;
  duckdb::InsertionOrderPreservingMap<std::string> params;
  params["Table"] = data.target.Name();
  params["Mode"] = std::string{irs::SnakeCaseName(plan.mode)};
  params["Segments"] = std::to_string(plan.segments);
  params["Merges"] = std::to_string(plan.merges.size());
  for (size_t i = 0; i < plan.merges.size(); ++i) {
    DescribeMerge(i + 1, plan.merges[i], params);
  }
  return params;
}

constexpr std::string_view kStepLabels[] = {
  "Prepare", "Norms", "Columns", "Columns Commit", "ANN", "Postings", "Commit",
};
static_assert(std::size(kStepLabels) == irs::kMergeStepCount);

void AppendRow(duckdb::DataChunk& chunk, size_t number,
               const search::MergeResult& merge) {
  const auto& stats = merge.outcome.stats;
  duckdb::vector<duckdb::Value> inputs;
  inputs.reserve(stats.inputs.size());
  for (const auto& input : stats.inputs) {
    inputs.emplace_back(input);
  }
  size_t column = 0;
  auto append = [&](duckdb::Value value) {
    chunk.data[column++].Append(std::move(value));
  };
  append(Bigint(number));
  append(duckdb::Value{irs::SnakeCaseName(search::OutcomeOf(merge.outcome))});
  append(duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(inputs)));
  append(Bigint(stats.input_docs));
  append(Bigint(stats.input_live_docs));
  append(Bigint(stats.input_bytes));
  append(NullIfEmpty(stats.output));
  append(Bigint(stats.output_docs));
  append(Bigint(stats.output_bytes));
  append(duckdb::Value::DOUBLE(Millis(stats.profile.TotalNanos())));
  for (const auto step : magic_enum::enum_values<irs::MergeStep>()) {
    append(duckdb::Value::DOUBLE(Millis(stats.profile.Nanos(step))));
  }
  append(NullIfEmpty(merge.error));
}

void Tally(CompactState& state, const search::MergeResult& merge) {
  const auto& stats = merge.outcome.stats;
  ++state.outcomes[static_cast<size_t>(search::OutcomeOf(merge.outcome))];
  state.written += stats.output_bytes;
  state.total_nanos += stats.profile.TotalNanos();
  for (const auto step : magic_enum::enum_values<irs::MergeStep>()) {
    state.profile.Add(step, stats.profile.Nanos(step));
  }
}

bool DeterministicRendering(duckdb::ClientContext& context) {
  const auto& settings =
    duckdb::ClientConfig::GetConfig(context).profiling_renderer_settings;
  const auto it = settings.find("deterministic");
  return it != settings.end() &&
         it->second.DefaultCastAs(duckdb::LogicalType::BOOLEAN)
           .GetValue<bool>();
}

duckdb::unique_ptr<duckdb::FunctionData> CompactBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  const auto relation = ReadRelation(input, "sdb_compact");
  const auto mode = ReadMode(input);
  auto segments = ReadSegments(input);
  const auto target_segments = ReadTargetSegments(input);
  if (mode == search::MergeMode::Segments && segments.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("sdb_compact mode segments requires segments"));
  }
  if (mode != search::MergeMode::Segments && !segments.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("sdb_compact segments requires mode segments"));
  }
  if (mode != search::MergeMode::Compact &&
      mode != search::MergeMode::Optimize && target_segments != 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("sdb_compact target_segments requires mode compact or optimize"));
  }

  auto target = ResolveWritable(context, relation, "sdb_compact");
  const auto snapshot = target.Snapshot();
  if (mode == search::MergeMode::Segments) {
    if (const auto unknown = search::UnknownSegments(snapshot, segments);
        !unknown.empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("segment(s) ", absl::StrJoin(unknown, ", "),
                              " not found in ", target.Name()));
    }
  }
  MergeRequest request{
    .mode = mode,
    .target_segments = target_segments != 0
                         ? target_segments
                         : search::DefaultTarget(mode, context),
    .exact_target = target_segments != 0,
    .segments = std::move(segments),
  };
  auto plan = target.Plan(snapshot, request, target.FieldOptions(),
                          duckdb::TaskScheduler::GetScheduler(context));
  AddResultColumns(return_types, names);
  return duckdb::make_uniq<CompactBindData>(
    std::move(target), std::move(request), std::move(plan));
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CompactInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  return duckdb::make_uniq<CompactState>(
    context, input.bind_data->Cast<CompactBindData>());
}

void CompactExecute(duckdb::ClientContext& context,
                    duckdb::TableFunctionInput& input,
                    duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<CompactState>();
  context.InterruptCheck();
  const irs::MergeProgress progress = [&context] {
    return StatementRunning(context);
  };
  search::MergeResult merge;
  if (!state.compaction.Next(progress, search::MergeOrigin::Sql, state.executor,
                             merge)) {
    return;
  }
  context.InterruptCheck();
  AppendRow(output, ++state.merges, merge);
  Tally(state, merge);
  SDB_PARK_ONCE_ON_FAILURE("pause_segment_rewrite_between_merges");
}

duckdb::unique_ptr<duckdb::NodeStatistics> CompactCardinality(
  duckdb::ClientContext&, const duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<duckdb::NodeStatistics>(
    bind_data->Cast<CompactBindData>().plan.merges.size());
}

duckdb::InsertionOrderPreservingMap<std::string> CompactToString(
  duckdb::TableFunctionToStringInput& input) {
  return ExplainParams(input.bind_data->Cast<CompactBindData>());
}

void CompactMetrics(duckdb::TableFunctionGetMetricsInput& input) {
  const auto& state = input.global_state->Cast<CompactState>();
  auto& metrics = input.operator_metrics;
  std::vector<std::string> summary;
  for (const auto outcome : magic_enum::enum_values<search::MergeOutcome>()) {
    if (const auto count = state.outcomes[static_cast<size_t>(outcome)];
        count != 0) {
      summary.emplace_back(
        absl::StrCat(irs::SnakeCaseName(outcome), " ", count));
    }
  }
  metrics.AddExtraInfo("Actual Outcome", summary.empty()
                                           ? std::string{"nothing ran"}
                                           : absl::StrJoin(summary, ", "));
  metrics.AddExtraInfo(
    "Actual Written",
    duckdb::StringUtil::BytesToHumanReadableString(state.written));
  const bool zero_times = DeterministicRendering(input.context);
  const auto shown = [zero_times](uint64_t nanos) {
    return Duration(zero_times ? 0 : nanos);
  };
  for (const auto step : magic_enum::enum_values<irs::MergeStep>()) {
    metrics.AddExtraInfo(
      absl::StrCat("Actual ", kStepLabels[static_cast<size_t>(step)]),
      shown(state.profile.Nanos(step)));
  }
  metrics.AddExtraInfo("Actual Total", shown(state.total_nanos));
}

duckdb::TableFunction CompactFunction() {
  duckdb::TableFunction function{"sdb_compact",
                                 duckdb::FunctionSignature().AddPositionalOnly(
                                   "relation", duckdb::LogicalType::VARCHAR),
                                 CompactExecute, CompactBind, CompactInit};
  function.cardinality = CompactCardinality;
  function.to_string = CompactToString;
  function.get_metrics = CompactMetrics;
  const auto segments_type =
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  function.GetSignature()
    .AddKeywordOnly(
      "mode", duckdb::LogicalType::VARCHAR,
      duckdb::Value{irs::SnakeCaseName(search::MergeMode::Background)})
    .AddKeywordOnly("segments", segments_type, duckdb::Value{segments_type})
    .AddKeywordOnly("target_segments", duckdb::LogicalType::BIGINT,
                    duckdb::Value{duckdb::LogicalType::BIGINT});
  return function;
}

}  // namespace

void RegisterSdbMaintenanceFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(CompactFunction());
}

}  // namespace sdb::connector
