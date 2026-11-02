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
#include <iresearch/utils/system_compiler.hpp>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/functions/function_values.h"
#include "connector/merge_target.h"
#include "connector/query_deadline.h"
#include "search/compaction/merge_planner.h"
#include "search/compaction/merge_registry.h"

namespace sdb::connector {
namespace {

constexpr size_t kListedNames = 8;

struct CompactJob {
  MergeTarget target;
  MergeRequest request;
  search::MergeRoundPlan plan;
};

struct CompactBindData final : duckdb::TableFunctionData {
  explicit CompactBindData(CompactJob job) : job{std::move(job)} {}

  bool SupportStatementCache() const final { return false; }

  CompactJob job;
};

struct CompactState final : duckdb::GlobalTableFunctionState {
  CompactState(duckdb::ClientContext& context, const CompactJob& job)
    : scheduler{duckdb::TaskScheduler::GetScheduler(context)},
      threads{duckdb::TaskScheduler::QueryThreads(context)},
      compaction{job.target, job.request, scheduler} {}

  duckdb::TaskScheduler& scheduler;
  duckdb::idx_t threads;
  ManualCompaction compaction;
  size_t merges = 0;
  std::array<size_t, magic_enum::enum_count<search::MergeOutcome>()> outcomes{};
  uint64_t written = 0;
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

duckdb::LogicalType StepsType() {
  duckdb::child_list_t<duckdb::LogicalType> fields;
  for (const auto name : search::kMergeStepColumns) {
    fields.emplace_back(duckdb::Identifier{name}, duckdb::LogicalType::DOUBLE);
  }
  return duckdb::LogicalType::STRUCT(std::move(fields));
}

std::vector<std::string> ResultNames() {
  return {
    "merge",           "outcome",     "inputs", "input_docs",
    "input_live_docs", "input_bytes", "output", "output_docs",
    "output_bytes",    "total_ms",    "steps",  "error",
  };
}

duckdb::vector<duckdb::LogicalType> ResultTypes() {
  return {
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::VARCHAR,
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::VARCHAR,
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::DOUBLE,
    StepsType(),
    duckdb::LogicalType::VARCHAR,
  };
}

std::string Bytes(uint64_t bytes) {
  return duckdb::StringUtil::BytesToHumanReadableString(bytes);
}

std::string Duration(uint64_t nanos) {
  return absl::StrFormat("%.1f ms", static_cast<double>(nanos) / 1e6);
}

double Millis(uint64_t nanos) { return static_cast<double>(nanos) / 1e6; }

template<typename Range, typename Format>
std::string JoinLimited(const Range& items, Format format) {
  std::vector<std::string> parts;
  size_t count = 0;
  for (const auto& item : items) {
    if (count++ < kListedNames) {
      parts.emplace_back(format(item));
    }
  }
  if (count > kListedNames) {
    parts.emplace_back(absl::StrCat("+", count - kListedNames, " more"));
  }
  return absl::StrJoin(parts, ", ");
}

std::string RowIdRange(const search::PlannedMerge& merge) {
  search::RowIdRange rowids;
  for (const auto& input : merge.inputs) {
    if (input.rowids.Empty()) {
      return {};
    }
    rowids = search::Union(rowids, input.rowids);
  }
  if (rowids.Empty()) {
    return {};
  }
  return absl::StrCat(", rowid ", rowids.first, "..", rowids.last);
}

std::string Plural(uint64_t count, std::string_view noun) {
  return absl::StrCat(count, " ", noun, count == 1 ? "" : "s");
}

void DescribeMerge(size_t number, const search::PlannedMerge& merge,
                   const MergeTarget& target,
                   duckdb::InsertionOrderPreservingMap<std::string>& params) {
  const auto key = absl::StrCat("Merge ", number);
  params[key] = absl::StrCat(Plural(merge.inputs.size(), "segment"), " -> 1, ",
                             merge.reason);
  params[absl::StrCat(key, " Inputs")] =
    JoinLimited(merge.inputs,
                [](const search::PlannedSegment& input) { return input.name; });
  params[absl::StrCat(key, " Docs")] = absl::StrCat(
    merge.docs, " (", merge.live_docs, " live)", RowIdRange(merge));
  params[absl::StrCat(key, " Bytes")] = absl::StrCat(
    Bytes(merge.bytes), " -> est ", Bytes(merge.EstimatedOutputBytes()));
  auto columns = absl::StrCat(merge.columns, ", ", Bytes(merge.column_bytes));
  if (merge.columns != 0) {
    absl::StrAppend(&columns, ", largest ",
                    target.ColumnName(merge.largest_column), " ",
                    Bytes(merge.largest_column_bytes));
  }
  params[absl::StrCat(key, " Columns")] = std::move(columns);
  params[absl::StrCat(key, " Postings")] = Plural(merge.fields, "field");
  if (merge.ann_columns != 0) {
    params[absl::StrCat(key, " ANN")] = Plural(merge.ann_columns, "column");
  }
}

duckdb::InsertionOrderPreservingMap<std::string> ExplainParams(
  const CompactJob& job) {
  const auto& plan = job.plan;
  duckdb::InsertionOrderPreservingMap<std::string> params;
  params["Table"] = job.target.Name();
  params["Mode"] = std::string{irs::SnakeCaseName(plan.mode)};
  params["Segments"] = std::to_string(plan.segments);
  params["Merges"] = std::to_string(plan.merges.size());
  for (size_t i = 0; i < plan.merges.size(); ++i) {
    DescribeMerge(i + 1, plan.merges[i], job.target, params);
  }
  if (!plan.skipped.empty()) {
    params["Not Merged"] =
      JoinLimited(plan.skipped, [](const search::SkippedSegment& skipped) {
        return absl::StrCat(skipped.name, " (", skipped.reason, ")");
      });
  }
  if (!plan.notes.empty()) {
    params["Notes"] = absl::StrJoin(plan.notes, "; ");
  }
  return params;
}

std::string_view StepLabel(irs::MergeStep step) {
  switch (step) {
    case irs::MergeStep::Prepare:
      return "Prepare";
    case irs::MergeStep::Norms:
      return "Norms";
    case irs::MergeStep::Columns:
      return "Columns";
    case irs::MergeStep::ColumnsCommit:
      return "Columns Commit";
    case irs::MergeStep::Ann:
      return "ANN";
    case irs::MergeStep::Postings:
      return "Postings";
    case irs::MergeStep::Commit:
      return "Commit";
  }
  SDB_UNREACHABLE();
}

duckdb::Value StepsValue(const duckdb::LogicalType& type,
                         const irs::MergeProfile& profile) {
  duckdb::vector<duckdb::Value> fields;
  for (const auto step : search::StepMillis(profile)) {
    fields.emplace_back(duckdb::Value::DOUBLE(step));
  }
  return duckdb::Value::STRUCT(type, std::move(fields));
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
  append(duckdb::Value{
    std::string{irs::SnakeCaseName(search::OutcomeOf(merge.outcome))}});
  append(duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(inputs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.input_docs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.input_live_docs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.input_bytes)));
  append(stats.output.empty() ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                              : duckdb::Value{stats.output});
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.output_docs)));
  append(duckdb::Value::BIGINT(static_cast<int64_t>(stats.output_bytes)));
  append(duckdb::Value::DOUBLE(Millis(stats.profile.TotalNanos())));
  append(StepsValue(chunk.data[column].GetType(), stats.profile));
  append(error.empty() ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                       : duckdb::Value{std::string{error}});
}

void Tally(CompactState& state, const search::MergeResult& merge) {
  const auto& stats = merge.outcome.stats;
  ++state.outcomes[static_cast<size_t>(search::OutcomeOf(merge.outcome))];
  state.written += stats.output_bytes;
  for (size_t step = 0; step < irs::kMergeStepCount; ++step) {
    const auto id = static_cast<irs::MergeStep>(step);
    state.profile.Add(id, stats.profile.Nanos(id));
  }
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

duckdb::unique_ptr<duckdb::FunctionData> CompactBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  const auto relation = ReadRelation(input, "sdb_compact");
  const auto mode = ReadMode(input);
  const auto segments = ReadSegments(input);
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
    .segments = segments,
  };
  auto plan = target.Plan(snapshot, request, target.FieldOptions(),
                          duckdb::TaskScheduler::GetScheduler(context));

  for (auto& name : ResultNames()) {
    names.emplace_back(duckdb::Identifier{std::move(name)});
  }
  return_types = ResultTypes();
  return duckdb::make_uniq<CompactBindData>(
    CompactJob{.target = std::move(target),
               .request = std::move(request),
               .plan = std::move(plan)});
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CompactInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  return duckdb::make_uniq<CompactState>(
    context, input.bind_data->Cast<CompactBindData>().job);
}

void CompactExecute(duckdb::ClientContext& context,
                    duckdb::TableFunctionInput& input,
                    duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<CompactState>();
  context.InterruptCheck();
  const irs::MergeProgress progress = [&context] {
    return StatementRunning(context);
  };
  const irs::PipelineMergeExecutor executor{*context.db, &state.scheduler,
                                            state.threads};
  search::MergeResult merge;
  if (!state.compaction.Next(progress, search::MergeOrigin::Sql, executor,
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
    bind_data->Cast<CompactBindData>().job.plan.merges.size());
}

duckdb::InsertionOrderPreservingMap<std::string> CompactToString(
  duckdb::TableFunctionToStringInput& input) {
  return ExplainParams(input.bind_data->Cast<CompactBindData>().job);
}

void CompactMetrics(duckdb::TableFunctionGetMetricsInput& input) {
  const auto& state = input.global_state->Cast<CompactState>();
  auto& metrics = input.operator_metrics;
  std::vector<std::string> summary;
  for (size_t outcome = 0; outcome < state.outcomes.size(); ++outcome) {
    if (state.outcomes[outcome] != 0) {
      summary.emplace_back(absl::StrCat(
        irs::SnakeCaseName(static_cast<search::MergeOutcome>(outcome)), " ",
        state.outcomes[outcome]));
    }
  }
  metrics.AddExtraInfo("Actual Outcome", summary.empty()
                                           ? std::string{"nothing ran"}
                                           : absl::StrJoin(summary, ", "));
  metrics.AddExtraInfo("Actual Written", Bytes(state.written));
  const bool zero_times = DeterministicRendering(input.context);
  const auto shown = [zero_times](uint64_t nanos) {
    return Duration(zero_times ? 0 : nanos);
  };
  for (size_t step = 0; step < irs::kMergeStepCount; ++step) {
    const auto id = static_cast<irs::MergeStep>(step);
    metrics.AddExtraInfo(absl::StrCat("Actual ", StepLabel(id)),
                         shown(state.profile.Nanos(id)));
  }
  metrics.AddExtraInfo("Actual Total", shown(state.profile.TotalNanos()));
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
    .AddKeywordOnly("mode", duckdb::LogicalType::VARCHAR,
                    duckdb::Value{std::string{
                      irs::SnakeCaseName(search::MergeMode::Background)}})
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
