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

#include <absl/strings/str_join.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/binder.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/enum_name.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/functions/function_values.h"
#include "connector/segment_rewrite/logical_segment_rewrite.h"
#include "connector/segment_rewrite/merge_target.h"
#include "search/compaction/copy_check.h"
#include "search/compaction/merge_planner.h"
#include "storage_engine/search_engine.h"

namespace sdb::connector {
namespace {

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
  if (const auto mode = search::ParseMergeMode(name)) {
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

duckdb::unique_ptr<duckdb::LogicalOperator> CompactBindOperator(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::TableIndex bind_index, duckdb::vector<duckdb::Identifier>& names) {
  SDB_ASSERT(input.binder);
  input.binder->SetAlwaysRequireRebind();
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
  auto target_count = target_segments != 0
                        ? target_segments
                        : search::DefaultTarget(mode, context);
  if (target.IsIndex() && mode == search::MergeMode::Compact) {
    target_count = 1;
  }
  const search::CopyChecker checker{
    target.FieldOptions(), duckdb::TaskScheduler::GetScheduler(context)};
  search::MergeRoundRequest request{
    .mode = mode,
    .settings = &target.Settings(),
    .merges = search::GetSearchEngine().MergeRoundSize(),
    .target_segments = target_count,
    .exact_target = target_segments != 0,
    .row_group_size = target.RowGroupSize(),
    .inputs_max = target.MergeInputsMax(),
    .segments = segments,
    .copy_check = &checker,
  };
  auto plan = search::PlanMergeRound(snapshot, request);
  auto job = std::make_shared<SegmentRewriteJob>(
    SegmentRewriteJob{.target = std::move(target), .plan = std::move(plan)});

  for (auto& name : SegmentRewriteColumnNames()) {
    names.emplace_back(duckdb::Identifier{std::move(name)});
  }
  return duckdb::make_uniq<LogicalSegmentRewrite>(bind_index.index,
                                                  std::move(job));
}

struct MaintenanceBindData final : duckdb::TableFunctionData {
  std::string relation;
};

struct MaintenanceState final : duckdb::GlobalTableFunctionState {
  bool done = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<MaintenanceState>();
  }
};

duckdb::unique_ptr<duckdb::FunctionData> BindRelation(
  duckdb::ClientContext& context, const duckdb::TableFunctionBindInput& input,
  std::string_view function) {
  auto data = duckdb::make_uniq<MaintenanceBindData>();
  data->relation = ReadRelation(input, function);
  ResolveWritable(context, data->relation, function);
  return data;
}

duckdb::unique_ptr<duckdb::FunctionData> RefreshBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  for (const auto* name : {"segments", "docs", "bytes", "total_ms"}) {
    names.emplace_back(name);
  }
  return_types.insert(return_types.end(), 3, duckdb::LogicalType::BIGINT);
  return_types.emplace_back(duckdb::LogicalType::DOUBLE);
  return BindRelation(context, input, "sdb_refresh");
}

duckdb::unique_ptr<duckdb::FunctionData> CleanupBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  for (const auto* name : {"files", "bytes", "total_ms"}) {
    names.emplace_back(name);
  }
  return_types.insert(return_types.end(), 2, duckdb::LogicalType::BIGINT);
  return_types.emplace_back(duckdb::LogicalType::DOUBLE);
  return BindRelation(context, input, "sdb_cleanup");
}

double MillisSince(std::chrono::steady_clock::time_point begin) {
  return std::chrono::duration<double, std::milli>(
           std::chrono::steady_clock::now() - begin)
    .count();
}

void RefreshExecute(duckdb::ClientContext& context,
                    duckdb::TableFunctionInput& input,
                    duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<MaintenanceState>();
  if (std::exchange(state.done, true)) {
    return;
  }
  const auto& data = input.bind_data->Cast<MaintenanceBindData>();
  const auto target = ResolveWritable(context, data.relation, "sdb_refresh");
  const auto begin = std::chrono::steady_clock::now();
  irs::containers::FlatHashSet<std::string> published;
  for (const auto& segment : target.Snapshot()) {
    published.emplace(segment.Meta().name);
  }
  target.Refresh();
  uint64_t segments = 0;
  uint64_t docs = 0;
  uint64_t bytes = 0;
  for (const auto& segment : target.Snapshot()) {
    if (published.contains(segment.Meta().name)) {
      continue;
    }
    const auto described = search::DescribeSegment(segment);
    ++segments;
    docs += described.docs;
    bytes += described.bytes;
  }
  output.data[0].Append(Bigint(segments));
  output.data[1].Append(Bigint(docs));
  output.data[2].Append(Bigint(bytes));
  output.data[3].Append(duckdb::Value::DOUBLE(MillisSince(begin)));
}

void CleanupExecute(duckdb::ClientContext& context,
                    duckdb::TableFunctionInput& input,
                    duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<MaintenanceState>();
  if (std::exchange(state.done, true)) {
    return;
  }
  const auto& data = input.bind_data->Cast<MaintenanceBindData>();
  const auto target = ResolveWritable(context, data.relation, "sdb_cleanup");
  const auto begin = std::chrono::steady_clock::now();
  const auto removed = target.Cleanup();
  output.data[0].Append(Bigint(removed.count));
  output.data[1].Append(Bigint(removed.bytes));
  output.data[2].Append(duckdb::Value::DOUBLE(MillisSince(begin)));
}

duckdb::FunctionSignature RelationSignature() {
  return duckdb::FunctionSignature().AddPositionalOnly(
    "relation", duckdb::LogicalType::VARCHAR);
}

duckdb::TableFunction CompactFunction() {
  duckdb::TableFunction function{"sdb_compact", RelationSignature(), nullptr};
  function.bind_operator = CompactBindOperator;
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
  loader.RegisterFunction(
    duckdb::TableFunction{"sdb_refresh", RelationSignature(), RefreshExecute,
                          RefreshBind, MaintenanceState::Init});
  loader.RegisterFunction(
    duckdb::TableFunction{"sdb_cleanup", RelationSignature(), CleanupExecute,
                          CleanupBind, MaintenanceState::Init});
}

}  // namespace sdb::connector
