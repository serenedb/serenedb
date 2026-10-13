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

#include "connector/functions/sdb_compact.h"

#include <absl/strings/str_join.h>

#include <algorithm>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/binder.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "connector/segment_rewrite/logical_segment_rewrite.h"
#include "connector/segment_rewrite/merge_target.h"
#include "query/config.h"
#include "search/compaction/copy_check.h"
#include "search/compaction/merge_planner.h"
#include "storage_engine/search_engine.h"

namespace sdb::connector {
namespace {

constinit SettingRef gTargetSegments{"sdb_compact_target_segments"};

const duckdb::Value& NamedParameter(const duckdb::TableFunctionBindInput& input,
                                    const char* name) {
  const auto entry = input.named_parameters.find(name);
  SDB_ASSERT(entry != input.named_parameters.end());
  return entry->second;
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

std::optional<uint32_t> ReadTargetSegments(
  const duckdb::TableFunctionBindInput& input) {
  const auto& target = NamedParameter(input, "target_segments");
  if (target.IsNull()) {
    return std::nullopt;
  }
  const auto value = target.GetValue<int64_t>();
  if (value < 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("sdb_compact target_segments must be positive"));
  }
  return static_cast<uint32_t>(value);
}

duckdb::unique_ptr<duckdb::LogicalOperator> CompactBindOperator(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::TableIndex bind_index, duckdb::vector<duckdb::Identifier>& names) {
  SDB_ASSERT(input.binder);
  input.binder->SetAlwaysRequireRebind();
  SDB_ASSERT(!input.inputs.empty());
  if (input.inputs[0].IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("sdb_compact requires a relation name"));
  }
  const auto relation = input.inputs[0].GetValue<std::string>();
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
      mode != search::MergeMode::Optimize && target_segments) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("sdb_compact target_segments requires mode compact or optimize"));
  }

  auto target = MergeTarget::Resolve(context, relation);
  const auto snapshot = target.Snapshot();
  if (mode == search::MergeMode::Segments) {
    if (const auto unknown = search::UnknownSegments(snapshot, segments);
        !unknown.empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("segment(s) ", absl::StrJoin(unknown, ", "),
                              " not found in ", target.Name()));
    }
  }
  const auto& settings = target.Settings();
  const auto default_target =
    mode == search::MergeMode::Optimize
      ? static_cast<uint32_t>(
          duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads())
      : gTargetSegments.Int(context);
  const search::CopyChecker checker{
    target.FieldOptions(), duckdb::TaskScheduler::GetScheduler(context)};
  search::MergeRoundRequest request{
    .mode = mode,
    .settings = &settings,
    .merges = search::GetSearchEngine().MergeRoundSize(),
    .target_segments = target_segments.value_or(default_target),
    .row_group_size = target.RowGroupSize(),
    .single_output = target.IsIndex(),
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

}  // namespace

void RegisterSdbCompactFunction(duckdb::DatabaseInstance& db) {
  duckdb::TableFunction function{"sdb_compact",
                                 duckdb::FunctionSignature().AddPositionalOnly(
                                   "relation", duckdb::LogicalType::VARCHAR),
                                 nullptr};
  function.bind_operator = CompactBindOperator;
  const auto segments_type =
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  function.GetSignature()
    .AddKeywordOnly("mode", duckdb::LogicalType::VARCHAR,
                    duckdb::Value{std::string{
                      search::MergeModeName(search::MergeMode::Background)}})
    .AddKeywordOnly("segments", segments_type, duckdb::Value{segments_type})
    .AddKeywordOnly("target_segments", duckdb::LogicalType::BIGINT,
                    duckdb::Value{duckdb::LogicalType::BIGINT});
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(std::move(function));
}

}  // namespace sdb::connector
