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

#include "connector/functions/sdb_segments.h"

#include <algorithm>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "connector/segment_rewrite/merge_target.h"
#include "search/compaction/merge_planner.h"

namespace sdb::connector {
namespace {

struct SegmentsBindData final : duckdb::TableFunctionData {
  std::string relation;
};

struct SegmentsState final : duckdb::GlobalTableFunctionState {
  std::vector<search::PlannedSegment> rows;
  size_t offset = 0;
  bool loaded = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<SegmentsState>();
  }
};

duckdb::unique_ptr<duckdb::FunctionData> SegmentsBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  SDB_ASSERT(!input.inputs.empty());
  if (input.inputs[0].IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("sdb_segments requires a relation name"));
  }
  auto data = duckdb::make_uniq<SegmentsBindData>();
  data->relation = input.inputs[0].GetValue<std::string>();
  MergeTarget::Resolve(context, data->relation);
  for (const auto* name : {"segment", "docs", "live_docs", "deleted_docs",
                           "bytes", "rowid_min", "rowid_max"}) {
    names.emplace_back(name);
  }
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  return_types.insert(return_types.end(), 6, duckdb::LogicalType::BIGINT);
  return data;
}

duckdb::Value OptionalBigint(const std::optional<int64_t>& value) {
  return value ? duckdb::Value::BIGINT(*value)
               : duckdb::Value{duckdb::LogicalType::BIGINT};
}

void SegmentsExecute(duckdb::ClientContext& context,
                     duckdb::TableFunctionInput& input,
                     duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<SegmentsState>();
  if (!state.loaded) {
    state.loaded = true;
    const auto& data = input.bind_data->Cast<SegmentsBindData>();
    const auto target = MergeTarget::Resolve(context, data.relation);
    const auto snapshot = target.Snapshot();
    state.rows.reserve(snapshot.size());
    for (const auto& segment : snapshot) {
      state.rows.emplace_back(search::DescribeSegment(segment));
    }
  }
  const auto end =
    std::min(state.rows.size(), state.offset + STANDARD_VECTOR_SIZE);
  for (; state.offset < end; ++state.offset) {
    const auto& row = state.rows[state.offset];
    output.data[0].Append(duckdb::Value{row.name});
    output.data[1].Append(
      duckdb::Value::BIGINT(static_cast<int64_t>(row.docs)));
    output.data[2].Append(
      duckdb::Value::BIGINT(static_cast<int64_t>(row.live_docs)));
    output.data[3].Append(
      duckdb::Value::BIGINT(static_cast<int64_t>(row.docs - row.live_docs)));
    output.data[4].Append(
      duckdb::Value::BIGINT(static_cast<int64_t>(row.bytes)));
    output.data[5].Append(OptionalBigint(row.rowid_min));
    output.data[6].Append(OptionalBigint(row.rowid_max));
  }
}

}  // namespace

void RegisterSdbSegmentsFunction(duckdb::DatabaseInstance& db) {
  duckdb::TableFunction function{"sdb_segments",
                                 duckdb::FunctionSignature().AddPositionalOnly(
                                   "relation", duckdb::LogicalType::VARCHAR),
                                 SegmentsExecute, SegmentsBind,
                                 SegmentsState::Init};
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(std::move(function));
}

}  // namespace sdb::connector
