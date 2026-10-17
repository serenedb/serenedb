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

#include <absl/algorithm/container.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "connector/segment_rewrite/merge_target.h"
#include "search/compaction/copy_check.h"
#include "search/compaction/merge_planner.h"

namespace sdb::connector {
namespace {

struct SegmentsBindData final : duckdb::TableFunctionData {
  std::string relation;
  bool columns = false;
};

struct ColumnRow {
  std::string segment;
  std::string column;
  std::string type;
  uint64_t rows = 0;
  uint64_t bytes = 0;
  std::string codecs;
  std::optional<uint64_t> reencoded_bytes;
  bool reencode = false;
};

struct SegmentsState final : duckdb::GlobalTableFunctionState {
  std::vector<search::PlannedSegment> rows;
  std::vector<ColumnRow> columns;
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
  const auto columns = input.named_parameters.find("columns");
  SDB_ASSERT(columns != input.named_parameters.end());
  data->columns = !columns->second.IsNull() && columns->second.GetValue<bool>();
  if (data->columns) {
    for (const auto* name : {"segment", "column", "type", "rows", "bytes",
                             "codecs", "reencoded_bytes", "verdict"}) {
      names.emplace_back(name);
    }
    return_types.insert(return_types.end(), 3, duckdb::LogicalType::VARCHAR);
    return_types.insert(return_types.end(), 2, duckdb::LogicalType::BIGINT);
    return_types.emplace_back(duckdb::LogicalType::VARCHAR);
    return_types.emplace_back(duckdb::LogicalType::BIGINT);
    return_types.emplace_back(duckdb::LogicalType::VARCHAR);
    return data;
  }
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

std::string Codecs(const irs::ColumnReader& column) {
  std::vector<std::string> names;
  for (const auto& block : column.DataBlocks()) {
    auto name = duckdb::CompressionTypeToString(block.codec->type);
    if (!absl::c_linear_search(names, name)) {
      names.emplace_back(std::move(name));
    }
  }
  return absl::StrJoin(names, ",");
}

void LoadColumns(duckdb::ClientContext& context, const MergeTarget& target,
                 const irs::IndexReader& snapshot,
                 std::vector<ColumnRow>& rows) {
  std::vector<const irs::SubReader*> segments;
  segments.reserve(snapshot.size());
  for (const auto& segment : snapshot) {
    segments.emplace_back(&segment);
  }
  const search::CopyChecker checker{
    target.FieldOptions(), duckdb::TaskScheduler::GetScheduler(context)};
  checker.Warm(segments);
  for (const auto* segment : segments) {
    const auto* reader = segment->GetColReader();
    SDB_ASSERT(reader);
    auto checks = search::CheckColumns(*segment, checker.Options());
    absl::c_sort(checks, [](const auto& lhs, const auto& rhs) {
      return lhs.column < rhs.column;
    });
    for (const auto& check : checks) {
      const auto* column = reader->Column(check.column);
      SDB_ASSERT(column);
      rows.emplace_back(ColumnRow{
        .segment = segment->Meta().name,
        .column = target.ColumnName(check.column),
        .type = column->Type().ToString(),
        .rows = check.rows,
        .bytes = check.bytes,
        .codecs = Codecs(*column),
        .reencoded_bytes = check.reencoded_bytes,
        .reencode = check.Reencode(),
      });
    }
  }
}

void EmitColumns(SegmentsState& state, duckdb::DataChunk& output) {
  const auto end =
    std::min(state.columns.size(), state.offset + STANDARD_VECTOR_SIZE);
  for (; state.offset < end; ++state.offset) {
    const auto& row = state.columns[state.offset];
    output.data[0].Append(duckdb::Value{row.segment});
    output.data[1].Append(duckdb::Value{row.column});
    output.data[2].Append(duckdb::Value{row.type});
    output.data[3].Append(
      duckdb::Value::BIGINT(static_cast<int64_t>(row.rows)));
    output.data[4].Append(
      duckdb::Value::BIGINT(static_cast<int64_t>(row.bytes)));
    output.data[5].Append(row.codecs.empty()
                            ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                            : duckdb::Value{row.codecs});
    if (row.reencoded_bytes) {
      output.data[6].Append(
        duckdb::Value::BIGINT(static_cast<int64_t>(*row.reencoded_bytes)));
      output.data[7].Append(duckdb::Value{row.reencode ? "re-encode" : "copy"});
    } else {
      output.data[6].Append(duckdb::Value{duckdb::LogicalType::BIGINT});
      output.data[7].Append(duckdb::Value{duckdb::LogicalType::VARCHAR});
    }
  }
}

void EmitSegments(SegmentsState& state, duckdb::DataChunk& output) {
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

void SegmentsExecute(duckdb::ClientContext& context,
                     duckdb::TableFunctionInput& input,
                     duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<SegmentsState>();
  const auto& data = input.bind_data->Cast<SegmentsBindData>();
  if (!state.loaded) {
    state.loaded = true;
    const auto target = MergeTarget::Resolve(context, data.relation);
    const auto snapshot = target.Snapshot();
    if (data.columns) {
      LoadColumns(context, target, snapshot, state.columns);
    } else {
      state.rows.reserve(snapshot.size());
      for (const auto& segment : snapshot) {
        state.rows.emplace_back(search::DescribeSegment(segment));
      }
    }
  }
  if (data.columns) {
    EmitColumns(state, output);
  } else {
    EmitSegments(state, output);
  }
}

}  // namespace

void RegisterSdbSegmentsFunction(duckdb::DatabaseInstance& db) {
  duckdb::TableFunction function{"sdb_segments",
                                 duckdb::FunctionSignature().AddPositionalOnly(
                                   "relation", duckdb::LogicalType::VARCHAR),
                                 SegmentsExecute, SegmentsBind,
                                 SegmentsState::Init};
  function.GetSignature().AddKeywordOnly(
    "columns", duckdb::LogicalType::BOOLEAN, duckdb::Value::BOOLEAN(false));
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(std::move(function));
}

}  // namespace sdb::connector
