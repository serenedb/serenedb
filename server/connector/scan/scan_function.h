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

#pragma once

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb.hpp>
#include <duckdb/common/explain_value.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <iresearch/index/index_reader.hpp>
#include <span>
#include <vector>

#include "connector/scan/scan_plan.h"
#include "connector/search_filter_printer.hpp"

namespace sdb::connector {

struct ScanBindData;

duckdb::TableFunction CreateIResearchScanFunction();

void RegisterIResearchScanFunction(duckdb::DatabaseInstance& db);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IResearchScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> IResearchScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void IResearchScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

void IResearchScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input);

double IResearchScanProgress(duckdb::ClientContext& context,
                             const duckdb::FunctionData* bind_data,
                             const duckdb::GlobalTableFunctionState* gstate);

void IResearchSetScanOrder(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data);

bool IResearchConsumeTopN(duckdb::ClientContext& context,
                          duckdb::FunctionData& bind_data,
                          const duckdb::TableFilterSet& filters,
                          const duckdb::BoundLimitNode& limit,
                          const duckdb::BoundLimitNode& offset);

bool IResearchConsumeCount(duckdb::ClientContext& context,
                           duckdb::FunctionData& bind_data);

duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> ScanToStringValue(
  duckdb::TableFunctionToStringInput& input);

duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>
IndexLookupToStringValue(duckdb::TableFunctionToStringInput& input);

irs::FilterTree IndexFilterTree(const ScanBindData& bind,
                                const irs::Filter& filter);

bool IResearchSupportsPushdownExtract(const duckdb::FunctionData& bind_data,
                                      const duckdb::LogicalIndex& col_idx);

duckdb::TableFilterPushdown IResearchSupportsPushdownFilter(
  duckdb::FunctionData& bind_data, duckdb::idx_t col_idx,
  duckdb::TableFilter& filter);

bool IResearchPushdownExpression(duckdb::ClientContext& context,
                                 const duckdb::LogicalGet& get,
                                 duckdb::Expression& expr);

duckdb::unique_ptr<duckdb::BaseStatistics> IResearchScanStatistics(
  duckdb::ClientContext& context,
  duckdb::TableFunctionGetStatisticsInput& input);

duckdb::vector<duckdb::PartitionStatistics> IResearchScanPartitionStats(
  duckdb::ClientContext& context, duckdb::GetPartitionStatsInput& input);

struct ScanRowGroup {
  uint32_t seg = 0;
  uint32_t rg = 0;

  auto operator<=>(const ScanRowGroup&) const = default;
};

struct SegmentRowGroups {
  SegmentRowGroups() = default;
  SegmentRowGroups(const irs::SubReader& segment, uint64_t row_group_size);

  uint32_t Count() const noexcept {
    return starts.empty() ? static_cast<uint32_t>(std::max<uint64_t>(
                              1, (docs + row_group_size - 1) / row_group_size))
                          : static_cast<uint32_t>(starts.size());
  }
  uint64_t Begin(uint32_t rg) const noexcept {
    return std::min(
      docs, starts.empty() ? uint64_t{rg} * row_group_size : starts[rg]);
  }
  uint64_t End(uint32_t rg) const noexcept {
    return rg + 1 < Count() ? Begin(rg + 1) : docs;
  }
  uint32_t Of(uint64_t row) const noexcept {
    return static_cast<uint32_t>(
      starts.empty() ? row / row_group_size
                     : absl::c_upper_bound(starts, row) - starts.begin() - 1);
  }

  std::span<const uint64_t> starts;
  uint64_t row_group_size = 0;
  uint64_t docs = 0;
};

std::vector<ScanRowGroup> OffsetSkippedRowGroups(
  const ScanBindData& bind, const irs::IndexReader& reader);

duckdb::vector<duckdb::column_t> IResearchGetRowIdColumns(
  duckdb::ClientContext& context,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data);

bool IResearchSupportsLookup(duckdb::ClientContext& context,
                             const duckdb::LogicalGet& get,
                             duckdb::TableFunctionLookupInfo& info);

duckdb::TableFunction IResearchGetLookupFunction(
  duckdb::ClientContext& context, const duckdb::LogicalGet& source);

duckdb::unique_ptr<duckdb::LogicalGet> IResearchGetLookupSource(
  duckdb::ClientContext& context, const duckdb::LogicalGet& get,
  const duckdb::vector<duckdb::ProjectionIndex>& columns,
  duckdb::TableIndex table_index);

}  // namespace sdb::connector
