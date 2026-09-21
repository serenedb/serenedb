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

#include <duckdb.hpp>
#include <duckdb/common/explain_value.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>

#include "connector/scan/scan_plan.h"

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
                          duckdb::FunctionData& bind_data, duckdb::idx_t limit,
                          duckdb::idx_t offset);

duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> ScanToStringValue(
  duckdb::TableFunctionToStringInput& input);

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

}  // namespace sdb::connector
