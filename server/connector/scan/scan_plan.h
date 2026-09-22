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

#include <duckdb/common/column_index.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/expression.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string_view>
#include <vector>

namespace sdb::connector {

struct ScanGlobalState;

// Counts the scans running now, so a plan sized for an idle machine is not
// chosen on a busy one. One per scan, for the scan's lifetime.
struct ScanInFlight {
  ScanInFlight() noexcept;
  ~ScanInFlight();
  ScanInFlight(const ScanInFlight&) = delete;
  ScanInFlight& operator=(const ScanInFlight&) = delete;
};

uint32_t FairShare(duckdb::ClientContext& context, uint32_t cap) noexcept;

// An upper bound on the rows this segment's column predicates admit, measured
// on a sample of the predicate rather than by decoding the column.
std::optional<uint64_t> EstimateColFilterRows(const irs::SubReader& seg,
                                              ScanGlobalState& g);
struct ScanBindData;
enum class ScanShape : uint8_t;

const irs::Filter& MatchAllFilter();

float StaticScoreFloor(const duckdb::Expression& expr, bool& exact);

void DecodeExtractPath(const duckdb::ColumnIndex& column_index,
                       const duckdb::LogicalType& root_type,
                       std::vector<std::string_view>& out);

void InitScanState(ScanGlobalState& state, duckdb::ClientContext* context,
                   const ScanBindData& bind_data,
                   duckdb::TableFunctionInitInput& input);

void ClassifyColumnstoreProjections(ScanGlobalState& state,
                                    const ScanBindData& bind_data);

ScanShape DecideShape(const ScanGlobalState& g, const ScanBindData& ss);

inline void EnsurePlanned(bool planned) {
  if (planned) [[likely]] {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("this search predicate has no index plan for this scan"));
}

}  // namespace sdb::connector
