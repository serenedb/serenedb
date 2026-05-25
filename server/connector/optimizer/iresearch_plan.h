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

#include <duckdb/common/vector.hpp>
#include <duckdb/planner/column_binding.hpp>
#include <duckdb/planner/expression.hpp>

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class LogicalGet;
class LogicalOperator;
class LogicalTopN;
class FunctionData;

}  // namespace duckdb
namespace sdb::optimizer {

void IresearchPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters);

void RegisterIresearchPlanOptimizer(duckdb::DatabaseInstance& db);

// True if `binding` resolves (through any chain of LogicalProjection /
// LogicalGet operators rooted at `op`) to a SereneDB scan's synthetic
// `kInvertedIndexScoreId` column. Shared between the optimizer rules and
// the `set_top_n_hint` callback.
bool BindingIsScoreColumn(duckdb::LogicalOperator& op,
                          duckdb::ColumnBinding binding);

// ANN TopK pushdown. Invoked from the `set_top_n_hint` callback when the
// `LogicalGet` underneath the `LogicalTopN` is a FullTable scan. On
// success: swaps the scan's `scan_source` from FullTable to ANNScan
// (carrying index_id, field_id, query_vector, top_k), absorbs claimable
// residual filters into an iresearch text filter on the ANN scan, and
// drops the filter chain above the get. The LogicalTopN node stays --
// HNSW returns rows pre-sorted by distance and the upstream operator
// re-checks.
bool TryAttachAnnTopK(duckdb::ClientContext& context,
                      duckdb::LogicalTopN& top_n, duckdb::LogicalGet& get);

}  // namespace sdb::optimizer
