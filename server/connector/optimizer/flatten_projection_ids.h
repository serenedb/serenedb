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

namespace duckdb {

class LogicalGet;
class LogicalOperator;

}  // namespace duckdb
namespace sdb::optimizer {

// Compact `get.column_ids` down to just the columns referenced by
// `get.projection_ids`, then clear `projection_ids`. Rewrites every
// `BoundColumnRefExpression` in `root` whose binding points to `get` so
// its `column_index` tracks the new compacted layout.
//
// Called from optimizer extensions (rocksdb_plan, iresearch_plan) right
// after swapping `get.function` to one of our specialised scan variants
// (all `filter_prune == false`). DuckDB's built-in RemoveUnusedColumns
// populates `projection_ids` only for `filter_prune == true` readers
// (e.g. parquet/json external tables); leaving those entries around after
// the swap is a landmine -- a subsequent RemoveUnusedColumns pass shrinks
// `column_ids` without rebuilding `projection_ids`, and
// LogicalGet::ResolveTypes then indexes off the end.
//
// Bindings stay valid because "projected output column i" is still "the
// i-th output", even though the underlying slot in `column_ids` moved.
// Filter-only columns (present in `column_ids` but absent from
// `projection_ids`) are dropped; that's safe because the caller just
// claimed the filter that referenced them.
void FlattenProjectionIds(duckdb::LogicalOperator& root,
                          duckdb::LogicalGet& get);

}  // namespace sdb::optimizer
