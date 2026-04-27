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

class DatabaseInstance;
}

namespace sdb::optimizer {

// Registers the iresearch_plan optimizer rule with DuckDB.
//
// Consolidates all iresearch-backed strategy selection in one place:
//
//   case 1: filter (+ optional scoring) (+ optional offsets)
//             -> SearchScan with filter_summary, scorer, emit_offsets
//   case 2: filter + topk-on-score (+ optional offsets)
//             -> SearchScan with score_top_k set
//   case 3: WHERE distance_func(col, const_vec) < radius
//             -> RangeSearchScan
//   case 4: ORDER BY distance_func(col, const_vec) ASC LIMIT k
//             -> ANNScan
//   case 5: row-count-only consumer (LogicalGet with zero projected
//           columns -- COUNT(*) / COUNT(1) / EXISTS(SELECT 1 ...))
//             -> CountScan (pass 2, after scorer/offsets attachment)
//
// Runs BEFORE rocksdb_plan so iresearch-only predicates (TSQUERY
// `@@`, distance, BM25, ...) always win when present. Rocksdb-side
// predicates that the iresearch rule doesn't claim flow through to
// rocksdb_plan unchanged.
//
// Mutation-context guard: if the scan we're optimising lives inside a
// LOGICAL_DELETE / LOGICAL_UPDATE / LOGICAL_MERGE_INTO subtree, the
// iresearch rule does NOT fire (eventual-consistency constraint -- the
// inverted index lags row writes by one commit cycle, so DML must read
// straight from the rocksdb layer). The rocksdb_plan rule still applies
// in mutation subtrees.
void RegisterIresearchPlanOptimizer(duckdb::DatabaseInstance& db);

}  // namespace sdb::optimizer
