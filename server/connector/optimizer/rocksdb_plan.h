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

// Registers the rocksdb_plan optimizer rule with DuckDB.
//
// Detects PK and SK predicate patterns above a `serenedb_scan` LogicalGet
// and swaps the LogicalGet's function to a more specialised scan that uses
// RocksDB MultiGet (point lookups) or a bounded prefix iterator (range
// scans). On match, claimed filter expressions are removed from the
// surrounding LogicalFilter (the operator is dropped if it becomes empty).
//
// Initial scope: PK equality / IN-list (point lookup). PK range and SK
// variants land in subsequent phases.
//
// Order of registration: this rule runs after the iresearch_plan rule so
// iresearch-only predicates always win. Mutation subtrees (DELETE /
// UPDATE / MERGE) are not skipped by this rule; rocksdb-backed scans are
// safe to drive mutations.
void RegisterRocksDBPlanOptimizer(duckdb::DatabaseInstance& db);

}  // namespace sdb::optimizer
