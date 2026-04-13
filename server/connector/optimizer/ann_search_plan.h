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

// Registers the ANN search plan optimizer with DuckDB.
// It detects the pattern:
//   ORDER BY <distance_func>(col, const_vector) ASC LIMIT k
// over a serenedb_scan, and rewrites it to use the HNSW index for
// efficient approximate nearest-neighbour retrieval.
//
// Index selection priority:
//   1. Explicit: the index named in the query via "FROM i ON t" syntax
//      (bind_data.table_entry->GetInvertedIndex() is non-null).
//   2. Auto: the first InvertedIndex on the table that covers the
//      distance column (fallback when no explicit index is given).
//
// Optimization is skipped when LIMIT is absent (top_k == 0).
void RegisterANNSearchOptimizer(duckdb::DatabaseInstance& db);

}  // namespace sdb::optimizer
