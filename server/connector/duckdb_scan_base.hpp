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

#include <atomic>
#include <duckdb.hpp>
#include <duckdb/function/table_function.hpp>
#include <vector>

#include "rocksdb/iterator.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

struct SereneDBScanBindData;

// Common state inherited by all per-scan global states.
// Holds the fields shared across every scan strategy: isolation context,
// projection mapping, virtual column metadata, and the termination flag.
struct CommonScanGlobalState : public duckdb::GlobalTableFunctionState {
  // Isolation: exactly one of txn / snapshot is set.
  rocksdb::Transaction* txn = nullptr;
  const rocksdb::Snapshot* snapshot = nullptr;

  // Maps output column index -> bind_data column index.
  // INVALID_INDEX marks virtual columns (rowid, tableoid, score).
  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;

  // Rowid virtual column
  bool has_generated_pk = false;
  bool scan_rowid = false;
  duckdb::idx_t rowid_output_idx = 0;

  // Tableoid virtual column
  bool scan_tableoid = false;
  duckdb::idx_t tableoid_output_idx = 0;
  int64_t tableoid_value = 0;

  // Score virtual column (set only for search scans with BM25/TFIDF scorer)
  bool scan_score = false;
  duckdb::idx_t score_output_idx = 0;

  bool finished = false;

  // Rows emitted -- read by the rows_scanned DuckDB callback.
  std::atomic<duckdb::idx_t> produced_rows{0};

  ~CommonScanGlobalState() override;
};

struct CommonScanLocalState : public duckdb::LocalTableFunctionState {};

// Fills the common fields of `state` from `bind_data` and `input`.
// Handles: snapshot/txn isolation setup, projection pushdown, has_generated_pk,
// and virtual column (rowid, tableoid, score) detection.
// Does NOT create iterators -- each scan's InitGlobal does that afterward.
void InitCommonState(CommonScanGlobalState& state,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data,
                     duckdb::TableFunctionInitInput& input);

// Read generated PK int64 values from RocksDB iterator keys into output.
// Key format: [ObjectId(8)][ColumnId(8)][PK int64 big-endian XOR 0x80].
duckdb::idx_t ReadGeneratedPKFromKeys(rocksdb::Iterator& it,
                                      duckdb::Vector& output,
                                      duckdb::idx_t max_rows);

// DuckDB rows_scanned callback: returns produced_rows from
// CommonScanGlobalState.
duckdb::idx_t CommonScanRowsScanned(duckdb::GlobalTableFunctionState& gstate,
                                    duckdb::LocalTableFunctionState&);

// DuckDB init_local callback: creates an empty CommonScanLocalState.
duckdb::unique_ptr<duckdb::LocalTableFunctionState> CommonScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

}  // namespace sdb::connector
