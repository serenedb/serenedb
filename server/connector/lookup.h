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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table_function.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "catalog/table_options.h"

namespace rocksdb {

class Snapshot;
class Transaction;

}  // namespace rocksdb
namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

struct SereneDBScanBindData;

// Cached state for File-backed point lookups. Built lazily on the first
// LookupRows call for a given query and reused across batches:
//
//   - `lookup_func`: the standalone per-format lookup TableFunction value
//     (MakeParquetLookupTableFunction / MakeCSVLookupTableFunction /
//     MakeJSONLookupTableFunction), picked by the file's extension.
//   - `bind_data`: the parquet/csv/json reader's MultiFileBindData, parsed
//     once at session-build time. The expensive part -- avoiding re-binding
//     per batch is the main perf win.
//   - `column_indexes`: physical reader columns to project, computed once.
//     Passed via TableFunctionInitInput to init_global per batch.
//   - `pk_lookups`: int64 buffer reused across batches; each call to
//     LookupRows decodes pk_bytes (SereneDB's primary_key encoding) into
//     this buffer and sorts ascending. Hands the sorted span to upstream
//     via TableFunctionInitInput::pk_lookups (no per-call alloc; upstream
//     sees int64 only -- no SereneDB encoding leak).
//   - `real_proj_slots` / `chunk_types`: projection mapping from the
//     reader's column order back to SereneDB's output slot indices.
//
// Owned by CommonScanGlobalState; destroyed at end-of-query.
struct FileLookupSession {
  duckdb::TableFunction lookup_func;
  duckdb::unique_ptr<duckdb::FunctionData> bind_data;
  // CSV-only: gstate carries the reusable scanner / parse_chunk and is built
  // once per query. For parquet/JSON the gstate's per-file scan progress
  // can't be replayed, so they re-init per batch and this stays null.
  duckdb::unique_ptr<duckdb::GlobalTableFunctionState> lookup_gstate;
  duckdb::vector<duckdb::ColumnIndex> column_indexes;
  std::vector<duckdb::idx_t> real_proj_slots;
  duckdb::vector<duckdb::LogicalType> chunk_types;
  duckdb::DataChunk scratch;
  std::vector<int64_t> pk_lookups;  // reusable: decoded + sorted per batch
  // True for CSV: cached gstate + per-call pk_lookups via TableFunctionInput.
  // False for parquet/JSON: per-batch init_global with init_input.pk_lookups.
  bool gstate_is_reusable = false;
};

// Fills real-column slots of `output` for rows identified by `pk_bytes`
// (positional; pk_bytes.size() == output rows). Backend-specific decoding
// happens internally:
//   - File tables: invokes the wrapper's sibling lookup TableFunction. The
//     first call builds + caches a FileLookupSession on `cached_session`
//     (binds the file once, projection mapping computed once); subsequent
//     calls reuse it.
//   - RocksDB tables: per-column MultiGet/Seek/Get dispatched on batch size.
//
// Virtual-column slots (rowid / tableoid / score / offsets) are the
// caller's responsibility. Missing rows raise ERROR_INTERNAL.
//
// `cached_session` is a unique_ptr held on the scan's global state; pass it
// in and it'll be initialized on first call.
void LookupRows(duckdb::ClientContext& context,
                const SereneDBScanBindData& bind_data,
                const rocksdb::Snapshot* snapshot,
                std::span<const duckdb::idx_t> projected_columns,
                std::span<const duckdb::LogicalType> projected_types,
                std::span<const catalog::Column::Id> bind_column_ids,
                rocksdb::Transaction* txn,
                std::span<const std::string_view> pk_bytes,
                std::shared_ptr<FileLookupSession>& cached_session,
                duckdb::DataChunk& output);

}  // namespace sdb::connector
