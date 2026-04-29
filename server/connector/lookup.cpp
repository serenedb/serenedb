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

#include "connector/lookup.h"

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/table.h"
#include "connector/duckdb_external_scan.h"
#include "connector/duckdb_scan_base.hpp"
#include "connector/duckdb_table_function.h"
#include "connector/primary_key.hpp"
#include "connector/rocksdb_lookup.h"

namespace sdb::connector {
namespace {

// Build the per-query File-lookup session. bind_data (file metadata, the
// expensive parse) is always cached. For CSV the lookup gstate is also
// built once and reused across batches (its scanner/parse_chunk are designed
// to be Reset per pk). For parquet/JSON we only cache bind_data + projection
// mapping; the lookup gstate is rebuilt each batch since its per-file scan
// progress can't be replayed.
std::shared_ptr<FileLookupSession> BuildSession(
  duckdb::ClientContext& context, std::shared_ptr<catalog::Table> table,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids) {
  auto session = std::make_shared<FileLookupSession>();

  duckdb::unique_ptr<duckdb::FunctionData> wrapper_bd;
  std::ignore = MakeExternalScanFunction(context, table,
                                         /*table_entry=*/nullptr, wrapper_bd);
  session->bind_data =
    std::move(wrapper_bd->Cast<ExternalScanBindData>().underlying_bind_data);

  const auto& fi = table->GetFileInfo();
  SDB_ENSURE(fi.storage_options, ERROR_INTERNAL);
  const std::string_view path = fi.storage_options->Path();
  session->lookup_func = MakeExternalLookupTableFunction(path);
  // CSV's and parquet's lookup TFs cache their gstate (they expose a
  // standalone init_global that bypasses MultiFileFunction). JSON still
  // re-inits per batch.
  const auto dot = path.rfind('.');
  if (dot != std::string_view::npos) {
    auto ext = path.substr(dot + 1);
    session->gstate_is_reusable =
      (ext == "csv" || ext == "tsv" || ext == "txt" || ext == "parquet");
  }

  // Map catalog id -> physical reader index: external tables declare
  // columns in the same order as the file, so the physical index is
  // the position in table->Columns().
  const auto& catalog_cols = table->Columns();
  containers::FlatHashMap<catalog::Column::Id, duckdb::idx_t> id_to_phys;
  id_to_phys.reserve(catalog_cols.size());
  for (duckdb::idx_t i = 0; i < catalog_cols.size(); ++i) {
    id_to_phys.emplace(catalog_cols[i].id, i);
  }

  session->column_indexes.reserve(projected_columns.size());
  session->real_proj_slots.reserve(projected_columns.size());
  session->chunk_types.reserve(projected_columns.size());
  for (duckdb::idx_t proj = 0; proj < projected_columns.size(); ++proj) {
    const auto bind_col = projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      // Virtual-column slot (rowid / tableoid / score / offsets) -- caller
      // fills.
      continue;
    }
    auto it = id_to_phys.find(bind_column_ids[bind_col]);
    SDB_ASSERT(it != id_to_phys.end(),
               "catalog column id not found in external table");
    session->column_indexes.emplace_back(it->second);
    session->real_proj_slots.push_back(proj);
    session->chunk_types.push_back(projected_types[proj]);
  }

  if (session->gstate_is_reusable) {
    // Build the heavy state once (CSV: file open + scanner + parse buffers).
    duckdb::TableFunctionInitInput init(session->bind_data.get(),
                                        session->column_indexes,
                                        /*projection_ids=*/{},
                                        /*filters=*/nullptr);
    session->lookup_gstate = session->lookup_func.init_global(context, init);
  }
  session->scratch.Initialize(context, session->chunk_types);
  return session;
}

// File branch: build (and cache) a FileLookupSession on first call; per
// batch decode + sort pk offsets and run the lookup TF. CSV reuses the
// cached gstate (offsets travel via TableFunctionInput); parquet/JSON
// re-init gstate with offsets baked in via init_input.
void LookupRowsFile(duckdb::ClientContext& context,
                    std::shared_ptr<catalog::Table> table,
                    std::span<const duckdb::idx_t> projected_columns,
                    std::span<const duckdb::LogicalType> projected_types,
                    std::span<const catalog::Column::Id> bind_column_ids,
                    std::span<const std::string_view> pk_bytes,
                    std::shared_ptr<FileLookupSession>& cached_session,
                    duckdb::DataChunk& output) {
  if (!cached_session) {
    cached_session = BuildSession(context, std::move(table), projected_columns,
                                  projected_types, bind_column_ids);
  }
  auto& s = *cached_session;

  // Decode SereneDB's primary_key encoding (8-byte big-endian, sign-flipped
  // for order preservation) into the cached int64 buffer, then sort. Sorting
  // ascending lets parquet skip row groups in O(log) and CSV/JSON dispense
  // offsets via a forward-only cursor.
  s.pk_lookups.resize(pk_bytes.size());
  for (duckdb::idx_t i = 0; i < pk_bytes.size(); ++i) {
    s.pk_lookups[i] = primary_key::ReadSigned<int64_t>(pk_bytes[i]);
  }
  std::ranges::sort(s.pk_lookups);

  s.scratch.Reset();
  if (s.gstate_is_reusable) {
    duckdb::TableFunctionInput in(s.bind_data.get(), /*local_state=*/nullptr,
                                  s.lookup_gstate.get());
    in.pk_lookups = s.pk_lookups;
    s.lookup_func.function(context, in, s.scratch);
  } else {
    duckdb::TableFunctionInitInput init(s.bind_data.get(), s.column_indexes,
                                        /*projection_ids=*/{},
                                        /*filters=*/nullptr);
    init.pk_lookups = s.pk_lookups;
    auto gstate = s.lookup_func.init_global(context, init);
    duckdb::ThreadContext thread_ctx(context);
    duckdb::ExecutionContext exec_ctx(context, thread_ctx,
                                      /*pipeline=*/nullptr);
    duckdb::unique_ptr<duckdb::LocalTableFunctionState> lstate;
    if (s.lookup_func.init_local) {
      lstate = s.lookup_func.init_local(exec_ctx, init, gstate.get());
    }
    duckdb::TableFunctionInput in(s.bind_data.get(), lstate.get(),
                                  gstate.get());
    s.lookup_func.function(context, in, s.scratch);
  }

  // Zero-copy fan-out: point each real-column slot of `output` at the
  // corresponding scratch vector. Virtual-column slots (rowid / score /
  // tableoid / offsets) are left untouched for the caller to fill. The
  // referenced scratch storage lives on the cached session, so it stays
  // alive for the caller's use of `output` (one batch).
  for (duckdb::idx_t c = 0; c < s.real_proj_slots.size(); ++c) {
    output.data[s.real_proj_slots[c]].Reference(s.scratch.data[c]);
  }
}

}  // namespace

void LookupRows(duckdb::ClientContext& context,
                const SereneDBScanBindData& bind_data,
                const rocksdb::Snapshot* snapshot,
                std::span<const duckdb::idx_t> projected_columns,
                std::span<const duckdb::LogicalType> projected_types,
                std::span<const catalog::Column::Id> bind_column_ids,
                rocksdb::Transaction* txn,
                std::span<const std::string_view> pk_bytes,
                std::shared_ptr<FileLookupSession>& cached_session,
                duckdb::DataChunk& output) {
  if (pk_bytes.empty()) {
    return;
  }
  if (bind_data.table && bind_data.table->GetTableType() == TableType::File) {
    // PK is decoded by the upstream lookup TableFunction as 8-byte
    // big-endian signed-int (sign-flipped for order preservation):
    //  - parquet: row index in the file
    //  - csv / json: byte offset of the row/record start
    LookupRowsFile(context, bind_data.table, projected_columns, projected_types,
                   bind_column_ids, pk_bytes, cached_session, output);
    return;
  }
  // RocksDB backend: same logic as the pre-refactor RocksDBRowMaterializer.
  RocksDBLookup lookup(bind_data.table ? bind_data.table->GetId() : ObjectId{},
                       snapshot, projected_columns, projected_types,
                       bind_column_ids, txn);
  lookup.Lookup(pk_bytes, output);
}

}  // namespace sdb::connector
