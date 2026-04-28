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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <vector>

#include <algorithm>

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

// Build the per-query File-lookup session: pick the right standalone lookup
// TableFunction by file extension, bind the underlying reader directly to
// get a MultiFileBindData (parses file metadata once -- the expensive part),
// compute the projection mapping (catalog id -> reader physical index,
// real-only column indexes, output slot fan-out), and call the lookup TF's
// init_global to allocate cached scratch / pk_lookups buffers.
std::shared_ptr<FileLookupSession> BuildSession(
  duckdb::ClientContext& context, std::shared_ptr<catalog::Table> table,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids) {
  auto session = std::make_shared<FileLookupSession>();

  // Bind the underlying reader to obtain its MultiFileBindData. We reuse
  // MakeExternalScanFunction's full bind path (file path resolution +
  // type/schema injection) but only keep the bind_data; the wrapper TF
  // it returns is for full-scan dispatch and isn't needed here.
  duckdb::unique_ptr<duckdb::FunctionData> wrapper_bd;
  (void)MakeExternalScanFunction(context, table, /*table_entry=*/nullptr,
                                 wrapper_bd);
  // ExternalScanBindData::underlying_bind_data is the raw parquet/csv/json
  // MultiFileBindData -- exactly what the standalone lookup TF expects.
  session->bind_data =
    std::move(wrapper_bd->Cast<ExternalScanBindData>().underlying_bind_data);

  const auto& fi = table->GetFileInfo();
  SDB_ENSURE(fi.storage_options, ERROR_INTERNAL);
  session->lookup_func =
    MakeExternalLookupTableFunction(fi.storage_options->Path());

  // Map catalog id -> physical reader index: external tables declare
  // columns in the same order as the file, so the physical index is
  // the position in table->Columns().
  const auto& catalog_cols = table->Columns();
  containers::FlatHashMap<catalog::Column::Id, duckdb::idx_t> id_to_phys;
  id_to_phys.reserve(catalog_cols.size());
  for (duckdb::idx_t i = 0; i < catalog_cols.size(); ++i) {
    id_to_phys.emplace(catalog_cols[i].id, i);
  }

  duckdb::vector<duckdb::ColumnIndex> column_indexes;
  column_indexes.reserve(projected_columns.size());
  session->real_proj_slots.reserve(projected_columns.size());
  session->chunk_types.reserve(projected_columns.size());
  for (duckdb::idx_t proj = 0; proj < projected_columns.size(); ++proj) {
    const auto bind_col = projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      // Virtual-column slot (rowid / tableoid / score / offsets) -- caller fills.
      continue;
    }
    auto it = id_to_phys.find(bind_column_ids[bind_col]);
    SDB_ASSERT(it != id_to_phys.end(),
               "catalog column id not found in external table");
    column_indexes.emplace_back(it->second);
    session->real_proj_slots.push_back(proj);
    session->chunk_types.push_back(projected_types[proj]);
  }

  // init_global of the lookup TF caches the projection (column_indexes /
  // projection_ids / filters) on its own gstate; per-batch the `function`
  // re-uses these and inits a fresh inner gstate with pk_lookups attached.
  duckdb::TableFunctionInitInput init(session->bind_data.get(), column_indexes,
                                       /*projection_ids=*/{},
                                       /*filters=*/nullptr);
  session->lookup_gstate =
    session->lookup_func.init_global(context, init);
  session->scratch.Initialize(context, session->chunk_types);
  return session;
}

// File branch: bind once (cached on `cached_session`), call the standalone
// per-format lookup TableFunction directly per batch with pk_bytes attached.
// The upstream callback drains all PKs into `session->scratch`; we then fan
// it out into the caller's `output` at the real-column slots only.
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
  // offsets via a forward-only cursor. Upstream sees int64 directly -- no
  // SereneDB-encoding leakage.
  s.pk_lookups.resize(pk_bytes.size());
  for (duckdb::idx_t i = 0; i < pk_bytes.size(); ++i) {
    s.pk_lookups[i] = primary_key::ReadSigned<int64_t>(pk_bytes[i]);
  }
  std::ranges::sort(s.pk_lookups);

  s.scratch.Reset();
  duckdb::TableFunctionInput in(s.bind_data.get(), /*local_state=*/nullptr,
                                 s.lookup_gstate.get());
  in.pk_lookups = s.pk_lookups;
  s.lookup_func.function(context, in, s.scratch);

  const auto scanned = s.scratch.size();
  for (duckdb::idx_t c = 0; c < s.real_proj_slots.size(); ++c) {
    duckdb::VectorOperations::Copy(s.scratch.data[c],
                                   output.data[s.real_proj_slots[c]], scanned,
                                   0, 0);
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
