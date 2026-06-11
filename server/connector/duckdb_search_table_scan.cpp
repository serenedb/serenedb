////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "connector/duckdb_search_table_scan.hpp"

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "catalog/catalog.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/transaction.h"
#include "search/search_table_shard.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchTableScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  SDB_ASSERT(!bind_data.IsViewBacked(),
             "SearchTableScan reached a view-backed bind -- views route "
             "through CreateIResearchScanFunction, not here");
  const auto& tbd = bind_data.As<TableScanBindData>();
  auto state = duckdb::make_uniq<SearchTableScanGlobalState>();

  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto shard = snapshot->GetTableShard(tbd.table->GetId());
  SDB_ASSERT(shard);
  SDB_ASSERT(shard->GetStorage() == catalog::StorageKind::kSearch,
             "SearchTableScan dispatched against a non-search shard");
  auto& search_shard = basics::downCast<search::SearchTableShard>(*shard);

  state->reader = conn_ctx.SearchTxn().EnsureSearchTableReader(
    shard->GetId(), [&] { return search_shard.GetDirectoryReader(); });

  // COUNT(*)-style scan projects no real columns -- answer with the live row
  // count instead of materialising columns.
  if (IsCountOnlyScan(bind_data, input)) {
    state->count_only = true;
    state->count_remaining = state->reader->live_docs_count();
    return state;
  }

  const auto num_bind_columns = bind_data.column_ids.size();
  state->client_context = &context;
  state->cs_projections.reserve(input.column_ids.size());
  for (duckdb::idx_t out_slot = 0; out_slot < input.column_ids.size();
       ++out_slot) {
    const auto col_id = input.column_ids[out_slot];
    if (col_id >= num_bind_columns) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("Virtual columns (rowid, tableoid, score) on a search-"
                "backed table are not yet supported (column slot ",
                out_slot, ")"));
    }
    const auto catalog_col_id = bind_data.column_ids[col_id];
    state->cs_projections.push_back(
      ColumnstoreProjection{.output_slot = out_slot, .column_id = catalog_col_id});
  }

  return state;
}

void SearchTableScanFunction(duckdb::ClientContext& /*context*/,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchTableScanGlobalState>();
  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }
  if (gstate.count_only) {
    const auto n =
      std::min<uint64_t>(gstate.count_remaining, STANDARD_VECTOR_SIZE);
    output.SetCardinality(n);
    gstate.count_remaining -= n;
    gstate.produced_rows.fetch_add(n, std::memory_order_relaxed);
    gstate.finished = (gstate.count_remaining == 0);
    return;
  }
  SDB_ASSERT(gstate.reader);
  const auto& reader = *gstate.reader;
  const auto batch_size = static_cast<duckdb::idx_t>(STANDARD_VECTOR_SIZE);

  duckdb::idx_t produced = 0;
  // Stop at segment boundary: ColumnstoreMaterializer::Scan writes from slot
  // 0, so two segments cannot share one batch.
  while (produced == 0 && gstate.segment_idx < reader.size()) {
    auto& segment = reader[gstate.segment_idx];
    const auto seg_docs = segment.docs_count();
    if (gstate.doc_in_seg >= seg_docs) {
      ++gstate.segment_idx;
      gstate.doc_in_seg = 0;
      gstate.materializer.reset();
      continue;
    }

    if (!gstate.materializer) {
      const auto* cs_reader = segment.GetColReader();
      if (!cs_reader) {
        // Segment has no columnstore (e.g. an empty pre-INSERT commit).
        ++gstate.segment_idx;
        gstate.doc_in_seg = 0;
        continue;
      }
      gstate.materializer = std::make_unique<ColumnstoreMaterializer>(
        *cs_reader, gstate.cs_projections, gstate.client_context);
    }

    const auto take =
      std::min<duckdb::idx_t>(batch_size, seg_docs - gstate.doc_in_seg);
    gstate.materializer->Scan(gstate.doc_in_seg, take, output,
                              /*output_start=*/0);
    gstate.doc_in_seg += take;
    produced = take;
  }

  if (produced == 0) {
    gstate.finished = true;
  }
  output.SetCardinality(produced);
  gstate.produced_rows.fetch_add(produced, std::memory_order_relaxed);
}

}  // namespace sdb::connector
