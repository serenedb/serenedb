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

  // Resolve projection. input.column_ids holds DuckDB-side indices; for
  // real columns each index is a position into bind_data.column_ids (the
  // catalog column id). Virtual columns (rowid / tableoid / IndexFeatures-
  // backed virtuals) are not supported in this stage and either return
  // NOT_IMPLEMENTED or silently produce nothing -- pure column scans only.
  const auto num_bind_columns = bind_data.column_ids.size();
  state->field_ids.reserve(input.column_ids.size());
  state->output_slots.reserve(input.column_ids.size());
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
    state->field_ids.push_back(static_cast<irs::field_id>(catalog_col_id));
    state->output_slots.push_back(out_slot);
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
  SDB_ASSERT(gstate.reader);
  const auto& reader = *gstate.reader;
  const auto batch_size = static_cast<duckdb::idx_t>(STANDARD_VECTOR_SIZE);

  duckdb::idx_t produced = 0;
  // Loop until we materialise at least one row or run out of segments.
  // Stop at segment boundary -- ColumnstoreMaterializer::Scan writes to
  // output starting at slot 0, so two segments cannot share one batch.
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
      const auto* cs_reader = segment.CsReader();
      if (!cs_reader) {
        // Segment has no columnstore -- e.g. an empty pre-INSERT commit.
        // Skip it; nothing to materialise.
        ++gstate.segment_idx;
        gstate.doc_in_seg = 0;
        continue;
      }
      gstate.materializer = std::make_unique<ColumnstoreMaterializer>(
        *cs_reader, gstate.field_ids, gstate.output_slots);
    }

    const auto take =
      std::min<duckdb::idx_t>(batch_size, seg_docs - gstate.doc_in_seg);
    gstate.materializer->Scan(gstate.doc_in_seg, take, output);
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
