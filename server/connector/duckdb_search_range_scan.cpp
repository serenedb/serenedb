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

#include "connector/duckdb_search_range_scan.hpp"

#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/index_reader.hpp>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/key_utils.hpp"
#include "connector/rocksdb_row_materializer.h"
#include "connector/search_remove_filter.hpp"
#include "pg/connection_context.h"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchRangeScanGlobalState>();
  InitCommonState(*state, context, bind_data, input);

  // Eager: run HNSW range search + collect pks + build materializer
  // here so the function() loop is just hash-lookups per batch.
  auto& rss = bind_data.scan_source->Cast<RangeSearchScan>();
  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(rss.index_id);

  if (snapshot.reader.size() > 0) {
    std::vector<float> dis;
    std::vector<int64_t> ids;

    irs::HNSWRangeSearchInfo info{
      .query = reinterpret_cast<const irs::byte_type*>(rss.query_vector.data()),
      .radius = rss.radius,
    };
    snapshot.reader.RangeSearch(rss.field_name, info, dis, ids);

    auto& reader = snapshot.reader;
    state->ann_pk_bytes.reserve(ids.size());
    for (size_t i = 0; i < ids.size(); ++i) {
      auto [seg_id, doc_id] =
        irs::UnpackSegmentWithDoc(static_cast<uint64_t>(ids[i]));
      if (seg_id >= reader.size()) {
        continue;
      }
      const auto& segment = reader[seg_id];
      const auto* pk_col = segment.column(kPkFieldName);
      if (!pk_col) {
        continue;
      }
      auto pk_iter = pk_col->iterator(irs::ColumnHint::Normal);
      if (!pk_iter) {
        continue;
      }
      const auto* pk_val = irs::get<irs::PayAttr>(*pk_iter);
      if (!pk_val) {
        continue;
      }
      if (pk_iter->seek(doc_id) != doc_id) {
        continue;
      }
      auto val = pk_val->value;
      state->ann_pk_bytes.emplace_back(
        reinterpret_cast<const char*>(val.data()), val.size());
    }
  }

  if (!state->ann_pk_bytes.empty()) {
    state->materializer = MakeRowMaterializer(
      context, bind_data, state->snapshot, state->ann_pk_bytes,
      state->projected_columns, state->projected_types, bind_data.column_ids);
  }
  return duckdb::unique_ptr_cast<SearchRangeScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

void SearchRangeScanFunction(duckdb::ClientContext& /*context*/,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchRangeScanGlobalState>();

  if (gstate.finished || gstate.ann_pk_bytes.empty()) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  const size_t total = gstate.ann_pk_bytes.size();
  const size_t batch_start = gstate.ann_current_idx;

  if (batch_start >= total) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  const size_t batch_size =
    std::min<size_t>(STANDARD_VECTOR_SIZE, total - batch_start);

  // Virtual-column slots (rowid / tableoid): handled inline here.
  for (duckdb::idx_t proj = 0; proj < gstate.projected_columns.size(); ++proj) {
    if (gstate.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (gstate.scan_tableoid && proj == gstate.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(gstate.tableoid_value));
    } else {
      auto* data_ptr =
        duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < batch_size; ++i) {
        data_ptr[i] = static_cast<int64_t>(batch_start + i);
      }
    }
  }

  // Real columns: stream from the materializer constructed during
  // InitGlobal-after-iresearch.
  std::vector<std::string_view> pk_batch;
  pk_batch.reserve(batch_size);
  for (duckdb::idx_t i = 0; i < batch_size; ++i) {
    pk_batch.emplace_back(gstate.ann_pk_bytes[batch_start + i]);
  }
  gstate.materializer->Materialize(pk_batch, output);

  gstate.ann_current_idx += batch_size;
  output.SetCardinality(static_cast<duckdb::idx_t>(batch_size));
  if (batch_size > 0) {
    gstate.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);
  }

  if (gstate.ann_current_idx >= total) {
    gstate.finished = true;
  }
}

}  // namespace sdb::connector
