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

#include "connector/duckdb_search_ann_scan.hpp"

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/index_reader.hpp>
#include <limits>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/ann_filter.hpp"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/key_utils.hpp"
#include "connector/rocksdb_row_materializer.h"
#include "connector/row_materializer.h"
#include "connector/search_remove_filter.hpp"
#include "pg/connection_context.h"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchAnnScanGlobalState>();
  InitCommonState(*state, context, bind_data, input);

  // Eager: run HNSW + collect all pk bytes + construct the materializer
  // here so by the time SearchAnnScanFunction is first called every
  // batch is just a hash-lookup.
  auto& ann = bind_data.scan_source->Cast<ANNScan>();
  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(ann.index_id);

  if (snapshot.reader.size() > 0 && ann.top_k > 0) {
    const size_t top_k = ann.top_k;
    std::vector<float> dis(top_k, std::numeric_limits<float>::max());
    std::vector<int64_t> ids(top_k, -1);

    irs::HNSWSearchInfo info{
      .query = reinterpret_cast<const irs::byte_type*>(ann.query_vector.data()),
      .top_k = top_k,
    };
    info.params.efSearch = std::max<size_t>(top_k, ann.ef_search);

    std::unique_ptr<ANNFilter> selector;
    if (!ann.filter_expressions.empty()) {
      std::vector<duckdb::idx_t> filter_projection(
        ann.filter_column_ids.size());
      std::vector<duckdb::LogicalType> filter_types(
        ann.filter_column_ids.size());
      for (size_t i = 0; i < ann.filter_column_ids.size(); ++i) {
        filter_projection[i] = i;
        const auto cat_id = ann.filter_column_ids[i];
        const auto it = std::find(bind_data.column_ids.begin(),
                                  bind_data.column_ids.end(), cat_id);
        if (it == bind_data.column_ids.end()) {
          filter_projection.clear();
          break;
        }
        filter_types[i] =
          bind_data.column_types[it - bind_data.column_ids.begin()];
      }
      if (!filter_projection.empty()) {
        auto filter_mat = MakeRowMaterializer(
          context, bind_data, state->snapshot, /*all_pks=*/{},
          filter_projection, filter_types, ann.filter_column_ids, nullptr);

        // Deep-copy the stashed expressions so the bind_data copy survives
        // subsequent query invocations that share this plan.
        std::vector<duckdb::unique_ptr<duckdb::Expression>> expr_copies;
        expr_copies.reserve(ann.filter_expressions.size());
        for (const auto& e : ann.filter_expressions) {
          expr_copies.push_back(e->Copy());
        }

        selector = std::make_unique<ANNFilter>(
          context, snapshot.reader, std::move(filter_mat),
          std::move(expr_copies), std::move(filter_types));
        info.params.sel = selector.get();
      }
    }

    snapshot.reader.Search(ann.field_name, info, dis.data(), ids.data());

    state->ann_pk_bytes.reserve(top_k);
    auto& reader = snapshot.reader;
    for (size_t i = 0; i < top_k; ++i) {
      if (ids[i] == -1) {
        continue;
      }
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
      state->projected_columns, state->projected_types, bind_data.column_ids,
      nullptr);
  }
  return duckdb::unique_ptr_cast<SearchAnnScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

void SearchAnnScanFunction(duckdb::ClientContext& /*context*/,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchAnnScanGlobalState>();

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
      auto* data =
        duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < batch_size; ++i) {
        data[i] = static_cast<int64_t>(batch_start + i);
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
