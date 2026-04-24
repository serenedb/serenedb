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
namespace {

void RangeSearchImpl(SearchRangeScanGlobalState& state,
                     duckdb::ClientContext& context) {
  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(state.scan->index_id);
  auto& reader = snapshot.reader;

  if (reader.size() == 0) {
    state.finished = true;
    return;
  }
  std::vector<float> dis;
  std::vector<int64_t> ids;
  irs::HNSWRangeSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(state.scan->query_vector.data()),
    .radius = state.scan->radius,
  };
  reader.RangeSearch(state.scan->field_name, info, dis, ids);
  state.pk_bytes.reserve(ids.size());
  for (auto id : ids) {
    auto [seg_id, doc_id] =
      irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id));
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
    state.pk_bytes.emplace_back(reinterpret_cast<const char*>(val.data()),
                                val.size());
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchRangeScanGlobalState>();
  InitCommonState(*state, context, bind_data, input);
  state->scan = &bind_data.scan_source->Cast<RangeSearchScan>();

  state->materializer = MakeRowMaterializer(
    context, bind_data, state->snapshot, {}, state->projected_columns,
    state->projected_types, bind_data.column_ids, nullptr);
  return duckdb::unique_ptr_cast<SearchRangeScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

void SearchRangeScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchRangeScanGlobalState>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  SDB_ASSERT(gstate.scan);
  if (gstate.pk_bytes.empty()) {
    RangeSearchImpl(gstate, context);
  }

  const size_t total = gstate.pk_bytes.size();
  const size_t batch_start = gstate.current_idx;

  if (batch_start >= total) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  const size_t batch_size =
    std::min<size_t>(STANDARD_VECTOR_SIZE, total - batch_start);

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

  std::vector<std::string_view> pk_batch;
  pk_batch.reserve(batch_size);
  for (duckdb::idx_t i = 0; i < batch_size; ++i) {
    pk_batch.emplace_back(gstate.pk_bytes[batch_start + i]);
  }
  gstate.materializer->Materialize(pk_batch, output);

  gstate.current_idx += batch_size;
  output.SetCardinality(static_cast<duckdb::idx_t>(batch_size));
  SDB_ASSERT(batch_size > 0);
  gstate.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);
  gstate.finished = gstate.current_idx >= total;
}

}  // namespace sdb::connector
