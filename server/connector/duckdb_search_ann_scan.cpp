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

#include "connector/duckdb_search_ann_scan.h"

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/index_reader.hpp>
#include <limits>

#include "basics/assert.h"
#include "basics/string_utils.h"
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
namespace {

std::optional<irs::bytes_view> LookupSegmentValue(
  int64_t id, const irs::IndexReader& reader) {
  auto [seg_id, doc_id] = irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id));
  if (seg_id >= reader.size()) {
    return std::nullopt;
  }
  const auto& segment = reader[seg_id];
  const auto* pk_col = segment.column(kPkFieldName);
  if (!pk_col) {
    return std::nullopt;
  }
  auto pk_iter = pk_col->iterator(irs::ColumnHint::Normal);
  if (!pk_iter) {
    return std::nullopt;
  }
  const auto* pk_val = irs::get<irs::PayAttr>(*pk_iter);
  if (!pk_val) {
    return std::nullopt;
  }
  if (pk_iter->seek(doc_id) != doc_id) {
    return std::nullopt;
  }
  auto val = pk_val->value;
  return val;
}

void SetupANNFilter(std::unique_ptr<ANNFilter>& filter,
                    duckdb::ClientContext& context, const ANNScan& scan,
                    const rocksdb::Snapshot* rocks_snapshot,
                    const SereneDBScanBindData& bind_data) {
  std::vector<duckdb::idx_t> filter_projection(scan.filter_column_ids.size());
  std::vector<duckdb::LogicalType> filter_types(scan.filter_column_ids.size());
  for (size_t i = 0; i < scan.filter_column_ids.size(); ++i) {
    filter_projection[i] = i;
    const auto cat_id = scan.filter_column_ids[i];
    const auto it = std::find(bind_data.column_ids.begin(),
                              bind_data.column_ids.end(), cat_id);
    if (it == bind_data.column_ids.end()) {
      filter_projection.clear();
      break;
    }
    filter_types[i] = bind_data.column_types[it - bind_data.column_ids.begin()];
  }
  if (!filter_projection.empty()) {
    auto filter_mat = MakeRowMaterializer(
      context, bind_data, rocks_snapshot, /*all_pks=*/{}, filter_projection,
      filter_types, scan.filter_column_ids, nullptr);

    // Deep-copy the stashed expressions so the bind_data copy survives
    // subsequent query invocations that share this plan.
    std::vector<duckdb::unique_ptr<duckdb::Expression>> expr_copies;
    expr_copies.reserve(scan.filter_expressions.size());
    for (const auto& e : scan.filter_expressions) {
      expr_copies.push_back(e->Copy());
    }

    auto& search_snapshot =
      GetSereneDBContext(context).EnsureSearchSnapshot(scan.index_id);

    filter = std::make_unique<ANNFilter>(
      context, search_snapshot.reader, std::move(filter_mat),
      std::move(expr_copies), std::move(filter_types));
  }
}

void ANNSearchImpl(SearchAnnScanGlobalState& state,
                   duckdb::ClientContext& context) {
  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(state.scan->index_id);
  auto& reader = snapshot.reader;
  if (reader.size() == 0) {
    state.finished = true;
    return;
  }

  SDB_ASSERT(state.scan->top_k > 0);
  const size_t top_k = state.scan->top_k;
  std::vector<float> dis(top_k, std::numeric_limits<float>::max());
  std::vector<int64_t> ids(top_k, -1);

  irs::HNSWSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(state.scan->query_vector.data()),
    .top_k = top_k,
  };
  info.params.efSearch = std::max<size_t>(top_k, state.scan->ef_search);
  info.params.sel = state.filter.get();
  reader.Search(state.scan->field_name, info, dis.data(), ids.data());

  state.pk_bytes.reserve(top_k);
  for (size_t i = 0; i < top_k; ++i) {
    if (ids[i] == -1) {
      continue;
    }
    auto val = LookupSegmentValue(static_cast<int64_t>(ids[i]), reader);
    if (!val) {
      continue;
    }
    state.pk_bytes.emplace_back(reinterpret_cast<const char*>(val->data()),
                                val->size());
  }
}

}  // namespace

ANNFilter::ANNFilter(duckdb::ClientContext& context,
                     const irs::IndexReader& reader,
                     std::unique_ptr<RowMaterializer> materializer,
                     std::vector<duckdb::unique_ptr<duckdb::Expression>> exprs,
                     std::vector<duckdb::LogicalType> filter_types)
  : _reader{reader},
    _materializer{std::move(materializer)},
    _exprs{std::move(exprs)},
    _executor{context} {
  for (const auto& e : _exprs) {
    _executor.AddExpression(*e);
  }
  duckdb::vector<duckdb::LogicalType> scratch_types(filter_types.begin(),
                                                    filter_types.end());
  _scratch.Initialize(duckdb::Allocator::DefaultAllocator(), scratch_types);

  duckdb::vector<duckdb::LogicalType> bool_types(_exprs.size(),
                                                 duckdb::LogicalType::BOOLEAN);
  _bool_out.Initialize(duckdb::Allocator::DefaultAllocator(), bool_types);
}

bool ANNFilter::is_member(faiss::idx_t id) const {
  auto val = LookupSegmentValue(static_cast<int64_t>(id), _reader);

  if (!val) {
    return false;
  }
  std::string_view pk{reinterpret_cast<const char*>(val->data()), val->size()};

  _scratch.Reset();
  std::array<std::string_view, 1> pks{pk};
  _materializer->Materialize(pks, _scratch);
  _scratch.SetCardinality(1);

  _bool_out.Reset();
  _bool_out.SetCardinality(1);
  _executor.Execute(_scratch, _bool_out);

  for (duckdb::idx_t i = 0; i < _bool_out.ColumnCount(); ++i) {
    auto& vec = _bool_out.data[i];
    auto value = vec.GetValue(0);
    if (value.IsNull()) {
      return false;
    }
    if (!duckdb::BooleanValue::Get(value)) {
      return false;
    }
  }
  return true;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchAnnScanGlobalState>();
  InitCommonState(*state, context, bind_data, input);
  state->scan = &bind_data.scan_source->Cast<ANNScan>();
  SetupANNFilter(state->filter, context, *state->scan, state->snapshot,
                 bind_data);

  state->materializer = MakeRowMaterializer(
    context, bind_data, state->snapshot, {}, state->projected_columns,
    state->projected_types, bind_data.column_ids, nullptr);
  return duckdb::unique_ptr_cast<SearchAnnScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchAnnScanGlobalState>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  SDB_ASSERT(gstate.scan);

  if (gstate.pk_bytes.empty()) {
    ANNSearchImpl(gstate, context);
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
