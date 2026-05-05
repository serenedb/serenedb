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

#include "connector/duckdb_search_range_scan.h"

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/index_reader.hpp>
#include <ranges>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/duckdb_ann_filter.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/key_utils.hpp"
#include "connector/pk_batch_helpers.h"
#include "connector/search_pk_lookup.h"
#include "connector/search_remove_filter.hpp"
#include "pg/connection_context.h"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {
namespace {

void RangeSearchImpl(SearchRangeScanGlobalState& state,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data) {
  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(state.scan->index_id);
  auto& reader = snapshot.reader;

  if (reader.size() == 0) {
    state.finished = true;
    state.results_ready = true;
    return;
  }
  std::vector<float> dis;
  std::vector<int64_t> ids;
  irs::HNSWRangeSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(state.scan->query_vector.data()),
    .radius = state.scan->radius,
  };
  info.params.sel = state.filter.get();
  reader.RangeSearch(state.scan->field_name, info, dis, ids);

  bool has_real =
    std::any_of(state.projected_columns.begin(), state.projected_columns.end(),
                [](auto p) { return p != duckdb::DConstants::INVALID_INDEX; });

  if (has_real && !ids.empty()) {
    if (!state.index_source) {
      state.index_source = MakeIndexSource(
        context, bind_data, state.snapshot, state.txn, state.projected_columns,
        state.projected_types, bind_data.column_ids);
    }
    if (std::holds_alternative<std::monostate>(state.pk_batch)) {
      state.pk_batch = state.index_source->CreatePkBatch();
    }

    auto segments =
      ids | std::views::transform([](int64_t id) {
        return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).first;
      });
    auto doc_ids =
      ids | std::views::transform([](int64_t id) {
        return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).second;
      });

    state.total_results = std::visit(
      [&](auto& pk) -> size_t {
        using T = std::decay_t<decltype(pk)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          SDB_ASSERT(false, "pk_batch must be initialised");
          return 0;
        } else {
          pk.Reset();
          if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
            pk.EnsureInit(duckdb::Allocator::DefaultAllocator());
          }
          pk.Resize(ids.size());
          LookupSegmentsValues(segments, doc_ids, reader, ids.size(),
                               [&](size_t orig, std::string_view pk_bytes) {
                                 SetPrimaryKey(pk, orig, pk_bytes);
                               });
          return PkCompactResolved(pk, ids.size());
        }
      },
      state.pk_batch);
  } else {
    state.total_results = ids.size();
  }
  state.results_ready = true;
}

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchRangeScanGlobalState>();
  InitCommonState(*state, context, bind_data, input);
  state->scan = &bind_data.scan_source->Cast<RangeSearchScan>();
  InitAnnFilter(state->filter, context, state->scan->filter_expressions,
                state->scan->filter_column_ids, state->scan->index_id,
                state->snapshot, bind_data);

  return duckdb::unique_ptr_cast<SearchRangeScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

void SearchRangeScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchRangeScanGlobalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  SDB_ASSERT(gstate.scan);
  if (!gstate.results_ready) {
    RangeSearchImpl(gstate, context, bind_data);
  }

  const size_t total = gstate.total_results;
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

  if (!std::holds_alternative<std::monostate>(gstate.pk_batch)) {
    gstate.index_source->Materialize(context, gstate.pk_batch, batch_start,
                                     batch_size, output);
  }

  gstate.current_idx += batch_size;
  output.SetCardinality(static_cast<duckdb::idx_t>(batch_size));
  SDB_ASSERT(batch_size > 0);
  gstate.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);
  gstate.finished = gstate.current_idx >= total;
}

}  // namespace sdb::connector
