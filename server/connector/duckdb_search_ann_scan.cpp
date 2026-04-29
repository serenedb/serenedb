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
#include <queue>
#include <ranges>
#include <span>
#include <tuple>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/duckdb_ann_filter.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/key_utils.hpp"
#include "connector/rocksdb_row_materializer.h"
#include "connector/row_materializer.h"
#include "connector/search_pk_lookup.h"
#include "connector/search_remove_filter.hpp"
#include "pg/connection_context.h"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {
namespace {

bool ClaimNextLiveSegment(std::atomic_size_t& next_segment,
                          size_t total_segments, const irs::IndexReader& reader,
                          size_t& out) {
  while (true) {
    const size_t s = next_segment.fetch_add(1, std::memory_order_relaxed);
    if (s >= total_segments) {
      return false;
    }
    if (reader[s].live_docs_count() != 0) {
      out = s;
      return true;
    }
  }
}

void ANNSearchSegment(const irs::SubReader& segment_reader,
                      std::optional<ANNFilter>& filter,
                      SearchAnnScanGlobalState& gstate,
                      SearchAnnScanLocalState& lstate,
                      duckdb::ClientContext& context) {
  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(gstate.scan->index_id);
  auto& reader = snapshot.reader;

  SDB_ASSERT(gstate.scan->top_k > 0);
  const size_t top_k = gstate.scan->top_k;

  irs::HNSWSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(gstate.scan->query_vector.data()),
    .top_k = top_k,
    .global_threshold = gstate.global_kth_dis.load(std::memory_order_relaxed),
  };
  info.params.efSearch = std::max<size_t>(top_k, gstate.scan->ef_search);
  info.params.sel = filter.has_value() ? &*filter : nullptr;

  SDB_ASSERT(reader);

  segment_reader.Search(gstate.scan->field_name, info, lstate.buffer);

  const float local_kth = lstate.buffer.dis[0];
  float cur = gstate.global_kth_dis.load(std::memory_order_relaxed);
  while (local_kth < cur && !gstate.global_kth_dis.compare_exchange_weak(
                              cur, local_kth, std::memory_order_relaxed)) {
  }
}

void EmitLocalData(SearchAnnScanGlobalState& g, SearchAnnScanLocalState& l) {
  std::move(l.buffer).ReorderResult();
  while (!l.ids.empty() && l.ids.back() == -1) {
    l.ids.pop_back();
  }
  l.dis.resize(l.ids.size());
  std::lock_guard lock{g.m};
  g.dis.emplace_back(std::move(l.dis));
  g.ids.emplace_back(std::move(l.ids));
}

void EmitResult(SearchAnnScanGlobalState& g, duckdb::DataChunk& output) {
  const size_t k = g.scan->top_k;
  const size_t num_lists = g.dis.size();

  using HeapEntry = std::tuple<float, size_t, size_t>;
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> heap;
  for (size_t i = 0; i < num_lists; ++i) {
    if (!g.dis[i].empty()) {
      heap.emplace(g.dis[i][0], i, 0);
    }
  }

  std::vector<float> top_dis;
  std::vector<int64_t> top_ids;
  top_dis.reserve(k);
  top_ids.reserve(k);
  while (!heap.empty() && top_ids.size() < k) {
    auto [d, li, pi] = heap.top();
    heap.pop();
    const auto id = g.ids[li][pi];
    SDB_ASSERT(id != -1);
    top_dis.push_back(d);
    top_ids.push_back(id);
    const auto next = pi + 1;
    if (next < g.dis[li].size() && g.ids[li][next] != -1) {
      heap.emplace(g.dis[li][next], li, next);
    }
  }

  const auto n = top_ids.size();
  auto segments =
    top_ids | std::views::transform([](int64_t id) {
      return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).first;
    });
  auto doc_ids =
    top_ids | std::views::transform([](int64_t id) {
      return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).second;
    });
  std::vector<std::string> pk_storage(n);
  LookupSegmentsValues(segments, doc_ids, *g.reader, pk_storage);

  std::vector<std::string_view> pk_views;
  pk_views.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    SDB_ASSERT(!pk_storage[i].empty());
    pk_views.emplace_back(pk_storage[i]);
  }
  const auto count = static_cast<duckdb::idx_t>(pk_views.size());

  for (duckdb::idx_t proj = 0; proj < g.projected_columns.size(); ++proj) {
    if (g.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (g.scan_tableoid && proj == g.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(g.tableoid_value));
    } else {
      auto* data_ptr =
        duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < count; ++i) {
        data_ptr[i] = static_cast<int64_t>(i);
      }
    }
  }

  g.materializer->Materialize(pk_views, output);
  output.SetCardinality(count);
  g.produced_rows.fetch_add(count, std::memory_order_relaxed);
}

void RangeSearchSegment(const irs::SubReader& sub,
                        std::optional<ANNFilter>& filter,
                        SearchRangeScanGlobalState& g,
                        SearchRangeScanLocalState& l) {
  std::vector<float> dis;
  std::vector<int64_t> ids;
  irs::HNSWRangeSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(g.scan->query_vector.data()),
    .radius = g.scan->radius,
  };
  if (g.scan->ef_search > 0) {
    info.params.efSearch = static_cast<size_t>(g.scan->ef_search);
  }
  info.params.sel = filter.has_value() ? &*filter : nullptr;
  sub.RangeSearch(g.scan->field_name, info, dis, ids);

  while (!ids.empty() && ids.back() == -1) {
    ids.pop_back();
  }

  const auto n = ids.size();
  if (n == 0) {
    return;
  }

  auto segments =
    ids | std::views::transform([](int64_t id) {
      return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).first;
    });
  auto doc_ids =
    ids | std::views::transform([](int64_t id) {
      return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).second;
    });
  const size_t base = l.pk_bytes.size();
  l.pk_bytes.resize(base + n);
  std::span<std::string> tail{l.pk_bytes.data() + base, n};
  LookupSegmentsValues(segments, doc_ids, *g.reader, tail);
}

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto gstate = duckdb::make_uniq<SearchAnnScanGlobalState>();
  InitCommonState(*gstate, context, bind_data, input);
  gstate->scan = &bind_data.scan_source->Cast<ANNScan>();

  InitAnnFilterContext(gstate->filter_ctx, context,
                       gstate->scan->filter_expression.get(),
                       gstate->scan->filter_column_ids, gstate->scan->index_id,
                       gstate->snapshot, bind_data);

  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(gstate->scan->index_id);
  gstate->total_segments = snapshot.reader.size();
  gstate->remained_segments = gstate->total_segments;

  gstate->materializer =
    MakeRowMaterializer(context, bind_data, gstate->snapshot, /*all_pks=*/{},
                        gstate->projected_columns, gstate->projected_types,
                        bind_data.column_ids, nullptr);
  gstate->reader = &snapshot.reader;
  return gstate;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchAnnScanInitLocal(
  duckdb::ExecutionContext& /*context*/,
  duckdb::TableFunctionInitInput& /*input*/,
  duckdb::GlobalTableFunctionState* global_state) {
  auto& gstate = global_state->Cast<SearchAnnScanGlobalState>();
  auto lstate = duckdb::make_uniq<SearchAnnScanLocalState>(gstate.scan->top_k);
  return lstate;
}

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<SearchAnnScanGlobalState>();
  auto& l = data.local_state->Cast<SearchAnnScanLocalState>();
  size_t processed = 0;
  size_t segment;
  SDB_ASSERT(g.reader);
  std::optional<ANNFilter> filter;
  while (ClaimNextLiveSegment(g.next_segment, g.total_segments, *g.reader,
                              segment)) {
    const auto& reader = (*g.reader)[segment];
    if (g.filter_ctx) {
      filter.emplace(*g.filter_ctx, reader);
    }
    ANNSearchSegment(reader, filter, g, l, context);
    processed++;
    filter.reset();
  }
  if (!processed) {
    output.SetCardinality(0);
    return;
  }
  EmitLocalData(g, l);
  auto remained =
    g.remained_segments.fetch_sub(processed, std::memory_order_acq_rel);
  if (remained != processed) {
    output.SetCardinality(0);
    return;
  }
  // Merge result in a single thread
  EmitResult(g, output);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto gstate = duckdb::make_uniq<SearchRangeScanGlobalState>();
  InitCommonState(*gstate, context, bind_data, input);
  gstate->scan = &bind_data.scan_source->Cast<RangeSearchScan>();

  InitAnnFilterContext(gstate->filter_ctx, context,
                       gstate->scan->filter_expression.get(),
                       gstate->scan->filter_column_ids, gstate->scan->index_id,
                       gstate->snapshot, bind_data);

  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(gstate->scan->index_id);
  gstate->reader = &snapshot.reader;
  gstate->total_segments = snapshot.reader.size();

  gstate->materializer =
    MakeRowMaterializer(context, bind_data, gstate->snapshot, /*all_pks=*/{},
                        gstate->projected_columns, gstate->projected_types,
                        bind_data.column_ids, nullptr);
  return gstate;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchRangeScanInitLocal(
  duckdb::ExecutionContext& /*context*/,
  duckdb::TableFunctionInitInput& /*input*/,
  duckdb::GlobalTableFunctionState* /*global_state*/) {
  return duckdb::make_uniq<SearchRangeScanLocalState>();
}

void SearchRangeScanFunction(duckdb::ClientContext& /*context*/,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<SearchRangeScanGlobalState>();
  auto& l = data.local_state->Cast<SearchRangeScanLocalState>();

  std::optional<ANNFilter> filter;
  size_t segment;
  while (l.pk_bytes.size() - l.current_idx < STANDARD_VECTOR_SIZE &&
         ClaimNextLiveSegment(g.next_segment, g.total_segments, *g.reader,
                              segment)) {
    const auto& sub = (*g.reader)[segment];
    if (g.filter_ctx) {
      filter.emplace(*g.filter_ctx, sub);
    }
    RangeSearchSegment(sub, filter, g, l);
    filter.reset();
  }
  if (l.current_idx == l.pk_bytes.size()) {
    output.SetCardinality(0);
    return;
  }

  const size_t total = l.pk_bytes.size();
  const size_t batch_start = l.current_idx;
  const size_t batch_size =
    std::min<size_t>(STANDARD_VECTOR_SIZE, total - batch_start);

  for (duckdb::idx_t proj = 0; proj < g.projected_columns.size(); ++proj) {
    if (g.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (g.scan_tableoid && proj == g.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(g.tableoid_value));
    } else {
      auto* data_ptr =
        duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < batch_size; ++i) {
        data_ptr[i] = static_cast<int64_t>(i);
      }
    }
  }

  std::vector<std::string_view> pk_batch;
  pk_batch.reserve(batch_size);
  for (size_t i = 0; i < batch_size; ++i) {
    pk_batch.emplace_back(l.pk_bytes[batch_start + i]);
  }

  {
    std::lock_guard lock{g.materializer_mutex};
    g.materializer->Materialize(pk_batch, output);
  }
  output.SetCardinality(static_cast<duckdb::idx_t>(batch_size));
  l.current_idx += batch_size;
  g.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);
}

}  // namespace sdb::connector
