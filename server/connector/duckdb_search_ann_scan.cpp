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
#include "connector/lookup.h"
#include "connector/search_remove_filter.hpp"
#include "pg/connection_context.h"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {
namespace {

int ReadEfSearch(duckdb::ClientContext& context) {
  duckdb::Value v;
  if (context.TryGetCurrentSetting("sdb_ef_search", v) && !v.IsNull()) {
    return v.GetValue<int32_t>();
  }
  return 0;
}

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
                      CompositeScanFilter& filter,
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
  if (gstate.ef_search > 0) {
    info.params.efSearch = gstate.ef_search;
  }
  info.params.sel = filter.Empty() ? nullptr : &filter;

  SDB_ASSERT(reader);

  segment_reader.Search(gstate.scan->field_name, info, lstate.buffer);

  const float local_kth = lstate.buffer.dis[0];
  float cur = gstate.global_kth_dis.load(std::memory_order_relaxed);
  while (local_kth < cur && !gstate.global_kth_dis.compare_exchange_weak(
                              cur, local_kth, std::memory_order_relaxed)) {
  }
}

void EmitLocalData(SearchAnnScanGlobalState& g, SearchAnnScanLocalState& l) {
  l.buffer.ReorderResult();
  while (!l.ids.empty() && l.ids.back() == -1) {
    l.ids.pop_back();
  }
  l.dis.resize(l.ids.size());
  std::lock_guard lock{g.m};
  g.dis.emplace_back(std::move(l.dis));
  g.ids.emplace_back(std::move(l.ids));
}

void EmitResult(duckdb::ClientContext& context,
                const SereneDBScanBindData& bind_data,
                SearchAnnScanGlobalState& g, duckdb::DataChunk& output) {
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

  LookupRows(context, bind_data, g.snapshot, g.projected_columns,
             g.projected_types, bind_data.column_ids, g.txn, pk_views,
             g.file_lookup_session, output);

  output.SetCardinality(count);
  g.produced_rows.fetch_add(count, std::memory_order_relaxed);
}

void RangeSearchSegment(const irs::SubReader& sub, CompositeScanFilter& filter,
                        SearchRangeScanGlobalState& g,
                        SearchRangeScanLocalState& l) {
  std::vector<float> dis;
  std::vector<int64_t> ids;
  irs::HNSWRangeSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(g.scan->query_vector.data()),
    .radius = g.scan->radius,
  };
  if (g.ef_search > 0) {
    info.params.efSearch = static_cast<size_t>(g.ef_search);
  }
  info.params.sel = filter.Empty() ? nullptr : &filter;
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
  gstate->ef_search = ReadEfSearch(context);

  InitAnnFilterContext(gstate->filter_ctx, context, *gstate->scan,
                       gstate->snapshot, bind_data);

  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(gstate->scan->index_id);
  gstate->total_segments = snapshot.reader.size();
  gstate->remained_segments = gstate->total_segments;

  gstate->reader = &snapshot.reader;
  if (gstate->scan->text_filter) {
    gstate->text_filter_query =
      gstate->scan->text_filter->prepare({.index = snapshot.reader});
  }
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
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();
  size_t processed = 0;
  size_t segment;
  SDB_ASSERT(g.reader);
  while (ClaimNextLiveSegment(g.next_segment, g.total_segments, *g.reader,
                              segment)) {
    std::vector<std::unique_ptr<ScanFilter>> filters;
    const auto& reader = (*g.reader)[segment];
    if (g.text_filter_query) {
      filters.emplace_back(
        std::make_unique<TextScanFilter>(*g.text_filter_query, reader));
    }
    if (g.filter_ctx) {
      filters.emplace_back(std::make_unique<ANNFilter>(*g.filter_ctx, reader));
    }
    CompositeScanFilter filter{std::move(filters)};
    ANNSearchSegment(reader, filter, g, l, context);
    processed++;
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
  EmitResult(context, bind_data, g, output);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto gstate = duckdb::make_uniq<SearchRangeScanGlobalState>();
  InitCommonState(*gstate, context, bind_data, input);
  gstate->scan = &bind_data.scan_source->Cast<RangeSearchScan>();
  gstate->ef_search = ReadEfSearch(context);

  InitAnnFilterContext(gstate->filter_ctx, context, *gstate->scan,
                       gstate->snapshot, bind_data);

  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(gstate->scan->index_id);
  gstate->reader = &snapshot.reader;
  gstate->total_segments = snapshot.reader.size();
  if (gstate->scan->text_filter) {
    gstate->text_filter_query =
      gstate->scan->text_filter->prepare({.index = snapshot.reader});
  }
  return gstate;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchRangeScanInitLocal(
  duckdb::ExecutionContext& /*context*/,
  duckdb::TableFunctionInitInput& /*input*/,
  duckdb::GlobalTableFunctionState* /*global_state*/) {
  return duckdb::make_uniq<SearchRangeScanLocalState>();
}

void SearchRangeScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<SearchRangeScanGlobalState>();
  auto& l = data.local_state->Cast<SearchRangeScanLocalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();

  size_t segment;
  while (l.pk_bytes.size() - l.current_idx < STANDARD_VECTOR_SIZE &&
         ClaimNextLiveSegment(g.next_segment, g.total_segments, *g.reader,
                              segment)) {
    std::vector<std::unique_ptr<ScanFilter>> filters;
    const auto& sub = (*g.reader)[segment];
    if (g.text_filter_query) {
      filters.emplace_back(
        std::make_unique<TextScanFilter>(*g.text_filter_query, sub));
    }
    if (g.filter_ctx) {
      filters.emplace_back(std::make_unique<ANNFilter>(*g.filter_ctx, sub));
    }
    CompositeScanFilter filter{std::move(filters)};
    RangeSearchSegment(sub, filter, g, l);
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
        data_ptr[i] = static_cast<int64_t>(batch_start + i);
      }
    }
  }

  // Real columns: look up directly per batch. The HNSW result PKs were
  // collected in InitGlobal; we just stream them through LookupRows.
  std::vector<std::string_view> pk_batch;
  pk_batch.reserve(batch_size);
  for (size_t i = 0; i < batch_size; ++i) {
    pk_batch.emplace_back(l.pk_bytes[batch_start + i]);
  }

  LookupRows(context, bind_data, g.snapshot, g.projected_columns,
             g.projected_types, bind_data.column_ids, g.txn, pk_batch,
             l.file_lookup_session, output);
  output.SetCardinality(static_cast<duckdb::idx_t>(batch_size));
  l.current_idx += batch_size;
  g.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);
}

}  // namespace sdb::connector
