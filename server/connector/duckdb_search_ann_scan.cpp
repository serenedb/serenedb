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
#include "basics/logger/logger.h"
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
  // faiss caps the search at efSearch candidates with do_dis_check=true (the
  // default), so an efSearch lower than top_k yields fewer than top_k results.
  // Make sure the search has at least enough budget to return top_k.
  const int requested_ef =
    gstate.ef_search > 0 ? gstate.ef_search : info.params.efSearch;
  info.params.efSearch = std::max(requested_ef, static_cast<int>(top_k));
  info.params.sel = filter.has_value() ? &*filter : nullptr;

  SDB_ASSERT(reader);

  segment_reader.Search(gstate.scan->field_name, info, lstate.buffer);

  const float local_kth = lstate.buffer.dis[0];
  float cur = gstate.global_kth_dis.load(std::memory_order_relaxed);
  while (local_kth < cur && !gstate.global_kth_dis.compare_exchange_weak(
                              cur, local_kth, std::memory_order_relaxed)) {
  }
}

// Snapshot the worker buffer (sorted ascending after ReorderResult) into the
// global per-segment lists. Copied (not moved) because faiss heap_heapify
// resets the buffer at the start of each Search call -- if we moved the
// vectors out, the buffer's spans would dangle and the next segment's
// Search would write into freed memory.
void EmitLocalData(SearchAnnScanGlobalState& g, SearchAnnScanLocalState& l) {
  l.buffer.ReorderResult();
  size_t valid = l.ids.size();
  while (valid > 0 && l.ids[valid - 1] == -1) {
    --valid;
  }
  SDB_PRINT("ANN EmitLocalData: buffer_size=", l.ids.size(), " valid=", valid,
            " first_id=", (valid ? l.ids[0] : int64_t{-1}),
            " first_dis=", (valid ? l.dis[0] : 0.0f));
  if (valid == 0) {
    return;
  }
  std::vector<float> dis_out(l.dis.begin(), l.dis.begin() + valid);
  std::vector<int64_t> ids_out(l.ids.begin(), l.ids.begin() + valid);
  std::lock_guard lock{g.m};
  g.dis.emplace_back(std::move(dis_out));
  g.ids.emplace_back(std::move(ids_out));
}

size_t LocalPkSize(const PrimaryKeyBatch& b) {
  return std::visit(
    [](const auto& v) -> size_t {
      using T = std::decay_t<decltype(v)>;
      if constexpr (std::is_same_v<T, std::monostate>) {
        return 0;
      } else {
        return PrimaryKeysSize(v);
      }
    },
    b);
}

void MergeResult(duckdb::ClientContext& context,
                 const SereneDBScanBindData& bind_data,
                 SearchAnnScanGlobalState& g) {
  const size_t k = g.scan->top_k;
  const size_t num_lists = g.dis.size();

  using HeapEntry = std::tuple<float, size_t, size_t>;
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> heap;
  for (size_t i = 0; i < num_lists; ++i) {
    if (!g.dis[i].empty()) {
      heap.emplace(g.dis[i][0], i, 0);
    }
  }

  std::vector<int64_t> top_ids;
  top_ids.reserve(k);
  while (!heap.empty() && top_ids.size() < k) {
    auto [d, li, pi] = heap.top();
    heap.pop();
    const auto id = g.ids[li][pi];
    SDB_ASSERT(id != -1);
    top_ids.push_back(id);
    const auto next = pi + 1;
    if (next < g.dis[li].size() && g.ids[li][next] != -1) {
      heap.emplace(g.dis[li][next], li, next);
    }
  }

  const size_t n = top_ids.size();
  const bool has_real =
    std::any_of(g.projected_columns.begin(), g.projected_columns.end(),
                [](auto p) { return p != duckdb::DConstants::INVALID_INDEX; });

  SDB_PRINT("ANN MergeResult: num_lists=", num_lists, " k=", k, " n=", n,
            " has_real=", has_real);

  if (!has_real || n == 0) {
    g.total_results = n;
    return;
  }

  if (!g.index_source) {
    g.index_source = MakeIndexSource(context, bind_data, g.snapshot, g.txn,
                                     g.projected_columns, g.projected_types,
                                     bind_data.column_ids);
  }
  if (std::holds_alternative<std::monostate>(g.pk_batch)) {
    g.pk_batch = g.index_source->CreatePkBatch();
  }

  auto segments =
    top_ids | std::views::transform([](int64_t id) {
      return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).first;
    });
  auto doc_ids =
    top_ids | std::views::transform([](int64_t id) {
      return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).second;
    });

  g.total_results = std::visit(
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
        PkResize(pk, n);
        LookupSegmentsValues(segments, doc_ids, *g.reader, n,
                             [&](size_t orig, std::string_view pk_bytes) {
                               SetPrimaryKey(pk, orig, pk_bytes);
                             });
        const auto resolved = PkCompactResolved(pk, n);
        SDB_PRINT("ANN MergeResult: PkCompactResolved n=", n,
                  " resolved=", resolved);
        return resolved;
      }
    },
    g.pk_batch);
}

void EmitResult(duckdb::ClientContext& context,
                const SereneDBScanBindData& bind_data,
                SearchAnnScanGlobalState& g, duckdb::DataChunk& output) {
  std::lock_guard lock{g.m};
  const size_t total = g.total_results;
  const size_t batch_start = g.current_idx;
  if (g.finished || batch_start >= total) {
    g.finished = true;
    output.SetCardinality(0);
    return;
  }
  const size_t batch_size =
    std::min<size_t>(STANDARD_VECTOR_SIZE, total - batch_start);

  for (duckdb::idx_t proj = 0; proj < g.projected_columns.size(); ++proj) {
    if (g.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (g.scan_tableoid && proj == g.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(g.tableoid_value));
    }
  }

  if (!std::holds_alternative<std::monostate>(g.pk_batch)) {
    g.index_source->Materialize(context, g.pk_batch, batch_start, batch_size,
                                output);
  }
  g.current_idx += batch_size;
  output.SetCardinality(static_cast<duckdb::idx_t>(batch_size));
  SDB_ASSERT(batch_size > 0);
  g.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);

  g.finished = g.current_idx >= total;
}

void RangeSearchSegment(duckdb::ClientContext& context,
                        const SereneDBScanBindData& bind_data,
                        const irs::SubReader& sub,
                        std::optional<ANNFilter>& filter,
                        SearchRangeScanGlobalState& g,
                        SearchRangeScanLocalState& l) {
  std::vector<float> dis;
  std::vector<int64_t> ids;
  irs::HNSWRangeSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(g.scan->query_vector.data()),
    .radius = g.scan->effective_radius,
  };
  if (g.ef_search > 0) {
    info.params.efSearch = static_cast<size_t>(g.ef_search);
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

  if (!g.index_source) {
    // Worker-side lazy init. MakeIndexSource is deterministic for a given
    // bind_data + snapshot/txn, so a benign race between workers produces
    // equivalent objects.
    g.index_source = MakeIndexSource(context, bind_data, g.snapshot, g.txn,
                                     g.projected_columns, g.projected_types,
                                     bind_data.column_ids);
  }
  if (std::holds_alternative<std::monostate>(l.pk_batch)) {
    l.pk_batch = g.index_source->CreatePkBatch();
    if (auto* p = std::get_if<PrimaryKeysBytes>(&l.pk_batch)) {
      p->EnsureInit(duckdb::Allocator::DefaultAllocator());
    }
  }

  auto segments =
    ids | std::views::transform([](int64_t id) {
      return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).first;
    });
  auto doc_ids =
    ids | std::views::transform([](int64_t id) {
      return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id)).second;
    });

  std::visit(
    [&](auto& pk) {
      using T = std::decay_t<decltype(pk)>;
      if constexpr (std::is_same_v<T, std::monostate>) {
        SDB_ASSERT(false, "pk_batch must be initialised");
      } else {
        LookupSegmentsValues(segments, doc_ids, *g.reader, n,
                             [&](size_t /*orig*/, std::string_view pk_bytes) {
                               AppendPrimaryKey(pk, pk_bytes);
                             });
      }
    },
    l.pk_batch);

  g.total_results.fetch_add(n, std::memory_order_relaxed);
}

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto gstate = duckdb::make_uniq<SearchAnnScanGlobalState>();
  InitCommonState(*gstate, context, bind_data, input);
  gstate->scan = &bind_data.scan_source->Cast<ANNScan>();
  gstate->ef_search = ReadEfSearch(context);

  InitAnnFilterContext(gstate->filter_ctx, context,
                       gstate->scan->filter_expression.get(),
                       gstate->scan->filter_column_ids, gstate->scan->index_id,
                       gstate->snapshot, bind_data);

  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(gstate->scan->index_id);
  gstate->reader = &snapshot.reader;
  gstate->total_segments = snapshot.reader.size();

  // remained_segments must match what ClaimNextLiveSegment will hand out:
  // dead segments are skipped there, so per-worker `processed` counts only
  // live segments. If we initialized this to total_segments, the final
  // `remained == processed` check after fetch_sub would never be true when
  // any segment is dead, and the merge/emit branch would never run --
  // producing zero rows.
  size_t live = 0;
  for (size_t i = 0; i < gstate->total_segments; ++i) {
    if (snapshot.reader[i].live_docs_count() != 0) {
      ++live;
    }
  }
  gstate->remained_segments = live;

  SDB_PRINT("ANN InitGlobal: index_id=", gstate->scan->index_id.id(),
            " total_segments=", gstate->total_segments, " live=", live,
            " top_k=", gstate->scan->top_k);

  if (live == 0) {
    gstate->search_finished.store(true, std::memory_order_release);
    gstate->total_results = 0;
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
  std::optional<ANNFilter> filter;
  if (g.search_finished.load(std::memory_order_acquire)) {
    EmitResult(context, bind_data, g, output);
    return;
  }
  while (ClaimNextLiveSegment(g.next_segment, g.total_segments, *g.reader,
                              segment)) {
    const auto& reader = (*g.reader)[segment];
    if (g.filter_ctx) {
      filter.emplace(*g.filter_ctx, reader);
    }
    // faiss begin_multiple resets the heap each Search call, so a worker
    // running >1 segment must snapshot results per segment -- otherwise
    // the next Search would clobber the previous segment's top-k.
    l.buffer.ResetValues();
    ANNSearchSegment(reader, filter, g, l, context);
    EmitLocalData(g, l);
    processed++;
    filter.reset();
  }
  if (!processed) {
    output.SetCardinality(0);
    return;
  }
  auto remained =
    g.remained_segments.fetch_sub(processed, std::memory_order_acq_rel);
  if (remained != processed) {
    output.SetCardinality(0);
    return;
  }
  g.search_finished.store(true, std::memory_order_release);
  // Merge result in a single thread
  MergeResult(context, bind_data, g);
  EmitResult(context, bind_data, g, output);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto gstate = duckdb::make_uniq<SearchRangeScanGlobalState>();
  InitCommonState(*gstate, context, bind_data, input);
  gstate->scan = &bind_data.scan_source->Cast<RangeSearchScan>();
  gstate->ef_search = ReadEfSearch(context);

  InitAnnFilterContext(gstate->filter_ctx, context,
                       gstate->scan->filter_expression.get(),
                       gstate->scan->filter_column_ids, gstate->scan->index_id,
                       gstate->snapshot, bind_data);

  auto& snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(gstate->scan->index_id);
  gstate->reader = &snapshot.reader;
  gstate->total_segments = snapshot.reader.size();
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

  std::optional<ANNFilter> filter;
  size_t segment;
  while (LocalPkSize(l.pk_batch) - l.current_idx < STANDARD_VECTOR_SIZE &&
         ClaimNextLiveSegment(g.next_segment, g.total_segments, *g.reader,
                              segment)) {
    const auto& sub = (*g.reader)[segment];
    if (g.filter_ctx) {
      filter.emplace(*g.filter_ctx, sub);
    }
    RangeSearchSegment(context, bind_data, sub, filter, g, l);
    filter.reset();
  }

  const size_t total = LocalPkSize(l.pk_batch);
  const size_t batch_start = l.current_idx;
  if (batch_start >= total) {
    output.SetCardinality(0);
    return;
  }

  const size_t batch_size =
    std::min<size_t>(STANDARD_VECTOR_SIZE, total - batch_start);

  for (duckdb::idx_t proj = 0; proj < g.projected_columns.size(); ++proj) {
    if (g.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (g.scan_tableoid && proj == g.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(g.tableoid_value));
    }
  }

  SDB_ASSERT(g.index_source);
  if (!std::holds_alternative<std::monostate>(l.pk_batch)) {
    g.index_source->Materialize(context, l.pk_batch, batch_start, batch_size,
                                output);
  }

  output.SetCardinality(static_cast<duckdb::idx_t>(batch_size));
  l.current_idx += batch_size;
  g.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);
}

}  // namespace sdb::connector
