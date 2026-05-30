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
#include <iresearch/columnstore/format.hpp>
#include <iresearch/columnstore/read_context.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/proxy_filter.hpp>
#include <limits>
#include <numeric>
#include <span>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/exceptions.h"
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

bool ClaimNextLiveSegment(std::atomic_uint32_t& next_segment,
                          size_t total_segments, const irs::IndexReader& reader,
                          uint32_t& out) {
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

void ANNSearchSegment(const irs::SubReader& segment_reader, uint32_t segment_id,
                      CompositeScanFilter& composite,
                      SearchAnnScanGlobalState& gstate,
                      SearchAnnScanLocalState& lstate,
                      irs::columnstore::ReadContext& read_ctx) {
  SDB_ASSERT(gstate.scan->top_k > 0);
  const size_t top_k = gstate.scan->top_k;

  irs::HNSWSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(gstate.scan->query_vector.data()),
    .top_k = top_k,
    .global_threshold = gstate.global_kth_dis.load(std::memory_order_relaxed),
  };
  const int requested_ef =
    gstate.ef_search > 0 ? gstate.ef_search : info.params.efSearch;
  info.params.efSearch = std::max(requested_ef, static_cast<int>(top_k));
  info.params.sel = composite.Empty() ? nullptr : &composite;

  SDB_ASSERT(gstate.reader);

  segment_reader.Search(gstate.scan->field_id, info, lstate.buffer, segment_id,
                        read_ctx);

  const float local_kth = lstate.buffer.dis[0];
  float cur = gstate.global_kth_dis.load(std::memory_order_relaxed);
  while (local_kth < cur && !gstate.global_kth_dis.compare_exchange_weak(
                              cur, local_kth, std::memory_order_relaxed)) {
  }
}

void MergeResult(duckdb::ClientContext& context,
                 const SereneDBScanBindData& bind_data,
                 SearchAnnScanGlobalState& g) {
  const size_t k = g.scan->top_k;
  const size_t total = g.dis.size();
  const size_t take = std::min(k, total);

  std::vector<uint32_t> idx(total);
  absl::c_iota(idx, 0);
  auto cmp = [&](uint32_t a, uint32_t b) { return g.dis[a] < g.dis[b]; };
  if (take < total) {
    absl::c_nth_element(idx, idx.begin() + take, cmp);
  }
  std::sort(idx.begin(), idx.begin() + take, cmp);

  std::vector<int64_t> top_ids;
  top_ids.reserve(take);
  for (size_t i = 0; i < take; ++i) {
    const int64_t id = g.ids[idx[i]];
    if (id == -1) {
      continue;
    }
    top_ids.push_back(id);
  }

  const size_t n = top_ids.size();
  const bool has_real = absl::c_any_of(g.projected_columns, [](auto p) {
    return p != duckdb::DConstants::INVALID_INDEX;
  });

  if (!has_real || n == 0) {
    g.total_results = n;
    return;
  }

  g.merged_seg_docs.assign(n, SegDoc{});

  if (!g.has_external_projections) {
    // cs-only INCLUDE: skip PK fetch, just compute merged_seg_docs for the
    // score-order materializer.
    for (size_t i = 0; i < n; ++i) {
      auto [seg, doc] =
        irs::UnpackSegmentWithDoc(static_cast<uint64_t>(top_ids[i]));
      g.merged_seg_docs[i] = {
        .segment_idx = seg,
        .doc_pos = static_cast<irs::doc_id_t>(doc - irs::doc_limits::min())};
    }
    g.total_results = n;
    return;
  }

  if (!g.index_source) {
    g.index_source = MakeIndexSource(context, bind_data, g.snapshot, g.txn,
                                     g.external_projected_columns,
                                     g.projected_types, bind_data.column_ids);
  }
  if (std::holds_alternative<std::monostate>(g.pk_batch)) {
    g.pk_batch = g.index_source->CreatePkBatch();
  }

  g.total_results = std::visit(
    [&](auto& pk) -> size_t {
      using T = std::decay_t<decltype(pk)>;
      if constexpr (std::is_same_v<T, std::monostate>) {
        SDB_ASSERT(false, "pk_batch must be initialised");
        return 0;
      } else {
        pk.Reset();
        if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
          pk.EnsureInit(duckdb::Allocator::Get(context));
        }
        PkResize(pk, n);
        LookupSegmentsValues(
          top_ids,
          [](int64_t id) {
            return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id));
          },
          *g.reader, g.lookup_scratch,
          [&](size_t orig, std::string_view pk_bytes) {
            SetPrimaryKey(pk, orig, pk_bytes);
            auto [seg, doc] =
              irs::UnpackSegmentWithDoc(static_cast<uint64_t>(top_ids[orig]));
            g.merged_seg_docs[orig] = {.segment_idx = seg,
                                       .doc_pos = static_cast<irs::doc_id_t>(
                                         doc - irs::doc_limits::min())};
          });
        return PkCompactResolved(pk, n, g.merged_seg_docs);
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
    // Use SetChildCardinality so child Vector sizes drop to 0 too -- otherwise
    // downstream BinaryExecutor sees stale child vectors from the previous
    // batch and complains about size mismatches.
    output.SetChildCardinality(0);
    return;
  }
  // TODO(codeworse): think how to send the result in batches
  const size_t batch_size = total;

  for (duckdb::idx_t proj = 0; proj < g.projected_columns.size(); ++proj) {
    if (g.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (g.scan_tableoid && proj == g.tableoid_output_idx) {
      // batch_size here, not output.size(): SetChildCardinality runs at the
      // bottom of this function, so output.size() is still 0 and a Reference
      // with count=0 leaves the constant vector at zero rows, which then
      // mismatches the rest of the chunk in downstream BinaryExecutor.
      output.data[proj].Reference(duckdb::Value::BIGINT(g.tableoid_value),
                                  duckdb::count_t(batch_size));
    }
  }

  if (g.has_external_projections) {
    SDB_IF_FAILURE("SearchRocksDBLookupFault") { SDB_THROW(sdb::ERROR_DEBUG); }
    g.index_source->Materialize(context, g.pk_batch, batch_start, batch_size,
                                output);
  }
  if (!g.cs_projections.empty()) {
    std::span<const SegDoc> slice{g.merged_seg_docs.data() + batch_start,
                                  batch_size};
    MaterializeIncludeColumnsScoreOrder(g, *g.reader, slice, output);
  }
  g.current_idx += batch_size;
  output.SetChildCardinality(static_cast<duckdb::idx_t>(batch_size));
  SDB_ASSERT(batch_size > 0);
  g.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);

  g.finished = g.current_idx >= total;
}

void RangeSearchSegment(duckdb::ClientContext& context,
                        const SereneDBScanBindData& bind_data,
                        const irs::SubReader& sub, uint32_t segment_id,
                        CompositeScanFilter& composite,
                        SearchRangeScanGlobalState& g,
                        SearchRangeScanLocalState& l,
                        irs::columnstore::ReadContext& read_ctx) {
  l.range_buffer.dis.clear();
  l.range_buffer.ids.clear();

  irs::HNSWRangeSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(g.scan->query_vector.data()),
    .radius = g.scan->effective_radius,
  };
  if (g.ef_search > 0) {
    info.params.efSearch = static_cast<size_t>(g.ef_search);
  }
  info.params.sel = composite.Empty() ? nullptr : &composite;
  sub.RangeSearch(g.scan->field_id, info, l.range_buffer, segment_id, read_ctx);

  auto& ids = l.range_buffer.ids;
  while (!ids.empty() && ids.back() == -1) {
    ids.pop_back();
  }

  const auto n = ids.size();
  if (n == 0) {
    return;
  }

  if (g.has_external_projections) {
    {
      std::lock_guard lock{g.m};
      if (!g.index_source) {
        g.index_source = MakeIndexSource(
          context, bind_data, g.snapshot, g.txn, g.external_projected_columns,
          g.projected_types, bind_data.column_ids);
      }
      if (std::holds_alternative<std::monostate>(l.pk_batch)) {
        l.pk_batch = g.index_source->CreatePkBatch();
        if (auto* p = std::get_if<PrimaryKeysBytes>(&l.pk_batch)) {
          p->EnsureInit(duckdb::Allocator::Get(context));
        }
      }
    }

    std::visit(
      [&](auto& pk) {
        using T = std::decay_t<decltype(pk)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          SDB_ASSERT(false, "pk_batch must be initialised");
        } else {
          LookupSegmentsValues(
            ids,
            [](int64_t id) {
              return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id));
            },
            *g.reader, l.lookup_scratch,
            [&](size_t orig, std::string_view pk_bytes) {
              AppendPrimaryKey(pk, pk_bytes);
              auto [seg, doc] =
                irs::UnpackSegmentWithDoc(static_cast<uint64_t>(ids[orig]));
              l.seg_docs.push_back({.segment_idx = seg,
                                    .doc_pos = static_cast<irs::doc_id_t>(
                                      doc - irs::doc_limits::min())});
            });
        }
      },
      l.pk_batch);
  } else {
    // cs-only INCLUDE: skip PK fetch; doc_id maps directly to row position
    // for the score-order materializer.
    for (size_t i = 0; i < n; ++i) {
      auto [seg, doc] =
        irs::UnpackSegmentWithDoc(static_cast<uint64_t>(ids[i]));
      l.seg_docs.push_back(
        {.segment_idx = seg,
         .doc_pos = static_cast<irs::doc_id_t>(doc - irs::doc_limits::min())});
    }
  }

  g.total_results.fetch_add(n, std::memory_order_relaxed);
}

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto gstate = duckdb::make_uniq<SearchAnnScanGlobalState>();
  InitCommonState(*gstate, context, bind_data, input);
  ClassifyColumnstoreProjections(*gstate, bind_data);
  gstate->scan = &bind_data.scan_source->Cast<ANNScan>();
  gstate->ef_search = ReadEfSearch(context);

  InitAnnFilterContext(
    gstate->filter_ctx, context, gstate->scan->filter_expression.get(),
    gstate->scan->filter_column_ids, gstate->snapshot, bind_data);

  SDB_ASSERT(gstate->scan->snapshot);
  auto& snapshot = *gstate->scan->snapshot;
  gstate->reader = &snapshot.reader;
  gstate->total_segments = snapshot.reader.size();

  size_t live = 0;
  for (size_t i = 0; i < gstate->total_segments; ++i) {
    if (snapshot.reader[i].live_docs_count() != 0) {
      ++live;
    }
  }
  gstate->remained_segments = live;
  gstate->dis.reserve(gstate->MaxThreads() * gstate->scan->top_k);
  gstate->ids.reserve(gstate->MaxThreads() * gstate->scan->top_k);

  if (live == 0) {
    gstate->search_finished.store(true, std::memory_order_release);
    gstate->total_results = 0;
  }

  return gstate;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchAnnScanInitLocal(
  duckdb::ExecutionContext& /*context*/,
  duckdb::TableFunctionInitInput& /*input*/,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchAnnScanGlobalState>();
  std::lock_guard lock{gstate.m};
  auto& ids = gstate.ids;
  auto& dis = gstate.dis;
  auto ids_begin = ids.size();
  auto dis_begin = dis.size();
  ids.resize(ids.size() + gstate.scan->top_k);
  dis.resize(dis.size() + gstate.scan->top_k);
  SDB_ASSERT(ids.capacity() == gstate.MaxThreads() * gstate.scan->top_k);
  SDB_ASSERT(dis.capacity() == gstate.MaxThreads() * gstate.scan->top_k);
  auto lstate = duckdb::make_uniq<SearchAnnScanLocalState>(
    dis.data() + dis_begin, ids.data() + ids_begin, gstate.scan->top_k);
  if (gstate.scan->stored_text_filter) {
    auto& pf =
      basics::downCast<irs::ProxyFilter>(*gstate.scan->stored_text_filter);
    lstate->text_filter_query =
      pf.prepare({.index = *gstate.reader, .scorer = nullptr});
  }
  return lstate;
}

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<SearchAnnScanGlobalState>();
  auto& l = data.local_state->Cast<SearchAnnScanLocalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();
  size_t processed = 0;
  uint32_t segment;
  SDB_ASSERT(g.reader);
  CompositeScanFilter composite;
  if (l.text_filter_query) {
    composite.EnableText(*l.text_filter_query);
  }
  if (g.filter_ctx) {
    composite.EnableAnn(*g.filter_ctx);
  }
  while (ClaimNextLiveSegment(g.next_segment, g.total_segments, *g.reader,
                              segment)) {
    const auto& reader = (*g.reader)[segment];
    composite.Reset(*g.reader, segment);
    if (const auto* cs = reader.CsReader()) {
      irs::columnstore::ReadContext read_ctx{*cs};
      ANNSearchSegment(reader, segment, composite, g, l, read_ctx);
    }
    processed++;
  }
  auto remained =
    g.remained_segments.fetch_sub(processed, std::memory_order_acq_rel);
  if (processed == 0 || remained != processed) {
    output.SetChildCardinality(0);
    return;
  }
  g.search_finished.store(true, std::memory_order_release);
  // Last worker partitions the flat results to top-k and emits.
  MergeResult(context, bind_data, g);
  EmitResult(context, bind_data, g, output);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto gstate = duckdb::make_uniq<SearchRangeScanGlobalState>();
  InitCommonState(*gstate, context, bind_data, input);
  ClassifyColumnstoreProjections(*gstate, bind_data);
  gstate->scan = &bind_data.scan_source->Cast<RangeSearchScan>();
  gstate->ef_search = ReadEfSearch(context);

  InitAnnFilterContext(
    gstate->filter_ctx, context, gstate->scan->filter_expression.get(),
    gstate->scan->filter_column_ids, gstate->snapshot, bind_data);

  SDB_ASSERT(gstate->scan->snapshot);
  auto& snapshot = *gstate->scan->snapshot;
  gstate->reader = &snapshot.reader;
  gstate->total_segments = snapshot.reader.size();
  return gstate;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchRangeScanInitLocal(
  duckdb::ExecutionContext& /*context*/,
  duckdb::TableFunctionInitInput& /*input*/,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchRangeScanGlobalState>();
  auto lstate = duckdb::make_uniq<SearchRangeScanLocalState>();
  if (gstate.scan->stored_text_filter) {
    auto& pf =
      basics::downCast<irs::ProxyFilter>(*gstate.scan->stored_text_filter);
    lstate->text_filter_query =
      pf.prepare({.index = *gstate.reader, .scorer = nullptr});
  }
  return lstate;
}

void SearchRangeScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<SearchRangeScanGlobalState>();
  auto& l = data.local_state->Cast<SearchRangeScanLocalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();

  uint32_t segment;
  CompositeScanFilter composite;
  if (l.text_filter_query) {
    composite.EnableText(*l.text_filter_query);
  }
  if (g.filter_ctx) {
    composite.EnableAnn(*g.filter_ctx);
  }
  while (l.seg_docs.size() - l.current_idx < STANDARD_VECTOR_SIZE &&
         ClaimNextLiveSegment(g.next_segment, g.total_segments, *g.reader,
                              segment)) {
    const auto& sub = (*g.reader)[segment];
    composite.Reset(*g.reader, segment);
    if (const auto* cs = sub.CsReader()) {
      irs::columnstore::ReadContext read_ctx{*cs};
      RangeSearchSegment(context, bind_data, sub, segment, composite, g, l,
                         read_ctx);
    }
  }

  const size_t total = l.seg_docs.size();
  const size_t batch_start = l.current_idx;
  if (batch_start >= total) {
    output.SetChildCardinality(0);
    return;
  }

  const size_t batch_size =
    std::min<size_t>(STANDARD_VECTOR_SIZE, total - batch_start);

  for (duckdb::idx_t proj = 0; proj < g.projected_columns.size(); ++proj) {
    if (g.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (g.scan_tableoid && proj == g.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(g.tableoid_value),
                                  duckdb::count_t(batch_size));
    }
  }

  std::lock_guard lock{g.m};
  if (g.has_external_projections) {
    SDB_ASSERT(g.index_source);
    SDB_IF_FAILURE("SearchRocksDBLookupFault") { SDB_THROW(sdb::ERROR_DEBUG); }
    g.index_source->Materialize(context, l.pk_batch, batch_start, batch_size,
                                output);
  }
  if (!g.cs_projections.empty()) {
    std::span<const SegDoc> slice{l.seg_docs.data() + batch_start, batch_size};
    MaterializeIncludeColumnsScoreOrder(g, *g.reader, slice, output);
  }

  output.SetChildCardinality(static_cast<duckdb::idx_t>(batch_size));
  l.current_idx += batch_size;
  g.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);
}

}  // namespace sdb::connector
