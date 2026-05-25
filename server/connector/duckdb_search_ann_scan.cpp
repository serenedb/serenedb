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

}  // namespace

// Per-segment HNSW probe + cross-thread threshold CAS.
void SearchAnnScanLocalState::OnSegment(duckdb::ClientContext& /*ctx*/,
                                        const irs::SubReader& seg,
                                        uint32_t seg_idx,
                                        SearchAnnScanGlobalState& g) {
  SDB_ASSERT(g.scan->top_k > 0);
  composite.Reset(*g.reader, seg_idx);
  const auto* cs = seg.CsReader();
  if (!cs) {
    return;
  }
  irs::columnstore::ReadContext read_ctx{*cs};

  const size_t top_k = g.scan->top_k;
  irs::HNSWSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(g.scan->query_vector.data()),
    .top_k = top_k,
    .global_threshold = g.global_kth_dis.load(std::memory_order_relaxed),
  };
  const int requested_ef = g.ef_search > 0 ? g.ef_search : info.params.efSearch;
  info.params.efSearch = std::max(requested_ef, static_cast<int>(top_k));
  info.params.sel = composite.Empty() ? nullptr : &composite;

  SDB_ASSERT(g.reader);
  seg.Search(g.scan->field_id, info, buffer, seg_idx, read_ctx);

  // Tighten the cross-thread threshold so other workers can prune.
  const float local_kth = buffer.dis[0];
  float cur = g.global_kth_dis.load(std::memory_order_relaxed);
  while (local_kth < cur && !g.global_kth_dis.compare_exchange_weak(
                              cur, local_kth, std::memory_order_relaxed)) {
  }
}

// Prep this thread's emit buffer: filter -1 sentinels, sort by
// (seg, doc) for the forward-only ScanCursor, build pk_batch.
void SearchAnnScanLocalState::OnSegmentsExhausted(duckdb::ClientContext& ctx,
                                                  SearchAnnScanGlobalState& g) {
  const size_t cap = buffer.dis.size();
  std::vector<int64_t> top_ids;
  top_ids.reserve(cap);
  for (size_t i = 0; i < cap; ++i) {
    const int64_t id = buffer.ids[i];
    if (id == -1) {
      continue;
    }
    top_ids.push_back(id);
  }

  // Sort by (segment, doc_pos) -- forward-only ScanCursor invariant.
  std::sort(top_ids.begin(), top_ids.end(), [](int64_t a, int64_t b) {
    return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(a)) <
           irs::UnpackSegmentWithDoc(static_cast<uint64_t>(b));
  });

  const size_t n = top_ids.size();
  const bool has_real = absl::c_any_of(g.projected_columns, [](auto p) {
    return p != duckdb::DConstants::INVALID_INDEX;
  });

  if (!has_real || n == 0) {
    seg_docs.assign(n, SegDoc{});
    for (size_t i = 0; i < n; ++i) {
      auto [seg, doc] =
        irs::UnpackSegmentWithDoc(static_cast<uint64_t>(top_ids[i]));
      seg_docs[i] = {
        .segment_idx = seg,
        .doc_pos = static_cast<irs::doc_id_t>(doc - irs::doc_limits::min())};
    }
    return;
  }

  seg_docs.assign(n, SegDoc{});

  if (!g.has_external_projections) {
    // cs-only INCLUDE: no PK fetch needed; seg_docs is the materialiser
    // input directly.
    for (size_t i = 0; i < n; ++i) {
      auto [seg, doc] =
        irs::UnpackSegmentWithDoc(static_cast<uint64_t>(top_ids[i]));
      seg_docs[i] = {
        .segment_idx = seg,
        .doc_pos = static_cast<irs::doc_id_t>(doc - irs::doc_limits::min())};
    }
    return;
  }

  // External (RocksDB) projection: per-thread index_source.
  SDB_ASSERT(bind_data);
  if (!index_source) {
    index_source = MakeIndexSource(ctx, *bind_data, g.snapshot, g.txn,
                                   g.external_projected_columns,
                                   g.projected_types, bind_data->column_ids);
  }
  if (std::holds_alternative<std::monostate>(pk_batch)) {
    pk_batch = index_source->CreatePkBatch();
  }

  const size_t valid = std::visit(
    [&](auto& pk) -> size_t {
      using T = std::decay_t<decltype(pk)>;
      if constexpr (std::is_same_v<T, std::monostate>) {
        SDB_ASSERT(false, "pk_batch must be initialised");
        return 0;
      } else {
        pk.Reset();
        if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
          pk.EnsureInit(duckdb::Allocator::Get(ctx));
        }
        PkResize(pk, n);
        LookupSegmentsValues(
          top_ids,
          [](int64_t id) {
            return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(id));
          },
          *g.reader, lookup_scratch,
          [&](size_t orig, std::string_view pk_bytes) {
            SetPrimaryKey(pk, orig, pk_bytes);
            auto [seg, doc] =
              irs::UnpackSegmentWithDoc(static_cast<uint64_t>(top_ids[orig]));
            seg_docs[orig] = {.segment_idx = seg,
                              .doc_pos = static_cast<irs::doc_id_t>(
                                doc - irs::doc_limits::min())};
          });
        return PkCompactResolved(pk, n, seg_docs);
      }
    },
    pk_batch);
  seg_docs.resize(valid);
}

EmitOutput SearchAnnScanLocalState::EmitNextChunk(duckdb::ClientContext& ctx,
                                                  SearchAnnScanGlobalState& g,
                                                  duckdb::DataChunk& output) {
  if (!segments_exhausted) {
    return EmitOutput::NeedsSegment;
  }
  SDB_IF_FAILURE("SearchRocksDBLookupFault") {
    if (g.has_external_projections && current_idx < seg_docs.size()) {
      SDB_THROW(ERROR_DEBUG);
    }
  }
  if (EmitChunkFromBuffer(ctx, g, *this, seg_docs, std::span<const float>{},
                          output) > 0) {
    return EmitOutput::Chunk;
  }
  return EmitOutput::Finished;
}

void SearchRangeScanLocalState::OnSegment(duckdb::ClientContext& ctx,
                                          const irs::SubReader& seg,
                                          uint32_t seg_idx,
                                          SearchRangeScanGlobalState& g) {
  composite.Reset(*g.reader, seg_idx);
  const auto* cs = seg.CsReader();
  if (!cs) {
    return;
  }
  irs::columnstore::ReadContext read_ctx{*cs};

  range_buffer.dis.clear();
  range_buffer.ids.clear();

  irs::HNSWRangeSearchInfo info{
    .query =
      reinterpret_cast<const irs::byte_type*>(g.scan->query_vector.data()),
    .radius = g.scan->effective_radius,
  };
  if (g.ef_search > 0) {
    info.params.efSearch = static_cast<size_t>(g.ef_search);
  }
  info.params.sel = composite.Empty() ? nullptr : &composite;
  seg.RangeSearch(g.scan->field_id, info, range_buffer, seg_idx, read_ctx);

  auto& ids = range_buffer.ids;
  while (!ids.empty() && ids.back() == -1) {
    ids.pop_back();
  }

  const auto n = ids.size();
  if (n == 0) {
    return;
  }

  // Sort by (seg, doc) for the forward-only ScanCursor.
  std::sort(ids.begin(), ids.end(), [](int64_t a, int64_t b) {
    return irs::UnpackSegmentWithDoc(static_cast<uint64_t>(a)) <
           irs::UnpackSegmentWithDoc(static_cast<uint64_t>(b));
  });

  if (g.has_external_projections) {
    SDB_ASSERT(bind_data);
    if (!index_source) {
      index_source = MakeIndexSource(ctx, *bind_data, g.snapshot, g.txn,
                                     g.external_projected_columns,
                                     g.projected_types, bind_data->column_ids);
    }
    if (std::holds_alternative<std::monostate>(pk_batch)) {
      pk_batch = index_source->CreatePkBatch();
      if (auto* p = std::get_if<PrimaryKeysBytes>(&pk_batch)) {
        p->EnsureInit(duckdb::Allocator::Get(ctx));
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
            *g.reader, lookup_scratch,
            [&](size_t orig, std::string_view pk_bytes) {
              AppendPrimaryKey(pk, pk_bytes);
              auto [s, doc] =
                irs::UnpackSegmentWithDoc(static_cast<uint64_t>(ids[orig]));
              seg_docs.push_back({.segment_idx = s,
                                  .doc_pos = static_cast<irs::doc_id_t>(
                                    doc - irs::doc_limits::min())});
            });
        }
      },
      pk_batch);
  } else {
    // cs-only INCLUDE: skip PK fetch; seg_docs ordering set by RangeSearch.
    for (size_t i = 0; i < n; ++i) {
      auto [s, doc] = irs::UnpackSegmentWithDoc(static_cast<uint64_t>(ids[i]));
      seg_docs.push_back(
        {.segment_idx = s,
         .doc_pos = static_cast<irs::doc_id_t>(doc - irs::doc_limits::min())});
    }
  }

  g.total_results.fetch_add(n, std::memory_order_relaxed);
}

EmitOutput SearchRangeScanLocalState::EmitNextChunk(
  duckdb::ClientContext& ctx, SearchRangeScanGlobalState& g,
  duckdb::DataChunk& output) {
  SDB_IF_FAILURE("SearchRocksDBLookupFault") {
    if (g.has_external_projections && current_idx < seg_docs.size()) {
      SDB_THROW(ERROR_DEBUG);
    }
  }
  if (EmitChunkFromBuffer(ctx, g, *this, seg_docs, std::span<const float>{},
                          output) > 0) {
    return EmitOutput::Chunk;
  }
  return segments_exhausted ? EmitOutput::Finished : EmitOutput::NeedsSegment;
}

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

  auto& snapshot =
    GetSereneDBContext(context).GetSearchSnapshot(gstate->scan->index_id);
  gstate->reader = &snapshot.reader;
  gstate->total_segments = snapshot.reader.size();

  // resize (not reserve) -- spans need vector::size() to cover them
  // and data() must stay stable while threads write concurrently.
  const auto buf_size = gstate->MaxThreads() * gstate->scan->top_k;
  gstate->dis.resize(buf_size);
  gstate->ids.resize(buf_size);

  return gstate;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchAnnScanInitLocal(
  duckdb::ExecutionContext& /*context*/, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchAnnScanGlobalState>();
  const auto k = gstate.scan->top_k;
  const auto slot =
    gstate.next_slice_idx.fetch_add(1, std::memory_order_relaxed);
  auto lstate = duckdb::make_uniq<SearchAnnScanLocalState>(
    gstate.dis.data() + slot * k, gstate.ids.data() + slot * k, k);
  lstate->bind_data = &input.bind_data->Cast<SereneDBScanBindData>();
  if (gstate.scan->stored_text_filter) {
    auto& pf =
      basics::downCast<irs::ProxyFilter>(*gstate.scan->stored_text_filter);
    lstate->text_filter_query =
      pf.prepare({.index = *gstate.reader, .scorer = nullptr});
    lstate->composite.EnableText(*lstate->text_filter_query);
  }
  if (gstate.filter_ctx) {
    lstate->composite.EnableAnn(*gstate.filter_ctx);
  }
  return lstate;
}

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<SearchAnnScanGlobalState>();
  auto& l = data.local_state->Cast<SearchAnnScanLocalState>();
  SDB_ASSERT(g.reader);
  RunParallelInvertedIndexScan(context, g, l, output);
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

  auto& snapshot =
    GetSereneDBContext(context).GetSearchSnapshot(gstate->scan->index_id);
  gstate->reader = &snapshot.reader;
  gstate->total_segments = snapshot.reader.size();
  return gstate;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchRangeScanInitLocal(
  duckdb::ExecutionContext& /*context*/, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchRangeScanGlobalState>();
  auto lstate = duckdb::make_uniq<SearchRangeScanLocalState>();
  lstate->bind_data = &input.bind_data->Cast<SereneDBScanBindData>();
  if (gstate.scan->stored_text_filter) {
    auto& pf =
      basics::downCast<irs::ProxyFilter>(*gstate.scan->stored_text_filter);
    lstate->text_filter_query =
      pf.prepare({.index = *gstate.reader, .scorer = nullptr});
    lstate->composite.EnableText(*lstate->text_filter_query);
  }
  if (gstate.filter_ctx) {
    lstate->composite.EnableAnn(*gstate.filter_ctx);
  }
  return lstate;
}

void SearchRangeScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<SearchRangeScanGlobalState>();
  auto& l = data.local_state->Cast<SearchRangeScanLocalState>();
  RunParallelInvertedIndexScan(context, g, l, output);
}

}  // namespace sdb::connector
