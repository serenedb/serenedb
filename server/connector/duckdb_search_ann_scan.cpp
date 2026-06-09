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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/formats/hnsw/hnsw_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/proxy_filter.hpp>
#include <span>
#include <utility>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "basics/string_utils.h"
#include "connector/duckdb_ann_filter.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"

namespace sdb::connector {
namespace {

int ReadEfSearch(duckdb::ClientContext& context) {
  duckdb::Value v;
  if (context.TryGetCurrentSetting("sdb_ef_search", v) && !v.IsNull()) {
    return v.GetValue<int32_t>();
  }
  return 0;
}

template<typename Lstate>
void AttachTextFilter(Lstate& lstate, const SearchAnnScanGlobalState& gstate) {
  if (!gstate.scan->stored_filter) {
    return;
  }
  lstate.text_filter_query = gstate.scan->stored_filter->prepare(
    {.index = *gstate.reader, .scorer = nullptr});
  lstate.text_filter.emplace(*lstate.text_filter_query);
}

template<typename Buf>
void BuildSortedHits(const Buf& buf, size_t count,
                     const VectorScorerOptions& vs,
                     std::vector<irs::ScoreDoc>& out) {
  out.clear();
  out.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    const int64_t id = buf.ids[i];
    if (id == -1) {
      continue;
    }
    auto [seg, doc] = irs::UnpackSegmentWithDoc(id);
    out.push_back({.score = vs.TransformDistance(buf.dis[i]),
                   .doc = doc,
                   .segment_idx = seg});
  }
  // Forward-only ScanCursor invariant requires (segment, doc) order.
  absl::c_sort(out, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  });
}

template<typename Lstate, typename Gstate>
bool EmitHitsChunk(duckdb::ClientContext& ctx, Gstate& g, Lstate& lstate,
                   duckdb::DataChunk& output) {
  const size_t remaining = lstate.hits.size() - lstate.current_idx;
  if (remaining == 0) {
    return false;
  }
  const auto take = std::min<duckdb::idx_t>(remaining, STANDARD_VECTOR_SIZE);
  SDB_IF_FAILURE("SearchRocksDBLookupFault") {
    if (g.has_external_projections) {
      SDB_THROW(ERROR_DEBUG);
    }
  }
  const ScoreDocsView view{{lstate.hits.data() + lstate.current_idx, take}};
  const auto emitted = MaterializeChunk(ctx, g, lstate, view, output, 0);
  FinalizeBatch(ctx, g, lstate, output, emitted);
  lstate.current_idx += take;
  output.SetChildCardinality(emitted);
  return emitted > 0;
}

}  // namespace

void SearchAnnTopKLocalState::OnSegment(duckdb::ClientContext& ctx,
                                        const irs::SubReader& seg,
                                        uint32_t seg_idx,
                                        SearchAnnScanGlobalState& g) {
  const auto& vs = *g.scan->vector_scorer;
  const auto* cs = seg.CsReader();
  SDB_ASSERT(cs);
  if (text_filter) {
    text_filter->Reset(seg);
  }
  irs::ReadContext read_ctx{*cs};

  const size_t top_k = *g.scan->score_top_k;
  irs::HNSWSearchInfo info{
    .query = reinterpret_cast<const irs::byte_type*>(vs.query_vector.data()),
    .top_k = top_k,
    .global_threshold = g.global_kth_dis.load(std::memory_order_relaxed),
  };
  const int requested_ef = g.ef_search > 0 ? g.ef_search : info.params.efSearch;
  info.params.efSearch = std::max<int>(requested_ef, top_k);
  info.params.sel = text_filter ? &*text_filter : nullptr;

  buffer.ResetValues();
  seg.Search(vs.field_id, info, buffer, seg_idx, read_ctx);

  constexpr auto cmp = [](const std::pair<float, int64_t>& a,
                          const std::pair<float, int64_t>& b) {
    return a.first < b.first;
  };
  for (size_t i = 0; i < top_k_cap; ++i) {
    const int64_t id = buffer.ids[i];
    if (id == -1) {
      continue;
    }
    const float d = buffer.dis[i];
    if (top_hits.size() < top_k_cap) {
      top_hits.emplace_back(d, id);
      std::push_heap(top_hits.begin(), top_hits.end(), cmp);
    } else if (d < top_hits.front().first) {
      std::pop_heap(top_hits.begin(), top_hits.end(), cmp);
      top_hits.back() = {d, id};
      std::push_heap(top_hits.begin(), top_hits.end(), cmp);
    }
  }

  if (top_hits.size() == top_k_cap) {
    const float local_kth = top_hits.front().first;
    float cur = g.global_kth_dis.load(std::memory_order_relaxed);
    while (local_kth < cur && !g.global_kth_dis.compare_exchange_weak(
                                cur, local_kth, std::memory_order_relaxed)) {
    }
  }
}

void SearchAnnTopKLocalState::PrepEmitBuffer(duckdb::ClientContext& /*ctx*/,
                                             SearchAnnScanGlobalState& g) {
  const auto& vs = *g.scan->vector_scorer;
  hits.clear();
  hits.reserve(top_hits.size());
  for (auto& [d, id] : top_hits) {
    auto [seg, doc] = irs::UnpackSegmentWithDoc(id);
    hits.push_back(
      {.score = vs.TransformDistance(d), .doc = doc, .segment_idx = seg});
  }
  // Forward-only ScanCursor invariant requires (segment, doc) order.
  absl::c_sort(hits, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  });
  prepped = true;
}

bool SearchAnnTopKLocalState::OnSegmentsExhausted(duckdb::ClientContext& ctx,
                                                  SearchAnnScanGlobalState& g,
                                                  duckdb::DataChunk& output) {
  if (!prepped) {
    PrepEmitBuffer(ctx, g);
  }
  return EmitHitsChunk(ctx, g, *this, output);
}

void SearchAnnRangeLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                            const irs::SubReader& seg,
                                            uint32_t seg_idx,
                                            SearchAnnScanGlobalState& g) {
  const auto* cs = seg.CsReader();
  SDB_ASSERT(cs);
  hits.clear();
  current_idx = 0;
  irs::ReadContext read_ctx{*cs};

  if (text_filter) {
    text_filter->Reset(seg);
  }
  const auto& vs = *g.scan->vector_scorer;
  irs::HNSWRangeSearchInfo info{
    .query = reinterpret_cast<const irs::byte_type*>(vs.query_vector.data()),
    .radius = vs.EffectiveRadius(),
  };
  if (g.ef_search > 0) {
    info.params.efSearch = g.ef_search;
  }
  info.params.sel = text_filter ? &*text_filter : nullptr;
  range_buffer.dis.clear();
  range_buffer.ids.clear();
  seg.RangeSearch(vs.field_id, info, range_buffer, seg_idx, read_ctx);

  SDB_ASSERT(range_buffer.ids.size() == range_buffer.dis.size());
  BuildSortedHits(range_buffer, range_buffer.ids.size(), vs, hits);
}

duckdb::idx_t SearchAnnRangeLocalState::EmitChunk(duckdb::ClientContext& ctx,
                                                  SearchAnnScanGlobalState& g,
                                                  duckdb::DataChunk& output,
                                                  duckdb::idx_t output_start) {
  const size_t remaining = hits.size() - current_idx;
  if (remaining == 0) {
    return 0;
  }
  const auto budget = STANDARD_VECTOR_SIZE - output_start;
  const auto take = std::min<duckdb::idx_t>(remaining, budget);
  SDB_IF_FAILURE("SearchRocksDBLookupFault") {
    if (g.has_external_projections) {
      SDB_THROW(ERROR_DEBUG);
    }
  }
  const ScoreDocsView view{{hits.data() + current_idx, take}};
  const auto emitted =
    MaterializeChunk(ctx, g, *this, view, output, output_start);
  current_idx += take;
  return emitted;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto gstate = duckdb::make_uniq<SearchAnnScanGlobalState>();
  InitCommonState(*gstate, context, bind_data, input);
  ClassifyColumnstoreProjections(*gstate, bind_data);
  gstate->scan = &bind_data.scan_source->Cast<SearchScan>();
  gstate->ef_search = ReadEfSearch(context);

  auto& snapshot = *gstate->scan->snapshot;
  gstate->reader = &snapshot.reader;
  gstate->total_segments = snapshot.reader.size();

  return gstate;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchAnnScanInitLocal(
  duckdb::ExecutionContext& /*context*/, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchAnnScanGlobalState>();
  const auto& bd = input.bind_data->Cast<SereneDBScanBindData>();
  if (gstate.scan->score_top_k) {
    auto lstate =
      duckdb::make_uniq<SearchAnnTopKLocalState>(*gstate.scan->score_top_k);
    lstate->bind_data = &bd;
    AttachTextFilter(*lstate, gstate);
    return lstate;
  }
  auto lstate = duckdb::make_uniq<SearchAnnRangeLocalState>();
  lstate->bind_data = &bd;
  AttachTextFilter(*lstate, gstate);
  return lstate;
}

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<SearchAnnScanGlobalState>();
  if (g.scan->score_top_k) {
    auto& l = data.local_state->Cast<SearchAnnTopKLocalState>();
    RunCollectThenEmitScan(context, g, l, output);
  } else {
    auto& l = data.local_state->Cast<SearchAnnRangeLocalState>();
    RunStreamingScan(context, g, l, output);
  }
}

}  // namespace sdb::connector
