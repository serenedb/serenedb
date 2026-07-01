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
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

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
  lstate.text_filter_collector =
    gstate.scan->stored_filter->MakeCollector(nullptr);
  lstate.text_filter.emplace(*gstate.scan->stored_filter,
                             *lstate.text_filter_collector);
}

irs::ScoreDoc MakeScoreDoc(int64_t id, float dis,
                           const VectorScorerOptions& vs) {
  auto [seg, doc] = irs::UnpackSegmentWithDoc(id);
  return {.score = vs.TransformDistance(dis), .doc = doc, .segment_idx = seg};
}

template<typename PQ>
typename PQ::container_type& HeapContainer(PQ& pq) noexcept {
  struct Access : PQ {
    using PQ::c;
  };
  return static_cast<Access&>(pq).c;
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
    out.push_back(MakeScoreDoc(id, buf.dis[i], vs));
  }
  SortScoreDocsBySegDoc(out);
}

}  // namespace

void SearchAnnTopKLocalState::OnSegment(duckdb::ClientContext& ctx,
                                        const irs::SubReader& seg,
                                        uint32_t seg_idx,
                                        SearchAnnScanGlobalState& g) {
  const auto& vs = *g.scan->vector_scorer;
  const auto* col_reader = seg.GetColReader();
  SDB_ASSERT(col_reader);
  if (text_filter) {
    text_filter->Reset(seg);
  }
  irs::ReadContext read_ctx{*col_reader};

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

  for (size_t i = 0; i < top_k_cap; ++i) {
    const int64_t id = buffer.ids[i];
    if (id == -1) {
      continue;
    }
    const float d = buffer.dis[i];
    if (top_hits.size() == top_k_cap && d >= top_hits.top().first) {
      continue;
    }
    top_hits.emplace(d, id);
    if (top_hits.size() > top_k_cap) {
      top_hits.pop();
    }
  }

  if (top_hits.size() == top_k_cap) {
    const float local_kth = top_hits.top().first;
    float cur = g.global_kth_dis.load(std::memory_order_relaxed);
    while (local_kth < cur && !g.global_kth_dis.compare_exchange_weak(
                                cur, local_kth, std::memory_order_relaxed)) {
    }
  }
}

void SearchAnnTopKLocalState::PrepEmitBuffer(duckdb::ClientContext& /*ctx*/,
                                             SearchAnnScanGlobalState& g) {
  const auto& vs = *g.scan->vector_scorer;
  auto heap = std::move(HeapContainer(top_hits));
  hits.clear();
  hits.reserve(heap.size());
  for (const auto& [d, id] : heap) {
    hits.push_back(MakeScoreDoc(id, d, vs));
  }
  SortScoreDocsBySegDoc(hits);
  prepped = true;
}

bool SearchAnnTopKLocalState::OnSegmentsExhausted(duckdb::ClientContext& ctx,
                                                  SearchAnnScanGlobalState& g,
                                                  duckdb::DataChunk& output) {
  if (!prepped) {
    PrepEmitBuffer(ctx, g);
  }
  return EmitBufferedScoreDocs(ctx, g, *this, hits, current_idx, output);
}

void SearchAnnRangeLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                            const irs::SubReader& seg,
                                            uint32_t seg_idx,
                                            SearchAnnScanGlobalState& g) {
  const auto* col_reader = seg.GetColReader();
  SDB_ASSERT(col_reader);
  hits.clear();
  current_idx = 0;
  irs::ReadContext read_ctx{*col_reader};

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
  SDB_IF_FAILURE("SearchLookupFault") {
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
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto gstate = duckdb::make_uniq<SearchAnnScanGlobalState>();
  InitCommonState(*gstate, context, bind_data, input);
  ClassifyColumnstoreProjections(*gstate, bind_data);
  auto& ss = bind_data.scan_source->Cast<SearchScan>();
  gstate->scan = &ss;
  gstate->ef_search = ReadEfSearch(context);

  if (!gstate->scan->offsets.empty() && !gstate->scan->stored_filter) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() requires an inverted index scan in the same "
              "sub-query"));
  }

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
