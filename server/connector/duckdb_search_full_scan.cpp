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

#include "connector/duckdb_search_full_scan.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/vector_radius_filter.hpp>
#include <iresearch/search/vector_similarity_filter.hpp>
#include <iresearch/search/vector_similarity_query.hpp>
#include <iresearch/search/vector_similarity_scorer.hpp>
#include <iresearch/utils/string.hpp>
#include <span>
#include <type_traits>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "connector/columnstore_materializer.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/search_pk_lookup.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {
namespace {

const irs::Filter& MatchAllFilter() {
  static const irs::All kInstance;
  return kInstance;
}

constexpr uint32_t kRerankFactor = 4;

void RerankHits(SearchFullScanGlobalState& g, std::span<irs::ScoreDoc> hits) {
  SDB_ASSERT(g.vector_scorer != nullptr);
  SDB_ASSERT(g.reader != nullptr);
  const auto& vs = *g.vector_scorer;
  const std::span<const float> query{vs.query_vector};
  const auto d = static_cast<uint32_t>(vs.query_vector.size());
  size_t i = 0;
  while (i < hits.size()) {
    const uint32_t seg = hits[i].segment_idx;
    size_t j = i + 1;
    while (j < hits.size() && hits[j].segment_idx == seg) {
      ++j;
    }
    const auto& sub = (*g.reader)[seg];
    if (const auto* vec_col = sub.Column(vs.field_id); vec_col != nullptr) {
      irs::RerankExactDistances(sub, *vec_col, d, query, vs.metric,
                                hits.subspan(i, j - i));
    }
    i = j;
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchFullScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);

  auto& ss = bind_data.scan_source->Cast<SearchScan>();
  if (!ss.offsets.empty() && !ss.stored_filter) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() requires an inverted index scan in the same "
              "sub-query"));
  }
  state->scan = &ss;
  state->reader = &ss.snapshot->reader;
  state->total_segments = ss.snapshot->reader.size();
  state->vector_scorer = ss.vector_scorer ? &*ss.vector_scorer : nullptr;

  if ((state->count_only = ss.count_only) && !ss.stored_filter) {
    return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
  }
  if (ss.text_scorer) {
    state->scorer_obj = catalog::MakeScorer(*ss.text_scorer);
  } else if (ss.score_order) {
    state->scorer_obj = std::make_unique<irs::VectorSimilarityScorer>();
  }
  if (ss.vector_scorer) {
    state->owned_filter = MakeVectorFilter(*ss.vector_scorer, ss.stored_filter,
                                           ss.vector_scorer->EffectiveRadius());
    state->filter = state->owned_filter.get();
  } else {
    state->filter =
      ss.stored_filter ? ss.stored_filter.get() : &MatchAllFilter();
  }
  state->queries.resize(ss.snapshot->reader.size());
  state->collectors.resize(state->MaxThreads());

  if (state->count_only) {
    return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
  }

  if (ss.score_top_k && (ss.text_scorer || ss.score_order)) {
    state->parallel_topk = true;
    if (ss.vector_scorer &&
        ss.vector_scorer->quant != irs::VectorQuantization::None) {
      state->rerank_pool =
        kRerankFactor * static_cast<uint32_t>(*ss.score_top_k);
    }
  }
  if (ss.score_order && *ss.score_order == duckdb::OrderType::ASCENDING) {
    state->global_kth_score.store(std::numeric_limits<irs::score_t>::max(),
                                  std::memory_order_relaxed);
  }

  ClassifyColumnstoreProjections(*state, bind_data);

  return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

namespace {

const irs::QueryBuilder& EnsureSegmentQuery(SearchFullScanGlobalState& g,
                                            CommonScanLocalState& l,
                                            const irs::SubReader& seg,
                                            uint32_t seg_idx) {
  auto& q = g.queries[seg_idx];
  if (!q) {
    irs::PrepareCollector* collector = nullptr;
    if (g.scorer_obj) {
      if (!l.prepare_collector) {
        const uint32_t slot =
          g.collector_slots.fetch_add(1, std::memory_order_relaxed);
        SDB_ASSERT(slot < g.collectors.size());
        g.collectors[slot] = g.filter->MakeCollector(g.scorer_obj.get());
        l.prepare_collector = g.collectors[slot].get();
      }
      collector = l.prepare_collector;
    }
    q = g.filter->PrepareSegment(seg, {.collector = collector});
  }
  return *q;
}

template<typename Lstate>
void BuildOffsetsEntries(Lstate& lstate, duckdb::TableFunctionInitInput& input,
                         const SereneDBScanBindData& bd) {
  const auto& ss = bd.scan_source->Cast<SearchScan>();
  if (ss.offsets.empty()) {
    return;
  }
  std::vector<size_t> ss_idx_at_bind(bd.column_ids.size(),
                                     std::numeric_limits<size_t>::max());
  size_t k = 0;
  for (size_t i = 0; i < bd.column_ids.size(); ++i) {
    if (bd.column_ids[i] == catalog::Column::kInvertedIndexOffsetsId) {
      ss_idx_at_bind[i] = k++;
    }
  }
  duckdb::idx_t out_slot = 0;
  for (auto col_id : input.column_ids) {
    if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID ||
        col_id >= duckdb::VIRTUAL_COLUMN_START) {
      ++out_slot;
      continue;
    }
    if (col_id >= bd.column_ids.size()) {
      continue;
    }
    if (bd.column_ids[col_id] == catalog::Column::kInvertedIndexOffsetsId) {
      const auto ss_idx = ss_idx_at_bind[col_id];
      SDB_ASSERT(ss_idx < ss.offsets.size());
      FieldEntry entry;
      entry.output_idx = out_slot;
      entry.limit = ss.offsets[ss_idx].limit;
      entry.id = static_cast<irs::field_id>(ss.offsets[ss_idx].column_id);
      lstate.offsets_entries.push_back(std::move(entry));
    }
    ++out_slot;
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchFullScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchFullScanGlobalState>();
  const auto& bd = input.bind_data->Cast<SereneDBScanBindData>();
  if (gstate.count_only) {
    auto lstate = duckdb::make_uniq<SearchFullScanCountLocalState>();
    if (gstate.queries.empty()) {
      lstate->local_count = gstate.reader->live_docs_count();
      lstate->segments_exhausted = true;
    }
    return lstate;
  }
  if (gstate.parallel_topk) {
    auto lstate = duckdb::make_uniq<SearchFullScanTopKLocalState>();
    lstate->bind_data = &bd;
    auto& ss = bd.scan_source->Cast<SearchScan>();
    const size_t k = gstate.rerank_pool ? gstate.rerank_pool : *ss.score_top_k;
    lstate->hit_buf.resize(irs::BlockSize(k));
    lstate->hit_slice = std::span<irs::ScoreDoc>{lstate->hit_buf};
    if (ss.score_order) {
      lstate->local_threshold = *ss.score_order == duckdb::OrderType::ASCENDING
                                  ? std::numeric_limits<irs::score_t>::max()
                                  : std::numeric_limits<irs::score_t>::lowest();
    }
    BuildOffsetsEntries(*lstate, input, bd);
    return lstate;
  }
  auto lstate = duckdb::make_uniq<SearchFullScanScanLocalState>();
  lstate->bind_data = &bd;
  BuildOffsetsEntries(*lstate, input, bd);
  return lstate;
}

void SearchFullScanFunction(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchFullScanGlobalState>();
  if (gstate.count_only) {
    auto& l = data.local_state->Cast<SearchFullScanCountLocalState>();
    RunCollectThenEmitScan(context, gstate, l, output);
  } else if (gstate.parallel_topk) {
    auto& l = data.local_state->Cast<SearchFullScanTopKLocalState>();
    if (!l.prepared) {
      if (gstate.total_segments != 0) {
        PreparePhase(context, gstate, l);
      }
      l.prepared = true;
    }
    RunCollectThenEmitScan(context, gstate, l, output);
  } else {
    auto& l = data.local_state->Cast<SearchFullScanScanLocalState>();
    if (!l.prepared) {
      if (gstate.scorer_obj && gstate.total_segments != 0) {
        PreparePhase(context, gstate, l);
      }
      l.prepared = true;
    }
    RunStreamingScan(context, gstate, l, output);
  }
}

void IResearchSetScanOrder(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  if (!bind_data || !options || !options->row_limit.IsValid() ||
      !options->single_order_key) {
    return;
  }
  duckdb::Value v;
  if (context.TryGetCurrentSetting("sdb_disable_top_k_optimization", v) &&
      !v.IsNull() && v.GetValue<bool>()) {
    return;
  }
  auto& bd = bind_data->Cast<SereneDBScanBindData>();
  const auto order_col = options->column_idx.GetPrimaryIndex();
  if (order_col >= bd.column_ids.size() ||
      bd.column_ids[order_col] != catalog::Column::kInvertedIndexScoreId) {
    return;
  }
  auto& search_scan = bd.scan_source->Cast<SearchScan>();
  if (search_scan.score_top_k) {
    return;
  }
  if (search_scan.text_scorer) {
    if (options->order_type != duckdb::OrderType::DESCENDING) {
      return;
    }
    search_scan.score_top_k = options->row_limit.GetIndex();
    return;
  }
  if (search_scan.vector_scorer) {
    if (options->order_type != search_scan.vector_scorer->natural_order) {
      return;
    }
    search_scan.score_top_k = options->row_limit.GetIndex();
  }
}

namespace {

template<irs::Order O>
void CollectSegmentTopK(SearchFullScanTopKLocalState& s,
                        const irs::SubReader& seg, uint32_t seg_idx,
                        SearchFullScanGlobalState& g) {
  using C = irs::NthPartitionScoreCollector<O>;
  auto& search = s.bind_data->scan_source->Cast<SearchScan>();
  if (!std::holds_alternative<C>(s.collector)) {
    const size_t k = g.rerank_pool ? g.rerank_pool : *search.score_top_k;
    s.collector.template emplace<C>(s.local_threshold, k, s.hit_slice);
  }
  auto& collector = std::get<C>(s.collector);

  s.score_fetcher.Clear();
  collector.SetSegment(seg_idx);

  const auto seen_global = g.global_kth_score.load(std::memory_order_relaxed);
  if constexpr (O == irs::Order::ASC) {
    if (seen_global < s.local_threshold) {
      s.local_threshold = seen_global;
    }
  } else {
    if (seen_global > s.local_threshold) {
      s.local_threshold = seen_global;
    }
  }

  const auto& seg_query = EnsureSegmentQuery(g, s, seg, seg_idx);
  const bool wand_enabled =
    WandEnabled(s.bind_data->inverted_index.get(), search.text_scorer);
  auto it =
    seg.mask(seg_query.Execute({.wand = {.wand_enabled = wand_enabled}},
                               g.stats ? *g.stats : irs::StatsBuffer::Empty()));
  auto score_func = it->PrepareScore({
    .scorer = g.scorer_obj.get(),
    .segment = &seg,
    .fetcher = &s.score_fetcher,
  });
  if (auto* it_threshold = irs::GetMutable<irs::ScoreThresholdAttr>(it.get())) {
    collector.SetScoreThreshold(it_threshold->value);
  }
  it->Collect(score_func, s.score_fetcher, collector);
  collector.SetScoreThreshold(s.local_threshold);

  const irs::score_t kth = s.local_threshold;
  auto cur = g.global_kth_score.load(std::memory_order_relaxed);
  if constexpr (O == irs::Order::ASC) {
    while (kth < cur && !g.global_kth_score.compare_exchange_weak(
                          cur, kth, std::memory_order_relaxed)) {
    }
  } else {
    while (kth > cur && !g.global_kth_score.compare_exchange_weak(
                          cur, kth, std::memory_order_relaxed)) {
    }
  }
}

}  // namespace

void SearchFullScanTopKLocalState::OnSegment(duckdb::ClientContext& /*ctx*/,
                                             const irs::SubReader& seg,
                                             uint32_t seg_idx,
                                             SearchFullScanGlobalState& g) {
  auto& search = bind_data->scan_source->Cast<SearchScan>();
  const bool ascending =
    search.score_order && *search.score_order == duckdb::OrderType::ASCENDING;
  if (ascending) {
    CollectSegmentTopK<irs::Order::ASC>(*this, seg, seg_idx, g);
  } else {
    CollectSegmentTopK<irs::Order::DESC>(*this, seg, seg_idx, g);
  }
}

void SearchFullScanTopKLocalState::OnSegmentPrepare(
  duckdb::ClientContext& /*ctx*/, const irs::SubReader& seg, uint32_t seg_idx,
  SearchFullScanGlobalState& g) {
  EnsureSegmentQuery(g, *this, seg, seg_idx);
}

bool SearchFullScanTopKLocalState::OnSegmentsExhausted(
  duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
  duckdb::DataChunk& output) {
  if (!prepped) {
    PrepEmitBuffer(ctx, g);
  }

  const size_t remaining = top_hits.size() - current_idx;
  if (remaining == 0) {
    return false;
  }
  const auto take = std::min<size_t>(remaining, STANDARD_VECTOR_SIZE);
  const ScoreDocsView view{top_hits.subspan(current_idx, take)};
  const auto emitted = MaterializeChunk(ctx, g, *this, view, output, 0);
  const auto visible = FinalizeBatch(ctx, g, *this, output, emitted);
  current_idx += take;
  output.SetChildCardinality(visible);
  return emitted > 0;
}

void SearchFullScanTopKLocalState::PrepEmitBuffer(
  duckdb::ClientContext& /*ctx*/, SearchFullScanGlobalState& g) {
  prepped = true;
  if (std::holds_alternative<std::monostate>(collector)) {
    return;  // no segments claimed by this thread
  }

  const size_t accepted = std::visit(
    [](auto& c) -> size_t {
      if constexpr (std::is_same_v<std::decay_t<decltype(c)>, std::monostate>) {
        return 0;
      } else {
        return c.AcceptedCount();
      }
    },
    collector);
  auto accepted_slice = hit_slice.subspan(0, accepted);
  const auto by_seg_doc = [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  };
  size_t kept = accepted;
  if (g.rerank_pool > 0 && g.vector_scorer != nullptr) {
    absl::c_sort(accepted_slice, by_seg_doc);
    RerankHits(g, accepted_slice);
    const auto& search = bind_data->scan_source->Cast<SearchScan>();
    const size_t kreal = *search.score_top_k;
    if (kept > kreal) {
      const bool asc = search.score_order &&
                       *search.score_order == duckdb::OrderType::ASCENDING;
      std::nth_element(accepted_slice.begin(), accepted_slice.begin() + kreal,
                       accepted_slice.end(),
                       [asc](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
                         return asc ? l.score < r.score : l.score > r.score;
                       });
      kept = kreal;
      accepted_slice = hit_slice.subspan(0, kept);
    }
  }
  absl::c_sort(accepted_slice, by_seg_doc);
  top_hits = hit_slice.subspan(0, kept);
}

void SearchFullScanScanLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                                const irs::SubReader& seg,
                                                uint32_t seg_idx,
                                                SearchFullScanGlobalState& g) {
  current_seg_idx = seg_idx;
  const bool has_real = absl::c_any_of(g.projected_columns, [](auto p) {
    return p != duckdb::DConstants::INVALID_INDEX;
  });
  const bool bulk = has_real && !g.scan_score && !g.has_external_projections &&
                    g.scan->IsMatchAll() && !g.scan->EmitOffsets() &&
                    seg.live_docs_count() == seg.docs_count();
  if (bulk) {
    bulk_doc_in_seg = 0;
    bulk_seg_doc_count = seg.docs_count();
    streaming_doc.reset();
    return;
  }
  bulk_doc_in_seg = 0;
  bulk_seg_doc_count = 0;
  const auto& seg_query = EnsureSegmentQuery(g, *this, seg, seg_idx);
  streaming_doc = seg.mask(
    seg_query.Execute({}, g.stats ? *g.stats : irs::StatsBuffer::Empty()));
  if (g.has_external_projections &&
      !SegmentPkColumn(*g.reader, seg_idx).second) {
    streaming_doc.reset();
    return;
  }
  if (g.scan_score) {
    score_fetcher.Clear();
    streaming_score_function = streaming_doc->PrepareScore({
      .scorer = g.scorer_obj.get(),
      .segment = &seg,
      .fetcher = &score_fetcher,
    });
  }
}

void SearchFullScanScanLocalState::OnSegmentPrepare(
  duckdb::ClientContext& /*ctx*/, const irs::SubReader& seg, uint32_t seg_idx,
  SearchFullScanGlobalState& g) {
  EnsureSegmentQuery(g, *this, seg, seg_idx);
}

void SearchFullScanScanLocalState::AdvanceChunk(SearchFullScanGlobalState& g,
                                                duckdb::idx_t budget) {
  chunk_hits.clear();
  chunk_scores.clear();
  if (!streaming_doc) {
    return;
  }
  chunk_hits.reserve(budget);

  if (!g.scan_score) {
    while (chunk_hits.size() < budget) {
      auto doc_id = streaming_doc->advance();
      if (irs::doc_limits::eof(doc_id)) {
        streaming_doc.reset();
        return;
      }
      chunk_hits.push_back(doc_id);
    }
    return;
  }

  static_assert(STANDARD_VECTOR_SIZE % irs::kScoreBlock == 0,
                "kScoreBlock must divide STANDARD_VECTOR_SIZE");
  chunk_scores.reserve(budget);

  std::array<irs::doc_id_t, irs::kScoreBlock> block_docs;
  irs::scores_size_t block_count = 0;
  auto flush = [&](bool full) {
    if (!full && block_count == 0) {
      return;
    }
    score_fetcher.Fetch({block_docs.data(), block_count});
    const size_t pos = chunk_scores.size();
    chunk_scores.resize(pos + block_count);
    if (full) {
      streaming_score_function.ScoreBlock(&chunk_scores[pos]);
    } else {
      streaming_score_function.Score(&chunk_scores[pos], block_count);
    }
    block_count = 0;
  };

  while (chunk_hits.size() < budget) {
    auto doc_id = streaming_doc->advance();
    if (irs::doc_limits::eof(doc_id)) {
      flush(false);
      streaming_score_function = {};
      streaming_doc.reset();
      break;
    }
    streaming_doc->FetchScoreArgs(block_count);
    block_docs[block_count++] = doc_id;
    chunk_hits.push_back(doc_id);
    if (block_count == irs::kScoreBlock) {
      flush(true);
    }
  }
  if (block_count != 0) {
    flush(false);
  }
  SDB_ASSERT(chunk_scores.size() == chunk_hits.size());
}

duckdb::idx_t SearchFullScanScanLocalState::EmitChunk(
  duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
  duckdb::DataChunk& output, duckdb::idx_t output_start) {
  const auto budget = STANDARD_VECTOR_SIZE - output_start;
  if (bulk_doc_in_seg < bulk_seg_doc_count) {
    auto* mat =
      GetOrOpenSegmentMaterializer(*this, g, *g.reader, current_seg_idx);
    SDB_ENSURE(mat, sdb::ERROR_INTERNAL,
               "bulk cs scan: segment has no columnstore reader");
    const auto take =
      std::min<duckdb::idx_t>(budget, bulk_seg_doc_count - bulk_doc_in_seg);
    const bool fills_entire_vector =
      output_start == 0 && take == STANDARD_VECTOR_SIZE;
    mat->Scan(bulk_doc_in_seg, take, output, output_start, fills_entire_vector);
    const auto row_base =
      g.produced_rows.fetch_add(take, std::memory_order_relaxed);
    WriteVirtualColumns(g, row_base, take, ScoreDocsView{}, output,
                        output_start);
    bulk_doc_in_seg += take;
    return take;
  }
  if (!streaming_doc) {
    return 0;
  }
  AdvanceChunk(g, budget);
  if (chunk_hits.empty()) {
    return 0;
  }
  SDB_IF_FAILURE("SearchLookupFault") {
    if (g.has_external_projections) {
      SDB_THROW(ERROR_DEBUG);
    }
  }
  const StreamingHitsView view{
    .docs = {chunk_hits.data(), chunk_hits.size()},
    .scores = g.scan_score ? std::span<const float>{chunk_scores.data(),
                                                    chunk_scores.size()}
                           : std::span<const float>{},
    .fixed_seg = current_seg_idx,
  };
  return MaterializeChunk(ctx, g, *this, view, output, output_start);
}

void SearchFullScanCountLocalState::OnSegment(duckdb::ClientContext& /*ctx*/,
                                              const irs::SubReader& seg,
                                              uint32_t seg_idx,
                                              SearchFullScanGlobalState& g) {
  const auto& seg_query = EnsureSegmentQuery(g, *this, seg, seg_idx);
  auto doc = seg.mask(seg_query.Execute({}, irs::StatsBuffer::Empty()));
  local_count += doc->count();
}

bool SearchFullScanCountLocalState::OnSegmentsExhausted(
  duckdb::ClientContext& /*ctx*/, SearchFullScanGlobalState& g,
  duckdb::DataChunk& output) {
  if (local_emitted >= local_count) {
    return false;
  }
  const auto batch =
    std::min<duckdb::idx_t>(local_count - local_emitted, STANDARD_VECTOR_SIZE);
  output.SetChildCardinality(batch);
  g.produced_rows.fetch_add(batch, std::memory_order_relaxed);
  local_emitted += batch;
  return true;
}

}  // namespace sdb::connector
