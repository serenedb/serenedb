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
#include <iresearch/utils/string.hpp>
#include <span>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/full_scanner.h"
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

  if ((state->count_only = IsCountOnlyScan(bind_data, input)) &&
      !ss.stored_filter) {
    return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
  }
  state->filter = ss.stored_filter ? ss.stored_filter.get() : &MatchAllFilter();
  state->queries.resize(ss.snapshot->reader.size());

  if (state->count_only) {
    return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
  }

  if (ss.text_scorer) {
    state->scorer_obj = catalog::MakeScorer(*ss.text_scorer);
  }
  state->collectors.resize(state->MaxThreads());

  if (ss.score_top_k && ss.text_scorer) {
    state->parallel_topk = true;
  }

  ClassifyColumnstoreProjections(*state, bind_data);

  if (state->BulkChunkEligible() && !ss.text_scorer && !state->parallel_topk) {
    uint64_t rg_rows = bind_data.inverted_index->GetOptions().row_group_size;
    if (rg_rows == 0) {
      rg_rows = DEFAULT_ROW_GROUP_SIZE;
    }
    const uint64_t unit_rows = rg_rows >= DEFAULT_ROW_GROUP_SIZE
                                 ? rg_rows
                                 : DEFAULT_ROW_GROUP_SIZE / rg_rows * rg_rows;
    for (uint32_t si = 0; si < state->total_segments; ++si) {
      const auto& seg = (*state->reader)[si];
      const uint64_t docs = seg.docs_count();
      if (docs == 0) {
        continue;
      }
      if (seg.live_docs_count() != docs) {
        state->scan_units.push_back({si, 0, docs, /*bulk=*/false});
        continue;
      }
      for (uint64_t begin = 0; begin < docs; begin += unit_rows) {
        state->scan_units.push_back(
          {si, begin, std::min(unit_rows, docs - begin), /*bulk=*/true});
      }
    }
  }

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

void PreparePhase(duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
                  CommonScanLocalState& l) {
  for (;;) {
    const auto seg = g.prepare_segment.fetch_add(1, std::memory_order_relaxed);
    if (seg >= g.total_segments) {
      break;
    }
    SDB_ASSERT((*g.reader)[seg].live_docs_count() != 0);
    EnsureSegmentQuery(g, l, (*g.reader)[seg], seg);
    if (g.prepare_count.fetch_add(1, std::memory_order_acq_rel) + 1 ==
        g.total_segments) {
      const uint32_t used = g.collector_slots.load(std::memory_order_relaxed);
      auto& merged = *g.collectors[0];
      merged.MergeAll([&](irs::PrepareCollector::MergeSink sink) {
        for (uint32_t i = 1; i < used; ++i) {
          sink(*g.collectors[i]);
        }
      });
      g.stats.emplace(merged.Finish(irs::IResourceManager::gNoop));
      g.prepare_finished.Notify();
      return;
    }
  }
  g.prepare_finished.WaitForNotification();
}

void WriteChunkOffsets(std::vector<FieldEntry>& offsets_entries,
                       uint32_t& offsets_prepped_seg,
                       std::vector<highlight::HitRange>& offsets_doc_scratch,
                       const SearchFullScanGlobalState& g,
                       const HitsChunk& view, duckdb::DataChunk& output) {
  if (offsets_entries.empty()) {
    return;
  }
  for (const auto& entry : offsets_entries) {
    auto& list_vec = output.data[entry.output_idx];
    list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::ListVector::SetListSize(list_vec, 0);
    auto& child = duckdb::ListVector::GetChildMutable(list_vec);
    child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  }
  const irs::IndexReader& reader = *g.reader;
  const irs::SubReader* cached_seg = nullptr;
  for (size_t i = 0; i < view.size(); ++i) {
    const uint32_t seg_idx = view.seg(i);
    if (seg_idx != offsets_prepped_seg) {
      for (auto& entry : offsets_entries) {
        entry.state.Clear();
      }
      cached_seg = &reader[seg_idx];
      OffsetsCollector visitor{offsets_entries};
      const auto& seg_query = g.queries[seg_idx];
      SDB_ASSERT(seg_query);
      seg_query->Visit(visitor, irs::kNoBoost);
      offsets_prepped_seg = seg_idx;
    }
    for (auto& entry : offsets_entries) {
      FillRowOffsets(entry.state, *cached_seg, view.docs[i], entry.limit,
                     offsets_doc_scratch);
      WriteRowOffsets(output.data[entry.output_idx],
                      static_cast<duckdb::idx_t>(i), offsets_doc_scratch);
    }
  }
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
    const size_t k = *bd.scan_source->Cast<SearchScan>().score_top_k;
    lstate->hit_buf.resize(irs::BlockSize(k));
    lstate->hit_slice = std::span<irs::ScoreDoc>{lstate->hit_buf};
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

void SearchFullScanTopKLocalState::OnSegment(duckdb::ClientContext& ctx,
                                             const irs::SubReader& seg,
                                             uint32_t seg_idx,
                                             CommonScanGlobalState& g_) {
  auto& g = g_.Cast<SearchFullScanGlobalState>();
  auto& search = bind_data->scan_source->Cast<SearchScan>();
  if (!collector) {
    const size_t k = *search.score_top_k;
    collector.emplace(local_threshold, k, hit_slice);
  }

  score_fetcher.Clear();
  collector->SetSegment(seg_idx);

  const auto seen_global = g.global_kth_score.load(std::memory_order_relaxed);
  if (seen_global > local_threshold) {
    local_threshold = seen_global;
  }

  const auto& seg_query = EnsureSegmentQuery(g, *this, seg, seg_idx);
  const bool wand_enabled =
    WandEnabled(bind_data->inverted_index.get(), search.text_scorer);
  auto it =
    seg.mask(seg_query.Execute({.wand = {.wand_enabled = wand_enabled}},
                               g.stats ? *g.stats : irs::StatsBuffer::Empty()));
  auto score_func = it->PrepareScore({
    .scorer = g.scorer_obj.get(),
    .segment = &seg,
    .fetcher = &score_fetcher,
  });
  if (auto* it_threshold = irs::GetMutable<irs::ScoreThresholdAttr>(it.get())) {
    collector->SetScoreThreshold(it_threshold->value);
  }
  it->Collect(score_func, score_fetcher, *collector);
  collector->SetScoreThreshold(local_threshold);

  const irs::score_t kth = local_threshold;
  auto cur = g.global_kth_score.load(std::memory_order_relaxed);
  while (kth > cur && !g.global_kth_score.compare_exchange_weak(
                        cur, kth, std::memory_order_relaxed)) {
  }
}

bool SearchFullScanTopKLocalState::OnSegmentsExhausted(
  duckdb::ClientContext& ctx, CommonScanGlobalState& g_,
  duckdb::DataChunk& output) {
  auto& g = g_.Cast<SearchFullScanGlobalState>();
  if (!_prepared) {
    PrepareEmitBuffer(ctx, g);
  }
  return EmitBufferedScoreDocs(ctx, g, *this, top_hits, current_idx, output);
}

void SearchFullScanTopKLocalState::PrepareEmitBuffer(
  duckdb::ClientContext& /*ctx*/, SearchFullScanGlobalState& g) {
  _prepared = true;
  if (!collector) {
    return;
  }

  const size_t accepted = collector->AcceptedCount();
  auto accepted_slice = hit_slice.subspan(0, accepted);
  SortScoreDocsBySegDoc(accepted_slice);
  top_hits = accepted_slice;
}

void SearchFullScanScanLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                                const irs::SubReader& seg,
                                                uint32_t seg_idx,
                                                SearchFullScanGlobalState& g) {
  current_seg_idx = seg_idx;
  const bool bulk =
    g.BulkChunkEligible() && seg.live_docs_count() == seg.docs_count();
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
  if (g.has_external_projections && !PkColumnFor(*g.reader, seg_idx).second) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("inverted-index segment has no stored PK column but the query "
              "needs to map hits back to source rows"));
  }
  if (g.scan_score) {
    score_fetcher.Clear();
    streaming_score_function = streaming_doc->PrepareScore({
      .scorer = g.scorer_obj.get(),
      .segment = &seg,
      .fetcher = &score_fetcher,
    });
  }
  if (!hit_batcher) {
    hit_batcher = std::make_unique<HitBatcher>(
      g.cs_projections, g.has_external_projections, g.scan_score);
  }
  hit_batcher->BeginSegment(seg_idx, seg.GetColReader(), g.client_context);
}

void SearchFullScanScanLocalState::StartUnit(
  duckdb::ClientContext& ctx, const SearchFullScanGlobalState::ScanUnit& unit,
  SearchFullScanGlobalState& g) {
  if (unit.bulk) {
    current_seg_idx = unit.seg;
    bulk_doc_in_seg = unit.begin;
    bulk_seg_doc_count = unit.begin + unit.count;
    streaming_doc.reset();
    return;
  }
  StartSegment(ctx, (*g.reader)[unit.seg], unit.seg, g);
}

void SearchFullScanScanLocalState::PushHits(SearchFullScanGlobalState& g) {
  if (!streaming_doc) {
    if (!hit_batcher->Empty()) {
      hit_batcher->Finalize();
    }
    return;
  }
  if (!irs::doc_limits::valid(streaming_doc->value())) {
    streaming_doc->advance();
  }
  for (;;) {
    const auto doc = streaming_doc->value();
    if (irs::doc_limits::eof(doc)) {
      streaming_score_function = {};
      streaming_doc.reset();
      if (!hit_batcher->Ready() && !hit_batcher->Empty()) {
        hit_batcher->Finalize();
      }
      return;
    }
    const auto span = hit_batcher->OpenWindow(doc - irs::doc_limits::min());
    if (span == 0) {
      return;
    }
    if (g.scan_score) {
      const auto n = streaming_doc->EmitScoredDocs(
        hit_batcher->WindowHead(), hit_batcher->ScoreHead(), doc + span,
        streaming_score_function, &score_fetcher, doc);
      hit_batcher->CommitWindow(n);
    } else {
      const auto n =
        streaming_doc->EmitDocs(hit_batcher->WindowHead(), doc + span);
      hit_batcher->CommitWindow(n);
    }
  }
}

duckdb::idx_t SearchFullScanScanLocalState::EmitChunk(
  duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
  duckdb::DataChunk& output) {
  if (bulk_doc_in_seg < bulk_seg_doc_count) {
    auto* scanner =
      GetOrOpenSegmentFullScanner(*this, g, *g.reader, current_seg_idx);
    SDB_ENSURE(scanner, sdb::ERROR_INTERNAL,
               "bulk cs scan: segment has no columnstore reader");
    const auto take = std::min<duckdb::idx_t>(
      STANDARD_VECTOR_SIZE, bulk_seg_doc_count - bulk_doc_in_seg);
    scanner->Scan(bulk_doc_in_seg, take, output);
    AccountAndWriteVirtualColumns(g, take, {}, output);
    bulk_doc_in_seg += take;
    return take;
  }
  if (!hit_batcher || (!streaming_doc && hit_batcher->Empty())) {
    return 0;
  }
  if (!hit_batcher->Ready()) {
    PushHits(g);
    if (!hit_batcher->Ready()) {
      return 0;
    }
  }
  SDB_IF_FAILURE("SearchLookupFault") {
    if (g.has_external_projections) {
      SDB_THROW(ERROR_DEBUG);
    }
  }
  return EmitReadyBatch(ctx, g, *this, output);
}

void SearchFullScanCountLocalState::OnSegment(duckdb::ClientContext& /*ctx*/,
                                              const irs::SubReader& seg,
                                              uint32_t seg_idx,
                                              CommonScanGlobalState& g_) {
  auto& g = g_.Cast<SearchFullScanGlobalState>();
  const auto& seg_query = EnsureSegmentQuery(g, *this, seg, seg_idx);
  auto doc = seg.mask(seg_query.Execute({}, irs::StatsBuffer::Empty()));
  local_count += doc->count();
}

bool SearchFullScanCountLocalState::OnSegmentsExhausted(
  duckdb::ClientContext& /*ctx*/, CommonScanGlobalState& g_,
  duckdb::DataChunk& output) {
  auto& g = g_.Cast<SearchFullScanGlobalState>();
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

void SearchFullScanScanLocalState::EmitRowOffsets(CommonScanGlobalState& g_,
                                                  const HitsChunk& view,
                                                  duckdb::DataChunk& output) {
  WriteChunkOffsets(offsets_entries, offsets_prepped_seg, offsets_doc_scratch,
                    g_.Cast<SearchFullScanGlobalState>(), view, output);
}

void SearchFullScanTopKLocalState::EmitRowOffsets(CommonScanGlobalState& g_,
                                                  const HitsChunk& view,
                                                  duckdb::DataChunk& output) {
  WriteChunkOffsets(offsets_entries, offsets_prepped_seg, offsets_doc_scratch,
                    g_.Cast<SearchFullScanGlobalState>(), view, output);
}

void RunStreamingScan(duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
                      SearchFullScanScanLocalState& l,
                      duckdb::DataChunk& output) {
  for (;;) {
    const auto added = l.EmitChunk(ctx, g, output);
    SDB_ASSERT(added <= STANDARD_VECTOR_SIZE);
    if (added != 0) {
      FinalizeBatch(ctx, g, l, output, added);
      output.SetChildCardinality(added);
      return;
    }
    if (!g.scan_units.empty()) {
      const auto ui = g.next_unit.fetch_add(1, std::memory_order_relaxed);
      if (ui >= g.scan_units.size()) {
        break;
      }
      l.StartUnit(ctx, g.scan_units[ui], g);
      continue;
    }
    const auto seg_idx = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (seg_idx >= g.total_segments) {
      break;
    }
    const auto& seg = (*g.reader)[seg_idx];
    SDB_ASSERT(seg.live_docs_count() != 0);
    l.StartSegment(ctx, seg, seg_idx, g);
  }
  output.SetChildCardinality(0);
}

}  // namespace sdb::connector
