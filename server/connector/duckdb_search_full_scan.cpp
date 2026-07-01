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
#include <iresearch/search/automaton_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/filter_visitor.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/utils/automaton_utils.hpp>
#include <iresearch/utils/string.hpp>
#include <ranges>
#include <span>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
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

  if (ss.TsDictMode()) {
    state->count_only = false;
    state->ts_dict_mode = true;
    if (absl::c_any_of(state->projected_columns, [](auto p) {
          return p != duckdb::DConstants::INVALID_INDEX;
        })) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("ts_dict_agg() cannot be combined with other table columns"));
    }
    return state;
  }

  // Count-only with no filter reads live_docs_count() directly and skips the
  // per-segment prepare entirely (see SearchFullScanInitLocal).
  if ((state->count_only = ss.count_only) && !ss.stored_filter) {
    return state;
  }
  if (ss.text_scorer) {
    state->scorer_obj = catalog::MakeScorer(*ss.text_scorer);
  }
  state->filter = ss.stored_filter ? ss.stored_filter.get() : &MatchAllFilter();
  state->queries.resize(ss.snapshot->reader.size());
  state->collectors.resize(state->MaxThreads());

  if (state->count_only) {
    return state;
  }

  if (ss.score_top_k && ss.text_scorer) {
    state->parallel_topk = true;
  }

  ClassifyColumnstoreProjections(*state, bind_data);

  return state;
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

void BuildTsDictSlots(TsDictLocalState& lstate,
                      duckdb::TableFunctionInitInput& input,
                      const SereneDBScanBindData& bd, const SearchScan& ss) {
  // Each kind of term-dict virtual column carries the same sentinel catalog id,
  // so the k-th column of a kind (in append/bind order) belongs to the k-th
  // field request that asked for that kind.
  using duckdb::DConstants;
  using Req = SearchScan::TsDictRequest;
  using Field = TsDictLocalState::FieldState;

  const auto assign = [&](catalog::Column::Id cat, auto req, auto slot) {
    size_t i = 0;
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
      if (bd.column_ids[col_id] == cat) {
        while (ss.ts_dicts[i].*req == DConstants::INVALID_INDEX) {
          ++i;
        }
        lstate.fields[i++].*slot = out_slot;
      }
      ++out_slot;
    }
  };

  assign(catalog::Column::kInvertedIndexTermId, &Req::term_col_idx,
         &Field::term_slot);
  assign(catalog::Column::kInvertedIndexTermRawId, &Req::term_raw_col_idx,
         &Field::term_raw_slot);
  assign(catalog::Column::kInvertedIndexTermCountId, &Req::count_col_idx,
         &Field::count_slot);
  assign(catalog::Column::kInvertedIndexTermFreqId, &Req::freq_col_idx,
         &Field::freq_slot);
  assign(catalog::Column::kInvertedIndexTermScoreId, &Req::score_col_idx,
         &Field::score_slot);
}

}  // namespace

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchFullScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchFullScanGlobalState>();
  const auto& bd = input.bind_data->Cast<SereneDBScanBindData>();
  if (gstate.TsDictMode()) {
    auto lstate = duckdb::make_uniq<TsDictLocalState>();
    const auto& ss = bd.scan_source->Cast<SearchScan>();
    SDB_ASSERT(!ss.stored_filter || ss.ts_dicts.size() == 1);
    lstate->fields.resize(ss.ts_dicts.size());
    lstate->term_filter = ss.stored_filter.get();
    for (size_t i = 0; i < ss.ts_dicts.size(); ++i) {
      lstate->fields[i].field_id = ss.ts_dicts[i].field_id;
      lstate->fields[i].term_uses = ss.ts_dicts[i].term_uses;
    }
    BuildTsDictSlots(*lstate, input, bd, ss);
    return lstate;
  }
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
  if (gstate.TsDictMode()) {
    auto& l = data.local_state->Cast<TsDictLocalState>();
    RunStreamingScan(context, gstate, l, output);
  } else if (gstate.count_only) {
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
                                             SearchFullScanGlobalState& g) {
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
  if (!collector) {
    return;  // no segments claimed by this thread
  }

  const size_t accepted = collector->AcceptedCount();
  auto accepted_slice = hit_slice.subspan(0, accepted);
  absl::c_sort(
    accepted_slice, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
      return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
    });
  top_hits = hit_slice.subspan(0, accepted);
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

void TsDictLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                    const irs::SubReader& seg,
                                    uint32_t /*seg_idx*/,
                                    SearchFullScanGlobalState& /*g*/) {
  _seg = &seg;
  _dirty = seg.live_docs_count() != seg.docs_count();
  _cur_field = 0;
  if (!fields.empty()) {
    OpenField(0);
  }
}

void TsDictLocalState::OpenField(size_t field_idx) {
  const auto& f = fields[field_idx];
  const auto* reader = _seg->field(f.field_id);
  if (!reader || reader->size() == 0) {
    return;
  }

  const auto claim = [&]<typename F, typename W>() {
    const auto& filter = basics::downCast<F>(*term_filter);
    if (filter.field_id() == f.field_id) {
      _cursor.emplace<W>(*reader, filter.options());
    }
  };

  if (!term_filter) {
    if (f.term_uses == SearchScan::TsDictRequest::kUseMax && !_dirty) {
      irs::ByTermOptions options;
      const auto max_term = reader->max();
      options.term.assign(max_term.data(), max_term.size());
      _cursor.emplace<irs::ByTermIterator>(*reader, options);
    } else {
      _cursor.emplace<irs::AllTermIterator>(*reader);
    }
  } else if (const auto type = term_filter->type();
             type == irs::Type<irs::ByTerm>::id()) {
    claim.operator()<irs::ByTerm, irs::ByTermIterator>();
  } else if (type == irs::Type<irs::ByPrefix>::id()) {
    claim.operator()<irs::ByPrefix, irs::ByPrefixIterator>();
  } else if (type == irs::Type<irs::ByRange>::id()) {
    claim.operator()<irs::ByRange, irs::ByRangeIterator>();
  } else if (type == irs::Type<irs::ByTerms>::id()) {
    claim.operator()<irs::ByTerms, irs::ByTermsIterator>();
  } else if (type == irs::Type<irs::LevenshteinAutomatonFilter>::id()) {
    claim
      .operator()<irs::LevenshteinAutomatonFilter, irs::LevenshteinIterator>();
  } else if (type == irs::Type<irs::AutomatonFilter>::id()) {
    claim.operator()<irs::AutomatonFilter, irs::AllTermIterator>();
  }
}

duckdb::idx_t TsDictLocalState::EmitField(duckdb::DataChunk& output,
                                          duckdb::idx_t output_start,
                                          duckdb::idx_t budget) {
  using duckdb::DConstants;

  const auto& f = fields[_cur_field];
  const auto vec = [&](duckdb::idx_t slot) -> duckdb::Vector* {
    return slot == DConstants::INVALID_INDEX ? nullptr : &output.data[slot];
  };
  const auto data = [&]<typename T>(duckdb::idx_t slot) -> T* {
    auto* v = vec(slot);
    return v ? duckdb::FlatVector::GetDataMutable<T>(*v) : nullptr;
  };

  auto* term_vec = vec(f.term_slot);
  auto* raw_vec = vec(f.term_raw_slot);
  auto* term_data = data.operator()<duckdb::string_t>(f.term_slot);
  auto* raw_data = data.operator()<duckdb::string_t>(f.term_raw_slot);
  auto* count_data = data.operator()<int32_t>(f.count_slot);
  auto* freq_data = data.operator()<int64_t>(f.freq_slot);
  auto* score_data = data.operator()<float>(f.score_slot);

  const bool min_only = f.term_uses == SearchScan::TsDictRequest::kUseMin;
  const auto field_budget = min_only ? duckdb::idx_t{1} : budget;

  const auto n = std::visit(
    [&](auto& cur) -> duckdb::idx_t {
      const auto* meta = [&]() -> const irs::TermMeta* {
        if ((!count_data && !freq_data) || !cur.Valid()) {
          return nullptr;
        }
        const auto* meta = irs::get<irs::TermMeta>(cur.GetImpl());
        SDB_ENSURE(meta, sdb::ERROR_INTERNAL,
                   "ts_dict: term iterator has no term_meta");
        return meta;
      }();
      duckdb::idx_t count = 0;
      while (count < field_budget && cur.Valid()) {
        if (meta || _dirty) {
          cur.read();
        }
        uint32_t live_docs = 0;
        if (_dirty) {
          auto docs =
            _seg->mask(cur.GetImpl().postings(irs::IndexFeatures::None));
          if (count_data) {
            live_docs = docs->count();
          } else if (!irs::doc_limits::eof(docs->advance())) {
            live_docs = 1;
          }
          if (live_docs == 0) {
            cur.next();
            continue;
          }
        }
        const auto term = cur.value();
        const auto row = output_start + count;
        const auto* p = reinterpret_cast<const char*>(term.data());
        if (term_data) {
          term_data[row] =
            duckdb::StringVector::AddString(*term_vec, p, term.size());
        }
        if (raw_data) {
          raw_data[row] =
            duckdb::StringVector::AddStringOrBlob(*raw_vec, p, term.size());
        }
        if (count_data) {
          count_data[row] =
            static_cast<int32_t>(_dirty ? live_docs : meta->docs_count);
        }
        if (freq_data) {
          freq_data[row] = static_cast<int64_t>(meta->freq);
        }
        if (score_data) {
          score_data[row] = cur.Boost();
        }
        ++count;
        cur.next();
      }
      return count;
    },
    _cursor);

  if (min_only && n != 0) {
    _cursor.emplace<irs::AllTermIterator>();
  }
  if (n == 0) {
    return 0;
  }

  // The other fields' columns are NULL on rows produced for this field.
  for (size_t fi = 0; fi < fields.size(); ++fi) {
    if (fi == _cur_field) {
      continue;
    }
    const auto& of = fields[fi];
    for (const auto slot : {of.term_slot, of.term_raw_slot, of.count_slot,
                            of.freq_slot, of.score_slot}) {
      if (slot == DConstants::INVALID_INDEX) {
        continue;
      }
      auto& validity = duckdb::FlatVector::ValidityMutable(output.data[slot]);
      for (duckdb::idx_t i = 0; i < n; ++i) {
        validity.SetInvalid(output_start + i);
      }
    }
  }
  return n;
}

duckdb::idx_t TsDictLocalState::EmitChunk(duckdb::ClientContext& /*ctx*/,
                                          SearchFullScanGlobalState& g,
                                          duckdb::DataChunk& output,
                                          duckdb::idx_t output_start) {
  if (!_seg) {
    return 0;
  }
  const auto budget = STANDARD_VECTOR_SIZE - output_start;
  while (_cur_field < fields.size()) {
    if (const auto n = EmitField(output, output_start, budget); n != 0) {
      g.produced_rows.fetch_add(n, std::memory_order_relaxed);
      return n;
    }
    ++_cur_field;
    if (_cur_field < fields.size()) {
      OpenField(_cur_field);
    }
  }
  return 0;
}

}  // namespace sdb::connector
