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
#include <iresearch/index/index_source.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/automaton_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/conjunction.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/filter_visitor.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/vector_similarity_query.hpp>
#include <iresearch/search/vector_similarity_scorer.hpp>
#include <iresearch/utils/automaton_utils.hpp>
#include <iresearch/utils/string.hpp>
#include <ranges>
#include <span>
#include <type_traits>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/full_scanner.h"
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

uint32_t ReadRerankFactor(duckdb::ClientContext& context) {
  return ReadBoundedIntSetting(context, "sdb_rerank_factor", 0, 4);
}

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
    if (ss.stored_filter) {
      state->filter = ss.stored_filter.get();
      state->queries.resize(ss.snapshot->reader.size());
    }
    return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
  }

  // Count-only never coexists with pushed filters: filter columns stay
  // projected (filter_prune off), so IsCountOnlyScan rejects such plans.
  if ((state->count_only = IsCountOnlyScan(bind_data, input)) &&
      !ss.stored_filter && !ss.vector_scorer) {
    return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
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

  if (state->count_only) {
    return state;
  }

  if (ss.text_scorer) {
    state->scorer_obj = catalog::MakeScorer(*ss.text_scorer);
  } else if (ss.score_order) {
    state->scorer_obj = std::make_unique<irs::VectorSimilarityScorer>();
  }
  state->collectors.resize(state->MaxThreads());

  // Top-k enforces .col filters in the collector before candidates count
  // toward k. Filters that can only run post-materialize -- over virtual
  // output slots (row_expr) or over lookup columns (HasRowFetch,
  // FilterLookupColumns) -- demote to streaming, which applies them there.
  if (ss.score_top_k && (ss.text_scorer || ss.score_order) &&
      !state->row_expr && !state->table_filter.HasRowFetch()) {
    state->parallel_topk = true;
    if (ss.vector_scorer &&
        ss.vector_scorer->quant != irs::VectorQuantization::None) {
      state->rerank_pool =
        ReadRerankFactor(context) * static_cast<uint32_t>(*ss.score_top_k);
    }
  }
  if (ss.score_order && *ss.score_order == duckdb::OrderType::ASCENDING) {
    state->global_kth_score.store(std::numeric_limits<irs::score_t>::max(),
                                  std::memory_order_relaxed);
  }

  ClassifyColumnstoreProjections(*state, bind_data);

  if (state->BulkChunkEligible() && !ss.text_scorer && !state->parallel_topk &&
      state->table_filter.Empty() && !state->row_expr) {
    // Search tables carry no inverted_index; fall back to the default unit.
    uint64_t rg_rows = bind_data.inverted_index
                         ? bind_data.inverted_index->GetOptions().row_group_size
                         : 0;
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
      entry.id = ss.offsets[ss_idx].column_id;
      lstate.offsets_entries.push_back(std::move(entry));
    }
    ++out_slot;
  }
}

uint32_t WhereLiveDocs(irs::TermIterator& it, const irs::SubReader& seg,
                       const irs::QueryBuilder& where, bool count_all) {
  std::vector<irs::ScoreAdapter> itrs;
  itrs.reserve(2);
  itrs.emplace_back(it.postings(irs::IndexFeatures::None));
  itrs.emplace_back(where.Execute({}, irs::StatsBuffer::Empty()));
  auto docs = irs::MakeConjunction(irs::ScoreMergeType::Noop, {},
                                   seg.docs_count(), std::move(itrs));
  if (count_all) {
    return static_cast<uint32_t>(docs->count());
  }
  return irs::doc_limits::eof(docs->advance()) ? 0 : 1;
}

uint32_t MaskedLiveDocs(irs::TermIterator& it, const irs::SubReader& seg,
                        bool count_all) {
  auto docs = seg.mask(it.postings(irs::IndexFeatures::None));
  if (count_all) {
    return static_cast<uint32_t>(docs->count());
  }
  return irs::doc_limits::eof(docs->advance()) ? 0 : 1;
}

class MinMaxTermsIterator : public irs::TermIterator {
 public:
  MinMaxTermsIterator(const std::array<irs::bytes_view, 2>& terms,
                      size_t count) noexcept
    : _terms{terms}, _count{count} {}

  bool next() final {
    if (_next == _count) {
      return false;
    }
    ++_next;
    return true;
  }

  irs::bytes_view value() const noexcept final { return _terms[_next - 1]; }

  void read() final {}

  irs::DocIterator::ptr postings(irs::IndexFeatures /*features*/) const final {
    return irs::DocIterator::empty();
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id /*id*/) noexcept final {
    return nullptr;
  }

 private:
  std::array<irs::bytes_view, 2> _terms;
  size_t _count;
  size_t _next = 0;
};

uint32_t NullFieldLiveCount(const irs::SubReader& seg, irs::field_id field,
                            TsDictLocalState::CountMode count_mode,
                            const irs::QueryBuilder* where_query) {
  using Mode = TsDictLocalState::CountMode;
  const auto* reader = seg.field(field);
  if (!reader) {
    return 0;
  }
  if (count_mode == Mode::kMeta && seg.live_docs_count() == seg.docs_count()) {
    return static_cast<uint32_t>(reader->docs_count());
  }
  auto it = reader->iterator(irs::SeekMode::NORMAL);
  SDB_ASSERT(it);
  if (!it->next()) {
    return 0;
  }
  const auto live = count_mode == Mode::kWhere
                      ? WhereLiveDocs(*it, seg, *where_query, true)
                      : MaskedLiveDocs(*it, seg, true);
  SDB_ASSERT(!it->next());
  return static_cast<uint32_t>(live);
}

void BuildTsDictSlots(TsDictLocalState& lstate,
                      duckdb::TableFunctionInitInput& input,
                      const SereneDBScanBindData& bd, const SearchScan& ss) {
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
  duckdb::ExecutionContext& /*context*/, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchFullScanGlobalState>();
  const auto& bd = input.bind_data->Cast<SereneDBScanBindData>();
  if (gstate.ts_dict_mode) {
    auto lstate = duckdb::make_uniq<TsDictLocalState>();
    const auto& ss = bd.scan_source->Cast<SearchScan>();
    lstate->fields.resize(ss.ts_dicts.size());
    for (size_t i = 0; i < ss.ts_dicts.size(); ++i) {
      lstate->fields[i].field_id = ss.ts_dicts[i].field_id;
      lstate->fields[i].null_field_id = ss.ts_dicts[i].null_field_id;
      lstate->fields[i].term_uses = ss.ts_dicts[i].term_uses;
      lstate->fields[i].having_filter = ss.ts_dicts[i].having_filter.get();
    }
    BuildTsDictSlots(*lstate, input, bd, ss);
    return lstate;
  }
  if (gstate.count_only) {
    auto lstate = duckdb::make_uniq<SearchFullScanCountLocalState>();
    InitLocalFilter(*lstate, gstate);
    if (gstate.queries.empty()) {
      lstate->local_count = gstate.reader->live_docs_count();
      lstate->segments_exhausted = true;
    }
    return lstate;
  }
  if (gstate.parallel_topk) {
    auto lstate = duckdb::make_uniq<SearchFullScanTopKLocalState>();
    lstate->bind_data = &bd;
    InitLocalFilter(*lstate, gstate);
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
  InitLocalFilter(*lstate, gstate);
  BuildOffsetsEntries(*lstate, input, bd);
  return lstate;
}

void SearchFullScanFunction(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchFullScanGlobalState>();
  if (gstate.ts_dict_mode) {
    auto& l = data.local_state->Cast<TsDictLocalState>();
    RunStreamingScan(context, gstate, l, output);
  } else if (gstate.count_only) {
    auto& l = data.local_state->Cast<SearchFullScanCountLocalState>();
    RunCollectThenEmitScan(context, gstate, l, output);
  } else if (gstate.parallel_topk) {
    auto& l = data.local_state->Cast<SearchFullScanTopKLocalState>();
    if (!l.prepared) {
      if (gstate.total_segments != 0 && !gstate.vector_scorer) {
        PreparePhase(context, gstate, l);
      }
      l.prepared = true;
    }
    RunCollectThenEmitScan(context, gstate, l, output);
  } else {
    auto& l = data.local_state->Cast<SearchFullScanScanLocalState>();
    if (!l.prepared) {
      if (gstate.scorer_obj && gstate.total_segments != 0 &&
          !gstate.vector_scorer) {
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

template<irs::Order O, bool Filtered>
void CollectSegmentTopK([[maybe_unused]] duckdb::ClientContext& ctx,
                        SearchFullScanTopKLocalState& s,
                        const irs::SubReader& seg, uint32_t seg_idx,
                        SearchFullScanGlobalState& g) {
  using C = irs::NthPartitionScoreCollectorT<O, Filtered>;
  [[maybe_unused]] auto verdict =
    duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE;
  if constexpr (Filtered) {
    verdict = s.filter.StartSegment(ctx, seg);
    if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE) {
      return;
    }
  }
  auto& search = s.bind_data->scan_source->Cast<SearchScan>();
  if (!std::holds_alternative<C>(s.collector)) {
    const size_t k = g.rerank_pool ? g.rerank_pool : *search.score_top_k;
    s.collector.template emplace<C>(s.local_threshold, k, s.hit_slice);
  }
  auto& collector = std::get<C>(s.collector);

  s.score_fetcher.Clear();
  if constexpr (Filtered) {
    collector.SetFilter(
      verdict == duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE ? &s.filter
                                                                    : nullptr);
  }
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
  const irs::StatsBuffer& stats =
    g.stats ? *g.stats : irs::StatsBuffer::Empty();

  bool done = false;
  if constexpr (!Filtered) {
    if (seg.docs_mask() == nullptr &&
        seg_query.CollectTopK(collector, {.wand = {.wand_enabled = false}},
                              stats)) {
      collector.SetScoreThreshold(s.local_threshold);
      done = true;
    }
  }
  if (!done) {
    const bool wand_enabled =
      WandEnabled(s.bind_data->inverted_index.get(), search.text_scorer);
    auto it = seg.mask(
      seg_query.Execute({.wand = {.wand_enabled = wand_enabled}}, stats));
    auto score_func = it->PrepareScore({
      .scorer = g.scorer_obj.get(),
      .segment = &seg,
      .fetcher = &s.score_fetcher,
    });
    if (auto* it_threshold =
          irs::GetMutable<irs::ScoreThresholdAttr>(it.get())) {
      collector.SetScoreThreshold(it_threshold->value);
    }
    it->Collect(score_func, s.score_fetcher, collector);
    collector.SetScoreThreshold(s.local_threshold);
  }

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

void SearchFullScanTopKLocalState::OnSegment(duckdb::ClientContext& ctx,
                                             const irs::SubReader& seg,
                                             uint32_t seg_idx,
                                             CommonScanGlobalState& g_) {
  auto& g = g_.Cast<SearchFullScanGlobalState>();
  auto& search = bind_data->scan_source->Cast<SearchScan>();
  const bool ascending =
    search.score_order && *search.score_order == duckdb::OrderType::ASCENDING;
  const bool filtered = !g.table_filter.Empty();
  if (filtered) {
    if (ascending) {
      CollectSegmentTopK<irs::Order::ASC, true>(ctx, *this, seg, seg_idx, g);
    } else {
      CollectSegmentTopK<irs::Order::DESC, true>(ctx, *this, seg, seg_idx, g);
    }
  } else {
    if (ascending) {
      CollectSegmentTopK<irs::Order::ASC, false>(ctx, *this, seg, seg_idx, g);
    } else {
      CollectSegmentTopK<irs::Order::DESC, false>(ctx, *this, seg, seg_idx, g);
    }
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
  size_t kept = accepted;
  if (g.rerank_pool > 0 && g.vector_scorer != nullptr) {
    SortScoreDocsBySegDoc(accepted_slice);
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
  SortScoreDocsBySegDoc(accepted_slice);
  top_hits = hit_slice.subspan(0, kept);
}

void SearchFullScanScanLocalState::StartSegment(duckdb::ClientContext& ctx,
                                                const irs::SubReader& seg,
                                                uint32_t seg_idx,
                                                SearchFullScanGlobalState& g) {
  current_seg_idx = seg_idx;
  bulk_doc_in_seg = 0;
  bulk_seg_doc_count = 0;
  chunk_hits.clear();
  chunk_scores.clear();
  chunk_cursor = 0;
  if (filter.StartSegment(ctx, seg) ==
      duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE) {
    streaming_doc.reset();
    return;
  }
  // A pushed filter (column, row-fetch, or row_expr) rules out the bulk
  // columnstore fast path: bulk emits every live row untouched.
  const bool bulk = g.BulkChunkEligible() &&
                    seg.live_docs_count() == seg.docs_count() &&
                    g.table_filter.Empty() && !g.row_expr;
  if (bulk) {
    bulk_seg_doc_count = seg.docs_count();
    streaming_doc.reset();
    return;
  }
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
      g.cs_projections,
      g.has_external_projections ? catalog::term_dict::kPKFieldId
                                 : irs::field_limits::invalid(),
      g.scan_score);
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
      const auto doc_id = filter.NextUnpruned(*streaming_doc);
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
    const auto doc_id = filter.NextUnpruned(*streaming_doc);
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
  if (!hit_batcher || (!streaming_doc && hit_batcher->Empty() &&
                       chunk_cursor == chunk_hits.size())) {
    return 0;
  }
  if (filter.Empty()) {
    // No scan filter: stream matching docs straight into the batcher.
    if (!hit_batcher->Ready()) {
      PushHits(g);
      if (!hit_batcher->Ready()) {
        return 0;
      }
    }
  } else {
    // Filter-first: collect candidate docs, run the scan filter over just the
    // filter columns, and stage only survivors into the batcher before it
    // materializes them. A fully-filtered chunk loops for the next one.
    while (!hit_batcher->Ready()) {
      while (chunk_cursor < chunk_hits.size() && !hit_batcher->Ready()) {
        const uint64_t row = chunk_hits[chunk_cursor] - irs::doc_limits::min();
        const auto span = hit_batcher->OpenWindow(row);
        if (span == 0) {
          break;
        }
        auto* const head = hit_batcher->WindowHead();
        auto* const scores = g.scan_score ? hit_batcher->ScoreHead() : nullptr;
        const uint64_t win_end = row + span;
        duckdb::idx_t k = 0;
        while (chunk_cursor < chunk_hits.size() &&
               chunk_hits[chunk_cursor] - irs::doc_limits::min() < win_end) {
          head[k] = chunk_hits[chunk_cursor];
          if (scores != nullptr) {
            scores[k] = chunk_scores[chunk_cursor];
          }
          ++k;
          ++chunk_cursor;
        }
        hit_batcher->CommitWindow(k);
      }
      if (hit_batcher->Ready()) {
        break;
      }
      if (!streaming_doc) {
        if (hit_batcher->Empty()) {
          return 0;
        }
        hit_batcher->Finalize();
        break;
      }
      AdvanceChunk(g, STANDARD_VECTOR_SIZE);
      const auto survivors = filter.FilterHits(chunk_hits, chunk_scores);
      chunk_hits.resize(survivors);
      if (!chunk_scores.empty()) {
        chunk_scores.resize(survivors);
      }
      chunk_cursor = 0;
    }
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
      // Lookup-column filters run here, post-materialize, on the emit's single
      // Materialize output (FilterSelection); compacts to survivors in place.
      const auto kept = l.filter.FilterLookupColumns(output, added);
      // Enforce row_expr (filters over virtual/extract slots) on the
      // materialized chunk; a fully-rejected batch loops for the next one.
      const auto survivors = ApplyRowFilter(ctx, g, l, output, kept);
      if (survivors != 0) {
        output.SetChildCardinality(survivors);
        return;
      }
      output.Reset();
      continue;
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

void RunStreamingScan(duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
                      TsDictLocalState& l, duckdb::DataChunk& output) {
  for (;;) {
    duckdb::idx_t collected = 0;
    bool exhausted = false;
    while (collected < STANDARD_VECTOR_SIZE) {
      const auto added = l.EmitChunk(ctx, g, output, collected);
      SDB_ASSERT(collected + added <= STANDARD_VECTOR_SIZE);
      collected += added;
      if (added != 0) {
        continue;
      }
      const auto seg_idx =
        g.next_segment.fetch_add(1, std::memory_order_relaxed);
      if (seg_idx >= g.total_segments) {
        exhausted = true;
        break;
      }
      const auto& seg = (*g.reader)[seg_idx];
      SDB_ASSERT(seg.live_docs_count() != 0);
      l.StartSegment(ctx, seg, seg_idx, g);
    }
    // Enforce row_expr (scalar predicates over the emitted term rows, e.g. a
    // pushed `body LIKE 'a%'`) that the term enumeration did not consume as an
    // acceptor. A chunk fully rejected by it loops for the next terms.
    const auto survivors = ApplyRowFilter(ctx, g, l, output, collected);
    if (survivors != 0 || exhausted) {
      output.SetChildCardinality(survivors);
      return;
    }
    output.Reset();
  }
}

void TsDictLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                    const irs::SubReader& seg, uint32_t seg_idx,
                                    SearchFullScanGlobalState& g) {
  _seg = &seg;
  _count_mode = seg.live_docs_count() != seg.docs_count() ? CountMode::kMasked
                                                          : CountMode::kMeta;
  _where_query = nullptr;
  _next_field = fields.empty() ? nullptr : fields.data();
  if (g.filter) {
    const auto& where_query = EnsureSegmentQuery(g, *this, seg, seg_idx);
    if (irs::doc_limits::eof(
          where_query.Execute({}, irs::StatsBuffer::Empty())->advance())) {
      _next_field = nullptr;
      return;
    }
    _where_query = &where_query;
    _count_mode = CountMode::kWhere;
  }
}

irs::TermIterator::ptr TsDictLocalState::MakeTermSource(
  const FieldState& field, const irs::TermReader& reader) {
  if (field.having_filter) {
    auto cursor = field.having_filter->CompileTermIterator(reader);
    SDB_ENSURE(cursor, sdb::ERROR_INTERNAL,
               "ts_dict: claimed having filter failed to compile a term "
               "iterator");
    return cursor;
  }
  const bool max_only = field.term_uses == TsDictTermUses::kMax;
  const bool min_max =
    field.term_uses == (TsDictTermUses::kMin | TsDictTermUses::kMax);
  if ((max_only || min_max) && _count_mode != CountMode::kMasked) {
    std::array<irs::bytes_view, 2> terms;
    size_t count = 0;
    const auto max = reader.max();
    if (_count_mode == CountMode::kMeta) {
      if (min_max) {
        terms[count++] = reader.min();
      }
      terms[count++] = max;
    } else {
      auto it = reader.iterator(irs::SeekMode::RandomOnly);
      const auto pin = [&](irs::bytes_view term) {
        if (!it->seek(term) ||
            WhereLiveDocs(*it, *_seg, *_where_query, false) == 0) {
          return false;
        }
        terms[count++] = term;
        return true;
      };
      if (!(min_max ? pin(reader.min()) && pin(max) : pin(max))) {
        count = 0;
      }
    }
    if (count != 0) {
      _cursor_mode = CountMode::kMeta;
      return irs::memory::make_managed<MinMaxTermsIterator>(terms, count);
    }
  }
  return reader.iterator(irs::SeekMode::NORMAL);
}

bool TsDictLocalState::NextField() {
  while (_next_field) {
    const auto& field = *_next_field++;
    if (_next_field == fields.data() + fields.size()) {
      _next_field = nullptr;
    }
    _field = &field;
    _null_pending = irs::field_limits::valid(field.null_field_id);
    _cursor_mode = _count_mode;
    _cursor.reset();
    if (const auto* reader = _seg->field(field.field_id);
        reader && reader->size() != 0) {
      _cursor = MakeTermSource(field, *reader);
    }
    if (_cursor || _null_pending) {
      return true;
    }
  }
  return false;
}

namespace {

struct TsDictEmitContext {
  duckdb::string_t* term_data;
  duckdb::string_t* raw_data;
  int32_t* count_data;
  int64_t* freq_data;
  float* score_data;
  duckdb::Vector* term_vec;
  duckdb::Vector* raw_vec;
  const irs::TermMeta* meta;
  const irs::TermBoost* boost;
  const irs::SubReader* seg;
  TsDictLocalState::CountMode count_mode;
  const irs::QueryBuilder* where_query;
  duckdb::idx_t row;
  duckdb::idx_t end_row;
};

struct TsDictEmitter {
  explicit TsDictEmitter(TsDictEmitContext ctx) noexcept : ctx{ctx} {}

  void Emit(irs::bytes_view term, uint32_t docs) {
    const auto row = ctx.row;
    const auto* p = reinterpret_cast<const char*>(term.data());
    if (ctx.term_data) {
      ctx.term_data[row] =
        duckdb::StringVector::AddString(*ctx.term_vec, p, term.size());
    }
    if (ctx.raw_data) {
      ctx.raw_data[row] =
        duckdb::StringVector::AddStringOrBlob(*ctx.raw_vec, p, term.size());
    }
    if (ctx.count_data) {
      ctx.count_data[row] = static_cast<int32_t>(docs);
    }
    if (ctx.freq_data) {
      ctx.freq_data[row] = static_cast<int64_t>(ctx.meta->freq);
    }
    if (ctx.score_data) {
      ctx.score_data[row] = ctx.boost ? ctx.boost->value : irs::kNoBoost;
    }
    ++ctx.row;
  }

  uint32_t LiveDocs(irs::TermIterator& it) const {
    using Mode = TsDictLocalState::CountMode;
    switch (ctx.count_mode) {
      case Mode::kMeta:
        return ctx.meta ? ctx.meta->docs_count : 1;
      case Mode::kMasked:
        return MaskedLiveDocs(it, *ctx.seg, ctx.count_data);
      case Mode::kWhere:
        return WhereLiveDocs(it, *ctx.seg, *ctx.where_query, ctx.count_data);
    }
    return 0;
  }

  void OnTerm(irs::TermIterator& it) {
    if (ctx.meta) {
      it.read();
    }
    const auto live_docs = LiveDocs(it);
    if (live_docs != 0) {
      Emit(it.value(), live_docs);
    }
  }

  TsDictEmitContext ctx;
};

}  // namespace

duckdb::idx_t TsDictLocalState::EmitField(duckdb::DataChunk& output,
                                          duckdb::idx_t output_start,
                                          duckdb::idx_t capacity) {
  using duckdb::DConstants;

  if (!_cursor && !_null_pending) {
    return 0;
  }
  const auto& field = *_field;
  const auto vec = [&](duckdb::idx_t slot) -> duckdb::Vector* {
    return slot == DConstants::INVALID_INDEX ? nullptr : &output.data[slot];
  };
  const auto data = [&]<typename T>(duckdb::idx_t slot) -> T* {
    auto* v = vec(slot);
    return v ? duckdb::FlatVector::GetDataMutable<T>(*v) : nullptr;
  };

  auto* term_vec = vec(field.term_slot);
  auto* raw_vec = vec(field.term_raw_slot);
  auto* term_data = data.operator()<duckdb::string_t>(field.term_slot);
  auto* raw_data = data.operator()<duckdb::string_t>(field.term_raw_slot);
  auto* count_data = data.operator()<int32_t>(field.count_slot);
  auto* freq_data = data.operator()<int64_t>(field.freq_slot);
  auto* score_data = data.operator()<float>(field.score_slot);

  const bool min_only = field.term_uses == TsDictTermUses::kMin;
  const auto field_capacity = min_only ? duckdb::idx_t{1} : capacity;

  duckdb::idx_t n = 0;
  if (_cursor && field_capacity != 0) {
    const auto* meta = [&]() -> const irs::TermMeta* {
      if (!count_data && !freq_data) {
        return nullptr;
      }
      const auto* meta = irs::get<irs::TermMeta>(*_cursor);
      SDB_ENSURE(meta, sdb::ERROR_INTERNAL,
                 "ts_dict: term iterator has no term_meta");
      return meta;
    }();
    TsDictEmitter emitter{TsDictEmitContext{
      .term_data = term_data,
      .raw_data = raw_data,
      .count_data = count_data,
      .freq_data = freq_data,
      .score_data = score_data,
      .term_vec = term_vec,
      .raw_vec = raw_vec,
      .meta = meta,
      .boost = score_data ? irs::get<irs::TermBoost>(*_cursor) : nullptr,
      .seg = _seg,
      .count_mode = _cursor_mode,
      .where_query = _where_query,
      .row = output_start,
      .end_row = output_start + field_capacity}};
    while (emitter.ctx.row < emitter.ctx.end_row) {
      if (!_cursor->next()) {
        _cursor.reset();
        break;
      }
      emitter.OnTerm(*_cursor);
    }
    n = emitter.ctx.row - output_start;
  }

  if (min_only && n != 0) {
    _cursor.reset();
  }
  if (_null_pending && n < field_capacity && !_cursor) {
    _null_pending = false;
    n += AppendNullRow(output, field, output_start + n);
  }
  if (n == 0) {
    return 0;
  }

  // The other fields' columns are NULL on rows produced for this field.
  for (const auto& other : fields) {
    if (&other == _field) {
      continue;
    }
    for (const auto slot :
         {other.term_slot, other.term_raw_slot, other.count_slot,
          other.freq_slot, other.score_slot}) {
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

duckdb::idx_t TsDictLocalState::AppendNullRow(duckdb::DataChunk& output,
                                              const FieldState& field,
                                              duckdb::idx_t row) {
  const auto nulls =
    NullFieldLiveCount(*_seg, field.null_field_id, _count_mode, _where_query);
  if (nulls == 0) {
    return 0;
  }
  const auto set_null = [&](duckdb::idx_t slot) {
    if (slot != duckdb::DConstants::INVALID_INDEX) {
      duckdb::FlatVector::SetNull(output.data[slot], row, true);
    }
  };
  set_null(field.term_slot);
  set_null(field.term_raw_slot);
  set_null(field.freq_slot);
  set_null(field.score_slot);
  if (field.count_slot != duckdb::DConstants::INVALID_INDEX) {
    duckdb::FlatVector::GetDataMutable<int32_t>(
      output.data[field.count_slot])[row] = static_cast<int32_t>(nulls);
  }
  return 1;
}

duckdb::idx_t TsDictLocalState::EmitChunk(duckdb::ClientContext& /*ctx*/,
                                          SearchFullScanGlobalState& g,
                                          duckdb::DataChunk& output,
                                          duckdb::idx_t output_start) {
  const auto capacity = STANDARD_VECTOR_SIZE - output_start;
  do {
    if (const auto n = EmitField(output, output_start, capacity); n != 0) {
      g.produced_rows.fetch_add(n, std::memory_order_relaxed);
      return n;
    }
  } while (NextField());
  return 0;
}

}  // namespace sdb::connector
