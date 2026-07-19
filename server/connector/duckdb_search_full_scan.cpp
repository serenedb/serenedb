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
#include <cmath>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/storage/table/column_segment.hpp>
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
#include <mutex>
#include <ranges>
#include <span>
#include <type_traits>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/full_scanner.h"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/search_pk_lookup.h"
#include "iresearch/index/table_filter_iterator.hpp"
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

void RerankHits(IResearchScanGlobalState& g, std::span<irs::ScoreDoc> hits) {
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

// Current lower-bound score from the dynamic TOP_N boundary, or min() when it
// is not yet initialized or is not a lower bound (text-only path: WAND scores
// are strictly positive). Seeds the streaming WAND threshold; the exact
// boundary is still enforced by the HitBatcher score filter, so an over-loose
// threshold only skips fewer blocks (never wrong). WAND pruning is strict
// (skips `block_max <= threshold`), so a `>=` boundary steps one ulp down --
// docs scoring exactly the boundary are still needed for tie-breaking.
irs::score_t CurrentWandThreshold(duckdb::DynamicFilterData& dyn) {
  std::lock_guard<duckdb::mutex> guard(dyn.lock);
  if (!dyn.initialized.load(std::memory_order_relaxed) ||
      dyn.constant.IsNull() ||
      (dyn.comparison_type != duckdb::ExpressionType::COMPARE_GREATERTHAN &&
       dyn.comparison_type !=
         duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO)) {
    return std::numeric_limits<irs::score_t>::min();
  }
  const auto boundary = dyn.constant.GetValue<float>();
  if (dyn.comparison_type ==
      duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
    return std::nextafter(boundary, std::numeric_limits<irs::score_t>::min());
  }
  return boundary;
}

// The fastest mode that can apply every pushed filter. `.col` filters run in
// every mode (codec filter + zonemap); a score filter only runs where scores
// are computed and a lookup-column filter only during source materialization,
// so count-shaped plans carrying one take Stream instead of Count. CountFast
// additionally requires no predicate and no filters at all -- the whole-reader
// live count is the answer.
ScanMode DecideScanMode(const IResearchScanGlobalState& g,
                        const SereneDBScanBindData& ss,
                        const duckdb::TableFunctionInitInput& input) {
  if (ss.TsDictMode()) {
    return ScanMode::TsDict;
  }
  const bool score_filter = absl::c_any_of(
    g.col_filters,
    [](const IResearchScanGlobalState::ColFilter& f) { return f.is_score; });
  if (!g.has_lookup_filter && !score_filter && IsCountOnlyScan(ss, input)) {
    return !ss.stored_filter && !ss.vector_scorer && g.col_filters.empty()
             ? ScanMode::CountFast
             : ScanMode::Count;
  }
  // Vector (ANN) search tolerates a lookup filter on the Top: path: the
  // collector over-fetches a pool, the lookup filter drops non-matches during
  // materialization, and the TOP_N above trims to the exact k. A text lookup
  // filter still forces streaming (exact), so it never reaches here anyway
  // (score_top_k stays unset -- AfterLimit).
  if (ss.score_top_k && (ss.text_scorer || ss.score_order) &&
      (!g.has_lookup_filter || ss.vector_scorer)) {
    return ScanMode::TopK;
  }
  return ScanMode::Stream;
}

}  // namespace

// ORDER BY <covered column> LIMIT: order segments best-first by the order
// key's per-file statistics -- the whole-file analogue of duckdb's
// RowGroupReorderer. Scheduling only: the TopN above still sorts, so segments
// with unusable statistics simply fall to the null end of the walk.
void BuildSegmentScanOrder(IResearchScanGlobalState& g,
                           const SereneDBScanBindData::ScanOrder& order) {
  const auto field = static_cast<irs::field_id>(order.column.id());
  struct Key {
    uint32_t seg;
    duckdb::Value value;
  };
  std::vector<Key> keys;
  keys.reserve(g.claimable_segments);
  for (uint32_t claimed = 0; claimed < g.claimable_segments; ++claimed) {
    const auto si = g.SegmentAt(claimed);
    duckdb::Value v;
    const auto* col_reader = (*g.reader)[si].GetColReader();
    const auto* reader = col_reader ? col_reader->Column(field) : nullptr;
    if (reader) {
      v = duckdb::RowGroupReorderer::RetrieveStat(
        reader->MergedStatistics(), order.order_by, order.column_type);
    }
    keys.push_back({si, std::move(v)});
  }
  const bool nulls_first =
    order.null_order == duckdb::OrderByNullType::NULLS_FIRST;
  const bool asc = order.order_type == duckdb::OrderType::ASCENDING;
  absl::c_sort(keys, [&](const Key& l, const Key& r) {
    const bool ln = l.value.IsNull();
    const bool rn = r.value.IsNull();
    if (ln || rn) {
      return ln != rn ? (ln ? nulls_first : !nulls_first) : l.seg < r.seg;
    }
    const auto& lo = asc ? l.value : r.value;
    const auto& hi = asc ? r.value : l.value;
    return lo != hi ? lo < hi : l.seg < r.seg;
  });
  g.segment_order.clear();
  g.segment_order.reserve(keys.size());
  for (const auto& k : keys) {
    g.segment_order.push_back(k.seg);
  }
}

void ClassifySegmentColFilters(
  const irs::SubReader& seg, IResearchScanGlobalState& g,
  ColFilterStateCache& states,
  TableFilterDocIterator::SegmentClassification& out);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IResearchScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<IResearchScanGlobalState>();

  InitScanState(*state, context, bind_data, input);

  auto& ss = bind_data;
  if (!ss.offsets.empty() && !ss.stored_filter) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() requires an inverted index scan in the same "
              "sub-query"));
  }
  state->scan = &ss;
  state->reader = &ss.snapshot->reader;
  state->total_segments = ss.snapshot->reader.size();
  state->claimable_segments = static_cast<uint32_t>(state->total_segments);
  state->vector_scorer = ss.vector_scorer ? &*ss.vector_scorer : nullptr;

  state->mode = DecideScanMode(*state, ss, input);
  if (state->mode == ScanMode::TsDict) {
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
    return duckdb::unique_ptr_cast<IResearchScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
  }

  if (state->mode == ScanMode::CountFast) {
    // The whole-reader live count answers at init (see InitLocal); no
    // queries, one thread, no segment is touched.
    return duckdb::unique_ptr_cast<IResearchScanGlobalState,
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

  if (!state->col_filters.empty() && state->total_segments != 0) {
    ColFilterStateCache init_states;
    TableFilterDocIterator::SegmentClassification cls;
    state->segment_order.reserve(state->total_segments);
    for (uint32_t si = 0; si < state->total_segments; ++si) {
      ClassifySegmentColFilters((*state->reader)[si], *state, init_states, cls);
      if (!cls.segment_dead) {
        state->segment_order.push_back(si);
      }
    }
    if (state->segment_order.size() == state->total_segments) {
      state->segment_order.clear();
    } else {
      state->claimable_segments =
        static_cast<uint32_t>(state->segment_order.size());
    }
  }

  if (state->mode == ScanMode::Count) {
    return state;
  }

  if (ss.text_scorer) {
    state->scorer_obj = catalog::MakeScorer(*ss.text_scorer);
  } else if (ss.score_order) {
    state->scorer_obj = std::make_unique<irs::VectorSimilarityScorer>();
  }
  state->collectors.resize(state->MaxThreads());

  if (state->mode == ScanMode::TopK) {
    if (state->score_static_floor >
        std::numeric_limits<irs::score_t>::lowest()) {
      // Static score floor (Lucene min_score): the collectors start at the
      // bound and enforce it -- the stripped filter's replacement
      // (HandleScoreFilter recorded the floor only where it is the
      // collector's raw space) -- and WAND skips below-floor blocks from the
      // first window.
      state->global_kth_score.store(state->score_static_floor,
                                    std::memory_order_relaxed);
    }
    if (ss.vector_scorer &&
        (ss.vector_scorer->quant != irs::VectorQuantization::None ||
         state->has_lookup_filter)) {
      state->rerank_pool =
        ReadRerankFactor(context) * static_cast<uint32_t>(*ss.score_top_k);
    }
  } else {
    // Streaming text-score WAND: a pushed dynamic TOP_N score boundary (score
    // DESC TOP_N -- only a lower bound can seed block-max skipping) or a
    // static score floor on a WAND-enabled text scorer lets the streaming
    // DocIterator skip below-threshold blocks (its ScoreThresholdAttr is
    // seeded before each emit); the HitBatcher score filter still enforces
    // the exact boundary. Honors the same kill switch as the in-scan top-k
    // rule (IResearchSetScanOrder): with it set, WAND must not engage
    // anywhere.
    duckdb::Value disable_topk;
    const bool topk_disabled =
      context.TryGetCurrentSetting("sdb_disable_top_k_optimization",
                                   disable_topk) &&
      !disable_topk.IsNull() && disable_topk.GetValue<bool>();
    const bool dynamic_bound =
      state->score_dynamic_filter &&
      (state->score_dynamic_filter->comparison_type ==
         duckdb::ExpressionType::COMPARE_GREATERTHAN ||
       state->score_dynamic_filter->comparison_type ==
         duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO);
    const bool static_bound =
      state->score_static_floor > std::numeric_limits<irs::score_t>::lowest();
    state->wand_streaming =
      !topk_disabled && ss.text_scorer && state->scan_score &&
      (dynamic_bound || static_bound) &&
      WandEnabled(bind_data.inverted_index.get(), ss.text_scorer);
  }
  ClassifyColumnstoreProjections(*state, bind_data);

  if (ss.scan_order && state->mode == ScanMode::Stream) {
    BuildSegmentScanOrder(*state, *ss.scan_order);
  }

  if (state->mode == ScanMode::Stream && state->BulkChunkEligible() &&
      !ss.text_scorer && !state->has_lookup_filter) {
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
    // Walking claim order keeps units grouped whole-segment best-first
    // (intra-segment unit order stays natural -- rows within a file are
    // unordered anyway).
    for (uint32_t claimed = 0; claimed < state->claimable_segments; ++claimed) {
      const auto si = state->SegmentAt(claimed);
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

  return duckdb::unique_ptr_cast<IResearchScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

namespace {

const irs::QueryBuilder& EnsureSegmentQuery(IResearchScanGlobalState& g,
                                            IResearchScanLocalState& l,
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

void PreparePhase(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                  IResearchScanLocalState& l) {
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
                       const IResearchScanGlobalState& g, const HitsChunk& view,
                       duckdb::DataChunk& output) {
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
  const auto& ss = bd;
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
  if (count_mode == Mode::Meta && seg.live_docs_count() == seg.docs_count()) {
    return static_cast<uint32_t>(reader->docs_count());
  }
  auto it = reader->iterator(irs::SeekMode::NORMAL);
  SDB_ASSERT(it);
  if (!it->next()) {
    return 0;
  }
  const auto live = count_mode == Mode::Where
                      ? WhereLiveDocs(*it, seg, *where_query, true)
                      : MaskedLiveDocs(*it, seg, true);
  SDB_ASSERT(!it->next());
  return static_cast<uint32_t>(live);
}

void BuildTsDictSlots(TsDictLocalState& lstate,
                      duckdb::TableFunctionInitInput& input,
                      const SereneDBScanBindData& bd,
                      const SereneDBScanBindData& ss) {
  using duckdb::DConstants;
  using Req = SereneDBScanBindData::TsDictRequest;
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

duckdb::unique_ptr<duckdb::LocalTableFunctionState> IResearchScanInitLocal(
  duckdb::ExecutionContext& /*context*/, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<IResearchScanGlobalState>();
  const auto& bd = input.bind_data->Cast<SereneDBScanBindData>();
  if (gstate.mode == ScanMode::TsDict) {
    auto lstate = duckdb::make_uniq<TsDictLocalState>();
    const auto& ss = bd;
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
  if (gstate.mode == ScanMode::CountFast || gstate.mode == ScanMode::Count) {
    auto lstate = duckdb::make_uniq<SearchFullScanCountLocalState>();
    if (gstate.mode == ScanMode::CountFast) {
      lstate->local_count = gstate.reader->live_docs_count();
      lstate->segments_exhausted = true;
    }
    return lstate;
  }
  if (gstate.mode == ScanMode::TopK) {
    auto lstate = duckdb::make_uniq<SearchFullScanTopKLocalState>();
    lstate->bind_data = &bd;
    if (!gstate.vector_scorer) {
      lstate->local_threshold = std::numeric_limits<irs::score_t>::min();
    }
    auto& ss = bd;
    const size_t k = gstate.rerank_pool ? gstate.rerank_pool : *ss.score_top_k;
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

void IResearchScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<IResearchScanGlobalState>();
  // filter_prune: fill the scan's own column_ids-sized chunk (filter-only
  // columns included, read for the filter), then reference just the projected
  // subset into output -- a reorder that drops filter-only columns, exactly
  // like PhysicalTableScan does with all_columns/projection_ids.
  duckdb::DataChunk* target = &output;
  if (!gstate.output_projection_ids.empty()) {
    auto& base = data.local_state->Cast<IResearchScanLocalState>();
    if (base.scan_chunk.ColumnCount() == 0) {
      duckdb::vector<duckdb::LogicalType> types(gstate.projected_types.begin(),
                                                gstate.projected_types.end());
      base.scan_chunk.Initialize(context, types);
    }
    base.scan_chunk.Reset();
    target = &base.scan_chunk;
  }
  auto& out = *target;
  switch (gstate.mode) {
    case ScanMode::TsDict: {
      auto& l = data.local_state->Cast<TsDictLocalState>();
      RunStreamingScan(context, gstate, l, out);
      break;
    }
    case ScanMode::CountFast:
    case ScanMode::Count: {
      auto& l = data.local_state->Cast<SearchFullScanCountLocalState>();
      RunCountScan(context, gstate, l, out);
      break;
    }
    case ScanMode::TopK: {
      auto& l = data.local_state->Cast<SearchFullScanTopKLocalState>();
      if (!l.prepared) {
        if (gstate.total_segments != 0 && !gstate.vector_scorer) {
          PreparePhase(context, gstate, l);
        }
        l.prepared = true;
      }
      RunTopKScan(context, gstate, l, out);
      break;
    }
    case ScanMode::Stream: {
      auto& l = data.local_state->Cast<SearchFullScanScanLocalState>();
      if (!l.prepared) {
        if (gstate.scorer_obj && gstate.total_segments != 0 &&
            !gstate.vector_scorer) {
          PreparePhase(context, gstate, l);
        }
        l.prepared = true;
      }
      RunStreamingScan(context, gstate, l, out);
      break;
    }
  }
  if (target != &output) {
    output.ReferenceColumns(*target, gstate.output_projection_ids);
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
  if (order_col >= bd.column_ids.size()) {
    return;
  }
  const auto col_id = bd.column_ids[order_col];
  if (col_id != catalog::Column::kInvertedIndexScoreId) {
    // ORDER BY <covered .col column> LIMIT: iterate segments best-first by the
    // column's per-file statistics (duckdb's row-group reorder, one level up).
    // Only covered columns have `.col` statistics.
    const auto* info =
      bd.inverted_index ? bd.inverted_index->FindColumnInfo(col_id) : nullptr;
    const bool stored =
      bd.IsSearchTableEntry() || (info != nullptr && info->IsStored());
    if (stored && !bd.scan_order) {
      bd.scan_order = SereneDBScanBindData::ScanOrder{
        col_id, options->order_type, options->null_order, options->order_by,
        options->column_type};
    }
    return;
  }
  auto& search_scan = bd;
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

// Wraps `inner` so it yields only docs whose covered `.col` values pass the
// segment's active filters (codec Filter + zonemap). An empty `active` set
// (nothing pushed, or everything ALWAYS_TRUE for this segment) returns the
// inner iterator unwrapped -- the non-filtered path.
// Transparent to every DocIterator consumer (count/Collect/EmitScoredDocs).
irs::DocIterator::ptr MaybeWrapColFilter(
  irs::DocIterator::ptr inner, const irs::SubReader& seg,
  std::span<const TableFilterDocIterator::FilterSpec> active,
  IResearchScanGlobalState& g, ColFilterStateCache& states) {
  if (active.empty()) {
    return inner;
  }
  const auto* col_reader = seg.GetColReader();
  SDB_ASSERT(col_reader != nullptr,
             "`.col` table filter requires a columnstore segment");
  return irs::memory::make_managed<TableFilterDocIterator>(
    std::move(inner), *col_reader, active, *g.client_context, states);
}

// Whole-segment classification of the pushed `.col`/score filters against the
// segment's per-column file statistics -- duckdb RowGroup::CheckZonemap one
// stats level up. ALWAYS_FALSE/FALSE_OR_NULL for any filter kills the segment
// before its postings are iterated (NULL evaluations don't pass a WHERE filter
// either); ALWAYS_TRUE drops the filter for this segment (zonemap-only filters
// stay: their bound can tighten later); TRUE_OR_NULL swaps in the pre-built
// IS NOT NULL filter (duckdb propagate_get policy). A column absent from the
// segment's columnstore is all NULL there: the filter evaluates once against a
// single NULL -- failing kills the segment, passing drops the filter (nothing
// to prune, and there is no reader to bind). Score specs (computed, no stored
// stats) stay active. Writes into `out` so the caller's buffers are reused
// across segments.
void ClassifySegmentColFilters(
  const irs::SubReader& seg, IResearchScanGlobalState& g,
  ColFilterStateCache& states,
  TableFilterDocIterator::SegmentClassification& out) {
  out.segment_dead = false;
  out.active.clear();
  if (g.col_filters.empty()) {
    return;
  }
  const auto* col_reader = seg.GetColReader();
  for (const auto& cf : g.col_filters) {
    TableFilterDocIterator::FilterSpec spec{
      .field = cf.field,
      .filter = cf.filter,
      .is_score = cf.is_score,
      .is_dynamic = cf.is_dynamic,
      .zonemap_only = cf.zonemap_only,
      .null_check = cf.null_check,
      .not_null = cf.not_null.get(),
    };
    if (spec.is_score) {
      out.active.push_back(spec);
      continue;
    }
    const auto* reader =
      col_reader != nullptr ? col_reader->Column(spec.field) : nullptr;
    if (reader == nullptr) {
      duckdb::Vector null_row{duckdb::Value{cf.type}, duckdb::count_t{1}};
      duckdb::SelectionVector sel;
      duckdb::idx_t approved = 1;
      duckdb::ColumnSegment::FilterSelection(
        sel, null_row, states.State(*g.client_context, *spec.filter), 1,
        approved);
      if (approved == 0) {
        out.segment_dead = true;
        out.active.clear();
        return;
      }
      continue;
    }
    const auto& stats = reader->MergedStatistics();
    const auto verdict =
      spec.filter->Cast<duckdb::ExpressionFilter>().CheckStatistics(
        stats, states.State(*g.client_context, *spec.filter));
    switch (verdict) {
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE:
      case duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL:
        out.segment_dead = true;
        out.active.clear();
        return;
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE:
        if (spec.zonemap_only) {
          out.active.push_back(spec);
        }
        break;
      case duckdb::FilterPropagateResult::FILTER_TRUE_OR_NULL:
        if (!spec.zonemap_only && spec.not_null != nullptr) {
          // The replacement is a bare IS NOT NULL, so it evaluates on the
          // validity child alone.
          spec.filter = spec.not_null;
          spec.null_check = irs::NullCheckKind::IsNotNull;
        }
        out.active.push_back(spec);
        break;
      case duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE:
        out.active.push_back(spec);
        break;
    }
  }
}

namespace {

void CollectSegmentTopK(SearchFullScanTopKLocalState& s,
                        const irs::SubReader& seg, uint32_t seg_idx,
                        IResearchScanGlobalState& g) {
  ClassifySegmentColFilters(seg, g, s.filter_states, s.seg_cls);
  const auto& cls = s.seg_cls;
  if (cls.segment_dead) {
    return;
  }
  using C = irs::NthPartitionScoreCollector;
  auto& search = *s.bind_data;
  if (!std::holds_alternative<C>(s.collector)) {
    const size_t k = g.rerank_pool ? g.rerank_pool : *search.score_top_k;
    s.collector.template emplace<C>(s.local_threshold, k, s.hit_slice);
  }
  auto& collector = std::get<C>(s.collector);

  s.score_fetcher.Clear();
  collector.SetSegment(seg_idx);

  const auto seen_global = g.global_kth_score.load(std::memory_order_relaxed);
  if (seen_global > s.local_threshold) {
    s.local_threshold = seen_global;
  }

  const auto& seg_query = EnsureSegmentQuery(g, s, seg, seg_idx);
  const irs::StatsBuffer& stats =
    g.stats ? *g.stats : irs::StatsBuffer::Empty();

  const bool wand_enabled =
    WandEnabled(s.bind_data->inverted_index.get(), search.text_scorer);
  irs::DocIterator::ptr it = seg.mask(seg_query.Execute(
    {.wand = {.wand_enabled = wand_enabled},
     .top_k_collect = search.vector_scorer.has_value() && cls.active.empty()},
    stats));
  // Filter the collected docs by the covered `.col` values, so top-k is
  // selected over survivors (codec Filter + zonemap in the wrapper).
  it = MaybeWrapColFilter(std::move(it), seg, cls.active, g, s.filter_states);
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
  while (kth > cur && !g.global_kth_score.compare_exchange_weak(
                        cur, kth, std::memory_order_relaxed)) {
  }
}

}  // namespace

void RunTopKScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                 SearchFullScanTopKLocalState& l, duckdb::DataChunk& output) {
  while (!l.segments_exhausted) {
    const auto claimed = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (claimed >= g.claimable_segments) {
      l.segments_exhausted = true;
      break;
    }
    const auto seg = g.SegmentAt(claimed);
    const auto& sub = (*g.reader)[seg];
    SDB_ASSERT(sub.live_docs_count() != 0);
    CollectSegmentTopK(l, sub, seg, g);
  }
  if (!l.emit_prepared) {
    l.PrepareEmitBuffer(ctx, g);
  }
  if (!EmitBufferedScoreDocs(ctx, g, l, l.top_hits, l.current_idx, output)) {
    output.SetChildCardinality(0);
  }
}

void SearchFullScanTopKLocalState::PrepareEmitBuffer(
  duckdb::ClientContext& /*ctx*/, IResearchScanGlobalState& g) {
  emit_prepared = true;
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
    // Rerank exact distances only when the collector's scores are approximate
    // (quantized); a non-quantized pool (over-fetched only to survive a lookup
    // filter) already carries exact distances.
    if (g.vector_scorer->quant != irs::VectorQuantization::None) {
      RerankHits(g, accepted_slice);
    }
    const auto& search = *bind_data;
    const size_t kreal = *search.score_top_k;
    // Trim the over-fetched pool to the exact k only when nothing downstream
    // drops rows. With a lookup filter, keep the whole pool so the lookup can
    // discard non-matches and still yield up to k survivors (the TOP_N above
    // trims to the exact k).
    if (!g.has_lookup_filter && kept > kreal) {
      std::nth_element(accepted_slice.begin(), accepted_slice.begin() + kreal,
                       accepted_slice.end(),
                       [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
                         return l.score > r.score;
                       });
      kept = kreal;
      accepted_slice = hit_slice.subspan(0, kept);
    }
  }
  SortScoreDocsBySegDoc(accepted_slice);
  top_hits = hit_slice.subspan(0, kept);
}

void SearchFullScanScanLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                                const irs::SubReader& seg,
                                                uint32_t seg_idx,
                                                IResearchScanGlobalState& g) {
  current_seg_idx = seg_idx;
  bulk_doc_in_seg = 0;
  bulk_seg_doc_count = 0;
  // A lookup filter rules out the bulk columnstore fast path: bulk emits every
  // live row untouched.
  const bool bulk = g.BulkChunkEligible() &&
                    seg.live_docs_count() == seg.docs_count() &&
                    !g.has_lookup_filter;
  if (bulk) {
    bulk_seg_doc_count = seg.docs_count();
    streaming_doc.reset();
    return;
  }
  const auto& seg_query = EnsureSegmentQuery(g, *this, seg, seg_idx);
  // The `.col`/score filters run inside the HitBatcher (RowGroup::Scan-style
  // filter+materialize), so the DocIterator streams unfiltered -- no
  // TableFilterDocIterator on this path. When a dynamic TOP_N score boundary is
  // pushed on a WAND-enabled text scorer, run WAND so below-threshold blocks
  // are skipped (PushHits seeds the threshold from the boundary before each
  // emit).
  streaming_doc =
    seg.mask(seg_query.Execute({.wand = {.wand_enabled = g.wand_streaming}},
                               g.stats ? *g.stats : irs::StatsBuffer::Empty()));
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
  SDB_ASSERT(classified_seg == seg_idx,
             "segment filters are classified at the claim site");
  hit_batcher->BeginSegment(seg_idx, seg.GetColReader(), g.client_context,
                            &filter_states, seg_cls.active);
}

void SearchFullScanScanLocalState::StartUnit(
  duckdb::ClientContext& ctx, const IResearchScanGlobalState::ScanUnit& unit,
  IResearchScanGlobalState& g) {
  if (unit.bulk) {
    current_seg_idx = unit.seg;
    bulk_doc_in_seg = unit.begin;
    bulk_seg_doc_count = unit.begin + unit.count;
    streaming_doc.reset();
    return;
  }
  StartSegment(ctx, (*g.reader)[unit.seg], unit.seg, g);
}

void SearchFullScanScanLocalState::PushHits(IResearchScanGlobalState& g) {
  if (!streaming_doc) {
    if (!hit_batcher->Empty()) {
      hit_batcher->Finalize();
    }
    return;
  }
  for (;;) {
    auto next = streaming_doc->value();
    // Before the first window value() is unpositioned; the self-positioning
    // Emit skips to the first match itself (no advance() prime). Afterwards
    // value() is the postcondition next match. `max = cursor + span` caps each
    // window to the row-group boundary, so a cursor that starts ahead of the
    // first match just yields an empty window and value() jumps to the match.
    auto cursor = irs::doc_limits::valid(next) ? next : irs::doc_limits::min();
    if (!irs::doc_limits::eof(next) && !hit_batcher->Filters().Empty()) {
      // Zonemap skip: raise the emit floor past definitely-dead blocks (the
      // self-positioning Emit skips to it) instead of staging and dropping
      // their windows; when everything left is dead, the segment is done.
      const auto rows = hit_batcher->SegmentRowCount();
      auto row = static_cast<uint64_t>(cursor - irs::doc_limits::min());
      for (auto dead = hit_batcher->Filters().DeadUntil(row);
           dead != 0 && row < rows;
           dead = hit_batcher->Filters().DeadUntil(row)) {
        row = dead;
      }
      if (row >= rows) {
        next = irs::doc_limits::eof();
      } else {
        cursor = irs::doc_limits::min() + static_cast<irs::doc_id_t>(row);
      }
    }
    if (irs::doc_limits::eof(next)) {
      streaming_score_function = {};
      streaming_doc.reset();
      if (!hit_batcher->Ready() && !hit_batcher->Empty()) {
        hit_batcher->Finalize();
      }
      return;
    }
    const auto span = hit_batcher->OpenWindow(cursor - irs::doc_limits::min());
    if (span == 0) {
      return;
    }
    if (g.scan_score) {
      if (g.wand_streaming) {
        // Seed the WAND threshold from the static score floor and the dynamic
        // TOP_N boundary's current value; blocks that cannot beat it are
        // skipped this window.
        if (auto* t =
              irs::GetMutable<irs::ScoreThresholdAttr>(streaming_doc.get())) {
          auto v = g.score_static_floor;
          if (g.score_dynamic_filter) {
            v = std::max(v, CurrentWandThreshold(*g.score_dynamic_filter));
          }
          t->value = v;
        }
      }
      const auto n = streaming_doc->EmitScoredDocs(
        hit_batcher->WindowHead(), hit_batcher->ScoreHead(), cursor + span,
        streaming_score_function, &score_fetcher, cursor);
      // User-facing scores in the batcher: the score-column filter (applied on
      // _scores in EmitFiltered) and the output vector both see the one value.
      ApplyScoreEmit(g, hit_batcher->ScoreHead(), n);
      hit_batcher->CommitWindow(n);
    } else {
      const auto n = streaming_doc->EmitDocs(hit_batcher->WindowHead(), cursor,
                                             cursor + span);
      hit_batcher->CommitWindow(n);
    }
  }
}

duckdb::idx_t SearchFullScanScanLocalState::EmitChunk(
  duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
  duckdb::DataChunk& output) {
  // Bulk columnstore chunks: a chunk whose rows are all dropped by the `.col`
  // filters produces 0 survivors while the unit still has rows to scan -- keep
  // scanning within the unit rather than returning 0 (which RunStreamingScan
  // reads as "unit drained" and would skip the rest of the unit).
  while (bulk_doc_in_seg < bulk_seg_doc_count) {
    auto* scanner =
      GetOrOpenSegmentFullScanner(*this, g, *g.reader, current_seg_idx);
    SDB_ENSURE(scanner, "bulk cs scan: segment has no columnstore reader");
    // Zonemap skip: jump the cursor past a definitely-dead block in one step
    // instead of scanning it vector by vector.
    const auto dead_end = scanner->DeadUntil(bulk_doc_in_seg);
    if (dead_end > bulk_doc_in_seg) {
      bulk_doc_in_seg = std::min<uint64_t>(dead_end, bulk_seg_doc_count);
      continue;
    }
    const auto take = std::min<duckdb::idx_t>(
      STANDARD_VECTOR_SIZE, bulk_seg_doc_count - bulk_doc_in_seg);
    const auto produced = scanner->Scan(bulk_doc_in_seg, take, output);
    bulk_doc_in_seg += take;
    if (produced != 0) {
      AccountAndWriteVirtualColumns(g, produced, nullptr, output);
      return produced;
    }
    output.Reset();
  }
  // Streaming: a window entirely dropped by the filters likewise must not read
  // as "segment drained" -- pull the next ready batch until one has survivors
  // or the batcher is genuinely empty.
  for (;;) {
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
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
    }
    const auto produced = EmitReadyBatch(ctx, g, *this, output);
    if (produced != 0) {
      return produced;
    }
    output.Reset();
  }
}

void RunCountScan(duckdb::ClientContext& /*ctx*/, IResearchScanGlobalState& g,
                  SearchFullScanCountLocalState& l, duckdb::DataChunk& output) {
  while (!l.segments_exhausted) {
    const auto claimed = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (claimed >= g.claimable_segments) {
      l.segments_exhausted = true;
      break;
    }
    const auto seg = g.SegmentAt(claimed);
    const auto& sub = (*g.reader)[seg];
    SDB_ASSERT(sub.live_docs_count() != 0);
    ClassifySegmentColFilters(sub, g, l.filter_states, l.seg_cls);
    if (l.seg_cls.segment_dead) {
      continue;
    }
    if (l.seg_cls.active.empty() && !g.scan->stored_filter &&
        !g.vector_scorer) {
      // No predicate and the whole-file statistics settled every pushed
      // filter for this segment: the live count answers without touching the
      // postings.
      l.local_count += sub.live_docs_count();
      continue;
    }
    const auto& seg_query = EnsureSegmentQuery(g, l, sub, seg);
    auto doc = MaybeWrapColFilter(
      sub.mask(seg_query.Execute({}, irs::StatsBuffer::Empty())), sub,
      l.seg_cls.active, g, l.filter_states);
    l.local_count += doc->count();
  }
  if (l.local_emitted >= l.local_count) {
    output.SetChildCardinality(0);
    return;
  }
  const auto batch = std::min<duckdb::idx_t>(l.local_count - l.local_emitted,
                                             STANDARD_VECTOR_SIZE);
  output.SetChildCardinality(batch);
  g.produced_rows.fetch_add(batch, std::memory_order_relaxed);
  l.local_emitted += batch;
}

duckdb::idx_t EmitReadyBatch(duckdb::ClientContext& ctx,
                             IResearchScanGlobalState& g,
                             SegDocBufferedScanLocalState& l,
                             duckdb::DataChunk& output) {
  if (!g.cs_projections.empty()) {
    SDB_IF_FAILURE("SearchIncludeFetchFault") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
  }
  if (g.has_external_projections) {
    SDB_ASSERT(l.bind_data);
    if (!l.index_source) {
      l.index_source =
        MakeIndexSource(ctx, *l.bind_data, g.external_projected_columns,
                        g.projected_types, l.bind_data->column_ids,
                        const_cast<duckdb::TableFilterSet*>(g.pushed_filters));
    }
    if (l.pk_batch.kind == PrimaryKeyBatch::Kind::None) {
      l.pk_batch.kind = l.index_source->PkKind();
    }
    l.pk_batch.Reset();
  }
  const auto batch = l.hit_batcher->Emit(output);
  if (batch.pk != nullptr) {
    SDB_IF_FAILURE("SearchPkFetchFault") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
    batch.pk->Flatten(batch.count);
    AppendPrimaryKeysFromVector(l.pk_batch, *batch.pk, batch.count);
  }
  const HitsChunk view{
    .docs = batch.docs,
    .scores = batch.scores,
    .fixed_seg = batch.seg,
  };
  WriteChunkOffsets(l.offsets_entries, l.offsets_prepped_seg,
                    l.offsets_doc_scratch, g, view, output);
  AccountAndWriteVirtualColumns(g, batch.count, batch.score_vec, output);
  return batch.count;
}

duckdb::idx_t FinalizeBatch(duckdb::ClientContext& ctx,
                            IResearchScanGlobalState& g,
                            SegDocBufferedScanLocalState& l,
                            duckdb::DataChunk& output,
                            duckdb::idx_t collected) {
  if (collected == 0 || !g.has_external_projections) {
    return collected;
  }
  SDB_ASSERT(l.index_source);
  SDB_ASSERT(l.pk_batch.Size() == collected);
  // Returns the survivor count: the lookup applies pushed lookup-column filters
  // natively and compacts to survivors (== collected when no lookup filter).
  return l.index_source->Materialize(ctx, l.pk_batch, 0, collected, output);
}

bool EmitBufferedScoreDocs(duckdb::ClientContext& ctx,
                           IResearchScanGlobalState& g,
                           SegDocBufferedScanLocalState& l,
                           std::span<const irs::ScoreDoc> hits,
                           size_t& current_idx, duckdb::DataChunk& output) {
  if (!l.hit_batcher) {
    l.hit_batcher = std::make_unique<HitBatcher>(
      g.cs_projections,
      g.has_external_projections ? catalog::term_dict::kPKFieldId
                                 : irs::field_limits::invalid(),
      g.scan_score);
    l.hit_batcher->BeginSegment(std::numeric_limits<uint32_t>::max(), nullptr,
                                g.client_context);
  }
  SDB_IF_FAILURE("SearchLookupFault") {
    if (g.has_external_projections) {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
  }
  auto& batcher = *l.hit_batcher;
  // Emit one materialized batch at a time; a batch fully dropped by a lookup
  // filter loops for the next one instead of surfacing an empty chunk.
  for (;;) {
    while (!batcher.Ready()) {
      if (current_idx == hits.size()) {
        if (batcher.Empty()) {
          return false;
        }
        batcher.Finalize();
        if (!batcher.Ready()) {
          return false;
        }
        break;
      }
      const auto& hit = hits[current_idx];
      // Hits are sorted by (segment, doc); a segment change drains the batch.
      if (hit.segment_idx != batcher.Segment()) {
        if (!batcher.Empty()) {
          batcher.Finalize();
          continue;
        }
        batcher.BeginSegment(hit.segment_idx,
                             (*g.reader)[hit.segment_idx].GetColReader(),
                             g.client_context);
      }
      // Bulk-stage the ascending run of same-segment hits that fall in one
      // columnstore window (OpenWindow bounds it to a row group / output
      // vector) and commit it in one shot -- the window API the streaming path
      // uses. A full batcher yields span 0, so we emit the ready batch and
      // retry this hit after the drain.
      const auto row = hit.doc - irs::doc_limits::min();
      const auto span = batcher.OpenWindow(row);
      if (span == 0) {
        break;
      }
      auto* out_docs = batcher.WindowHead();
      auto* out_scores = g.scan_score ? batcher.ScoreHead() : nullptr;
      const auto seg = batcher.Segment();
      duckdb::idx_t n = 0;
      while (current_idx != hits.size() &&
             hits[current_idx].segment_idx == seg &&
             (hits[current_idx].doc - irs::doc_limits::min()) < row + span) {
        out_docs[n] = hits[current_idx].doc;
        if (out_scores != nullptr) {
          out_scores[n] = hits[current_idx].score;
        }
        ++n;
        ++current_idx;
      }
      // Map the collector's raw scores to the user-facing value in the batcher,
      // symmetric with the streaming path (AccountAndWriteVirtualColumns copies
      // them straight out).
      if (out_scores != nullptr) {
        ApplyScoreEmit(g, out_scores, n);
      }
      batcher.CommitWindow(n);
    }
    const auto emitted = EmitReadyBatch(ctx, g, l, output);
    const auto kept = FinalizeBatch(ctx, g, l, output, emitted);
    if (kept != 0) {
      output.SetChildCardinality(kept);
      return true;
    }
    output.Reset();
  }
}

void RunStreamingScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                      SearchFullScanScanLocalState& l,
                      duckdb::DataChunk& output) {
  for (;;) {
    const auto added = l.EmitChunk(ctx, g, output);
    SDB_ASSERT(added <= STANDARD_VECTOR_SIZE);
    if (added != 0) {
      // The lookup fetches the source columns for the materialized pks and
      // applies any pushed lookup-column filters natively (FilterSelection +
      // late materialization), returning the survivor count (== added when no
      // lookup filter applies).
      const auto kept = FinalizeBatch(ctx, g, l, output, added);
      if (kept != 0) {
        output.SetChildCardinality(kept);
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
      const auto& unit = g.scan_units[ui];
      // Units subdivide a segment; the whole-file classification is computed
      // once per segment and reused across its units.
      if (l.classified_seg != unit.seg) {
        ClassifySegmentColFilters((*g.reader)[unit.seg], g, l.filter_states,
                                  l.seg_cls);
        l.classified_seg = unit.seg;
      }
      if (l.seg_cls.segment_dead) {
        continue;
      }
      l.StartUnit(ctx, unit, g);
      continue;
    }
    const auto claimed = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (claimed >= g.claimable_segments) {
      break;
    }
    const auto seg_idx = g.SegmentAt(claimed);
    const auto& seg = (*g.reader)[seg_idx];
    SDB_ASSERT(seg.live_docs_count() != 0);
    ClassifySegmentColFilters(seg, g, l.filter_states, l.seg_cls);
    l.classified_seg = seg_idx;
    if (l.seg_cls.segment_dead) {
      continue;
    }
    l.StartSegment(ctx, seg, seg_idx, g);
  }
  output.SetChildCardinality(0);
}

void RunStreamingScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
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
    if (collected != 0 || exhausted) {
      output.SetChildCardinality(collected);
      return;
    }
    output.Reset();
  }
}

void TsDictLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                    const irs::SubReader& seg, uint32_t seg_idx,
                                    IResearchScanGlobalState& g) {
  _seg = &seg;
  count_mode = seg.live_docs_count() != seg.docs_count() ? CountMode::Masked
                                                         : CountMode::Meta;
  where_query = nullptr;
  _next_field = fields.empty() ? nullptr : fields.data();
  if (g.filter) {
    const auto& query = EnsureSegmentQuery(g, *this, seg, seg_idx);
    if (irs::doc_limits::eof(
          query.Execute({}, irs::StatsBuffer::Empty())->advance())) {
      _next_field = nullptr;
      return;
    }
    where_query = &query;
    count_mode = CountMode::Where;
  }
}

irs::TermIterator::ptr TsDictLocalState::MakeTermSource(
  const FieldState& field, const irs::TermReader& reader) {
  if (field.having_filter) {
    auto cursor = field.having_filter->CompileTermIterator(reader);
    SDB_ENSURE(cursor,
               "ts_dict: claimed having filter failed to compile a term "
               "iterator");
    return cursor;
  }
  const bool max_only = field.term_uses == TsDictTermUses::kMax;
  const bool min_max =
    field.term_uses == (TsDictTermUses::kMin | TsDictTermUses::kMax);
  if ((max_only || min_max) && count_mode != CountMode::Masked) {
    std::array<irs::bytes_view, 2> terms;
    size_t count = 0;
    const auto max = reader.max();
    if (count_mode == CountMode::Meta) {
      if (min_max) {
        terms[count++] = reader.min();
      }
      terms[count++] = max;
    } else {
      auto it = reader.iterator(irs::SeekMode::RandomOnly);
      const auto pin = [&](irs::bytes_view term) {
        if (!it->seek(term) ||
            WhereLiveDocs(*it, *_seg, *where_query, false) == 0) {
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
      _cursor_mode = CountMode::Meta;
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
    _cursor_mode = count_mode;
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
      case Mode::Meta:
        return ctx.meta ? ctx.meta->docs_count : 1;
      case Mode::Masked:
        return MaskedLiveDocs(it, *ctx.seg, ctx.count_data);
      case Mode::Where:
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
      SDB_ENSURE(meta, "ts_dict: term iterator has no term_meta");
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
      .where_query = where_query,
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
    NullFieldLiveCount(*_seg, field.null_field_id, count_mode, where_query);
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
                                          IResearchScanGlobalState& g,
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
