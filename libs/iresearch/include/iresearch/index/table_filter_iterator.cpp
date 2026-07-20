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

#include "iresearch/index/table_filter_iterator.hpp"

#include <absl/algorithm/container.h>

#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/filter/table_filter_functions.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>

#include "basics/assert.h"

namespace sdb::connector {

duckdb::TableFilterState& ColFilterStateCache::State(
  duckdb::ClientContext& context, const duckdb::TableFilter& filter) {
  auto& e = Find(filter);
  if (!e.state) {
    e.state = duckdb::TableFilterState::Initialize(context, filter);
  }
  return *e.state;
}

irs::ColumnReader::VectorScratch& ColFilterStateCache::Scratch(
  const duckdb::TableFilter& filter, const duckdb::LogicalType& type) {
  auto& e = Find(filter);
  if (!e.scratch) {
    e.scratch = std::make_unique<irs::ColumnReader::VectorScratch>(type);
  }
  SDB_ASSERT(e.scratch->vector.GetType() == type,
             "one pushed filter evaluates one column, whose type is fixed");
  return *e.scratch;
}

ColFilterStateCache::Entry& ColFilterStateCache::Find(
  const duckdb::TableFilter& filter) {
  for (auto& e : _entries) {
    if (e.filter == &filter) {
      return e;
    }
  }
  return _entries.emplace_back(Entry{.filter = &filter});
}

void ColFilterChain::Bind(const irs::ColReader& col_reader,
                          irs::ReadContext& ctx,
                          std::span<const ColFilterSpec> specs,
                          duckdb::ClientContext& context,
                          ColFilterStateCache& states) {
  _context = &context;
  _states = &states;
  _cols.clear();
  _cols.reserve(specs.size());
  for (const auto& spec : specs) {
    if (spec.is_score) {
      continue;
    }
    const auto* reader = col_reader.Column(spec.field);
    SDB_ASSERT(reader != nullptr,
               "classification resolves filters on absent columns");
    // A structured column's own blocks (if any) belong to its plumbing -- a
    // list parent's blocks are its offsets -- so their statistics do not
    // describe the column's values: only same-typed block statistics form a
    // usable zonemap.
    const auto blocks = reader->DataBlocks();
    const bool has_zonemap =
      !blocks.empty() && blocks.front().statistics.GetType() == reader->Type();
    _cols.push_back(Col{
      .reader = reader,
      .field = spec.field,
      .filter = spec.filter,
      .expr = &spec.filter->Cast<duckdb::ExpressionFilter>(),
      .is_dynamic = spec.is_dynamic,
      .null_check = spec.null_check,
      .has_blocks = has_zonemap,
      .list_like = reader->Type().id() == duckdb::LogicalTypeId::LIST ||
                   reader->Type().id() == duckdb::LogicalTypeId::MAP,
      .state = &states.State(context, *spec.filter),
      .scan = reader->InitScan(ctx),
    });
  }
}

bool ColFilterChain::AttachOutputSlot(irs::field_id field, duckdb::idx_t slot) {
  for (auto& c : _cols) {
    // A children-backed column filters on a compact decode whose scan state
    // ends past the window -- it cannot double as the slot's materialization;
    // the caller scans it as a plain projected column instead.
    if (c.field == field && c.has_blocks) {
      c.output_slots.push_back(slot);
      return true;
    }
  }
  return false;
}

void ColFilterChain::FinishBind() {
  // Filter-only columns (not projected, which includes every children-backed
  // column -- see AttachOutputSlot) decode into a cache-owned scratch just to
  // evaluate the predicate.
  for (auto& c : _cols) {
    if (c.output_slots.empty()) {
      c.scratch = &_states->Scratch(*c.filter, c.reader->Type());
    }
  }
  // Reordering must not evaluate a throwing expression on rows an earlier
  // filter would have excluded (duckdb's AdaptiveFilter applies the same
  // guard); keep such chains in push order.
  const bool can_reorder =
    _cols.size() > 1 && absl::c_none_of(_cols, [](const Col& c) {
      return c.expr->expr->CanThrow();
    });
  if (can_reorder) {
    // The learned order survives rebinding onto the next segment as long as
    // the active filter count is unchanged.
    if (!_adaptive || _adaptive->GetPermutation().size() != _cols.size()) {
      _adaptive = duckdb::make_uniq<duckdb::AdaptiveFilter>(_cols.size());
    }
  } else {
    _adaptive.reset();
  }
}

duckdb::idx_t ColFilterChain::FilterWindow(uint64_t anchor, duckdb::idx_t span,
                                           duckdb::SelectionVector& sel,
                                           duckdb::idx_t survivors,
                                           duckdb::DataChunk* output) {
  duckdb::AdaptiveFilterState timing;
  if (_adaptive) {
    timing = _adaptive->BeginFilter();
  }
  for (duckdb::idx_t i = 0; i < _cols.size(); ++i) {
    auto& f = _cols[_adaptive ? _adaptive->GetPermutation()[i] : i];
    if (survivors == 0) {
      break;
    }
    f.deferred = false;
    // The cached verdict is trusted even for dynamic filters: DeadUntil
    // re-evaluates them per window on every scan path, and a dynamic bound
    // only tightens -- every dynamic filter is advisory (prune-only), so a
    // one-window-stale verdict merely under-prunes, never drops a live row.
    if (f.has_blocks && (anchor >= f.checked.end || anchor < f.checked.begin)) {
      f.checked = f.reader->Locate(anchor, f.checked);
      const auto& stats = f.reader->RowGroupStatistics(f.checked.block);
      f.verdict = f.expr->CheckStatistics(stats, *f.state);
    }
    // A window crossing the block boundary cannot use the block's verdict.
    const auto z = anchor + span <= f.checked.end
                     ? f.verdict
                     : duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
    if (z == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
        z == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL) {
      survivors = 0;
      break;
    }
    if (z == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE) {
      // Nothing to evaluate this window; a projected filter column instead
      // materializes the final survivors in FinishOutputs (dense gather, no
      // predicate).
      f.deferred = !f.output_slots.empty();
      continue;
    }
    if (!f.has_blocks) {
      // Children-backed column (struct/list parent): GatherFilter's codec /
      // segment machinery does not apply. Decode the current survivors through
      // the virtual gather the materialization path uses, narrow on the
      // compact vector, and map the surviving positions back to span offsets.
      auto& scratch = f.scratch->Reset();
      if (f.list_like) {
        duckdb::ListVector::SetListSize(scratch, 0);
      }
      if (span <= STANDARD_VECTOR_SIZE) {
        f.reader->GatherDense(f.scan, anchor, sel, survivors, span, scratch);
      } else {
        f.reader->GatherScatter(f.scan, anchor, sel, survivors, scratch, 0);
      }
      duckdb::SelectionVector compact;
      duckdb::idx_t approved = survivors;
      duckdb::ColumnSegment::FilterSelection(compact, scratch, *f.state,
                                             survivors, approved);
      for (duckdb::idx_t k = 0; k < approved; ++k) {
        sel.set_index(k, sel.get_index(compact.get_index(k)));
      }
      survivors = approved;
      continue;
    }
    // The validity-only null-check evaluation produces no values, so it is
    // for filter-only columns; a projected null-check column decodes into its
    // output slot like any other filter column (it must materialize anyway).
    const bool filter_only = f.output_slots.empty();
    const auto null_check =
      filter_only ? f.null_check : irs::NullCheckKind::None;
    auto& target =
      filter_only ? f.scratch->Reset() : output->data[f.output_slots.front()];
    survivors = f.reader->GatherFilter(f.scan, anchor, span, sel, survivors,
                                       *f.filter, *f.state, null_check, target);
  }
  if (_adaptive) {
    _adaptive->EndFilter(timing);
  }
  return survivors;
}

duckdb::idx_t ColFilterChain::FilterDocs(irs::doc_id_t* docs,
                                         irs::score_t* scores,
                                         duckdb::idx_t n) {
  if (_cols.empty() || n == 0) {
    return n;
  }
  if (!_sel_data) {
    _sel_data =
      duckdb::make_buffer<duckdb::SelectionData>(STANDARD_VECTOR_SIZE);
  }
  duckdb::idx_t w = 0;
  duckdb::idx_t i = 0;
  while (i < n) {
    // Group the ascending docs that fall in one columnstore block: zonemap
    // and the codec filter both work per block.
    const uint64_t anchor = docs[i] - irs::doc_limits::min();
    const uint64_t rg_end = _cols.front().reader->RowGroupEnd(anchor);
    duckdb::idx_t j = i;
    while (j < n && (docs[j] - irs::doc_limits::min()) < rg_end) {
      ++j;
    }
    const duckdb::idx_t run = j - i;
    const auto span = static_cast<duckdb::idx_t>(
      (docs[j - 1] - irs::doc_limits::min()) - anchor + 1);
    SDB_ASSERT(run <= STANDARD_VECTOR_SIZE);
    _sel.Initialize(_sel_data);
    for (duckdb::idx_t k = 0; k < run; ++k) {
      _sel.set_index(k, (docs[i + k] - irs::doc_limits::min()) - anchor);
    }
    const auto survivors = FilterWindow(anchor, span, _sel, run, nullptr);
    CompactByOffsets(_sel, survivors, anchor, docs + i,
                     scores != nullptr ? scores + i : nullptr, docs + w,
                     scores != nullptr ? scores + w : nullptr);
    w += survivors;
    i = j;
  }
  return w;
}

uint64_t ColFilterChain::DeadUntil(uint64_t row) {
  uint64_t dead_end = 0;
  for (auto& f : _cols) {
    if (!f.has_blocks || row >= f.reader->RowCount()) {
      continue;
    }
    const bool stale = row >= f.checked.end || row < f.checked.begin;
    if (f.is_dynamic || stale) {
      if (stale) {
        f.checked = f.reader->Locate(row, f.checked);
      }
      const auto& stats = f.reader->RowGroupStatistics(f.checked.block);
      f.verdict = f.expr->CheckStatistics(stats, *f.state);
    }
    if (f.verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
        f.verdict == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL) {
      dead_end = std::max<uint64_t>(dead_end, f.checked.end);
    }
  }
  return dead_end;
}

void ColFilterChain::FinishOutputs(uint64_t anchor, duckdb::idx_t span,
                                   const duckdb::SelectionVector& sel,
                                   duckdb::idx_t survivors,
                                   duckdb::DataChunk& output) {
  for (auto& f : _cols) {
    if (f.output_slots.empty()) {
      continue;
    }
    auto& first = output.data[f.output_slots.front()];
    if (f.deferred) {
      if (survivors != 0) {
        if (span <= STANDARD_VECTOR_SIZE) {
          f.reader->GatherDense(f.scan, anchor, sel, survivors, span, first);
        } else {
          f.reader->GatherScatter(f.scan, anchor, sel, survivors, first, 0);
        }
      }
    } else if (survivors != span) {
      // survivors == span means every span row passed (offsets are strictly
      // ascending), so the decoded vector already IS the output -- skip the
      // dictionary view.
      first.Slice(sel, survivors);
    }
    for (std::size_t j = 1; j < f.output_slots.size(); ++j) {
      output.data[f.output_slots[j]].Reference(first);
    }
  }
}

duckdb::idx_t ColFilterChain::FilterScores(const duckdb::TableFilter& filter,
                                           duckdb::TableFilterState& state,
                                           irs::doc_id_t* docs,
                                           irs::score_t* scores,
                                           duckdb::idx_t n) {
  if (n == 0) {
    return 0;
  }
  duckdb::Vector svec{duckdb::LogicalType::FLOAT,
                      reinterpret_cast<duckdb::data_ptr_t>(scores), n};
  // An unset selection is the identity [0,n); FilterSelection builds the
  // incremental itself, narrows to the survivors, and allocates its own
  // result buffer -- so a cheap local handle is enough (and keeps the
  // caller's selection free for the `.col` pass, which FilterSelection's
  // repoint would otherwise shrink). Scores carry no nulls, so the
  // default-valid FLAT view suffices.
  duckdb::SelectionVector score_sel;
  duckdb::idx_t kept = n;
  duckdb::ColumnSegment::FilterSelection(score_sel, svec, state, n, kept);
  // Survivor indices are ascending, so writes never clobber unread input.
  for (duckdb::idx_t s = 0; s < kept; ++s) {
    const auto idx = score_sel.get_index(s);
    docs[s] = docs[idx];
    scores[s] = scores[idx];
  }
  return kept;
}

void ColFilterChain::CompactByOffsets(const duckdb::SelectionVector& sel,
                                      duckdb::idx_t survivors, uint64_t anchor,
                                      const irs::doc_id_t* docs_in,
                                      const irs::score_t* scores_in,
                                      irs::doc_id_t* docs_out,
                                      irs::score_t* scores_out) {
  duckdb::idx_t k = 0;
  for (duckdb::idx_t s = 0; s < survivors; ++s) {
    const uint64_t want = sel.get_index(s);
    while (((docs_in[k] - irs::doc_limits::min()) - anchor) != want) {
      ++k;
    }
    docs_out[s] = docs_in[k];
    if (scores_out != nullptr) {
      scores_out[s] = scores_in[k];
    }
  }
}

TableFilterDocIterator::TableFilterDocIterator(
  irs::DocIterator::ptr inner, const irs::ColReader& col_reader,
  std::span<const FilterSpec> filters, duckdb::ClientContext& context,
  ColFilterStateCache& states)
  : _inner{std::move(inner)}, _ctx{col_reader} {
  for (const auto& spec : filters) {
    if (spec.is_score) {
      // The score is computed, not stored in `.col` -- filtered on the score
      // vector in FilterBlock, so it takes no columnstore reader.
      _score_filter = spec.filter;
      _score_state = &states.State(context, *spec.filter);
    }
  }
  _filters.Bind(col_reader, _ctx, filters, context, states);
  _filters.FinishBind();
}

duckdb::idx_t TableFilterDocIterator::FilterBlock(irs::doc_id_t* docs,
                                                  irs::score_t* scores,
                                                  duckdb::idx_t n) {
  if ((_filters.Empty() && _score_filter == nullptr) || n == 0) {
    return n;
  }
  duckdb::idx_t out = n;

  // Phase 1: the score filter is the cheap one -- a comparison on the already
  // computed, in-memory scores, with no columnstore read. Run it first so the
  // `.col` pass below reads only survivors and skips whole blocks whose docs
  // were all score-rejected.
  if (_score_filter && scores) {
    out = ColFilterChain::FilterScores(*_score_filter, *_score_state, docs,
                                       scores, out);
  }

  // Phase 2: `.col` filters -- per-block zonemap skip + codec filter over the
  // (score-)survivors, compacting in place.
  return _filters.FilterDocs(docs, scores, out);
}

uint32_t TableFilterDocIterator::count() {
  // count() never scores, so a pushed score filter cannot be applied here --
  // the mode decision (DecideScanMode) must route such plans to streaming.
  SDB_ASSERT(_score_filter == nullptr);
  // Self-positioning EmitDocs drives the walk from a running `min`; no external
  // advance() prime (which is invalid for iterators that don't implement it).
  uint32_t total = 0;
  auto min = irs::doc_limits::min();
  while (!irs::doc_limits::eof(min)) {
    if (!_filters.Empty()) {
      // Zonemap skip: raise the emit floor past a definitely-dead block
      // (EmitDocs self-positions to `min`) instead of emitting and dropping
      // it window by window.
      const auto dead_end = _filters.DeadUntil(min - irs::doc_limits::min());
      if (dead_end != 0) {
        min = irs::doc_limits::min() + static_cast<irs::doc_id_t>(dead_end);
        continue;
      }
    }
    const auto max = min + STANDARD_VECTOR_SIZE;
    const auto n = _inner->EmitDocs(_docbuf.data(), min, max);
    total += static_cast<uint32_t>(FilterBlock(_docbuf.data(), nullptr, n));
    _doc = min = _inner->value();  // postcondition: first doc >= max (or eof)
  }
  return total;
}

uint32_t TableFilterDocIterator::EmitDocs(irs::doc_id_t* out, irs::doc_id_t min,
                                          irs::doc_id_t max) {
  // Emit straight into the caller's buffer and compact in place: FilterBlock
  // only ever shrinks [0, n), so no staging copy is needed.
  const auto n = _inner->EmitDocs(out, min, max);
  const auto survivors = FilterBlock(out, nullptr, n);
  _doc = _inner->value();
  return static_cast<uint32_t>(survivors);
}

uint32_t TableFilterDocIterator::EmitScoredDocs(
  irs::doc_id_t* out, irs::score_t* scores, irs::doc_id_t max,
  const irs::ScoreFunction& scorer, irs::ColumnArgsFetcher* fetcher,
  irs::doc_id_t min) {
  const auto n = _inner->EmitScoredDocs(out, scores, max, scorer, fetcher, min);
  const auto survivors = FilterBlock(out, scores, n);
  _doc = _inner->value();
  return static_cast<uint32_t>(survivors);
}

void TableFilterDocIterator::Collect(const irs::ScoreFunction& scorer,
                                     irs::ColumnArgsFetcher& fetcher,
                                     irs::ScoreCollector& collector) {
  // Self-positioning EmitScoredDocs drives the walk from a running `min`; no
  // external advance() prime (invalid for non-advance() iterators).
  auto min = irs::doc_limits::min();
  while (!irs::doc_limits::eof(min)) {
    if (!_filters.Empty()) {
      // Zonemap skip: raise the emit floor past a definitely-dead block
      // (EmitScoredDocs self-positions to `min`) instead of scoring and
      // dropping it window by window.
      const auto dead_end = _filters.DeadUntil(min - irs::doc_limits::min());
      if (dead_end != 0) {
        min = irs::doc_limits::min() + static_cast<irs::doc_id_t>(dead_end);
        continue;
      }
    }
    const auto max = min + STANDARD_VECTOR_SIZE;
    const auto n = _inner->EmitScoredDocs(_docbuf.data(), _scorebuf.data(), max,
                                          scorer, &fetcher, min);
    const auto survivors = FilterBlock(_docbuf.data(), _scorebuf.data(), n);
    collector.AddDocs(_docbuf.data(), survivors, _scorebuf.data());
    _doc = min = _inner->value();
  }
}

}  // namespace sdb::connector
