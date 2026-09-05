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

#include <array>
#include <bit>
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
#include "iresearch/search/common/window.hpp"

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
    const auto type_id = reader->Type().id();
    const bool list_like = type_id == duckdb::LogicalTypeId::LIST ||
                           type_id == duckdb::LogicalTypeId::MAP;
    const bool nested = list_like || type_id == duckdb::LogicalTypeId::ARRAY ||
                        type_id == duckdb::LogicalTypeId::STRUCT ||
                        type_id == duckdb::LogicalTypeId::UNION ||
                        type_id == duckdb::LogicalTypeId::VARIANT;
    _cols.push_back(Col{
      .reader = reader,
      .field = spec.field,
      .filter = spec.filter,
      .expr = &spec.filter->Cast<duckdb::ExpressionFilter>(),
      .is_dynamic = spec.is_dynamic,
      .null_check = spec.null_check,
      .list_like = list_like,
      .nested = nested,
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
    if (c.field == field && !c.nested) {
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
    if (!f.nested && (anchor >= f.checked.end || anchor < f.checked.begin)) {
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
    if (f.nested) {
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
    while (j < n && (docs[j] - irs::doc_limits::min()) < rg_end &&
           (docs[j] - irs::doc_limits::min()) - anchor < STANDARD_VECTOR_SIZE) {
      ++j;
    }
    const duckdb::idx_t run = j - i;
    const auto span = static_cast<duckdb::idx_t>(
      (docs[j - 1] - irs::doc_limits::min()) - anchor + 1);
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

template<bool Keep>
duckdb::idx_t ColFilterChain::WalkMask(irs::doc_id_t base, uint64_t* mask,
                                       duckdb::idx_t words) {
  SDB_ASSERT(!_cols.empty());
  duckdb::idx_t total = 0;
  if (!_sel_data) {
    _sel_data =
      duckdb::make_buffer<duckdb::SelectionData>(STANDARD_VECTOR_SIZE);
  }
  const auto* const reader = _cols.front().reader;
  duckdb::idx_t w = 0;
  uint64_t word = 0;
  // A word is cleared as it is loaded: what a run has consumed is never read
  // from memory again, so the load and the clear are one pass and only the
  // survivors of a `Keep` walk are written back.
  const auto load = [&] {
    while (w != words) {
      word = mask[w];
      mask[w] = 0;
      ++w;
      if (word != 0) {
        return true;
      }
    }
    return false;
  };
  const auto bit = [&] {
    return (w - 1) * 64 + static_cast<uint64_t>(std::countr_zero(word));
  };
  const auto skip = [&](uint64_t end) {
    while (end >= w * 64) {
      if (!load()) {
        return false;
      }
    }
    if (const auto lo = (w - 1) * 64; end > lo) {
      word &= ~uint64_t{0} << (end - lo);
    }
    return word != 0 || load();
  };
  if (!load()) {
    return 0;
  }
  for (;;) {
    const uint64_t off = bit();
    // The row of a bit is computed from the bit, never from a base of its
    // own: bit zero of a whole segment's set stands for document zero, which
    // is no row at all.
    const uint64_t anchor = (base + off) - irs::doc_limits::min();
    if (const auto dead = DeadUntil(anchor); dead != 0) {
      if (!skip(dead + irs::doc_limits::min() - base)) {
        return total;
      }
      continue;
    }
    const uint64_t limit =
      reader->RowGroupEnd(anchor) + irs::doc_limits::min() - base;
    _sel.Initialize(_sel_data);
    duckdb::idx_t run = 0;
    uint64_t last = off;
    bool more = true;
    for (;;) {
      const auto b = bit();
      if (b >= limit || b - off >= STANDARD_VECTOR_SIZE) {
        break;
      }
      _sel.set_index(run, static_cast<duckdb::idx_t>(b - off));
      ++run;
      last = b;
      word &= word - 1;
      if (word == 0 && !load()) {
        more = false;
        break;
      }
    }
    const auto span = static_cast<duckdb::idx_t>(last - off + 1);
    const auto survivors = FilterWindow(anchor, span, _sel, run, nullptr);
    if constexpr (Keep) {
      for (duckdb::idx_t k = 0; k != survivors; ++k) {
        const auto b = off + _sel.get_index(k);
        mask[b / 64] |= uint64_t{1} << (b % 64);
      }
    }
    total += survivors;
    if (!more) {
      return total;
    }
  }
}

duckdb::idx_t ColFilterChain::CountMask(irs::doc_id_t base, uint64_t* mask,
                                        duckdb::idx_t words) {
  // A count reaches the chain only with columns bound: nothing else can rule
  // a document out, so an empty chain would not have been passed at all.
  SDB_ASSERT(!_cols.empty());
  return WalkMask<false>(base, mask, words);
}

duckdb::idx_t ColFilterChain::FilterMask(irs::doc_id_t base, uint64_t* mask,
                                         duckdb::idx_t words) {
  if (_cols.empty()) {
    // Only the computed-score filter is bound, and it has already cleared its
    // own bits; what is left is the answer.
    duckdb::idx_t total = 0;
    for (duckdb::idx_t w = 0; w != words; ++w) {
      total += static_cast<duckdb::idx_t>(std::popcount(mask[w]));
    }
    return total;
  }
  return WalkMask<true>(base, mask, words);
}

void ColFilterChain::Rewind(irs::ReadContext& ctx) {
  for (auto& f : _cols) {
    f.scan = f.reader->InitScan(ctx);
  }
}

uint64_t ColFilterChain::DeadUntil(uint64_t row) {
  uint64_t dead_end = 0;
  for (auto& f : _cols) {
    if (f.nested || row >= f.reader->RowCount()) {
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

duckdb::idx_t ColFilterChain::FilterMaskScores(
  const duckdb::TableFilter& /*filter*/, duckdb::TableFilterState& state,
  uint64_t* mask, const irs::score_t* scores, duckdb::idx_t words) {
  duckdb::idx_t total = 0;
  std::array<irs::score_t, 64> vals;
  std::array<uint8_t, 64> offs;
  for (duckdb::idx_t w = 0; w != words; ++w) {
    auto word = mask[w];
    if (word == 0) {
      continue;
    }
    const auto base = static_cast<uint64_t>(w) * 64;
    duckdb::idx_t run = 0;
    for (auto scan = word; scan != 0; scan &= scan - 1) {
      const auto bit = static_cast<uint64_t>(std::countr_zero(scan));
      offs[run] = static_cast<uint8_t>(bit);
      vals[run] = scores[base + bit];
      ++run;
    }
    duckdb::Vector svec{duckdb::LogicalType::FLOAT,
                        reinterpret_cast<duckdb::data_ptr_t>(vals.data()), run};
    duckdb::SelectionVector sel;
    duckdb::idx_t kept = run;
    duckdb::ColumnSegment::FilterSelection(sel, svec, state, run, kept);
    uint64_t out = 0;
    for (duckdb::idx_t k = 0; k != kept; ++k) {
      out |= uint64_t{1} << offs[sel.get_index(k)];
    }
    mask[w] = out;
    total += kept;
  }
  return total;
}

duckdb::idx_t ColFilterChain::FilterDocsScores(
  const duckdb::TableFilter& filter, duckdb::TableFilterState& state,
  irs::doc_id_t* docs, irs::score_t* scores, duckdb::idx_t n) {
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

}  // namespace sdb::connector
