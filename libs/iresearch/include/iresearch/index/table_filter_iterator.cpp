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

#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/storage/table/column_segment.hpp>

#include "basics/assert.h"

namespace sdb::connector {

TableFilterDocIterator::TableFilterDocIterator(
  irs::DocIterator::ptr inner, const irs::ColReader& col_reader,
  std::span<const FilterSpec> filters, duckdb::ClientContext& context)
  : _inner{std::move(inner)}, _ctx{col_reader} {
  _sel_data = duckdb::make_buffer<duckdb::SelectionData>(STANDARD_VECTOR_SIZE);
  _sel.Initialize(_sel_data);
  _filters.reserve(filters.size());
  for (const auto& spec : filters) {
    if (spec.is_score) {
      // The score is computed, not stored in `.col` -- filtered on the score
      // vector in FilterBlock, so it takes no columnstore reader.
      _score_filter = spec.filter;
      _score_state =
        duckdb::TableFilterState::Initialize(context, *spec.filter);
      continue;
    }
    const auto* reader = col_reader.Column(spec.field);
    SDB_ASSERT(reader != nullptr,
               "table-filter column missing from the segment columnstore");
    _filters.push_back(FilterCol{
      .reader = reader,
      .filter = spec.filter,
      .state = duckdb::TableFilterState::Initialize(context, *spec.filter),
      .scan = reader->InitScan(_ctx),
      .scratch =
        std::make_unique<irs::ColumnReader::VectorScratch>(reader->Type()),
    });
  }
}

duckdb::idx_t TableFilterDocIterator::FilterBlock(irs::doc_id_t* docs,
                                                  irs::score_t* scores,
                                                  duckdb::idx_t n) {
  if ((_filters.empty() && _score_filter == nullptr) || n == 0) {
    return n;
  }
  duckdb::idx_t out = n;

  // Phase 1: the score filter is the cheap one -- a comparison on the already
  // computed, in-memory scores, with no columnstore read. Run it first so the
  // `.col` pass below reads only survivors and skips whole blocks whose docs
  // were all score-rejected.
  if (_score_filter && scores) {
    duckdb::Vector svec{duckdb::LogicalType::FLOAT,
                        reinterpret_cast<duckdb::data_ptr_t>(scores), out};
    // An unset selection is the identity [0,out); FilterSelection builds the
    // incremental itself, narrows to the survivors, and allocates its own
    // result buffer -- so a cheap local handle is enough (and keeps _sel free
    // for the `.col` pass, which FilterSelection's repoint would otherwise
    // shrink). Scores carry no nulls, so the default-valid FLAT view suffices.
    duckdb::SelectionVector score_sel;
    duckdb::idx_t kept = out;
    duckdb::ColumnSegment::FilterSelection(score_sel, svec, *_score_state, out,
                                           kept);
    // Survivor indices are ascending, so writes never clobber unread input.
    for (duckdb::idx_t s = 0; s < kept; ++s) {
      const auto idx = score_sel.get_index(s);
      docs[s] = docs[idx];
      scores[s] = scores[idx];
    }
    out = kept;
  }

  // Phase 2: `.col` filters -- per-block zonemap skip + codec filter over the
  // (score-)survivors, compacting in place. `_sel` holds the current block's
  // survivor offsets; FilterSelection repoints it to a buffer sized to the
  // entering survivor count, so every block rebinds it to the full-capacity
  // `_sel_data` store before refilling (else set_index overflows the heap).
  if (!_filters.empty()) {
    duckdb::idx_t w = 0;
    duckdb::idx_t i = 0;
    while (i < out) {
      // Group the ascending docs that fall in one columnstore block: zonemap
      // and the codec filter both work per block.
      const uint64_t anchor = docs[i] - irs::doc_limits::min();
      const uint64_t rg_end = _filters.front().reader->RowGroupEnd(anchor);
      duckdb::idx_t j = i;
      while (j < out && (docs[j] - irs::doc_limits::min()) < rg_end) {
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
      duckdb::idx_t survivors = run;
      for (auto& f : _filters) {
        if (survivors == 0) {
          break;
        }
        const auto block = f.reader->Locate(anchor).block;
        const auto& expr = f.filter->Cast<duckdb::ExpressionFilter>();
        const auto z =
          expr.CheckStatistics(f.reader->RowGroupStatistics(block));
        if (z == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE) {
          survivors = 0;
          break;
        }
        if (z == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE) {
          continue;
        }
        survivors =
          f.reader->GatherFilter(f.scan, anchor, span, _sel, survivors,
                                 *f.filter, *f.state, f.scratch->Reset());
      }
      // `_sel[0..survivors)` hold surviving span offsets (ascending); map back
      // to docs and compact in place (w <= i + k, so writes never clobber
      // unread input).
      duckdb::idx_t k = 0;
      for (duckdb::idx_t s = 0; s < survivors; ++s) {
        const uint64_t want = _sel.get_index(s);
        while (((docs[i + k] - irs::doc_limits::min()) - anchor) != want) {
          ++k;
        }
        docs[w] = docs[i + k];
        if (scores) {
          scores[w] = scores[i + k];
        }
        ++w;
      }
      i = j;
    }
    out = w;
  }
  return out;
}

uint32_t TableFilterDocIterator::count() {
  // Self-positioning EmitDocs drives the walk from a running `min`; no external
  // advance() prime (which is invalid for iterators that don't implement it).
  uint32_t total = 0;
  auto min = irs::doc_limits::min();
  while (!irs::doc_limits::eof(min)) {
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
    const auto max = min + STANDARD_VECTOR_SIZE;
    const auto n = _inner->EmitScoredDocs(_docbuf.data(), _scorebuf.data(), max,
                                          scorer, &fetcher, min);
    const auto survivors = FilterBlock(_docbuf.data(), _scorebuf.data(), n);
    collector.AddDocs(_docbuf.data(), survivors, _scorebuf.data());
    _doc = min = _inner->value();
  }
}

}  // namespace sdb::connector
