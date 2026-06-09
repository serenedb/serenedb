////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "phrase_filter.hpp"

#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/states/phrase_state.hpp"
#include "iresearch/search/states_cache.hpp"
#include "iresearch/search/top_terms_collector.hpp"
#include "iresearch/utils/automaton_utils.hpp"

namespace irs {
namespace {

struct TopTermsCollectorImpl final : FilterVisitor {
  explicit TopTermsCollectorImpl(size_t size) : _impl{size} {
    SDB_ASSERT(size);
  }

  void Prepare(const SubReader& segment, const TermReader& field,
               const SeekTermIterator& terms) final {
    _impl.Prepare(segment, field, terms);
  }

  void Visit(score_t boost) final { _impl.Visit(boost); }

  field_visitor ToVisitor() {
    // TODO(mbkkt) we can avoid by_terms, but needs to change
    // TopTermsCollector, to make it able keep equal elements
    ByTermsOptions::search_terms terms;
    _impl.Visit([&](TopTerm<score_t>& term) {
      terms.emplace(std::move(term.term), term.key);
    });
    return [terms = std::move(terms)](const SubReader& segment,
                                      const TermReader& field,
                                      FilterVisitor& visitor) {
      return ByTerms::visit(segment, field, terms, visitor);
    };
  }

 private:
  TopTermsCollector<TopTerm<score_t>> _impl;
};

struct GetVisitor {
  field_visitor operator()(const ByTermOptions& part) const {
    return [term = bytes_view(part.term)](const SubReader& segment,
                                          const TermReader& field,
                                          FilterVisitor& visitor) {
      return ByTerm::visit(segment, field, term, visitor);
    };
  }

  field_visitor operator()(const ByPrefixOptions& part) const {
    return [term = bytes_view(part.term)](const SubReader& segment,
                                          const TermReader& field,
                                          FilterVisitor& visitor) {
      return ByPrefix::visit(segment, field, term, visitor);
    };
  }

  field_visitor operator()(const ByWildcardOptions&) const {
    SDB_UNREACHABLE();
    return {};
  }

  field_visitor operator()(const AutomatonOptions& part) const {
    return AutomatonFilter::visitor(part.acceptor);
  }

  field_visitor operator()(const ByEditDistanceOptions& part) const {
    if (part.max_terms != 0) {
      return {};
    }
    return ByEditDistance::visitor(part);
  }

  field_visitor operator()(const ByTermsOptions& part) const {
    return
      [terms = &part.terms](const SubReader& segment, const TermReader& field,
                            FilterVisitor& visitor) {
        return ByTerms::visit(segment, field, *terms, visitor);
      };
  }

  field_visitor operator()(const ByRangeOptions& part) const {
    return
      [range = &part.range](const SubReader& segment, const TermReader& field,
                            FilterVisitor& visitor) {
        return ByRange::visit(segment, field, *range, visitor);
      };
  }

  field_visitor operator()(const ByRegexpOptions&) const {
    SDB_UNREACHABLE();
    return {};
  }
};

struct PrepareVisitor : util::Noncopyable {
  auto operator()(const ByTermOptions& opts) const {
    return ByTerm::prepare(ctx, id, opts.term);
  }

  auto operator()(const ByPrefixOptions& part) const {
    return ByPrefix::prepare(ctx, id, part.term, part.scored_terms_limit);
  }

  Filter::Query::ptr operator()(const ByWildcardOptions&) const {
    SDB_UNREACHABLE();
    return {};
  }

  auto operator()(const AutomatonOptions& part) const {
    return PrepareAutomatonFilter(ctx, id, part.acceptor,
                                  part.scored_terms_limit);
  }

  auto operator()(const ByEditDistanceOptions& part) const {
    return ByEditDistance::prepare(ctx, id, part.term, part.max_terms,
                                   part.max_distance, part.provider,
                                   part.with_transpositions, part.prefix);
  }

  Filter::Query::ptr operator()(const ByTermsOptions&) const { return {}; }

  auto operator()(const ByRangeOptions& part) const {
    return ByRange::prepare(ctx, id, part.range, part.scored_terms_limit);
  }

  Filter::Query::ptr operator()(const ByRegexpOptions&) const {
    SDB_UNREACHABLE();
    return {};
  }

  PrepareVisitor(const PrepareContext& ctx, irs::field_id id) noexcept
    : ctx{ctx}, id{id} {}

  const PrepareContext& ctx;
  const irs::field_id id;
};

// Filter visitor for phrase queries
template<typename PhraseStates>
class PhraseTermVisitor final : public FilterVisitor,
                                private util::Noncopyable {
 public:
  explicit PhraseTermVisitor(PhraseStates& phrase_states) noexcept
    : _phrase_states(phrase_states) {}

  void Prepare(const SubReader& segment, const TermReader& field,
               const SeekTermIterator& terms) noexcept final {
    _segment = &segment;
    _reader = &field;
    _terms = &terms;
    _found = true;
  }

  void Visit(score_t boost) final {
    SDB_ASSERT(_terms && _segment && _reader);

    // disallow negative boost
    boost = std::max(0.f, boost);

    // Only if it has scorer
    if (_collectors) {
      if (_stats_size <= _term_offset) {
        // variadic phrase case
        _collectors->PushBack();
        SDB_ASSERT(_stats_size == _term_offset);
        ++_stats_size;
        _volatile_boost |= (boost != kNoBoost);
      }

      _collectors->Collect(_term_offset++, *_terms);
    }
    _phrase_states.emplace_back(_terms->cookie(), boost);
  }

  void Reset() noexcept { _volatile_boost = false; }

  void Reset(FlatTermBuffer* collectors) noexcept {
    _found = false;
    _terms = nullptr;
    _term_offset = 0;
    _collectors = collectors;
    if (_collectors) {
      _stats_size = collectors->Size();
    }
  }

  bool Found() const noexcept { return _found; }

  bool VolatileBoost() const noexcept { return _volatile_boost; }

 private:
  size_t _term_offset = 0;
  size_t _stats_size = 0;
  const SubReader* _segment{};
  const TermReader* _reader{};
  PhraseStates& _phrase_states;
  FlatTermBuffer* _collectors = nullptr;
  const SeekTermIterator* _terms = nullptr;
  bool _found = false;
  bool _volatile_boost = false;
};

bool Valid(const TermReader* reader) noexcept {
  static_assert(FixedPhraseQuery::kRequiredFeatures ==
                VariadicPhraseQuery::kRequiredFeatures);
  // check field reader exists with required features
  return reader != nullptr && (reader->meta().index_features &
                               FixedPhraseQuery::kRequiredFeatures) ==
                                FixedPhraseQuery::kRequiredFeatures;
}

Filter::Query::ptr FixedPrepareCollect(const PrepareContext& ctx,
                                       irs::field_id id,
                                       const ByPhraseOptions& options) {
  const auto phrase_size = options.size();
  const auto is_ord_empty = !ctx.scorer;

  // stats collectors
  FieldCollector field_stats;
  TermCollectorsFlat term_stats{ctx.scorer, phrase_size};

  // per segment phrase states
  FixedPhraseQuery::states_t phrase_states{ctx.memory, ctx.index.size()};

  // per segment phrase terms
  FixedPhraseState::Terms phrase_terms{{ctx.memory}};
  phrase_terms.reserve(phrase_size);

  // iterate over the segments
  PhraseTermVisitor<decltype(phrase_terms)> ptv(phrase_terms);

  for (const auto& segment : ctx.index) {
    // get term dictionary for field
    const auto* reader = segment.field(id);
    if (!Valid(reader)) {
      continue;
    }

    // collect field statistics once per segment
    field_stats.Collect(*reader);
    ptv.Reset(&term_stats);

    for (const auto& word : options) {
      SDB_ASSERT(std::get_if<ByTermOptions>(&word.part));
      ByTerm::visit(segment, *reader, std::get<ByTermOptions>(word.part).term,
                    ptv);
      if (!ptv.Found() && is_ord_empty) {
        break;
      }
    }

    // we have not found all needed terms
    if (phrase_terms.size() != phrase_size) {
      phrase_terms.clear();
      continue;
    }

    auto& state = phrase_states.insert(segment);
    state.terms = std::move(phrase_terms);
    state.reader = reader;

    phrase_terms.clear();
    phrase_terms.reserve(phrase_size);
  }

#ifndef SDB_GTEST  // TODO(mbkkt) adjust tests
  if (phrase_states.empty()) {
    return Filter::Query::empty();
  }
#endif

  // offset of the first term in a phrase
  SDB_ASSERT(!options.empty());

  // finish stats
  bstring stats(GetStatsSize(ctx.scorer), 0);
  auto* stats_buf = stats.data();

  FixedPhraseQuery::positions_t positions(phrase_size);
  auto pos_itr = positions.begin();

  size_t term_idx = 0;
  PosAttr::value_t look_back = 0;
  for (const auto& term : options) {
    pos_itr->offs_max = term.offs_max;
    pos_itr->offs_min = term.offs_min;
    pos_itr->lead_offset = look_back += term.offs_max;
    term_stats.Finish(stats_buf, term_idx, &field_stats);
    ++pos_itr;
    ++term_idx;
  }

  return memory::make_tracked<FixedPhraseQuery>(
    ctx.memory, std::move(phrase_states), std::move(positions),
    std::move(stats), ctx.boost);
}

Filter::Query::ptr VariadicPrepareCollect(const PrepareContext& ctx,
                                          irs::field_id id,
                                          const ByPhraseOptions& options) {
  const auto phrase_size = options.size();
  // stats collectors
  FieldCollector field_stats;

  std::vector<field_visitor> phrase_part_visitors;
  phrase_part_visitors.reserve(phrase_size);
  TermCollectorsVariadic phrase_part_stats{ctx.scorer, phrase_size};

  std::vector<field_visitor*> all_terms_visitors;
  std::vector<TopTermsCollectorImpl> top_terms_collectors;

  for (const auto& word : options) {
    auto& visitor =
      phrase_part_visitors.emplace_back(std::visit(GetVisitor{}, word.part));
    if (!visitor) {
      auto& opts = std::get<ByEditDistanceOptions>(word.part);
      visitor = ByEditDistance::visitor(opts);
      all_terms_visitors.push_back(&visitor);
      top_terms_collectors.emplace_back(opts.max_terms);
    }
  }

  if (!all_terms_visitors.empty()) {
    // TODO(mbkkt) we should move all terms search to here
    // And make second loop for index only to make correct order of terms
    for (const auto& segment : ctx.index) {
      // get term dictionary for field
      const auto* reader = segment.field(id);
      if (!Valid(reader)) {
        continue;
      }
      auto it = top_terms_collectors.begin();
      for (auto* visitor : all_terms_visitors) {
        (*visitor)(segment, *reader, *it++);
      }
    }
    auto it = top_terms_collectors.begin();
    for (auto* visitor : all_terms_visitors) {
      *visitor = it++->ToVisitor();
    }
  }

  // per segment phrase states
  VariadicPhraseQuery::states_t phrase_states{ctx.memory, ctx.index.size()};

  // per segment phrase terms: number of terms per part
  ManagedVector<size_t> num_terms(phrase_size, {ctx.memory});
  VariadicPhraseState::Terms phrase_terms{{ctx.memory}};
  // reserve space for at least 1 term per part
  phrase_terms.reserve(phrase_size);

  // iterate over the segments
  const auto is_ord_empty = !ctx.scorer;

  PhraseTermVisitor<decltype(phrase_terms)> ptv(phrase_terms);

  for (const auto& segment : ctx.index) {
    // get term dictionary for field
    const auto* reader = segment.field(id);
    if (!Valid(reader)) {
      continue;
    }

    // collect field statistics once per segment
    field_stats.Collect(*reader);
    ptv.Reset();  // reset boost volaitility mark

    size_t found_parts = 0;
    for (auto& visitor : phrase_part_visitors) {
      const auto was_terms_count = phrase_terms.size();
      ptv.Reset(phrase_part_stats.GetCollector(found_parts));
      visitor(segment, *reader, ptv);
      const auto new_terms_count = phrase_terms.size() - was_terms_count;
      // TODO(mbkkt) Avoid unnecessary work for min_match > 1 queries
      if (new_terms_count != 0) {
        num_terms[found_parts++] = new_terms_count;
      } else if (is_ord_empty) {
        break;
      }
    }

    // we have not found all needed terms
    if (found_parts != phrase_size) {
      phrase_terms.clear();
      continue;
    }

    auto& state = phrase_states.insert(segment);
    state.terms = std::move(phrase_terms);
    state.num_terms = std::move(num_terms);
    state.reader = reader;
    state.volatile_boost = !is_ord_empty && ptv.VolatileBoost();
    SDB_ASSERT(phrase_size == state.num_terms.size());

    phrase_terms.clear();
    phrase_terms.reserve(phrase_size);
    // reserve space for at least 1 term per part
    num_terms.clear();
    num_terms.resize(phrase_size);
  }

#ifndef SDB_GTEST  // TODO(mbkkt) adjust tests
  if (phrase_states.empty()) {
    return Filter::Query::empty();
  }
#endif

  // offset of the first term in a phrase
  SDB_ASSERT(!options.empty());
  // finish stats
  SDB_ASSERT(phrase_size == phrase_part_stats.Size());
  bstring stats(GetStatsSize(ctx.scorer), 0);
  auto* stats_buf = stats.data();

  VariadicPhraseQuery::positions_t positions(phrase_size);
  auto position = positions.begin();
  PosAttr::value_t look_back = 0;
  size_t part_idx = 0;
  for (const auto& term : options) {
    SDB_ASSERT(position != positions.end());
    position->offs_max = term.offs_max;
    position->offs_min = term.offs_min;
    position->lead_offset = look_back += term.offs_max;
    phrase_part_stats.Finish(stats_buf, part_idx, &field_stats);
    ++position;
    ++part_idx;
  }

  return memory::make_tracked<VariadicPhraseQuery>(
    ctx.memory, std::move(phrase_states), std::move(positions),
    std::move(stats), ctx.boost);
}

}  // namespace

Filter::Query::ptr ByPhrase::Prepare(const PrepareContext& ctx,
                                     irs::field_id id,
                                     const ByPhraseOptions& options) {
  if (!irs::field_limits::valid(id) || options.empty()) {
    return Query::empty();
  }

  if (1 == options.size()) {
    auto query = std::visit(PrepareVisitor{ctx, id}, options.begin()->part);
    if (query) {
      return query;
    }
  }

  // prepare phrase stats (collector for each term)
  if (options.simple()) {
    return FixedPrepareCollect(ctx, id, options);
  }

  return VariadicPrepareCollect(ctx, id, options);
}

bool ByPhraseOptions::LowerWildcardParts() {
  bool changed = false;
  for (auto& info : _phrase) {
    if (const auto* w = std::get_if<ByWildcardOptions>(&info.part); w) {
      bstring buf;
      const auto lim = w->scored_terms_limit;
      info.part = ExecuteWildcard(
        buf, bytes_view{w->term},
        [](bytes_view term) -> phrase_part {
          ByTermOptions opts;
          opts.term = term;
          return opts;
        },
        [lim](bytes_view term) -> phrase_part {
          ByPrefixOptions opts;
          opts.term = term;
          opts.scored_terms_limit = lim;
          return opts;
        },
        [lim](bytes_view term) -> phrase_part {
          return AutomatonOptions{FromWildcard(term), term, lim};
        });
      changed = true;
    } else if (const auto* r = std::get_if<ByRegexpOptions>(&info.part); r) {
      bstring buf;
      const auto lim = r->scored_terms_limit;
      const auto syntax = r->syntax;
      info.part = ExecuteRegexp(
        buf, bytes_view{r->pattern},
        [](bytes_view term) -> phrase_part {
          ByTermOptions opts;
          opts.term = term;
          return opts;
        },
        [lim](bytes_view prefix) -> phrase_part {
          ByPrefixOptions opts;
          opts.term = prefix;
          opts.scored_terms_limit = lim;
          return opts;
        },
        [lim, syntax](bytes_view pattern) -> phrase_part {
          return AutomatonOptions{
            FromRegexp(pattern, kDefaultMaxDfaStates, syntax), pattern, lim};
        });
      changed = true;
    }
  }
  return changed;
}

}  // namespace irs
