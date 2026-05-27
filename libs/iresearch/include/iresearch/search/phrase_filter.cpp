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

#include "basics/down_cast.h"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/states/phrase_state.hpp"
#include "iresearch/search/states_cache.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/top_terms_collector.hpp"

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

  void Merge(TopTermsCollectorImpl&& other) {
    _impl.Merge(std::move(other._impl));
  }

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

  field_visitor operator()(const ByWildcardOptions& part) const {
    return ByWildcard::visitor(part.term);
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

  field_visitor operator()(const ByRegexpOptions& part) const {
    return ByRegexp::visitor(part.pattern, part.syntax);
  }
};

struct PrepareVisitor : util::Noncopyable {
  auto operator()(const ByTermOptions& opts) const {
    return ByTerm::prepare(ctx, field, opts.term);
  }

  auto operator()(const ByPrefixOptions& part) const {
    return ByPrefix::Prepare(ctx, field, part.term, part.scored_terms_limit);
  }

  auto operator()(const ByWildcardOptions& part) const {
    return ByWildcard::Prepare(ctx, field, part.term, part.scored_terms_limit);
  }

  auto operator()(const ByEditDistanceOptions& part) const {
    return ByEditDistance::prepare(ctx, field, part.term, part.max_terms,
                                   part.max_distance, part.provider,
                                   part.with_transpositions, part.prefix);
  }

  Filter::Query::ptr operator()(const ByTermsOptions&) const { return {}; }

  auto operator()(const ByRangeOptions& part) const {
    return ByRange::Prepare(ctx, field, part.range, part.scored_terms_limit);
  }

  auto operator()(const ByRegexpOptions& part) const {
    return ByRegexp::Prepare(ctx, field, part.pattern, part.scored_terms_limit,
                             part.syntax);
  }

  PrepareVisitor(const PrepareContext& ctx, std::string_view field) noexcept
    : ctx{ctx}, field{field} {}

  const PrepareContext& ctx;
  const std::string_view field;
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
    SDB_ASSERT(_terms && _collectors && _segment && _reader);

    // disallow negative boost
    boost = std::max(0.f, boost);

    if (_stats_size <= _term_offset) {
      // variadic phrase case
      _collectors->push_back();
      SDB_ASSERT(_stats_size == _term_offset);
      ++_stats_size;
      _volatile_boost |= (boost != kNoBoost);
    }

    _collectors->collect(*_segment, *_reader, _term_offset++, *_terms);
    _phrase_states.emplace_back(_terms->cookie(), boost);
  }

  void Reset() noexcept { _volatile_boost = false; }

  void Reset(TermCollectors& collectors) noexcept {
    _found = false;
    _terms = nullptr;
    _term_offset = 0;
    _collectors = &collectors;
    _stats_size = collectors.size();
  }

  bool Found() const noexcept { return _found; }

  bool VolatileBoost() const noexcept { return _volatile_boost; }

 private:
  size_t _term_offset = 0;
  size_t _stats_size = 0;
  const SubReader* _segment{};
  const TermReader* _reader{};
  PhraseStates& _phrase_states;
  TermCollectors* _collectors = nullptr;
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

class VariadicPhraseBuffer final : public Filter::ScoredBuffer {
 public:
  VariadicPhraseBuffer(const PrepareContext& ctx, std::string_view field,
                       const ByPhraseOptions& options, score_t boost = kNoBoost)
    : ScoredBuffer{ctx, boost},
      _field{field},
      _options{options},
      _memory{&ctx.memory},
      _scorer{ctx.scorer},
      _field_stats{ctx.scorer},
      _states{ctx.memory, ctx.index.size()} {
    const auto phrase_size = _options.size();
    _visitors.reserve(phrase_size);
    _part_stats.reserve(phrase_size);
    for (const auto& word : _options) {
      _part_stats.emplace_back(ctx.scorer, 0);
      auto& visitor =
        _visitors.emplace_back(std::visit(GetVisitor{}, word.part));
      if (!visitor) {
        const auto& opts = std::get<ByEditDistanceOptions>(word.part);
        visitor = ByEditDistance::visitor(opts);
        _topn_part_idx.push_back(_visitors.size() - 1);
        _top_terms.emplace_back(opts.max_terms);
      }
    }
  }

  void PrepareSegment(const SubReader& segment) final {
    if (_top_terms.empty()) {
      CollectSegment(segment);
      return;
    }
    const auto* reader = segment.field(_field);
    if (!Valid(reader)) {
      return;
    }
    for (size_t k = 0, n = _topn_part_idx.size(); k < n; ++k) {
      _visitors[_topn_part_idx[k]](segment, *reader, _top_terms[k]);
    }
  }

  void Merge(PrepareBuffer&& other) final {
    auto& rhs = sdb::basics::downCast<VariadicPhraseBuffer>(other);
    if (!_top_terms.empty()) {
      SDB_ASSERT(_states.empty() && rhs._states.empty());
      for (size_t k = 0, n = _top_terms.size(); k < n; ++k) {
        _top_terms[k].Merge(std::move(rhs._top_terms[k]));
      }
      return;
    }
    _field_stats.collect(std::move(rhs._field_stats));
    for (size_t i = 0, n = _part_stats.size(); i < n; ++i) {
      auto& lhs = _part_stats[i];
      auto& other_stats = rhs._part_stats[i];
      while (lhs.size() < other_stats.size()) {
        lhs.push_back();
      }
      while (other_stats.size() < lhs.size()) {
        other_stats.push_back();
      }
      lhs.collect(std::move(other_stats));
    }
    _states.Merge(std::move(rhs._states));
  }

  bool Empty() const noexcept final { return _states.empty(); }

  Filter::Query::ptr Compile(const PrepareContext& ctx) && final {
    if (!_top_terms.empty()) {
      for (size_t k = 0, n = _topn_part_idx.size(); k < n; ++k) {
        _visitors[_topn_part_idx[k]] = _top_terms[k].ToVisitor();
      }
      for (const auto& segment : ctx.index) {
        CollectSegment(segment);
      }
    }

#ifndef SDB_GTEST  // TODO(mbkkt) adjust tests
    if (_states.empty()) {
      return Filter::Query::empty();
    }
#endif

    const auto phrase_size = _options.size();
    SDB_ASSERT(!_options.empty());
    SDB_ASSERT(phrase_size == _part_stats.size());
    bstring stats(GetStatsSize(ctx.scorer), 0);
    auto* stats_buf = stats.data();
    auto collector = _part_stats.begin();

    VariadicPhraseQuery::positions_t positions(phrase_size);
    auto position = positions.begin();
    PosAttr::value_t look_back = 0;
    for (const auto& term : _options) {
      SDB_ASSERT(position != positions.end());
      position->offs_max = term.offs_max;
      position->offs_min = term.offs_min;
      position->lead_offset = look_back += term.offs_max;
      for (size_t i = 0, size = collector->size(); i < size; ++i) {
        collector->finish(stats_buf, i, _field_stats, ctx.index);
      }
      ++position;
      ++collector;
    }

    return memory::make_tracked<VariadicPhraseQuery>(
      ctx.memory, std::move(_states), std::move(positions), std::move(stats),
      _boost);
  }

 private:
  void CollectSegment(const SubReader& segment) {
    const auto* reader = segment.field(_field);
    if (!Valid(reader)) {
      return;
    }

    _field_stats.collect(segment, *reader);

    const auto phrase_size = _options.size();
    VariadicPhraseState::Terms phrase_terms{{*_memory}};
    phrase_terms.reserve(phrase_size);
    ManagedVector<size_t> num_terms(phrase_size, {*_memory});

    const auto is_ord_empty = !_scorer;
    PhraseTermVisitor<decltype(phrase_terms)> ptv(phrase_terms);
    ptv.Reset();  // reset boost volatility mark

    size_t found_parts = 0;
    for (const auto& visitor : _visitors) {
      const auto was_terms_count = phrase_terms.size();
      ptv.Reset(_part_stats[found_parts]);
      visitor(segment, *reader, ptv);
      const auto new_terms_count = phrase_terms.size() - was_terms_count;
      // TODO(mbkkt) Avoid unnecessary work for min_match > 1 queries
      if (new_terms_count != 0) {
        num_terms[found_parts++] = new_terms_count;
      } else if (is_ord_empty) {
        break;
      }
    }

    if (found_parts != phrase_size) {
      return;
    }

    auto& state = _states.insert(segment);
    state.terms = std::move(phrase_terms);
    state.num_terms = std::move(num_terms);
    state.reader = reader;
    state.volatile_boost = !is_ord_empty && ptv.VolatileBoost();
    SDB_ASSERT(phrase_size == state.num_terms.size());
  }

  std::string _field;
  ByPhraseOptions _options;
  IResourceManager* _memory;
  const Scorer* _scorer;
  FieldCollectors _field_stats;
  std::vector<TermCollectors> _part_stats;
  std::vector<field_visitor> _visitors;
  std::vector<size_t> _topn_part_idx;
  std::vector<TopTermsCollectorImpl> _top_terms;
  VariadicPhraseQuery::states_t _states;
};

class FixedPhraseBuffer final : public Filter::ScoredBuffer {
 public:
  FixedPhraseBuffer(const PrepareContext& ctx, std::string_view field,
                    const ByPhraseOptions& options, score_t boost = kNoBoost)
    : ScoredBuffer{ctx, boost},
      _field{field},
      _options{&options},
      _memory{&ctx.memory},
      _field_stats{ctx.scorer},
      _term_stats{ctx.scorer, options.size()},
      _states{ctx.memory, ctx.index.size()} {}

  void PrepareSegment(const SubReader& segment) final {
    const auto* reader = segment.field(_field);
    if (!Valid(reader)) {
      return;
    }

    _field_stats.collect(segment, *reader);

    FixedPhraseState::Terms phrase_terms{{*_memory}};
    phrase_terms.reserve(_options->size());

    PhraseTermVisitor<FixedPhraseState::Terms> ptv(phrase_terms);
    ptv.Reset(_term_stats);

    const auto is_ord_empty = (_term_stats.empty()) ? true : false;
    for (const auto& word : *_options) {
      SDB_ASSERT(std::get_if<ByTermOptions>(&word.part));
      ByTerm::visit(segment, *reader, std::get<ByTermOptions>(word.part).term,
                    ptv);
      if (!ptv.Found() && is_ord_empty) {
        break;
      }
    }

    if (phrase_terms.size() != _options->size()) {
      return;
    }

    auto& state = _states.insert(segment);
    state.terms = std::move(phrase_terms);
    state.reader = reader;
  }

  void Merge(PrepareBuffer&& other) final {
    auto& rhs = sdb::basics::downCast<FixedPhraseBuffer>(other);
    _field_stats.collect(std::move(rhs._field_stats));
    _term_stats.collect(std::move(rhs._term_stats));
    _states.Merge(std::move(rhs._states));
  }

  bool Empty() const noexcept final { return _states.empty(); }

  Filter::Query::ptr Compile(const PrepareContext& ctx) && final {
    bstring stats(GetStatsSize(ctx.scorer), 0);
    auto* stats_buf = stats.data();

    FixedPhraseQuery::positions_t positions(_options->size());
    auto pos_itr = positions.begin();

    size_t term_idx = 0;
    PosAttr::value_t look_back = 0;
    for (const auto& term : *_options) {
      pos_itr->offs_max = term.offs_max;
      pos_itr->offs_min = term.offs_min;
      pos_itr->lead_offset = look_back += term.offs_max;
      _term_stats.finish(stats_buf, term_idx, _field_stats, ctx.index);
      ++pos_itr;
      ++term_idx;
    }

    return memory::make_tracked<FixedPhraseQuery>(
      ctx.memory, std::move(_states), std::move(positions), std::move(stats),
      _boost);
  }

 private:
  std::string_view _field;
  const ByPhraseOptions* _options;
  IResourceManager* _memory;
  FieldCollectors _field_stats;
  TermCollectors _term_stats;
  FixedPhraseQuery::states_t _states;
};

}  // namespace

std::unique_ptr<Filter::PrepareBuffer> ByPhrase::CreateBuffer(
  const PrepareContext& ctx) const {
  if (field().empty() || options().empty()) {
    return std::make_unique<EmptyBuffer>();
  }

  if (options().simple()) {
    if (options().size() == 1) {
      const auto& term = std::get<ByTermOptions>(options().begin()->part).term;
      return std::make_unique<ByTerm::Buffer>(ctx, field(), term, Boost());
    }
    return std::make_unique<FixedPhraseBuffer>(ctx, field(), options(),
                                               Boost());
  }

  return std::make_unique<VariadicPhraseBuffer>(ctx, field(), options(),
                                                Boost());
}

std::unique_ptr<Filter::PrepareBuffer> ByPhrase::CreateFixedBuffer(
  const PrepareContext& ctx, std::string_view field,
  const ByPhraseOptions& options) {
  SDB_ASSERT(!field.empty());
  SDB_ASSERT(!options.empty());
  SDB_ASSERT(options.simple());
  return std::make_unique<FixedPhraseBuffer>(ctx, field, options);
}

Filter::Query::ptr ByPhrase::Prepare(const PrepareContext& ctx,
                                     std::string_view field,
                                     const ByPhraseOptions& options) {
  if (field.empty() || options.empty()) {
    // empty field or phrase
    return Query::empty();
  }

  if (1 == options.size()) {
    auto query = std::visit(PrepareVisitor{ctx, field}, options.begin()->part);
    if (query) {
      return query;
    }
  }

  // prepare phrase stats (collector for each term)
  if (options.simple()) {
    FixedPhraseBuffer buf{ctx, field, options};
    return PrepareWithBuffer<FixedPhraseBuffer>(buf, ctx);
  }

  VariadicPhraseBuffer buf{ctx, field, options};
  return PrepareWithBuffer<VariadicPhraseBuffer>(buf, ctx);
}

}  // namespace irs
