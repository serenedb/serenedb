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

#include "basics/system-compiler.h"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/states/phrase_state.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/search/top_terms_selector.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"

namespace irs {
namespace {

enum class PhraseQueryKind { kEmpty, kSingleWord, kFixed, kVariadic };

// A phrase with a single non-term part (prefix/wildcard/range/...) reduces to
// that part's own filter: position matching is a no-op for one word.
std::unique_ptr<FilterWithBoost> MakeSinglePartFilter(
  irs::field_id field, const ByPhraseOptions& options) {
  const auto make = [&]<typename F, typename T>(const T& opts) {
    auto filter = std::make_unique<F>();
    *filter->mutable_field_id() = field;
    *filter->mutable_options() = opts;
    return filter;
  };
  return std::visit(
    [&]<typename T>(const T& opts) -> std::unique_ptr<FilterWithBoost> {
      if constexpr (std::is_same_v<T, ByTermOptions>) {
        return make.template operator()<ByTerm>(opts);
      } else if constexpr (std::is_same_v<T, ByPrefixOptions>) {
        return make.template operator()<ByPrefix>(opts);
      } else if constexpr (std::is_same_v<T, ByTermsOptions>) {
        return make.template operator()<ByTerms>(opts);
      } else if constexpr (std::is_same_v<T, ByRangeOptions>) {
        return make.template operator()<ByRange>(opts);
      } else if constexpr (std::is_same_v<T, AutomatonOptions>) {
        return make.template operator()<AutomatonFilter>(opts);
      } else if constexpr (std::is_same_v<T, LevenshteinAutomatonOptions>) {
        return make.template operator()<LevenshteinAutomatonFilter>(opts);
      } else {
        SDB_UNREACHABLE();
      }
    },
    options.begin()->part);
}

struct TopTermsVisitor final : FilterVisitor {
  explicit TopTermsVisitor(size_t size) : _impl{size} { SDB_ASSERT(size); }

  void Prepare(const SubReader& segment, const TermReader& field,
               SeekTermIterator& terms) final {
    _impl.Prepare(segment, field, terms);
  }

  bool Visit(score_t boost) final {
    _impl.Visit(boost);
    return true;
  }

  field_visitor ToVisitor() {
    // TODO(mbkkt) we can avoid by_terms, but needs to change
    // TopTermsSelector, to make it able keep equal elements
    ByTermsOptions options;
    _impl.Visit([&](TopTerm<score_t>& term) {
      options.terms.emplace(std::move(term.term), term.key);
    });
    return [options = std::move(options)](const SubReader& segment,
                                          const TermReader& field,
                                          FilterVisitor& visitor) {
      return ByTerms::visit(segment, field, options, visitor);
    };
  }

 private:
  TopTermsSelector<TopTerm<score_t>> _impl;
};

struct GetVisitor {
  field_visitor operator()(const ByTermOptions& options) const {
    return [&](const SubReader& segment, const TermReader& field,
               FilterVisitor& visitor) {
      return ByTerm::Visit(segment, field, options, visitor);
    };
  }

  field_visitor operator()(const ByPrefixOptions& options) const {
    return [&](const SubReader& segment, const TermReader& field,
               FilterVisitor& visitor) {
      return ByPrefix::visit(segment, field, options, visitor);
    };
  }
  field_visitor operator()(const auto&) const { SDB_UNREACHABLE(); }

  field_visitor operator()(const AutomatonOptions& options) const {
    SDB_ASSERT(options.compiled);
    return AutomatonFilter::visitor(options.compiled->acceptor);
  }

  field_visitor operator()(const LevenshteinAutomatonOptions& options) const {
    if (options.max_terms != 0) {
      return {};
    }
    return LevenshteinAutomatonFilter::visitor(options);
  }

  field_visitor operator()(const ByTermsOptions& options) const {
    return [&](const SubReader& segment, const TermReader& field,
               FilterVisitor& visitor) {
      return ByTerms::visit(segment, field, options, visitor);
    };
  }

  field_visitor operator()(const ByRangeOptions& options) const {
    return [&](const SubReader& segment, const TermReader& field,
               FilterVisitor& visitor) {
      return ByRange::visit(segment, field, options, visitor);
    };
  }
};

// Filter visitor for phrase queries
template<typename PhraseStates>
class PhraseTermVisitor final : public FilterVisitor,
                                private util::Noncopyable {
 public:
  explicit PhraseTermVisitor(PhraseStates& phrase_states) noexcept
    : _phrase_states(phrase_states) {}

  void Prepare(const SubReader& segment, const TermReader& field,
               SeekTermIterator& terms) noexcept final {
    _segment = &segment;
    _reader = &field;
    _terms = &terms;
    _found = true;
  }

  bool Visit(score_t boost) final {
    SDB_ASSERT(_terms && _segment && _reader);
    _terms->read();

    // disallow negative boost
    boost = std::max(0.f, boost);

    // Only if it has scorer
    if (_part) {
      if (_term_offset >= _part->size()) {
        _part->emplace_back();
      }
      (*_part)[_term_offset].Collect(*_terms);
      ++_term_offset;
      _volatile_boost |= (boost != kNoBoost);
    }
    _phrase_states.emplace_back(_terms->cookie(), boost);
    return true;
  }

  void Reset() noexcept { _volatile_boost = false; }

  void Reset(std::vector<TermCollector>* part) noexcept {
    _found = false;
    _terms = nullptr;
    _part = part;
    _term_offset = 0;
  }

  bool Found() const noexcept { return _found; }

  bool VolatileBoost() const noexcept { return _volatile_boost; }

 private:
  const SubReader* _segment{};
  const TermReader* _reader{};
  PhraseStates& _phrase_states;
  std::vector<TermCollector>* _part = nullptr;
  SeekTermIterator* _terms = nullptr;
  size_t _term_offset = 0;
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

PhraseQueryKind GetKind(irs::field_id field, const ByPhraseOptions& options) {
  if (!irs::field_limits::valid(field) || options.empty()) {
    return PhraseQueryKind::kEmpty;
  }
  if (1 == options.size()) {
    const auto& part = options.begin()->part;
    // a single multi-term part reduces to that part's own filter; a terms-set
    // part keeps the phrase machinery (its frequency exposure differs)
    if (!std::get_if<ByTermsOptions>(&part)) {
      return PhraseQueryKind::kSingleWord;
    }
  }
  if (options.simple()) {
    return PhraseQueryKind::kFixed;
  }
  return PhraseQueryKind::kVariadic;
}

FixedPhraseQuery::positions_t MakeFixedPositions(
  const ByPhraseOptions& options) {
  FixedPhraseQuery::positions_t positions(options.size());
  auto pos_itr = positions.begin();
  PosAttr::value_t look_back = 0;
  for (const auto& term : options) {
    pos_itr->offs_max = term.offs_max;
    pos_itr->offs_min = term.offs_min;
    pos_itr->lead_offset = look_back += term.offs_max;
    ++pos_itr;
  }
  return positions;
}

QueryBuilder::ptr FixedPrepareSegment(const SubReader& segment,
                                      const PrepareContext& ctx,
                                      irs::field_id field,
                                      const ByPhraseOptions& options) {
  const auto phrase_size = options.size();
  auto* collector = ctx.collector
                      ? &sdb::basics::downCast<PhraseCollector>(*ctx.collector)
                      : nullptr;
  const auto* scorer = collector ? collector->GetScorer() : nullptr;
  const auto is_ord_empty = !scorer;

  FixedPhraseState state{ctx.memory};
  state.terms.reserve(phrase_size);

  const auto* reader = segment.field(field);
  if (Valid(reader)) {
    if (collector) {
      collector->Field().Collect(*reader);
    }

    PhraseTermVisitor<decltype(state.terms)> ptv(state.terms);

    size_t part = 0;
    for (const auto& word : options) {
      SDB_ASSERT(std::get_if<ByTermOptions>(&word.part));
      ptv.Reset(collector ? &collector->Part(part++) : nullptr);
      ByTerm::Visit(segment, *reader, std::get<ByTermOptions>(word.part), ptv);
      if (!ptv.Found() && is_ord_empty) {
        break;
      }
    }
  }

  if (state.terms.size() != phrase_size) {
    return QueryBuilder::Empty();
  }

  state.reader = reader;

  return memory::make_tracked<FixedPhraseQuery>(
    ctx.memory, segment, std::move(state), MakeFixedPositions(options),
    ctx.boost);
}

QueryBuilder::ptr VariadicPrepareSegment(const SubReader& segment,
                                         const PrepareContext& ctx,
                                         irs::field_id field,
                                         const ByPhraseOptions& options) {
  const auto phrase_size = options.size();
  auto* collector = ctx.collector
                      ? &sdb::basics::downCast<PhraseCollector>(*ctx.collector)
                      : nullptr;
  const auto* scorer = collector ? collector->GetScorer() : nullptr;
  const auto is_ord_empty = !scorer;

  VariadicPhraseState state{ctx.memory};
  state.terms.reserve(phrase_size);
  ManagedVector<size_t> num_terms(phrase_size, {ctx.memory});

  const auto* reader = segment.field(field);
  if (Valid(reader)) {
    if (collector) {
      collector->Field().Collect(*reader);
    }

    std::vector<field_visitor> phrase_part_visitors;
    phrase_part_visitors.reserve(phrase_size);
    std::vector<field_visitor*> all_terms_visitors;
    std::vector<TopTermsVisitor> top_terms_visitors;

    for (const auto& word : options) {
      auto& visitor =
        phrase_part_visitors.emplace_back(std::visit(GetVisitor{}, word.part));
      if (!visitor) {
        auto& opts = std::get<LevenshteinAutomatonOptions>(word.part);
        visitor = LevenshteinAutomatonFilter::visitor(opts);
        all_terms_visitors.push_back(&visitor);
        top_terms_visitors.emplace_back(opts.max_terms);
      }
    }

    if (!all_terms_visitors.empty()) {
      auto it = top_terms_visitors.begin();
      for (auto* visitor : all_terms_visitors) {
        (*visitor)(segment, *reader, *it++);
      }
      it = top_terms_visitors.begin();
      for (auto* visitor : all_terms_visitors) {
        *visitor = it++->ToVisitor();
      }
    }

    PhraseTermVisitor<decltype(state.terms)> ptv(state.terms);
    ptv.Reset();

    size_t found_parts = 0;
    for (auto& visitor : phrase_part_visitors) {
      const auto was_terms_count = state.terms.size();
      ptv.Reset(collector ? &collector->Part(found_parts) : nullptr);
      visitor(segment, *reader, ptv);
      const auto new_terms_count = state.terms.size() - was_terms_count;
      if (new_terms_count != 0) {
        num_terms[found_parts++] = new_terms_count;
      } else if (is_ord_empty) {
        break;
      }
    }

    if (found_parts == phrase_size) {
      state.num_terms = std::move(num_terms);
      state.reader = reader;
      state.volatile_boost = !is_ord_empty && ptv.VolatileBoost();
      SDB_ASSERT(phrase_size == state.num_terms.size());
    } else {
      state.terms.clear();
    }
  }

  if (!state.reader) {
    return QueryBuilder::Empty();
  }

  VariadicPhraseQuery::positions_t positions(phrase_size);
  auto position = positions.begin();
  PosAttr::value_t look_back = 0;
  for (const auto& term : options) {
    SDB_ASSERT(position != positions.end());
    position->offs_max = term.offs_max;
    position->offs_min = term.offs_min;
    position->lead_offset = look_back += term.offs_max;
    ++position;
  }

  return memory::make_tracked<VariadicPhraseQuery>(
    ctx.memory, segment, std::move(state), std::move(positions), ctx.boost);
}

}  // namespace

QueryBuilder::ptr ByPhrase::PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.Boost(Boost());
  switch (GetKind(field_id(), options())) {
    case PhraseQueryKind::kEmpty:
      return QueryBuilder::Empty();
    case PhraseQueryKind::kSingleWord:
      return MakeSinglePartFilter(field_id(), options())
        ->PrepareSegment(segment, sub_ctx);
    case PhraseQueryKind::kFixed:
      return FixedPrepareSegment(segment, sub_ctx, field_id(), options());
    case PhraseQueryKind::kVariadic:
      return VariadicPrepareSegment(segment, sub_ctx, field_id(), options());
  }
  return QueryBuilder::Empty();
}

PrepareCollector::ptr ByPhrase::MakeCollector(const Scorer* scorer) const {
  switch (GetKind(field_id(), options())) {
    case PhraseQueryKind::kEmpty:
      return std::make_unique<NoopCollector>();
    case PhraseQueryKind::kSingleWord:
      return MakeSinglePartFilter(field_id(), options())->MakeCollector(scorer);
    case PhraseQueryKind::kFixed:
    case PhraseQueryKind::kVariadic:
      return std::make_unique<PhraseCollector>(scorer, options().size());
  }
  return std::make_unique<NoopCollector>();
}

bool ByPhraseOptions::LowerParts() {
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
    } else if (const auto* e = std::get_if<ByEditDistanceOptions>(&info.part);
               e) {
      const auto max_terms = e->max_terms;
      info.part = ExecuteLevenshtein(
        e->max_distance, e->provider, e->with_transpositions, e->prefix,
        e->term, [] -> phrase_part { return ByTermsOptions{}; },
        [&] -> phrase_part {
          ByTermOptions opts;
          opts.term.reserve(e->prefix.size() + e->term.size());
          opts.term += e->prefix;
          opts.term += e->term;
          return opts;
        },
        [max_terms](const ParametricDescription& d, bytes_view prefix,
                    bytes_view term) -> phrase_part {
          return LevenshteinAutomatonOptions{d, prefix, term, max_terms};
        });
      changed = true;
    }
  }
  return changed;
}

}  // namespace irs
