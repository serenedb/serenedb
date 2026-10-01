////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "levenshtein_filter.hpp"

#include <absl/algorithm/container.h>

#include <array>
#include <memory>
#include <optional>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/all_terms_visitor.hpp"
#include "iresearch/search/detail/multiterm_collector.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/search/detail/top_terms_selector.hpp"
#include "iresearch/search/filters/automaton_filter.hpp"
#include "iresearch/search/filters/filter_visitor.hpp"
#include "iresearch/search/filters/term_filter.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/levenshtein_default_pdp.hpp"
#include "iresearch/utils/levenshtein_utils.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/std.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs {
namespace {

IRS_FORCE_INLINE score_t Similarity(uint32_t distance, uint32_t size) noexcept {
  SDB_ASSERT(size);

  static_assert(sizeof(score_t) == sizeof(uint32_t));

  return 1.f - static_cast<score_t>(distance) / static_cast<score_t>(size);
}

struct AggregatedStatsVisitor : util::Noncopyable {
  AggregatedStatsVisitor(MultiTermState& state,
                         BlendedTermsCollector* collector, uint32_t thread,
                         const byte_type* stats) noexcept
    : state{state}, collector{collector}, thread{thread}, stats{stats} {}

  void operator()(const SubReader&, const TermReader& field, uint32_t) const {
    state.Prepare(&field);
  }

  void operator()(const PostingMeta& cookie) const {
    if (collector) {
      collector->Collect(thread, term, cookie);
    }
    state.Push(cookie, boost, stats);
  }

  MultiTermState& state;
  BlendedTermsCollector* collector;
  uint32_t thread;
  const byte_type* stats;
  bytes_view term;
  score_t boost{kNoBoost};
};

class LevenshteinIterator : public WrappedTermIterator {
 public:
  LevenshteinIterator(const TermReader& reader,
                      const LevenshteinAutomatonOptions& options)
    : LevenshteinIterator{[&] -> SeekTermIterator::ptr {
                            SDB_ENSURE(options.source,
                                       "filter has no acceptor");
                            return options.source->Iterator(reader);
                          }(),
                          options} {}

  LevenshteinIterator(SeekTermIterator::ptr&& impl,
                      const LevenshteinAutomatonOptions& options)
    : WrappedTermIterator{std::move(impl)},
      _payload{irs::get<PayAttr>(*_impl)},
      _no_distance{options.no_distance},
      _target_size{options.utf8_target_size} {
    if (_payload && _payload->value.empty()) {
      _payload = nullptr;
    }
  }

  score_t Boost() const noexcept { return _boost.value; }

  bool next() final {
    if (!_impl->next()) {
      return false;
    }
    Score();
    return true;
  }

  bool SeekPast(bytes_view key) {
    if (_impl->seek_ge(AfterKey(key)) == SeekResult::End) {
      return false;
    }
    Score();
    return true;
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    if (irs::Type<TermBoost>::id() == id) {
      return &_boost;
    }
    return _impl->GetMutable(id);
  }

 private:
  void Score() noexcept {
    const byte_type distance =
      _payload ? _payload->value.front() : _no_distance;
    _boost.value = Similarity(distance, Utf8SizeUpTo(_impl->value()));
  }

  uint32_t Utf8SizeUpTo(bytes_view term) const noexcept {
    const auto* it = term.data();
    const auto* end = it + term.size();
    uint32_t size = 0;
    for (; it != end && size != _target_size; it = utf8_utils::Next(it, end)) {
      ++size;
    }
    return size;
  }

  const PayAttr* _payload;
  byte_type _no_distance;
  uint32_t _target_size;
  TermBoost _boost;
};

template<typename Visitor>
void VisitImpl(const SubReader& segment, const TermReader& reader,
               const LevenshteinAutomatonOptions& options, Visitor&& visitor) {
  SDB_ASSERT(options.parametric);
  LevenshteinIterator it(reader, options);
  if (!it.next()) {
    return;
  }
  visitor.Prepare(segment, reader, it.GetImpl());
  VisitTerms(it, visitor);
}

template<typename OnTerms, typename OnTerm>
void WalkTopTerms(const TermReader& reader,
                  const LevenshteinAutomatonOptions& options, size_t limit,
                  OnTerms&& on_terms, OnTerm&& on_term) {
  SDB_ASSERT(options.parametric);
  std::optional<LevenshteinIterator> it{std::in_place, reader, options};
  if (!it->next()) {
    return;
  }
  on_terms(it->GetImpl());
  const auto prefix = options.parametric->LowerBound();
  const auto term = bytes_view{options.target}.substr(prefix.size());
  std::unique_ptr<const LevenshteinAcceptor> narrow;
  auto distance = static_cast<uint8_t>(options.no_distance - 1);
  std::array<size_t, ParametricDescription::kMaxDistance + 1> reached{};
  const auto reaches = [&](score_t boost, uint8_t d) {
    const auto best = Similarity(d, options.utf8_target_size);
    return options.with_ties ? boost > best : boost >= best;
  };
  for (;;) {
    const auto boost = it->Boost();
    on_term(boost);
    for (auto d = distance; reaches(boost, d); --d) {
      ++reached[d];
      if (d == 0) {
        break;
      }
    }
    auto narrowed = distance;
    while (narrowed != 0 && reached[narrowed] >= limit) {
      --narrowed;
    }
    if (narrowed != distance) {
      const auto& description =
        options.provider(narrowed, options.with_transpositions);
      if (description) {
        distance = narrowed;
        const bstring last{it->value()};
        auto acceptor = std::make_unique<const LevenshteinAcceptor>(
          description, prefix, term);
        it.emplace(reader.iterator(*acceptor), options);
        narrow = std::move(acceptor);
        if (!it->SeekPast(last)) {
          return;
        }
        on_terms(it->GetImpl());
        continue;
      }
    }
    if (!it->next()) {
      return;
    }
  }
}

template<typename Selector>
void SelectTopTerms(const SubReader& segment, const TermReader& reader,
                    const LevenshteinAutomatonOptions& options, size_t limit,
                    AggregatedStatsVisitor& aggregate_stats) {
  Selector selector{limit};
  WalkTopTerms(
    reader, options, limit,
    [&](TermIterator& terms) { selector.Prepare(segment, reader, terms); },
    [&](score_t key) { selector.Visit(key); });
  selector.Visit([&aggregate_stats](TopTermState<score_t>& s) {
    aggregate_stats.boost = std::max(0.f, s.key);
    aggregate_stats.term = s.term;
    s.Visit(aggregate_stats);
  });
}

template<typename Selector>
std::vector<TopTerm<score_t>> SelectTerms(
  const TermReader& reader, const LevenshteinAutomatonOptions& options,
  size_t limit) {
  Selector selector{limit};
  WalkTopTerms(
    reader, options, limit,
    [&](TermIterator& terms) { selector.Prepare(reader, terms); },
    [&](score_t key) { selector.Visit(key); });
  std::vector<TopTerm<score_t>> selected;
  selector.Visit(
    [&](TopTerm<score_t>& term) { selected.push_back(std::move(term)); });
  absl::c_sort(selected, [](const auto& lhs, const auto& rhs) {
    return lhs.term < rhs.term;
  });
  return selected;
}

class SelectedTermsIterator : public TermIterator {
 public:
  SelectedTermsIterator(SeekTermIterator::ptr&& impl,
                        std::vector<TopTerm<score_t>>&& terms) noexcept
    : _impl{std::move(impl)}, _terms{std::move(terms)} {}

  bytes_view value() const noexcept final { return _impl->value(); }

  const PostingMeta& cookie() const final { return _impl->cookie(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return _impl->postings(features);
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    if (irs::Type<TermBoost>::id() == id) {
      return &_boost;
    }
    return _impl->GetMutable(id);
  }

  bool next() final {
    while (_next != _terms.size()) {
      const auto& selected = _terms[_next++];
      if (_impl->seek(selected.term)) {
        _boost.value = selected.key;
        return true;
      }
    }
    return false;
  }

 private:
  SeekTermIterator::ptr _impl;
  std::vector<TopTerm<score_t>> _terms;
  size_t _next{0};
  TermBoost _boost;
};

uint32_t Utf8TargetSize(bytes_view prefix, bytes_view term) {
  return std::max(1U, static_cast<uint32_t>(utf8_utils::Length(prefix) +
                                            utf8_utils::Length(term)));
}

QueryBuilder::ptr PrepareLevenshteinSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id field,
  const LevenshteinAutomatonOptions& options, size_t terms_limit,
  score_t boost) {
  const auto* reader = segment.field(field);
  if (!reader) {
    return QueryBuilder::Empty();
  }

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost * boost, ScoreMergeType::Max);
  auto* collector =
    ctx.collector ? &irs::utils::downCast<BlendedTermsCollector>(*ctx.collector)
                  : nullptr;
  if (collector) {
    collector->Field(ctx.thread).Collect(*reader);
  }

  const auto stats = ctx.Record().stats;
  if (!terms_limit) {
    AllTermsVisitor term_collector{query->State(), collector, ctx.thread,
                                   stats};
    VisitImpl(segment, *reader, options, term_collector);
  } else {
    AggregatedStatsVisitor aggregate_stats{query->State(), collector,
                                           ctx.thread, stats};
    if (options.with_ties) {
      SelectTopTerms<TiedTermsSelector<TopTermState<score_t>>>(
        segment, *reader, options, terms_limit, aggregate_stats);
    } else {
      SelectTopTerms<TopTermsSelector<TopTermState<score_t>>>(
        segment, *reader, options, terms_limit, aggregate_stats);
    }
  }

  return MultiTermQuery::Finish(std::move(query), ctx);
}

}  // namespace

QueryBuilder::ptr ByEditDistance::PrepareSegment(const SubReader&,
                                                 const PrepareContext&) const {
  THROW_SQL_ERROR(
    ERR_MSG("ByEditDistance must be lowered by the optimizer before prepare"));
}

QueryBuilder::ptr LevenshteinAutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id id,
  const LevenshteinAutomatonOptions& options, score_t boost) {
  SDB_ASSERT(options.parametric);
  return PrepareLevenshteinSegment(segment, ctx, id, options, options.max_terms,
                                   boost);
}

field_visitor LevenshteinAutomatonFilter::visitor(
  const LevenshteinAutomatonOptions& options) {
  if (!options.parametric) {
    return [](const SubReader&, const TermReader&, FilterVisitor&) {};
  }

  return [options](const SubReader& segment, const TermReader& field,
                   FilterVisitor& visitor) {
    return VisitImpl(segment, field, options, visitor);
  };
}

QueryBuilder::ptr LevenshteinAutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  return PrepareSegment(segment, ctx, field_id(), options(), GetBoost());
}

PrepareCollector::ptr LevenshteinAutomatonFilter::MakeCollectorImpl(
  const Scorer* scorer, StatsArena& stats, uint32_t threads) const {
  return std::make_unique<BlendedTermsCollector>(scorer, stats, threads);
}

LevenshteinAutomatonOptions::LevenshteinAutomatonOptions(
  const ParametricDescription& d, ByEditDistanceAllOptions::pdp_f provider,
  bool with_transpositions, bytes_view prefix, bytes_view term,
  size_t max_terms)
  : parametric{std::make_shared<const LevenshteinAcceptor>(d, prefix, term)},
    source{MakeFuzzySource(parametric)},
    provider{provider ? provider : &DefaultPDP},
    utf8_target_size{Utf8TargetSize(prefix, term)},
    no_distance{static_cast<byte_type>(d.max_distance() + 1)},
    with_transpositions{with_transpositions},
    max_terms{max_terms} {
  target.reserve(prefix.size() + term.size());
  target += prefix;
  target += term;
}

Filter::ptr LowerLevenshtein(irs::field_id id,
                             const ByEditDistanceOptions& opts, score_t boost) {
  return ExecuteLevenshtein(
    opts.max_distance, opts.provider, opts.with_transpositions, opts.prefix,
    opts.term, [] -> Filter::ptr { return std::make_unique<Empty>(); },
    [&] -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = id;
      auto& target = filter->mutable_options()->term;
      target.reserve(opts.prefix.size() + opts.term.size());
      target += opts.prefix;
      target += opts.term;
      filter->SetBoost(boost);
      return filter;
    },
    [&](const ParametricDescription& d, const bytes_view prefix,
        const bytes_view term) -> Filter::ptr {
      LevenshteinAutomatonOptions lowered{
        d,      opts.provider, opts.with_transpositions,
        prefix, term,          opts.max_terms};
      auto filter = std::make_unique<LevenshteinAutomatonFilter>();
      *filter->mutable_field_id() = id;
      *filter->mutable_options() = std::move(lowered);
      filter->SetBoost(boost);
      return filter;
    });
}

TermPredicate::ptr LevenshteinAutomatonFilter::CompileTermPredicate() const {
  if (!options().parametric) {
    return nullptr;
  }
  return MakeTermPredicate(
    [acceptor = options().parametric](bytes_view term) noexcept {
      return acceptor->Matches(term);
    });
}

TermPredicate::ptr ByEditDistance::CompileTermPredicate() const {
  auto lowered = LowerLevenshtein(field_id(), options(), kNoBoost);
  if (!lowered) {
    return nullptr;
  }
  auto predicate = lowered->CompileTermPredicate();
  if (!predicate) {
    return nullptr;
  }
  return MakeTermPredicate([lowered = std::move(lowered),
                            predicate = std::move(predicate)](bytes_view term) {
    return predicate->Accepts(term);
  });
}

TermIterator::ptr LevenshteinAutomatonFilter::CompileTermIterator(
  const TermReader& reader) const {
  if (!options().parametric) {
    return nullptr;
  }
  const auto limit = options().max_terms;
  if (limit == 0) {
    return memory::make_managed<LevenshteinIterator>(reader, options());
  }
  auto selected = options().with_ties
                    ? SelectTerms<TiedTermsSelector<TopTerm<score_t>>>(
                        reader, options(), limit)
                    : SelectTerms<TopTermsSelector<TopTerm<score_t>>>(
                        reader, options(), limit);
  return memory::make_managed<SelectedTermsIterator>(reader.iterator(),
                                                     std::move(selected));
}

}  // namespace irs
