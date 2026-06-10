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

#include "basics/noncopyable.hpp"
#include "basics/shared.hpp"
#include "basics/std.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_terms_collector.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_collector.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/top_terms_collector.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/levenshtein_default_pdp.hpp"
#include "iresearch/utils/levenshtein_utils.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs {
namespace {

////////////////////////////////////////////////////////////////////////////////
/// @returns levenshtein similarity
////////////////////////////////////////////////////////////////////////////////
IRS_FORCE_INLINE score_t Similarity(uint32_t distance, uint32_t size) noexcept {
  SDB_ASSERT(size);

  static_assert(sizeof(score_t) == sizeof(uint32_t));

  return 1.f - static_cast<score_t>(distance) / static_cast<score_t>(size);
}

template<typename StatesType>
struct AggregatedStatsVisitor : util::Noncopyable {
  AggregatedStatsVisitor(StatesType& states, TermCollectorsFlat& stats) noexcept
    : stats(stats), states(states) {}

  void operator()(const irs::SubReader& segment, const irs::TermReader& field,
                  uint32_t docs_count) const {
    this->segment = &segment;
    this->field = &field;
    state = &states.insert(segment);
    state->reader = &field;
    state->scored_states_estimation += docs_count;
  }

  void operator()(SeekCookie::ptr& cookie) const {
    SDB_ASSERT(segment);
    SDB_ASSERT(field);
    stats.Collect(0, *cookie);
    state->scored_states.emplace_back(std::move(cookie), 0, boost);
  }

  TermCollectorsFlat& stats;
  StatesType& states;
  mutable typename StatesType::state_type* state{};
  mutable const SubReader* segment{};
  mutable const TermReader* field{};
  score_t boost{irs::kNoBoost};
};

class TopTermsCollectorImpl
  : public irs::TopTermsCollector<TopTermState<score_t>> {
 public:
  using BaseType = irs::TopTermsCollector<TopTermState<score_t>>;

  TopTermsCollectorImpl(size_t size, FieldCollector& stats)
    : BaseType(size), _stats(stats) {}

  void Prepare(const SubReader& segment, const TermReader& field,
               const SeekTermIterator& terms) {
    _stats.Collect(field);
    BaseType::Prepare(segment, field, terms);
  }

 private:
  FieldCollector& _stats;
};

//////////////////////////////////////////////////////////////////////////////
/// @brief visitation logic for levenshtein filter
/// @param segment segment reader
/// @param field term reader
/// @param matcher input matcher
/// @param visitor visitor
//////////////////////////////////////////////////////////////////////////////
template<typename Visitor>
void VisitImpl(const SubReader& segment, const TermReader& reader,
               const byte_type no_distance, const uint32_t utf8_target_size,
               const automaton_table_matcher& matcher, Visitor&& visitor) {
  SDB_ASSERT(fst::kError != matcher.Properties(0));
  auto terms = reader.iterator(matcher);

  if (!terms) [[unlikely]] {
    return;
  }

  if (terms->next()) {
    auto* payload = irs::get<PayAttr>(*terms);

    const byte_type* distance{&no_distance};
    if (payload && !payload->value.empty()) {
      distance = &payload->value.front();
    }

    visitor.Prepare(segment, reader, *terms);

    do {
      terms->read();

      const auto utf8_value_size =
        static_cast<uint32_t>(utf8_utils::Length(terms->value()));
      const auto boost =
        Similarity(*distance, std::min(utf8_value_size, utf8_target_size));

      visitor.Visit(boost);
    } while (terms->next());
  }
}

template<typename Collector>
bool CollectTerms(const IndexReader& index, irs::field_id id,
                  const automaton& acceptor, uint32_t utf8_target_size,
                  byte_type no_distance, Collector& collector) {
  if (!Validate(acceptor)) {
    return false;
  }

  auto matcher = MakeAutomatonMatcher(acceptor);

  for (auto& segment : index) {
    if (auto* reader = segment.field(id); reader) {
      VisitImpl(segment, *reader, no_distance, utf8_target_size, matcher,
                collector);
    }
  }

  return true;
}

uint32_t Utf8TargetSize(bytes_view prefix, bytes_view term) {
  return std::max(1U, static_cast<uint32_t>(utf8_utils::Length(prefix) +
                                            utf8_utils::Length(term)));
}

Filter::Query::ptr PrepareLevenshteinFilter(
  const PrepareContext& ctx, irs::field_id id, const automaton& acceptor,
  uint32_t utf8_target_size, byte_type no_distance, size_t terms_limit) {
  FieldCollector field_stats;
  TermCollectorsFlat term_stats{ctx.scorer, 1};
  MultiTermQuery::States states{ctx.memory, ctx.index.size()};

  if (!terms_limit) {
    AllTermsCollector term_collector{states, field_stats, term_stats};
    term_collector.stat_index(0);  // aggregate stats from different terms

    if (!CollectTerms(ctx.index, id, acceptor, utf8_target_size, no_distance,
                      term_collector)) {
      return Filter::Query::empty();
    }
  } else {
    TopTermsCollectorImpl term_collector(terms_limit, field_stats);

    if (!CollectTerms(ctx.index, id, acceptor, utf8_target_size, no_distance,
                      term_collector)) {
      return Filter::Query::empty();
    }

    AggregatedStatsVisitor aggregate_stats{states, term_stats};
    term_collector.Visit([&aggregate_stats](TopTermState<score_t>& state) {
      aggregate_stats.boost = std::max(0.f, state.key);
      state.Visit(aggregate_stats);
    });
  }

  MultiTermQuery::Stats stats(
    1, MultiTermQuery::Stats::allocator_type{ctx.memory});
  stats.back().resize(GetStatsSize(ctx.scorer), 0);
  auto* stats_buf = stats[0].data();
  term_stats.Finish(stats_buf, 0, &field_stats);

  return memory::make_tracked<MultiTermQuery>(ctx.memory, std::move(states),
                                              std::move(stats), ctx.boost,
                                              ScoreMergeType::Max, size_t{1});
}

}  // namespace

field_visitor ByEditDistance::visitor(const ByEditDistanceAllOptions& opts) {
  return ExecuteLevenshtein(
    opts.max_distance, opts.provider, opts.with_transpositions, opts.prefix,
    opts.term,
    [] -> field_visitor {
      return [](const SubReader&, const TermReader&, FilterVisitor&) {};
    },
    [&opts] -> field_visitor {
      // must copy term as it may point to temporary string
      return [target = opts.prefix + opts.term](const SubReader& segment,
                                                const TermReader& field,
                                                FilterVisitor& visitor) {
        return ByTerm::visit(segment, field, target, visitor);
      };
    },
    [](const ParametricDescription& d, const bytes_view prefix,
       const bytes_view term) -> field_visitor {
      struct AutomatonContext : util::Noncopyable {
        AutomatonContext(const ParametricDescription& d, bytes_view prefix,
                         bytes_view term)
          : acceptor(MakeLevenshteinAutomaton(d, prefix, term)),
            matcher(MakeAutomatonMatcher(acceptor)) {}

        automaton acceptor;
        automaton_table_matcher matcher;
      };

      auto ctx = std::make_shared<AutomatonContext>(d, prefix, term);

      if (!Validate(ctx->acceptor)) {
        return [](const SubReader&, const TermReader&, FilterVisitor&) {};
      }

      const auto utf8_term_size =
        std::max(1U, static_cast<uint32_t>(utf8_utils::Length(prefix) +
                                           utf8_utils::Length(term)));
      const uint8_t max_distance = d.max_distance() + 1;

      return [ctx = std::move(ctx), utf8_term_size, max_distance](
               const SubReader& segment, const TermReader& field,
               FilterVisitor& visitor) mutable {
        return VisitImpl(segment, field, max_distance, utf8_term_size,
                         ctx->matcher, visitor);
      };
    });
}

Filter::Query::ptr ByEditDistance::prepare(const PrepareContext&) const {
  SDB_UNREACHABLE();
  return Query::empty();
}

Filter::Query::ptr LevenshteinAutomatonFilter::prepare(
  const PrepareContext& ctx, irs::field_id id,
  const LevenshteinAutomatonOptions& options) {
  return PrepareLevenshteinFilter(ctx, id, options.acceptor,
                                  options.utf8_target_size, options.no_distance,
                                  options.max_terms);
}

field_visitor LevenshteinAutomatonFilter::visitor(
  const LevenshteinAutomatonOptions& options) {
  if (!Validate(options.acceptor)) {
    return [](const SubReader&, const TermReader&, FilterVisitor&) {};
  }

  struct AutomatonContext : util::Noncopyable {
    explicit AutomatonContext(const automaton& a)
      : matcher{MakeAutomatonMatcher(a)} {}

    automaton_table_matcher matcher;
  };

  auto ctx = AutomatonContext{options.acceptor};

  return [context = std::move(ctx), utf8_target_size = options.utf8_target_size,
          no_distance = options.no_distance](const SubReader& segment,
                                             const TermReader& field,
                                             FilterVisitor& visitor) mutable {
    return VisitImpl(segment, field, no_distance, utf8_target_size,
                     context.matcher, visitor);
  };
}

Filter::Query::ptr LevenshteinAutomatonFilter::prepare(
  const PrepareContext& ctx) const {
  return prepare(ctx.Boost(Boost()), field_id(), options());
}

LevenshteinAutomatonOptions::LevenshteinAutomatonOptions(
  const ParametricDescription& d, bytes_view prefix, bytes_view term,
  size_t max_terms)
  : acceptor{MakeLevenshteinAutomaton(d, prefix, term)},
    utf8_target_size{Utf8TargetSize(prefix, term)},
    no_distance{static_cast<byte_type>(d.max_distance() + 1)},
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
      filter->boost(boost);
      return filter;
    },
    [&](const ParametricDescription& d, const bytes_view prefix,
        const bytes_view term) -> Filter::ptr {
      LevenshteinAutomatonOptions lowered{d, prefix, term, opts.max_terms};
      if (!Validate(lowered.acceptor)) {
        return std::make_unique<Empty>();
      }
      auto filter = std::make_unique<LevenshteinAutomatonFilter>();
      *filter->mutable_field_id() = id;
      *filter->mutable_options() = std::move(lowered);
      filter->boost(boost);
      return filter;
    });
}

}  // namespace irs
