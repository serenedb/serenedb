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
#include "iresearch/search/all_terms_visitor.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_selector.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/term_filter.hpp"
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

template<typename Invalid, typename Term, typename Levenshtein>
inline auto ExecuteLevenshtein(uint8_t max_distance,
                               ByEditDistanceOptions::pdp_f provider,
                               bool with_transpositions,
                               const bytes_view prefix, const bytes_view target,
                               Invalid&& inv, Term&& t, Levenshtein&& lev) {
  if (!provider) {
    provider = &DefaultPDP;
  }

  if (0 == max_distance) {
    return t();
  }

  SDB_ASSERT(provider);
  const auto& d = (*provider)(max_distance, with_transpositions);

  if (!d) {
    return inv();
  }

  return lev(d, prefix, target);
}

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

QueryBuilder::ptr PrepareLevenshteinSegment(const SubReader& segment,
                                            const PrepareContext& ctx,
                                            std::string_view field,
                                            bytes_view prefix, bytes_view term,
                                            size_t terms_limit,
                                            const ParametricDescription& d) {
  const auto acceptor = MakeLevenshteinAutomaton(d, prefix, term);

  if (!Validate(acceptor)) {
    return QueryBuilder::Empty();
  }

  auto matcher = MakeAutomatonMatcher(acceptor);
  const auto utf8_term_size =
    std::max(1U, static_cast<uint32_t>(utf8_utils::Length(prefix) +
                                       utf8_utils::Length(term)));
  const uint8_t max_distance = d.max_distance() + 1;

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, ScoreMergeType::Max, size_t{1});

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  if (!terms_limit) {
    auto& collector = sdb::basics::downCast<TermsCollector>(*ctx.collector);
    AllTermsVisitor term_collector{query->State(), collector.Field(),
                                   collector.Terms()};
    VisitImpl(segment, *reader, max_distance, utf8_term_size, matcher,
              term_collector);
  } else {
    auto& collector =
      sdb::basics::downCast<ScoredTermsCollector>(*ctx.collector);
    collector.Field().Collect(*reader);
    SampledMultiTermVisitor mtv{collector.Limited(), query->State()};
    VisitImpl(segment, *reader, max_distance, utf8_term_size, matcher, mtv);
  }

  return query;
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
        return ByTerm::Visit(segment, field, target, visitor);
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

QueryBuilder::ptr ByEditDistance::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  auto sub_ctx = ctx.Boost(Boost());
  const auto field = this->field();
  const auto term = bytes_view{options().term};
  const auto prefix = bytes_view{options().prefix};
  const auto scored_terms_limit = options().max_terms;

  return ExecuteLevenshtein(
    options().max_distance, options().provider, options().with_transpositions,
    prefix, term, [&] -> QueryBuilder::ptr { return QueryBuilder::Empty(); },
    [&] -> QueryBuilder::ptr {
      if (!prefix.empty() && !term.empty()) {
        bstring target;
        target.reserve(prefix.size() + term.size());
        target += prefix;
        target += term;
        return ByTerm::PrepareSegment(segment, sub_ctx, field, target);
      }

      return ByTerm::PrepareSegment(segment, sub_ctx, field,
                                    prefix.empty() ? term : prefix);
    },
    [&](const ParametricDescription& d, const bytes_view prefix,
        const bytes_view term) -> QueryBuilder::ptr {
      return PrepareLevenshteinSegment(segment, sub_ctx, field, prefix, term,
                                       scored_terms_limit, d);
    });
}

PrepareCollector::ptr ByEditDistance::MakeCollector(
  const Scorer* scorer) const {
  const auto term = bytes_view{options().term};
  const auto prefix = bytes_view{options().prefix};
  const auto scored_terms_limit = options().max_terms;

  return ExecuteLevenshtein(
    options().max_distance, options().provider, options().with_transpositions,
    prefix, term,
    [&] -> PrepareCollector::ptr {
      return std::make_unique<TermsCollector>(scorer, 1);
    },
    [&] -> PrepareCollector::ptr {
      return std::make_unique<TermsCollector>(scorer, 1);
    },
    [&](const ParametricDescription&, const bytes_view,
        const bytes_view) -> PrepareCollector::ptr {
      if (!scored_terms_limit) {
        return std::make_unique<TermsCollector>(scorer, 1);
      }
      return std::make_unique<ScoredTermsCollector>(scorer, scored_terms_limit);
    });
}

}  // namespace irs
