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

#include "basics/exceptions.h"
#include "basics/noncopyable.hpp"
#include "basics/shared.hpp"
#include "basics/std.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_terms_visitor.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/search/top_terms_selector.hpp"
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

uint32_t Utf8TargetSize(bytes_view prefix, bytes_view term) {
  return std::max(1U, static_cast<uint32_t>(utf8_utils::Length(prefix) +
                                            utf8_utils::Length(term)));
}

QueryBuilder::ptr PrepareLevenshteinSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id field,
  const automaton& acceptor, uint32_t utf8_target_size, byte_type no_distance,
  size_t terms_limit) {
  if (!Validate(acceptor)) {
    return QueryBuilder::Empty();
  }

  auto matcher = MakeAutomatonMatcher(acceptor);

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, ScoreMergeType::Max, size_t{1});

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  auto& collector = sdb::basics::downCast<TermsCollector>(*ctx.collector);
  AllTermsVisitor term_collector{query->State(), collector.Field(),
                                 collector.Terms()};

  if (!terms_limit) {
    VisitImpl(segment, *reader, no_distance, utf8_target_size, matcher,
              term_collector);
  } else {
    TopTermsSelector<TopTerm<score_t>> selector{terms_limit};
    VisitImpl(segment, *reader, no_distance, utf8_target_size, matcher,
              selector);

    ByTermsOptions::search_terms terms;
    selector.Visit([&](TopTerm<score_t>& candidate) {
      terms.emplace(std::move(candidate.term), candidate.key);
    });
    ByTerms::visit(segment, *reader, terms, term_collector);
  }

  return query;
}

}  // namespace

field_visitor ByEditDistance::visitor(const ByEditDistanceAllOptions&) {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ByEditDistance must be lowered by the optimizer before visitor");
}

QueryBuilder::ptr ByEditDistance::PrepareSegment(const SubReader&,
                                                 const PrepareContext&) const {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ByEditDistance must be lowered by the optimizer before prepare");
}

QueryBuilder::ptr LevenshteinAutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id id,
  const LevenshteinAutomatonOptions& options) {
  return PrepareLevenshteinSegment(segment, ctx, id, options.acceptor,
                                   options.utf8_target_size,
                                   options.no_distance, options.max_terms);
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

QueryBuilder::ptr LevenshteinAutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  return PrepareSegment(segment, ctx.Boost(Boost()), field_id(), options());
}

PrepareCollector::ptr LevenshteinAutomatonFilter::MakeCollector(
  const Scorer* scorer) const {
  return std::make_unique<TermsCollector>(scorer, 1);
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
