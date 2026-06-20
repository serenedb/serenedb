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

struct AggregatedStatsVisitor : util::Noncopyable {
  AggregatedStatsVisitor(MultiTermState& state, FieldCollector& field_stat,
                         TermCollector& term_stat) noexcept
    : state{state}, field_stat{field_stat}, term_stat{term_stat} {}

  void operator()(const SubReader&, const TermReader& field, uint32_t) const {
    if (!field_collected) {
      field_stat.Collect(field);
      field_collected = true;
    }
    state.Prepare(&field);
  }

  void operator()(SeekCookie::ptr& cookie) const {
    term_stat.Collect(*cookie);
    uint32_t docs_count = 0;
    if (auto* meta = irs::get<TermMeta>(*cookie)) {
      docs_count = meta->docs_count;
    }
    state.Push(MultiTermState::Entry{
      .cookie = std::move(cookie),
      .docs_count = docs_count,
      .boost = boost,
      .stat_offset = 0,
    });
  }

  MultiTermState& state;
  FieldCollector& field_stat;
  TermCollector& term_stat;
  score_t boost{kNoBoost};
  mutable bool field_collected{false};
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

uint32_t Utf8TargetSize(bytes_view prefix, bytes_view term) {
  return std::max(1U, static_cast<uint32_t>(utf8_utils::Length(prefix) +
                                            utf8_utils::Length(term)));
}

QueryBuilder::ptr PrepareLevenshteinSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id field,
  const automaton_table_matcher& matcher, uint32_t utf8_target_size,
  byte_type no_distance, size_t terms_limit, score_t boost) {
  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost * boost, ScoreMergeType::Max,
    size_t{1});

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  auto& collector = sdb::basics::downCast<ByTermsCollector>(*ctx.collector);

  if (!terms_limit) {
    AllTermsVisitor term_collector{query->State(), collector.Field(),
                                   collector.Terms()};
    VisitImpl(segment, *reader, no_distance, utf8_target_size, matcher,
              term_collector);
  } else {
    TopTermsSelector<TopTermState<score_t>> selector{terms_limit};
    VisitImpl(segment, *reader, no_distance, utf8_target_size, matcher,
              selector);

    AggregatedStatsVisitor aggregate_stats{query->State(), collector.Field(),
                                           collector.Terms()[0]};
    selector.Visit([&aggregate_stats](TopTermState<score_t>& s) {
      aggregate_stats.boost = std::max(0.f, s.key);
      s.Visit(aggregate_stats);
    });
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
  const LevenshteinAutomatonOptions& options, score_t boost) {
  SDB_ASSERT(options.compiled);
  return PrepareLevenshteinSegment(
    segment, ctx, id, options.compiled->matcher, options.utf8_target_size,
    options.no_distance, options.max_terms, boost);
}

field_visitor LevenshteinAutomatonFilter::visitor(
  const LevenshteinAutomatonOptions& options) {
  if (!options.compiled ||
      fst::kError == options.compiled->matcher.Properties(0)) {
    return [](const SubReader&, const TermReader&, FilterVisitor&) {};
  }

  return
    [compiled = options.compiled, utf8_target_size = options.utf8_target_size,
     no_distance = options.no_distance](const SubReader& segment,
                                        const TermReader& field,
                                        FilterVisitor& visitor) {
      return VisitImpl(segment, field, no_distance, utf8_target_size,
                       compiled->matcher, visitor);
    };
}

QueryBuilder::ptr LevenshteinAutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  return PrepareSegment(segment, ctx, field_id(), options(), Boost());
}

PrepareCollector::ptr LevenshteinAutomatonFilter::MakeCollector(
  const Scorer* scorer) const {
  return std::make_unique<ByTermsCollector>(scorer, 1);
}

LevenshteinAutomatonOptions::LevenshteinAutomatonOptions(
  const ParametricDescription& d, bytes_view prefix, bytes_view term,
  size_t max_terms)
  : compiled{std::make_shared<const CompiledAcceptor>(
      MakeLevenshteinAutomaton(d, prefix, term))},
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
      if (fst::kError == lowered.compiled->matcher.Properties(0)) {
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
