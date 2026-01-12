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

#include "range_filter.hpp"

#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_collector.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs {
namespace {

template<typename Visitor, typename Comparer>
void CollectTerms(const SubReader& segment, const TermReader& field,
                  SeekTermIterator& terms, Visitor& visitor, Comparer cmp) {
  auto* term = irs::get<TermAttr>(terms);

  if (!term) [[unlikely]] {
    return;
  }

  if (cmp(term->value)) {
    // read attributes
    terms.read();

    visitor.Prepare(segment, field, terms);

    do {
      visitor.Visit(kNoBoost);

      if (!terms.next()) {
        break;
      }

      terms.read();
    } while (cmp(term->value));
  }
}

template<typename Visitor>
void VisitImpl(const SubReader& segment, const TermReader& reader,
               const ByRangeOptions::range_type& rng, Visitor& visitor) {
  auto terms = reader.iterator(SeekMode::NORMAL);

  if (!terms) [[unlikely]] {
    return;
  }

  auto res = false;

  // seek to min
  switch (rng.min_type) {
    case BoundType::Unbounded:
      res = terms->next();
      break;
    case BoundType::Inclusive:
      res = seek_min<true>(*terms, rng.min);
      break;
    case BoundType::Exclusive:
      res = seek_min<false>(*terms, rng.min);
      break;
  }

  if (!res) {
    // reached the end, nothing to collect
    return;
  }

  // now we are on the target or the next term
  const bytes_view max = rng.max;

  switch (rng.max_type) {
    case BoundType::Unbounded:
      CollectTerms(segment, reader, *terms, visitor,
                   [](bytes_view) { return true; });
      break;
    case BoundType::Inclusive:
      CollectTerms(segment, reader, *terms, visitor,
                   [max](bytes_view term) { return term <= max; });
      break;
    case BoundType::Exclusive:
      CollectTerms(segment, reader, *terms, visitor,
                   [max](bytes_view term) { return term < max; });
      break;
  }
}

}  // namespace

Filter::Query::ptr ByRange::prepare(const PrepareContext& ctx,
                                    std::string_view field,
                                    const options_type::range_type& rng,
                                    size_t scored_terms_limit) {
  // TODO: optimize unordered case
  //  - seek to min
  //  - get ordinal position of the term
  //  - seek to max
  //  - get ordinal position of the term

  if (rng.min_type != BoundType::Unbounded &&
      rng.max_type != BoundType::Unbounded && rng.min == rng.max) {
    if (rng.min_type == rng.max_type && rng.min_type == BoundType::Inclusive) {
      // degenerated case
      return ByTerm::prepare(ctx, field, rng.min);
    }

    // can't satisfy conditon
    return Query::empty();
  }

  // object for collecting order stats
  LimitedSampleCollector<TermFrequency> collector(
    ctx.scorers.empty() ? 0 : scored_terms_limit);
  MultiTermQuery::States states{ctx.memory, ctx.index.size()};
  MultiTermVisitor mtv{collector, states};

  for (const auto& segment : ctx.index) {
    if (const auto* reader = segment.field(field); reader) {
      VisitImpl(segment, *reader, rng, mtv);
    }
  }

  MultiTermQuery::Stats stats{{ctx.memory}};
  collector.score(ctx.index, ctx.scorers, stats);

  return memory::make_tracked<MultiTermQuery>(ctx.memory, std::move(states),
                                              std::move(stats), ctx.boost,
                                              ScoreMergeType::Sum, size_t{1});
}

void ByRange::visit(const SubReader& segment, const TermReader& reader,
                    const options_type::range_type& rng,
                    FilterVisitor& visitor) {
  VisitImpl(segment, reader, rng, visitor);
}

}  // namespace irs
