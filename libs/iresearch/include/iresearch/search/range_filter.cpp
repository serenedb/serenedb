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
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_selector.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_iterator.hpp"

namespace irs {

namespace {}  // namespace

namespace {

enum class RangeKind {
  Term,
  Empty,
  Range,
};

RangeKind Classify(const ByRangeOptions::range_type& rng) noexcept {
  if (rng.min_type != BoundType::Unbounded &&
      rng.max_type != BoundType::Unbounded && rng.min == rng.max) {
    if (rng.min_type == rng.max_type && rng.min_type == BoundType::Inclusive) {
      return RangeKind::Term;
    }
    return RangeKind::Empty;
  }
  return RangeKind::Range;
}

struct RangeBounds {
  bstring lower;
  bstring upper;
};

RangeBounds MakeBounds(const ByRangeFilterOptions::range_type& range) {
  if (range.max_type == BoundType::Exclusive && range.max.empty()) {
    return {AfterKey({}), AfterKey({})};
  }
  RangeBounds bounds;
  switch (range.min_type) {
    case BoundType::Unbounded:
      break;
    case BoundType::Inclusive:
      bounds.lower = range.min;
      break;
    case BoundType::Exclusive:
      bounds.lower = AfterKey(range.min);
      break;
  }
  switch (range.max_type) {
    case BoundType::Unbounded:
      break;
    case BoundType::Inclusive:
      bounds.upper = AfterKey(range.max);
      break;
    case BoundType::Exclusive:
      bounds.upper = range.max;
      break;
  }
  return bounds;
}

BoundedTermIterator RangeIterator(
  const TermReader& reader, const ByRangeFilterOptions::range_type& range) {
  const auto bounds = MakeBounds(range);
  return BoundedTermIterator{reader.iterator(), bounds.lower, bounds.upper};
}

}  // namespace

QueryBuilder::ptr ByRange::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.boost *= Boost();
  return PrepareSegment(segment, sub_ctx, field_id(), options().range, Boost());
}

QueryBuilder::ptr ByRange::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const irs::field_id field,
                                          const options_type::range_type& rng,
                                          score_t boost) {
  // TODO: optimize unordered case
  //  - seek to min
  //  - get ordinal position of the term
  //  - seek to max
  //  - get ordinal position of the term

  switch (Classify(rng)) {
    case RangeKind::Term:
      return ByTerm::PrepareSegment(segment, ctx, field, rng.min);
    case RangeKind::Empty:
      return QueryBuilder::Empty();
    case RangeKind::Range:
      break;
  }

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, ScoreMergeType::Sum, size_t{1});

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  auto* collector =
    ctx.collector
      ? &sdb::basics::downCast<LimitedTermsCollector>(*ctx.collector)
      : nullptr;
  if (collector) {
    collector->Field().Collect(*reader);
  }
  SampledMultiTermVisitor mtv{collector ? &collector->Limited() : nullptr,
                              query->State()};
  auto terms = RangeIterator(*reader, rng);
  if (terms.next()) {
    mtv.Prepare(segment, *reader, terms.GetImpl());
    VisitTerms(terms, mtv);
  }
  return query;
}

PrepareCollector::ptr ByRange::MakeCollector(const Scorer* scorer) const {
  if (Classify(options().range) == RangeKind::Term) {
    return std::make_unique<ByTermsCollector>(scorer, 1);
  }
  return std::make_unique<LimitedTermsCollector>(scorer,
                                                 options().scored_terms_limit);
}

void ByRange::visit(const SubReader& segment, const TermReader& reader,
                    const ByRangeOptions& options, FilterVisitor& visitor) {
  auto terms = RangeIterator(reader, options.range);
  if (!terms.next()) {
    return;
  }
  visitor.Prepare(segment, reader, terms.GetImpl());
  VisitTerms(terms, visitor);
}

TermPredicate::ptr ByRange::CompileTermPredicate() const {
  const auto& range = options().range;
  return MakeTermPredicate(RangeAcceptor{{&range}, {&range}});
}

TermIterator::ptr ByRange::CompileTermIterator(const TermReader& reader) const {
  const auto bounds = MakeBounds(options().range);
  return memory::make_managed<BoundedTermIterator>(reader.iterator(),
                                                   bounds.lower, bounds.upper);
}

}  // namespace irs
