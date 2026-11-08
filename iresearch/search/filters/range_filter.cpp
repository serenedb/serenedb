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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/multiterm_collector.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/filter_visitor.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

bool IsEmptyRange(const ByRangeOptions::range_type& rng) noexcept {
  return rng.min_type != BoundType::Unbounded &&
         rng.max_type != BoundType::Unbounded && rng.min == rng.max &&
         (rng.min_type != BoundType::Inclusive ||
          rng.max_type != BoundType::Inclusive);
}

class ByRangeIterator : public SeekFirstTermIterator<ByRangeIterator> {
 public:
  ByRangeIterator(const TermReader& reader,
                  const ByRangeFilterOptions::range_type& range)
    : SeekFirstTermIterator{reader.IteratorUntil(
        range.max_type == BoundType::Unbounded ? bytes_view{}
                                               : bytes_view{range.max})},
      _range{&range} {}

  bool Start() {
    switch (_range->min_type) {
      case BoundType::Unbounded:
        return _impl->next();
      case BoundType::Inclusive:
        return seek_min<true>(*_impl, _range->min);
      case BoundType::Exclusive:
        return seek_min<false>(*_impl, _range->min);
    }
    SDB_UNREACHABLE();
  }

  bool next() final {
    return Step() && RangeMaxAcceptor{_range}(_impl->value());
  }

 private:
  const ByRangeFilterOptions::range_type* _range;
};

}  // namespace

QueryBuilder::ptr ByRange::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.boost *= GetBoost();
  return PrepareSegment(segment, sub_ctx, segment.field(field_id(), dict()),
                        options().range);
}

QueryBuilder::ptr ByRange::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const TermReader* reader,
                                          const options_type::range_type& rng) {
  if (IsEmptyRange(rng) || !reader) {
    return QueryBuilder::Empty();
  }
  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, ScoreMergeType::Sum);
  MultiTermVisitor mtv{ctx, query->State(), *reader};
  ByRangeIterator terms{*reader, rng};
  if (!terms.next()) {
    return QueryBuilder::Empty();
  }
  VisitTerms(terms, mtv);
  return MultiTermQuery::Finish(std::move(query), ctx);
}

PrepareCollector::ptr ByRange::MakeCollectorImpl(const Scorer* scorer,
                                                 StatsArena& stats,
                                                 uint32_t threads) const {
  return MakeMultiTermCollector(scorer, stats, threads);
}

void ByRange::visit(const SubReader& segment, const TermReader& reader,
                    const ByRangeOptions& options, FilterVisitor& visitor) {
  ByRangeIterator terms{reader, options.range};
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
  return memory::make_managed<ByRangeIterator>(reader, options().range);
}

}  // namespace irs
