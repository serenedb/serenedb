////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/search/filters/term_ranges_filter.hpp"

#include <absl/algorithm/container.h>

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/multiterm_collector.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/filter_visitor.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"

namespace irs {
namespace {

using Ranges = std::span<const TermRange>;

struct TermRangesAcceptor {
  Ranges ranges;

  bool operator()(bytes_view term) const noexcept {
    const auto range = absl::c_upper_bound(
      ranges, term, [](bytes_view value, const TermRange& range) {
        return value < range.min;
      });
    return range != ranges.begin() && term <= std::prev(range)->max;
  }
};

class ByTermRangesIterator : public WrappedTermIterator {
 public:
  ByTermRangesIterator(const TermReader& reader, Ranges ranges)
    : WrappedTermIterator{reader.iterator()}, _ranges{ranges} {}

  bool next() final {
    if (_ranges.empty()) {
      return false;
    }
    if (_started) {
      if (!_impl->next()) {
        return false;
      }
    } else {
      _started = true;
      if (!seek_min<true>(*_impl, _ranges.front().min)) {
        return false;
      }
    }
    for (; !_ranges.empty(); _ranges = _ranges.subspan(1)) {
      const auto& [min, max] = _ranges.front();
      if (_impl->value() > max) {
        continue;
      }
      if (_impl->value() >= min) {
        return true;
      }
      if (!seek_min<true>(*_impl, min)) {
        _ranges = {};
        return false;
      }
      if (_impl->value() <= max) {
        return true;
      }
    }
    return false;
  }

 private:
  Ranges _ranges;
  bool _started{false};
};

}  // namespace

QueryBuilder::ptr ByTermRanges::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto* reader = segment.field(field_id());
  if (!reader) {
    return QueryBuilder::Empty();
  }
  auto sub_ctx = ctx;
  sub_ctx.boost *= GetBoost();
  auto query = memory::make_tracked<MultiTermQuery>(
    sub_ctx.memory, segment, sub_ctx.memory, sub_ctx.boost,
    ScoreMergeType::Sum);
  MultiTermVisitor mtv{sub_ctx, query->State(), *reader};
  ByTermRangesIterator terms{*reader, options().ranges};
  if (!terms.next()) {
    return QueryBuilder::Empty();
  }
  mtv.Prepare(segment, *reader, terms.GetImpl());
  VisitTerms(terms, mtv);
  return MultiTermQuery::Finish(std::move(query), sub_ctx);
}

PrepareCollector::ptr ByTermRanges::MakeCollectorImpl(const Scorer* scorer,
                                                      StatsArena& stats,
                                                      uint32_t threads) const {
  if (!ScoresPerDoc(scorer)) {
    return std::make_unique<AllCollector>(scorer, stats);
  }
  return std::make_unique<MultiTermCollector>(scorer, stats, threads);
}

TermPredicate::ptr ByTermRanges::CompileTermPredicate() const {
  return MakeTermPredicate(TermRangesAcceptor{options().ranges});
}

TermIterator::ptr ByTermRanges::CompileTermIterator(
  const TermReader& reader) const {
  return memory::make_managed<ByTermRangesIterator>(reader, options().ranges);
}

}  // namespace irs
