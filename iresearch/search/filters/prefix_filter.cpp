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

#include "prefix_filter.hpp"

#include <algorithm>
#include <span>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/multiterm_collector.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/filter_visitor.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

class ByPrefixIterator : public WrappedTermIterator {
 public:
  ByPrefixIterator(const TermReader& reader, bytes_view prefix)
    : WrappedTermIterator{reader.iterator()}, _prefix{prefix} {}

  bool next() final {
    if (_started) {
      if (!_impl->next()) {
        return false;
      }
    } else {
      _started = true;
      if (SeekResult::End == _impl->seek_ge(_prefix)) {
        return false;
      }
    }
    return PrefixAcceptor{_prefix}(_impl->value());
  }

 private:
  bytes_view _prefix;
  bool _started{false};
};

class ByPrefixesIterator : public WrappedTermIterator {
 public:
  ByPrefixesIterator(const TermReader& reader,
                     std::span<const bstring> prefixes)
    : WrappedTermIterator{reader.iterator()}, _prefixes{prefixes} {
    if (!std::ranges::is_sorted(_prefixes)) {
      _sorted.assign(prefixes.begin(), prefixes.end());
      std::ranges::sort(_sorted);
      _prefixes = _sorted;
    }
  }

  bool next() final {
    if (_started) {
      if (!_impl->next()) {
        return false;
      }
    } else {
      _started = true;
      if (_prefixes.empty() ||
          SeekResult::End == _impl->seek_ge(_prefixes.front())) {
        return false;
      }
    }
    while (true) {
      const auto term = _impl->value();
      const bytes_view prefix = _prefixes[_current];
      if (term.starts_with(prefix)) {
        return true;
      }
      if (term < prefix) {
        if (SeekResult::End == _impl->seek_ge(prefix)) {
          return false;
        }
      } else if (++_current == _prefixes.size()) {
        return false;
      }
    }
  }

 private:
  std::vector<bstring> _sorted;
  std::span<const bstring> _prefixes;
  size_t _current{0};
  bool _started{false};
};

struct PrefixesAcceptor {
  std::span<const bstring> prefixes;

  bool operator()(bytes_view term) const noexcept {
    return std::ranges::any_of(
      prefixes, [&](bytes_view prefix) { return term.starts_with(prefix); });
  }
};

}  // namespace

QueryBuilder::ptr ByPrefix::PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.boost *= GetBoost();
  return PrepareSegment(segment, sub_ctx, field_id(), options().term);
}

QueryBuilder::ptr ByPrefix::PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx,
                                           const irs::field_id field,
                                           const bytes_view term) {
  const auto* reader = segment.field(field);
  if (!reader) {
    return QueryBuilder::Empty();
  }
  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, ScoreMergeType::Sum);
  MultiTermVisitor mtv{ctx, query->State(), *reader};
  ByPrefixIterator terms{*reader, term};
  if (!terms.next()) {
    return QueryBuilder::Empty();
  }
  mtv.Prepare(segment, *reader, terms.GetImpl());
  VisitTerms(terms, mtv);
  return MultiTermQuery::Finish(std::move(query), ctx);
}

PrepareCollector::ptr ByPrefix::MakeCollectorImpl(const Scorer* scorer,
                                                  StatsArena& stats,
                                                  uint32_t threads) const {
  if (!ScoresPerDoc(scorer)) {
    return std::make_unique<AllCollector>(scorer, stats);
  }
  return std::make_unique<MultiTermCollector>(scorer, stats, threads);
}

void ByPrefix::visit(const SubReader& segment, const TermReader& reader,
                     const ByPrefixOptions& options, FilterVisitor& visitor) {
  ByPrefixIterator terms{reader, options.term};
  if (!terms.next()) {
    return;
  }
  visitor.Prepare(segment, reader, terms.GetImpl());
  VisitTerms(terms, visitor);
}

TermPredicate::ptr ByPrefix::CompileTermPredicate() const {
  return MakeTermPredicate(PrefixAcceptor{options().term});
}

TermIterator::ptr ByPrefix::CompileTermIterator(
  const TermReader& reader) const {
  return memory::make_managed<ByPrefixIterator>(reader, options().term);
}

QueryBuilder::ptr ByPrefixes::PrepareSegment(const SubReader& segment,
                                             const PrepareContext& ctx) const {
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
  ByPrefixesIterator terms{*reader, options().prefixes};
  if (!terms.next()) {
    return QueryBuilder::Empty();
  }
  mtv.Prepare(segment, *reader, terms.GetImpl());
  VisitTerms(terms, mtv);
  return MultiTermQuery::Finish(std::move(query), sub_ctx);
}

PrepareCollector::ptr ByPrefixes::MakeCollectorImpl(const Scorer* scorer,
                                                    StatsArena& stats,
                                                    uint32_t threads) const {
  if (!ScoresPerDoc(scorer)) {
    return std::make_unique<AllCollector>(scorer, stats);
  }
  return std::make_unique<MultiTermCollector>(scorer, stats, threads);
}

TermPredicate::ptr ByPrefixes::CompileTermPredicate() const {
  return MakeTermPredicate(PrefixesAcceptor{options().prefixes});
}

TermIterator::ptr ByPrefixes::CompileTermIterator(
  const TermReader& reader) const {
  return memory::make_managed<ByPrefixesIterator>(reader, options().prefixes);
}

}  // namespace irs
