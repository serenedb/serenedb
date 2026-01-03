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

#include "iresearch/search/boolean_query.hpp"

#include "iresearch/search/conjunction.hpp"
#include "iresearch/search/disjunction.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"

namespace irs {
namespace {

template<bool Conjunction, typename It>
irs::ScoreAdapters MakeScoreAdapters(const ExecutionContext& ctx, It begin,
                                     It end) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  irs::ScoreAdapters itrs;
  itrs.reserve(size);
  if (Conjunction || size > 1) {
    ctx.wand.root = false;
    // TODO(mbkkt) ctx.wand.strict = true;
    // We couldn't do this for few reasons:
    // 1. It's small chance that we will use just term iterator (or + eof)
    // 2. I'm not sure about precision
  }
  do {
    auto docs = (*begin)->execute(ctx);
    ++begin;

    // filter out empty iterators
    if (irs::doc_limits::eof(docs->value())) {
      if constexpr (Conjunction) {
        return {};
      } else {
        continue;
      }
    }

    itrs.emplace_back(std::move(docs));
  } while (begin != end);

  return itrs;
}

// Returns disjunction iterator created from the specified queries
template<typename QueryIterator, typename... Args>
DocIterator::ptr MakeDisjunction(const ExecutionContext& ctx,
                                 irs::ScoreMergeType merge_type,
                                 QueryIterator begin, QueryIterator end,
                                 Args&&... args) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  // check the size before the execution
  if (0 == size) {
    // empty or unreachable search criteria
    return DocIterator::empty();
  }

  auto itrs = MakeScoreAdapters<false>(ctx, begin, end);
  if (itrs.empty()) {
    return DocIterator::empty();
  }

  return irs::ResolveMergeType(
    merge_type, ctx.scorers.buckets().size(),
    [&]<typename A>(A&& aggregator) -> DocIterator::ptr {
      using Disjunction = DisjunctionIterator<DocIterator::ptr, A>;
      return irs::MakeDisjunction<Disjunction>(ctx.wand, std::move(itrs),
                                               std::forward<A>(aggregator),
                                               std::forward<Args>(args)...);
    });
}

// Returns conjunction iterator created from the specified queries
template<typename QueryIterator, typename... Args>
DocIterator::ptr MakeConjunction(const ExecutionContext& ctx,
                                 irs::ScoreMergeType merge_type,
                                 QueryIterator begin, QueryIterator end,
                                 Args&&... args) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  // check size before the execution
  switch (size) {
    case 0:
      return DocIterator::empty();
    case 1:
      return (*begin)->execute(ctx);
  }

  auto itrs = MakeScoreAdapters<true>(ctx, begin, end);
  if (itrs.empty()) {
    return DocIterator::empty();
  }

  return irs::ResolveMergeType(
    merge_type, ctx.scorers.buckets().size(),
    [&]<typename A>(A&& aggregator) -> DocIterator::ptr {
      return irs::MakeConjunction(ctx.wand, std::forward<A>(aggregator),
                                  std::move(itrs), std::forward<Args>(args)...);
    });
}

}  // namespace

DocIterator::ptr BooleanQuery::execute(const ExecutionContext& ctx) const {
  if (empty()) {
    return DocIterator::empty();
  }

  SDB_ASSERT(_excl);
  const auto excl_begin = this->excl_begin();
  const auto end = this->end();

  auto incl = execute(ctx, begin(), excl_begin);

  if (excl_begin == end) {
    return incl;
  }

  // exclusion part does not affect scoring at all
  auto excl = MakeDisjunction(
    {.segment = ctx.segment, .scorers = Scorers::kUnordered, .ctx = ctx.ctx},
    irs::ScoreMergeType::Noop, excl_begin, end);

  // got empty iterator for excluded
  if (doc_limits::eof(excl->value())) {
    // pure conjunction/disjunction
    return incl;
  }

  return memory::make_managed<Exclusion>(std::move(incl), std::move(excl));
}

void BooleanQuery::visit(const irs::SubReader& segment,
                         irs::PreparedStateVisitor& visitor,
                         score_t boost) const {
  boost *= _boost;

  if (!visitor.Visit(*this, boost)) {
    return;
  }

  // FIXME(gnusi): visit exclude group?
  for (auto it = begin(), end = excl_begin(); it != end; ++it) {
    (*it)->visit(segment, visitor, boost);
  }
}

void BooleanQuery::prepare(const PrepareContext& ctx, ScoreMergeType merge_type,
                           queries_t queries, size_t exclude_start) {
  // apply boost to the current node
  _boost *= ctx.boost;
  // nothrow block
  _queries = std::move(queries);
  _excl = exclude_start;
  _merge_type = merge_type;
}

void BooleanQuery::prepare(const PrepareContext& ctx, ScoreMergeType merge_type,
                           std::span<const Filter* const> incl,
                           std::span<const Filter* const> excl) {
  queries_t queries{{ctx.memory}};
  queries.reserve(incl.size() + excl.size());
  // prepare included
  for (const auto* filter : incl) {
    queries.emplace_back(filter->prepare(ctx));
  }
  // prepare excluded
  for (const auto* filter : excl) {
    // exclusion part does not affect scoring at all
    queries.emplace_back(filter->prepare({
      .index = ctx.index,
      .memory = ctx.memory,
      .ctx = ctx.ctx,
    }));
  }
  prepare(ctx, merge_type, std::move(queries), incl.size());
}

DocIterator::ptr AndQuery::execute(const ExecutionContext& ctx, iterator begin,
                                   iterator end) const {
  return MakeConjunction(ctx, merge_type(), begin, end);
}

DocIterator::ptr OrQuery::execute(const ExecutionContext& ctx, iterator begin,
                                  iterator end) const {
  return MakeDisjunction(ctx, merge_type(), begin, end);
}

DocIterator::ptr MinMatchQuery::execute(const ExecutionContext& ctx,
                                        iterator begin, iterator end) const {
  SDB_ASSERT(std::distance(begin, end) >= 0);
  const auto size = size_t(std::distance(begin, end));

  // 1 <= min_match_count
  size_t min_match_count = std::max(size_t{1}, _min_match_count);

  // check the size before the execution
  if (0 == size || min_match_count > size) {
    // empty or unreachable search criteria
    return DocIterator::empty();
  } else if (min_match_count == size) {
    // pure conjunction
    return MakeConjunction(ctx, merge_type(), begin, end);
  }

  // min_match_count <= size
  min_match_count = std::min(size, min_match_count);

  auto itrs = MakeScoreAdapters<false>(ctx, begin, end);
  if (itrs.empty()) {
    return DocIterator::empty();
  }

  return ResolveMergeType(merge_type(), ctx.scorers.buckets().size(),
                          [&]<typename A>(A&& aggregator) -> DocIterator::ptr {
                            // FIXME(gnusi): use FAST version
                            using Disjunction =
                              MinMatchIterator<DocIterator::ptr, A>;
                            return MakeWeakDisjunction<Disjunction, A>(
                              ctx.wand, std::move(itrs), min_match_count,
                              std::forward<A>(aggregator));
                          });
}

}  // namespace irs
