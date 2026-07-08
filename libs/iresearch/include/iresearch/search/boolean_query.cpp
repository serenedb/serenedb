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

#include <iresearch/search/filter.hpp>

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting/iterator_doc.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/boost_iterator.hpp"
#include "iresearch/search/conjunction.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

// Executes child `query`, threading its own StatsBuffer subtree.
DocIterator::ptr ExecuteChild(const ExecutionContext& ctx,
                              const StatsBuffer& stats,
                              const QueryBuilder::ptr& query, size_t index) {
  if (!query) {
    return DocIterator::empty();
  }
  return query->Execute(
    ctx, stats.ChildCount() != 0 ? stats.Child(index) : StatsBuffer::Empty());
}

template<bool Conjunction, typename It>
ScoreAdapters MakeScoreAdapters(const ExecutionContext& ctx,
                                const StatsBuffer& stats, It begin, It end) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  ScoreAdapters itrs;
  itrs.reserve(size);
  for (size_t index = 0; begin != end; ++begin, ++index) {
    auto docs = ExecuteChild(ctx, stats, *begin, index);

    // filter out empty iterators
    if (doc_limits::eof(docs->value())) {
      if constexpr (Conjunction) {
        return {};
      } else {
        continue;
      }
    }

    itrs.emplace_back(std::move(docs));
  }

  // if (Conjunction || itrs.size() > 1) {
  //   TODO(mbkkt) ctx.wand.strict = true;
  // }
  return itrs;
}

// Returns disjunction iterator created from the specified queries
template<typename QueryIterator, typename... Args>
DocIterator::ptr MakeDisjunction(const ExecutionContext& ctx,
                                 const StatsBuffer& stats, size_t docs_count,
                                 ScoreMergeType merge_type, QueryIterator begin,
                                 QueryIterator end, Args&&... args) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  // check the size before the execution
  if (0 == size) {
    // empty or unreachable search criteria
    return DocIterator::empty();
  }

  auto itrs = MakeScoreAdapters<false>(ctx, stats, begin, end);
  if (itrs.empty()) {
    return DocIterator::empty();
  }

  return ResolveMergeType(stats.GetScorer() ? merge_type : ScoreMergeType::Noop,
                          [&]<ScoreMergeType MergeType> {
                            using Disjunction =
                              DisjunctionIterator<ScoreAdapter, MergeType>;
                            return MakeDisjunction<Disjunction>(
                              ctx.wand, static_cast<doc_id_t>(docs_count),
                              std::move(itrs), std::forward<Args>(args)...);
                          });
}

// Returns conjunction iterator created from the specified queries
template<typename QueryIterator, typename... Args>
DocIterator::ptr MakeConjunction(const ExecutionContext& ctx,
                                 const StatsBuffer& stats, doc_id_t docs_count,
                                 const Scorer* scorer,
                                 ScoreMergeType merge_type, QueryIterator begin,
                                 QueryIterator end, Args&&... args) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  // check size before the execution
  switch (size) {
    case 0:
      return DocIterator::empty();
    case 1:
      return ExecuteChild(ctx, stats, *begin, 0);
  }

  auto itrs = MakeScoreAdapters<true>(ctx, stats, begin, end);
  if (itrs.empty()) {
    return DocIterator::empty();
  }

  return MakeConjunction(scorer ? merge_type : ScoreMergeType::Noop, ctx.wand,
                         docs_count, std::move(itrs),
                         std::forward<Args>(args)...);
}

}  // namespace

DocIterator::ptr ExclusionQuery::Execute(const ExecutionContext& old,
                                         const StatsBuffer& stats) const {
  SDB_ASSERT(!_excludes.empty());
  ExecutionContext ctx{old};
  // TODO(mbkkt) enable back?
  ctx.wand.wand_enabled = false;

  const bool has_children = stats.ChildCount() != 0;

  auto incl = _include->Execute(
    ctx, has_children ? stats.Child(0) : StatsBuffer::Empty());

  ScoreAdapters excl_itrs;
  excl_itrs.reserve(_excludes.size());

  using TermWithFreq = PostingIteratorBase<
    IteratorTraitsImpl<FormatTraits128, true, false, false>>;
  using TermWithoutFreq = PostingIteratorBase<
    IteratorTraitsImpl<FormatTraits128, false, false, false>>;

  bool excl_has_term_with_freq = false;
  bool excl_has_term_without_freq = false;
  bool excl_has_abstract = false;

  for (size_t i = 0, count = _excludes.size(); i < count; ++i) {
    auto docs = _excludes[i]->Execute(
      ctx, has_children ? stats.Child(i + 1) : StatsBuffer::Empty());
    if (doc_limits::eof(docs->value())) {
      continue;
    }
    if (dynamic_cast<TermWithFreq*>(docs.get())) {
      excl_has_term_with_freq |= true;
    } else if (dynamic_cast<TermWithoutFreq*>(docs.get())) {
      excl_has_term_without_freq |= true;
    } else {
      excl_has_abstract |= true;
    }
    excl_itrs.emplace_back(std::move(docs));
  }

  if (excl_itrs.empty()) {
    return incl;
  }

  auto make =
    [&]<typename IncludeAdapter, typename ExcludeAdapter> -> DocIterator::ptr {
    using ExcludeAdapters = std::vector<ExcludeAdapter>;
    if (excl_itrs.size() == 1) {
      return memory::make_managed<
        ExclusionIterator<IncludeAdapter, ExcludeAdapter>>(
        IncludeAdapter{std::move(incl)},
        ExcludeAdapter{std::move(excl_itrs[0])});
    }
    if constexpr (std::is_same_v<ExcludeAdapters, ScoreAdapters>) {
      return memory::make_managed<
        ExclusionIterator<IncludeAdapter, ExcludeAdapters>>(
        IncludeAdapter{std::move(incl)}, std::move(excl_itrs));
    } else {
      ExcludeAdapters excl;
      excl.reserve(excl_itrs.size());
      for (auto& it : excl_itrs) {
        excl.emplace_back(std::move(it));
      }
      return memory::make_managed<
        ExclusionIterator<IncludeAdapter, ExcludeAdapters>>(
        IncludeAdapter{std::move(incl)}, std::move(excl));
    }
  };

  auto make_excl = [&]<typename IncludeAdapter>() -> DocIterator::ptr {
    if (excl_has_abstract ||
        (excl_has_term_without_freq && excl_has_term_with_freq)) {
      return make.template operator()<IncludeAdapter, ScoreAdapter>();
    }
    if (excl_has_term_with_freq) {
      return make
        .template operator()<IncludeAdapter, PostingAdapter<TermWithFreq>>();
    }
    SDB_ASSERT(excl_has_term_without_freq);
    return make
      .template operator()<IncludeAdapter, PostingAdapter<TermWithoutFreq>>();
  };

  if (dynamic_cast<TermWithFreq*>(incl.get())) {
    return make_excl.template operator()<PostingAdapter<TermWithFreq>>();
  } else if (dynamic_cast<TermWithoutFreq*>(incl.get())) {
    return make_excl.template operator()<PostingAdapter<TermWithoutFreq>>();
  } else {
    return make_excl.template operator()<ScoreAdapter>();
  }
}

void BooleanQuery::Visit(PreparedStateVisitor& visitor, score_t boost) const {
  boost *= _boost;

  if (!visitor.Visit(*this, boost)) {
    return;
  }

  for (auto it = begin(), last = end(); it != last; ++it) {
    (*it)->Visit(visitor, boost);
  }
}

DocIterator::ptr AndQuery::Execute(const ExecutionContext& ctx,
                                   const StatsBuffer& stats) const {
  const auto* scorer = stats.GetScorer();
  return MakeConjunction(ctx, stats,
                         static_cast<doc_id_t>(_segment.docs_count()), scorer,
                         merge_type(), begin(), end());
}

DocIterator::ptr OrQuery::Execute(const ExecutionContext& ctx,
                                  const StatsBuffer& stats) const {
  return MakeDisjunction(ctx, stats,
                         static_cast<doc_id_t>(_segment.docs_count()),
                         merge_type(), begin(), end());
}

DocIterator::ptr MinMatchQuery::Execute(const ExecutionContext& ctx,
                                        const StatsBuffer& stats) const {
  const auto begin = this->begin();
  const auto end = this->end();
  SDB_ASSERT(std::distance(begin, end) >= 0);
  const auto size = size_t(std::distance(begin, end));

  // 1 <= min_match_count
  size_t min_match_count = std::max(size_t{1}, _min_match_count);

  const auto* scorer = stats.GetScorer();

  // check the size before the execution
  if (0 == size || min_match_count > size) {
    // empty or unreachable search criteria
    return DocIterator::empty();
  } else if (min_match_count == size) {
    // pure conjunction
    return MakeConjunction(ctx, stats,
                           static_cast<doc_id_t>(_segment.docs_count()), scorer,
                           merge_type(), begin, end);
  }

  // min_match_count <= size
  min_match_count = std::min(size, min_match_count);

  auto itrs = MakeScoreAdapters<false>(ctx, stats, begin, end);
  if (itrs.empty()) {
    return DocIterator::empty();
  }

  return ResolveMergeType(
    scorer ? merge_type() : ScoreMergeType::Noop,
    [&]<ScoreMergeType MergeType> {
      // FIXME(gnusi): use FAST version
      using Disjunction = MinMatchIterator<ScoreAdapter, MergeType>;
      return MakeWeakDisjunction<Disjunction>(
        ctx.wand, static_cast<doc_id_t>(_segment.docs_count()), std::move(itrs),
        min_match_count);
    });
}

DocIterator::ptr BoostQuery::Execute(const ExecutionContext& old,
                                     const StatsBuffer& stats) const {
  ExecutionContext ctx{old};
  // TODO(mbkkt) enable back?
  ctx.wand.wand_enabled = false;

  const bool has_children = stats.ChildCount() != 0;
  const auto* scorer = stats.GetScorer();

  auto req =
    _req->Execute(ctx, has_children ? stats.Child(0) : StatsBuffer::Empty());
  if (!scorer || doc_limits::eof(req->value())) {
    return req;
  }

  using TermWithFreq = PostingIteratorBase<
    IteratorTraitsImpl<FormatTraits128, true, false, false>>;
  using TermAdapter = PostingAdapter<TermWithFreq>;

  ScoreAdapters opt_itrs;
  opt_itrs.reserve(_opt.size());
  bool opt_is_term = true;
  for (size_t i = 0, count = _opt.size(); i < count; ++i) {
    auto docs = _opt[i]->Execute(
      ctx, has_children ? stats.Child(i + 1) : StatsBuffer::Empty());
    if (doc_limits::eof(docs->value())) {
      continue;
    }
    if (!dynamic_cast<TermWithFreq*>(docs.get())) {
      opt_is_term = false;
    }
    opt_itrs.emplace_back(std::move(docs));
  }

  if (opt_itrs.empty()) {
    return req;
  }

  const bool req_is_term = dynamic_cast<TermWithFreq*>(req.get());

  auto make =
    [&]<typename RequiredAdapter, typename OptAdapter>() -> DocIterator::ptr {
    if (opt_itrs.size() == 1) {
      return memory::make_managed<BoostIterator<RequiredAdapter, OptAdapter>>(
        RequiredAdapter{std::move(req)}, OptAdapter{std::move(opt_itrs[0])});
    }
    std::vector<OptAdapter> opt_adapters;
    opt_adapters.reserve(opt_itrs.size());
    for (auto& it : opt_itrs) {
      opt_adapters.emplace_back(std::move(it));
    }
    return memory::make_managed<
      BoostIterator<RequiredAdapter, std::vector<OptAdapter>>>(
      RequiredAdapter{std::move(req)}, std::move(opt_adapters));
  };

  if (req_is_term && opt_is_term) {
    return make.template operator()<TermAdapter, TermAdapter>();
  }
  if (req_is_term) {
    return make.template operator()<TermAdapter, ScoreAdapter>();
  }
  if (opt_is_term) {
    return make.template operator()<ScoreAdapter, TermAdapter>();
  }
  return make.template operator()<ScoreAdapter, ScoreAdapter>();
}

void BoostQuery::Visit(PreparedStateVisitor& visitor, score_t boost) const {
  _req->Visit(visitor, boost);
}

}  // namespace irs
