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

template<bool Conjunction, typename It>
ScoreAdapters MakeScoreAdapters(const ExecutionContext& ctx, It begin, It end) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  ScoreAdapters itrs;
  itrs.reserve(size);
  do {
    auto docs = (*begin)->execute(ctx);
    ++begin;

    // filter out empty iterators
    if (doc_limits::eof(docs->value())) {
      if constexpr (Conjunction) {
        return {};
      } else {
        continue;
      }
    }

    itrs.emplace_back(std::move(docs));
  } while (begin != end);

  // if (Conjunction || itrs.size() > 1) {
  //   TODO(mbkkt) ctx.wand.strict = true;
  // }
  return itrs;
}

// Returns disjunction iterator created from the specified queries
template<typename QueryIterator, typename... Args>
DocIterator::ptr MakeDisjunction(const ExecutionContext& ctx,
                                 ScoreMergeType merge_type, QueryIterator begin,
                                 QueryIterator end, Args&&... args) {
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

  return ResolveMergeType(
    ctx.scorer ? merge_type : ScoreMergeType::Noop,
    [&]<ScoreMergeType MergeType> {
      using Disjunction = DisjunctionIterator<ScoreAdapter, MergeType>;
      return MakeDisjunction<Disjunction>(
        ctx.wand, static_cast<doc_id_t>(ctx.segment.docs_count()),
        std::move(itrs), std::forward<Args>(args)...);
    });
}

// Returns conjunction iterator created from the specified queries
template<typename QueryIterator, typename... Args>
DocIterator::ptr MakeConjunction(const ExecutionContext& ctx,
                                 ScoreMergeType merge_type, QueryIterator begin,
                                 QueryIterator end, Args&&... args) {
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

  return MakeConjunction(ctx.scorer ? merge_type : ScoreMergeType::Noop,
                         ctx.wand,
                         static_cast<doc_id_t>(ctx.segment.docs_count()),
                         std::move(itrs), std::forward<Args>(args)...);
}

DocIterator::ptr WrapExclusion(DocIterator::ptr incl, DocIterator::ptr excl) {
  using TermWithFreq = PostingIteratorBase<
    IteratorTraitsImpl<FormatTraits128, true, false, false>>;
  using TermWithoutFreq = PostingIteratorBase<
    IteratorTraitsImpl<FormatTraits128, false, false, false>>;

  auto make = [&]<typename IncludeAdapter, typename ExcludeAdapter>()
    -> DocIterator::ptr {
    return memory::make_managed<
      ExclusionIterator<IncludeAdapter, ExcludeAdapter>>(
      IncludeAdapter{std::move(incl)}, ExcludeAdapter{std::move(excl)});
  };

  auto make_excl = [&]<typename IncludeAdapter>() -> DocIterator::ptr {
    if (dynamic_cast<TermWithFreq*>(excl.get())) {
      return make
        .template operator()<IncludeAdapter, PostingAdapter<TermWithFreq>>();
    }
    if (dynamic_cast<TermWithoutFreq*>(excl.get())) {
      return make
        .template operator()<IncludeAdapter, PostingAdapter<TermWithoutFreq>>();
    }
    return make.template operator()<IncludeAdapter, ScoreAdapter>();
  };

  if (dynamic_cast<TermWithFreq*>(incl.get())) {
    return make_excl.template operator()<PostingAdapter<TermWithFreq>>();
  } else if (dynamic_cast<TermWithoutFreq*>(incl.get())) {
    return make_excl.template operator()<PostingAdapter<TermWithoutFreq>>();
  } else {
    return make_excl.template operator()<ScoreAdapter>();
  }
}

}  // namespace

DocIterator::ptr BooleanQuery::execute(const ExecutionContext& ctx) const {
  if (empty()) {
    return DocIterator::empty();
  }
  return execute(ctx, begin(), end());
}

DocIterator::ptr ExclusionQuery::execute(const ExecutionContext& old) const {
  ExecutionContext ctx{old};
  // TODO(mbkkt) enable back?
  ctx.wand.wand_enabled = false;

  auto incl = _include->execute(ctx);

  for (const auto& exclude : _excludes) {
    auto excl = exclude->execute(ctx);
    if (doc_limits::eof(excl->value())) {
      continue;
    }
    incl = WrapExclusion(std::move(incl), std::move(excl));
  }

  return incl;
}

void BooleanQuery::visit(const SubReader& segment,
                         PreparedStateVisitor& visitor, score_t boost) const {
  boost *= _boost;

  if (!visitor.Visit(*this, boost)) {
    return;
  }

  for (auto it = begin(), last = end(); it != last; ++it) {
    (*it)->visit(segment, visitor, boost);
  }
}

void BooleanQuery::prepare(const PrepareContext& ctx, ScoreMergeType merge_type,
                           queries_t queries) {
  // apply boost to the current node
  _boost *= ctx.boost;
  // nothrow block
  _queries = std::move(queries);
  _merge_type = merge_type;
}

Filter::Query::ptr PrepareExclusion(const PrepareContext& ctx,
                                    const Filter::ptr& include,
                                    std::span<const Filter::ptr> excludes) {
  SDB_ASSERT(!excludes.empty());
  Filter::Query::ptr incl;
  if (include == nullptr) {
    auto all = AllDocsProvider::Default(kNoBoost);
    incl = all->prepare(ctx);
  } else {
    incl = include->prepare(ctx);
  }

  // exclusion part does not affect scoring at all
  const PrepareContext excl_ctx{
    .index = ctx.index,
    .memory = ctx.memory,
    .ctx = ctx.ctx,
  };
  std::vector<Filter::Query::ptr> excl;
  excl.reserve(excludes.size());
  for (const auto& exclude : excludes) {
    SDB_ASSERT(exclude);
    excl.emplace_back(exclude->prepare(excl_ctx));
  }

  return memory::make_tracked<ExclusionQuery>(ctx.memory, std::move(incl),
                                              std::move(excl));
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

  return ResolveMergeType(
    ctx.scorer ? merge_type() : ScoreMergeType::Noop,
    [&]<ScoreMergeType MergeType> {
      // FIXME(gnusi): use FAST version
      using Disjunction = MinMatchIterator<ScoreAdapter, MergeType>;
      return MakeWeakDisjunction<Disjunction>(
        ctx.wand, static_cast<doc_id_t>(ctx.segment.docs_count()),
        std::move(itrs), min_match_count);
    });
}

void BoostQuery::Prepare(const PrepareContext& ctx, const Filter& req,
                         const Filter& opt) {
  _req = req.prepare(ctx);
  const auto tid = opt.type();
  if (tid == irs::Type<And>::id() || tid == irs::Type<Or>::id()) {
    const auto& opt_bool = sdb::basics::downCast<BooleanFilter>(opt);
    const auto opt_ctx = ctx.Boost(opt_bool.Boost());
    _opt.reserve(opt_bool.size());
    for (const auto& opt_filter : opt_bool) {
      _opt.emplace_back(opt_filter->prepare(opt_ctx));
    }
    return;
  }
  _opt.emplace_back(opt.prepare(ctx));
}

DocIterator::ptr BoostQuery::execute(const ExecutionContext& old) const {
  ExecutionContext ctx{old};
  // TODO(mbkkt) enable back?
  ctx.wand.wand_enabled = false;
  auto req = _req->execute(ctx);
  if (!ctx.scorer || doc_limits::eof(req->value())) {
    return req;
  }

  using TermWithFreq = PostingIteratorBase<
    IteratorTraitsImpl<FormatTraits128, true, false, false>>;
  using TermAdapter = PostingAdapter<TermWithFreq>;

  ScoreAdapters opt_itrs;
  opt_itrs.reserve(_opt.size());
  bool opt_is_term = true;
  for (const auto& q : _opt) {
    auto docs = q->execute(ctx);
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

void BoostQuery::visit(const SubReader& segment, PreparedStateVisitor& visitor,
                       score_t boost) const {
  _req->visit(segment, visitor, boost);
}

}  // namespace irs
