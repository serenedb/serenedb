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

template<bool Conjunction, typename It, typename Exec>
ScoreAdapters MakeScoreAdapters(It begin, It end, Exec&& exec) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  ScoreAdapters itrs;
  itrs.reserve(size);
  do {
    auto docs = exec(begin);
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
template<typename QueryIterator, typename Exec, typename... Args>
DocIterator::ptr MakeDisjunction(const ExecutionContext& ctx,
                                 doc_id_t docs_count, const Scorer* scorer,
                                 ScoreMergeType merge_type, QueryIterator begin,
                                 QueryIterator end, Exec&& exec,
                                 Args&&... args) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  // check the size before the execution
  if (0 == size) {
    // empty or unreachable search criteria
    return DocIterator::empty();
  }

  auto itrs = MakeScoreAdapters<false>(begin, end, std::forward<Exec>(exec));
  if (itrs.empty()) {
    return DocIterator::empty();
  }

  return ResolveMergeType(
    scorer ? merge_type : ScoreMergeType::Noop, [&]<ScoreMergeType MergeType> {
      using Disjunction = DisjunctionIterator<ScoreAdapter, MergeType>;
      return MakeDisjunction<Disjunction>(ctx.wand, docs_count, std::move(itrs),
                                          std::forward<Args>(args)...);
    });
}

// Returns conjunction iterator created from the specified queries
template<typename QueryIterator, typename Exec, typename... Args>
DocIterator::ptr MakeConjunction(const ExecutionContext& ctx,
                                 doc_id_t docs_count, const Scorer* scorer,
                                 ScoreMergeType merge_type, QueryIterator begin,
                                 QueryIterator end, Exec&& exec,
                                 Args&&... args) {
  SDB_ASSERT(begin <= end);
  const size_t size = std::distance(begin, end);
  // check size before the execution
  switch (size) {
    case 0:
      return DocIterator::empty();
    case 1:
      return exec(begin);
  }

  auto itrs = MakeScoreAdapters<true>(begin, end, std::forward<Exec>(exec));
  if (itrs.empty()) {
    return DocIterator::empty();
  }

  return MakeConjunction(scorer ? merge_type : ScoreMergeType::Noop, ctx.wand,
                         docs_count, std::move(itrs),
                         std::forward<Args>(args)...);
}

}  // namespace

DocIterator::ptr BooleanQuery::Execute(const ExecutionContext& old) const {
  if (empty()) {
    return DocIterator::empty();
  }

  SDB_ASSERT(_excl);
  const auto excl_begin = this->excl_begin();
  const auto end = this->end();
  ExecutionContext ctx{old};
  if (excl_begin != end) {
    // TODO(mbkkt) enable back?
    ctx.wand.wand_enabled = false;
  }

  const bool has_children = ctx.Stats().ChildCount() != 0;
  const auto child_ctx = [&](iterator it) {
    ExecutionContext sub{ctx};
    if (has_children) {
      sub.stats = &ctx.Stats().Child(static_cast<size_t>(it - begin()));
    } else {
      sub.stats = nullptr;
    }
    return sub;
  };

  auto incl = Execute(ctx, begin(), excl_begin);

  if (excl_begin == end) {
    return incl;
  }

  // TODO(gnusi): rewrite this to use ByTerms

  ScoreAdapters excl_itrs;
  excl_itrs.reserve(std::distance(excl_begin, end));

  using TermWithFreq = PostingIteratorBase<
    IteratorTraitsImpl<FormatTraits128, true, false, false>>;
  using TermWithoutFreq = PostingIteratorBase<
    IteratorTraitsImpl<FormatTraits128, false, false, false>>;

  bool excl_has_term_with_freq = false;
  bool excl_has_term_without_freq = false;
  bool excl_has_abstract = false;

  for (auto it = excl_begin; it != end; ++it) {
    if (!*it) {
      // excludes nothing in this segment
      continue;
    }
    const auto sub = child_ctx(it);
    auto docs = (*it)->Execute(sub);
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
      return memory::make_managed<ExclusionIterator<IncludeAdapter, ExcludeAdapter>>(
        IncludeAdapter{std::move(incl)},
        ExcludeAdapter{std::move(excl_itrs[0])});
    }
    if constexpr (std::is_same_v<ExcludeAdapters, ScoreAdapters>) {
      return memory::make_managed<ExclusionIterator<IncludeAdapter, ExcludeAdapters>>(
        IncludeAdapter{std::move(incl)}, std::move(excl_itrs));
    } else {
      ExcludeAdapters excl;
      excl.reserve(excl_itrs.size());
      for (auto& it : excl_itrs) {
        excl.emplace_back(std::move(it));
      }
      return memory::make_managed<ExclusionIterator<IncludeAdapter, ExcludeAdapters>>(
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

  // FIXME(gnusi): visit exclude group?
  for (auto it = begin(), end = excl_begin(); it != end; ++it) {
    if (*it) {
      (*it)->Visit(visitor, boost);
    }
  }
}

DocIterator::ptr AndQuery::Execute(const ExecutionContext& ctx, iterator begin,
                                   iterator end) const {
  const bool has_children = ctx.Stats().ChildCount() != 0;
  const auto* scorer = ctx.Stats().GetScorer();
  const auto exec = [&](iterator it) {
    if (!*it) {
      return DocIterator::empty();
    }
    if (!has_children) {
      ExecutionContext sub{ctx};
      sub.stats = nullptr;
      return (*it)->Execute(sub);
    }
    ExecutionContext sub{ctx};
    sub.stats = &ctx.Stats().Child(static_cast<size_t>(it - this->begin()));
    return (*it)->Execute(sub);
  };
  return MakeConjunction(ctx, static_cast<doc_id_t>(_segment.docs_count()),
                         scorer, merge_type(), begin, end, exec);
}

DocIterator::ptr OrQuery::Execute(const ExecutionContext& ctx, iterator begin,
                                  iterator end) const {
  const bool has_children = ctx.Stats().ChildCount() != 0;
  const auto* scorer = ctx.Stats().GetScorer();
  const auto exec = [&](iterator it) {
    if (!*it) {
      return DocIterator::empty();
    }
    if (!has_children) {
      ExecutionContext sub{ctx};
      sub.stats = nullptr;
      return (*it)->Execute(sub);
    }
    ExecutionContext sub{ctx};
    sub.stats = &ctx.Stats().Child(static_cast<size_t>(it - this->begin()));
    return (*it)->Execute(sub);
  };
  return MakeDisjunction(ctx, static_cast<doc_id_t>(_segment.docs_count()),
                         scorer, merge_type(), begin, end, exec);
}

DocIterator::ptr MinMatchQuery::Execute(const ExecutionContext& ctx,
                                        iterator begin, iterator end) const {
  SDB_ASSERT(std::distance(begin, end) >= 0);
  const auto size = size_t(std::distance(begin, end));

  // 1 <= min_match_count
  size_t min_match_count = std::max(size_t{1}, _min_match_count);

  const bool has_children = ctx.Stats().ChildCount() != 0;
  const auto* scorer = ctx.Stats().GetScorer();
  const auto exec = [&](iterator it) {
    if (!*it) {
      return DocIterator::empty();
    }
    if (!has_children) {
      ExecutionContext sub{ctx};
      sub.stats = nullptr;
      return (*it)->Execute(sub);
    }
    ExecutionContext sub{ctx};
    sub.stats = &ctx.Stats().Child(static_cast<size_t>(it - this->begin()));
    return (*it)->Execute(sub);
  };

  // check the size before the execution
  if (0 == size || min_match_count > size) {
    // empty or unreachable search criteria
    return DocIterator::empty();
  } else if (min_match_count == size) {
    // pure conjunction
    return MakeConjunction(ctx, static_cast<doc_id_t>(_segment.docs_count()),
                           scorer, merge_type(), begin, end, exec);
  }

  // min_match_count <= size
  min_match_count = std::min(size, min_match_count);

  auto itrs = MakeScoreAdapters<false>(begin, end, exec);
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

DocIterator::ptr BoostQuery::Execute(const ExecutionContext& old) const {
  ExecutionContext ctx{old};
  // TODO(mbkkt) enable back?
  ctx.wand.wand_enabled = false;

  const bool has_children = ctx.Stats().ChildCount() != 0;
  const auto* scorer = ctx.Stats().GetScorer();

  ExecutionContext req_ctx{ctx};
  req_ctx.stats = has_children ? &ctx.Stats().Child(0) : nullptr;
  auto req = _req->Execute(req_ctx);
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
    ExecutionContext opt_ctx{ctx};
    opt_ctx.stats = has_children ? &ctx.Stats().Child(i + 1) : nullptr;
    auto docs = _opt[i]->Execute(opt_ctx);
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
