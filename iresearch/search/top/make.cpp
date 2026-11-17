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

#include "iresearch/search/top/make.hpp"

#include <span>
#include <typeinfo>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/fold_reach.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/detail/phrase_match.hpp"
#include "iresearch/search/filters/query_profile.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"
#include "iresearch/search/top/empty.hpp"
#include "iresearch/search/top/family.hpp"

namespace irs::top {
namespace {

class ProfiledMember : public QueryProfile::Profiled<UnionMember> {
 public:
  ProfiledMember(UnionMember::ptr inner, QueryProfile::Executed& slot)
    : Profiled{std::move(inner), slot} {
    _doc = _inner->Value();
  }

  doc_id_t Seek(doc_id_t target) final {
    const auto doc = _counter.Sample([&] { return _inner->Seek(target); });
    _counter.Docs(doc == target);
    return _doc = doc;
  }

  doc_id_t AdvanceBlock(doc_id_t target) final {
    const auto last =
      _counter.Sample([&] { return _inner->AdvanceBlock(target); });
    _doc = _inner->Value();
    return last;
  }

  score_t MaxScore(doc_id_t last) final { return _inner->MaxScore(last); }

  doc_id_t SkipNonCompetitive(doc_id_t first, doc_id_t second,
                              score_t threshold) final {
    const auto doc = _counter.Sample(
      [&] { return _inner->SkipNonCompetitive(first, second, threshold); });
    _doc = _inner->Value();
    return doc;
  }

  void ForEachScoredBlock(doc_id_t max, Visit visit) final {
    uint64_t docs = 0;
    _counter.Time([&] {
      _inner->ForEachScoredBlock(
        max, [&](doc_id_t* block, uint32_t count, score_t* scores) {
          docs += count;
          visit(block, count, scores);
        });
    });
    _counter.Docs(docs);
    _doc = _inner->Value();
  }

  void Fill(doc_id_t min, doc_id_t max, uint64_t* mask, score_t* window) final {
    const auto before = QueryProfile::WindowDocs(mask, min, max);
    _counter.Time([&] { _inner->Fill(min, max, mask, window); });
    _counter.Docs(QueryProfile::WindowDocs(mask, min, max) - before);
    _doc = _inner->Value();
  }

  uint32_t ScoreCandidates(doc_id_t* docs, score_t* scores, uint32_t count,
                           bool required) final {
    const auto kept = _counter.Time(
      [&] { return _inner->ScoreCandidates(docs, scores, count, required); });
    _counter.Docs(kept);
    _doc = _inner->Value();
    return kept;
  }
};

class ProfiledClause : public QueryProfile::Profiled<PrunedClause> {
 public:
  using Profiled::Profiled;

  doc_id_t Probe(doc_id_t target) final {
    const auto doc = _counter.Sample([&] { return _inner->Probe(target); });
    _counter.Docs(doc == target);
    return doc;
  }

  doc_id_t AdvanceBlock(doc_id_t target) final {
    return _counter.Sample([&] { return _inner->AdvanceBlock(target); });
  }

  score_t MaxScore(doc_id_t last) final { return _inner->MaxScore(last); }

  void FetchScoreArgs(uint32_t slot) final { _inner->FetchScoreArgs(slot); }

  ScoreFunction PrepareScore() final { return _inner->PrepareScore(); }

  uint32_t TakeReads() final { return _inner->TakeReads(); }
};

template<typename Wrapper, typename Ptr>
Ptr ProfileMember(const QueryBuilder& query, Ptr node) {
  if (auto* profile = query.Profile(); profile && node) [[unlikely]] {
    return profile->WrapAs<Wrapper>(std::move(node), query.ProfileNode());
  }
  return node;
}

template<typename Query>
Root::ptr MakeAnyPhrase(const Query& query, const Context& ctx) {
  return irs::detail::ResolveMatch(
    query, [&]<irs::detail::PhraseMatch M> -> Root::ptr {
      if constexpr (M != irs::detail::PhraseMatch::Slop) {
        if (ctx.prune) {
          if (auto pruned = MakePhrasePruned<M>(query, ctx)) {
            return pruned;
          }
        }
      }
      return irs::detail::MakeScored<Family, irs::detail::PhraseKind<M>>(query,
                                                                         ctx);
    });
}

template<typename Result, typename Make>
Result MakePhraseOf(const QueryBuilder& query, Make&& make) {
  if (query.Kind() != QueryKind::Phrase) {
    return {};
  }
  const auto resolve = [&](const auto& phrase) {
    return irs::detail::ResolveMatch(
      phrase, [&]<irs::detail::PhraseMatch M> -> Result {
        if constexpr (M != irs::detail::PhraseMatch::Slop) {
          return make.template operator()<M>(phrase);
        } else {
          return {};
        }
      });
  };
  if (utils::downCast<PhraseQuery>(query).state.Fixed()) {
    return resolve(utils::downCast<FixedPhraseQuery>(query));
  }
  return resolve(utils::downCast<VariadicPhraseQuery>(query));
}

}  // namespace

UnionMember::ptr MakeUnionMember(const QueryBuilder& query,
                                 const Context& ctx) {
  return ProfileMember<ProfiledMember>(
    query, MakePhraseOf<UnionMember::ptr>(
             query, [&]<irs::detail::PhraseMatch M>(const auto& phrase) {
               return MakePhraseMember<M>(phrase, ctx);
             }));
}

PrunedClause::ptr MakeConjunctionClause(const QueryBuilder& query,
                                        const Context& ctx) {
  return ProfileMember<ProfiledClause>(
    query, MakePhraseOf<PrunedClause::ptr>(
             query, [&]<irs::detail::PhraseMatch M>(const auto& phrase) {
               return MakePhraseClause<M>(phrase, ctx);
             }));
}

Root::ptr MakeEmpty() { return memory::make_managed<Empty>(); }

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx) {
  if (query.Kind() == QueryKind::Empty) {
    return MakeEmpty();
  }
  const detail::FoldReachScope reach{ctx.span};
  return query.PlanTop(ctx);
}

Root::ptr Family::Make(const FixedPhraseQuery& query, const Context& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Root::ptr Family::Make(const VariadicPhraseQuery& query, const Context& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Root::ptr Family::Make(const TokenPhraseQuery& query, const Context& ctx) {
  if (PruneByBounds(ctx)) {
    if (auto pruned = MakeTokenPhrasePruned(query, ctx)) {
      return pruned;
    }
  }
  return irs::detail::MakeScored<Family, irs::detail::TokenPhraseKind>(query,
                                                                       ctx);
}

Root::ptr Family::Make(const TermQuery& query, const Context& ctx) {
  if (PruneByBounds(ctx) && query.State().cookie.docs_count != 1) {
    const irs::detail::PostingClause posting{
      .state = query.State(),
      .boost = query.Boost(),
      .stats = query.Stats(ScoredOf(ctx)),
    };
    if (auto pruned = MakePrunedPosting(posting, query.Segment(), ctx)) {
      return pruned;
    }
  }
  return irs::detail::MakeQuery<Family>(query, ctx);
}

Root::ptr Family::Make(const MultiTermQuery& query, const Context& ctx) {
  if (PruneByBounds(ctx)) {
    const auto& state = query.State();
    const auto* const field = state.Reader();
    const auto* const scorer = query.Stats(ScoredOf(ctx)).scorer;
    const auto boost = query.Boost();
    const std::span<const MultiTermState::Entry> terms{state.Terms()};
    if (terms.size() == 1) {
      const auto posting =
        irs::detail::ClauseOf(terms.front(), field, scorer, boost);
      if (posting.state.cookie.docs_count != 1) {
        if (auto pruned = MakePrunedPosting(posting, query.Segment(), ctx)) {
          return pruned;
        }
      }
    } else if (query.MergeType() == ScoreMergeType::Sum) {
      if (auto pruned = MakePrunedDisjunction<false>(
            terms, {}, irs::detail::UniformityOf(*field, scorer), field, scorer,
            boost, {}, {}, query.Segment(), ctx, ScoreMergeType::Sum)) {
        return pruned;
      }
    }
  }
  return irs::detail::MakeQuery<Family>(query, ctx);
}

}  // namespace irs::top
