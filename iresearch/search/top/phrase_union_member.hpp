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

#pragma once

#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/score_filter.hpp"
#include "iresearch/search/detail/two_phase.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/top/phrase_bound.hpp"
#include "iresearch/search/top/union_member.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Slots>
class PhraseUnionMember : public UnionMember,
                          irs::detail::TwoPhaseBase<Slots, true> {
  static_assert(Slots::kHasFreqBound,
                "a phrase with no bound cannot join a pruned union");
  using Base = irs::detail::TwoPhaseBase<Slots, true>;
  using Base::_slots;
  using Base::FetchScoreArgs;

 public:
  template<typename... Args>
  PhraseUnionMember(const SubReader& segment, const TermReader& field,
                    const irs::detail::ScoreArgs& args, bool stored,
                    Args&&... slots)
    : Base{segment, field, args, std::forward<Args>(slots)...},
      _bound{segment, field, args, stored},
      _fetcher{*args.fetcher} {
    _func = Base::PrepareScore();
  }

  bool Bounded() const noexcept { return _bound.Bounded(); }

  doc_id_t Seek(doc_id_t target) final {
    SDB_ASSERT(target > _doc);
    return Settle(_slots.Seek(target));
  }

  doc_id_t AdvanceBlock(doc_id_t target) final {
    return _bound.AdvanceBlock(_slots, target);
  }

  score_t MaxScore(doc_id_t last) final {
    return _bound.MaxScore(_slots, last);
  }

  doc_id_t SkipNonCompetitive(doc_id_t first, doc_id_t, score_t) final {
    return first;
  }

  void ForEachScoredBlock(doc_id_t max, Visit visit) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kScoreBlock];
    ABSL_CACHELINE_ALIGNED score_t scores[kScoreBlock];
    while (_doc < max) {
      uint32_t len = 0;
      do {
        docs[len] = _doc;
        FetchScoreArgs(len);
        ++len;
        Settle(_slots.Next(_doc));
      } while (len != kScoreBlock && _doc < max);
      if (len == kScoreBlock) {
        FetchAndScoreBlock(_fetcher, _func, docs, scores);
      } else {
        FetchAndScore(_fetcher, _func, docs, scores, len);
      }
      visit(docs, len, scores);
    }
  }

  void Fill(doc_id_t min, doc_id_t max, uint64_t* mask, score_t* window) final {
    ForEachScoredBlock(max, [&](doc_id_t* docs, uint32_t len, score_t* scores) {
      irs::detail::AddWhole<ScoreMergeType::Sum, false>(
        docs, docs + len, scores, min, nullptr, mask, window);
    });
  }

  uint32_t ScoreCandidates(doc_id_t* docs, score_t* scores, uint32_t count,
                           bool required) final {
    ABSL_CACHELINE_ALIGNED doc_id_t hit_docs[kScoreBlock];
    ABSL_CACHELINE_ALIGNED score_t hit_scores[kScoreBlock];
    uint32_t slots[kScoreBlock];
    uint32_t hits = 0;
    uint32_t out = 0;
    const auto merge = [&] {
      irs::detail::MergeHits(required, hit_docs, slots, hit_scores, hits, docs,
                             scores, out);
      hits = 0;
    };
    for (uint32_t i = 0; i != count && !doc_limits::eof(_doc); ++i) {
      const auto doc = docs[i];
      if (_doc < doc) {
        Seek(doc);
      }
      if (_doc != doc) {
        continue;
      }
      hit_docs[hits] = doc;
      FetchScoreArgs(hits);
      slots[hits] = i;
      if (++hits == kScoreBlock) {
        FetchAndScoreBlock(_fetcher, _func, hit_docs, hit_scores);
        merge();
      }
    }
    if (hits != 0) {
      FetchAndScore(_fetcher, _func, hit_docs, hit_scores, hits);
      merge();
    }
    return required ? out : count;
  }

 private:
  doc_id_t Settle(doc_id_t doc) {
    while (!doc_limits::eof(doc) && !_slots.Match(doc)) {
      doc = _slots.Next(doc);
    }
    return _doc = doc;
  }

  PhraseBound _bound;
  ScoreFunction _func;
  ColumnArgsFetcher& _fetcher;
};

}  // namespace irs::top
