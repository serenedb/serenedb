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
#include "iresearch/search/detail/posting_leaf.hpp"
#include "iresearch/search/detail/score_filter.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/top/phrase_bound.hpp"
#include "iresearch/search/top/union_member.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Slots>
class PhraseUnionMember : public UnionMember {
  static_assert(Slots::kHasFreqBound,
                "a phrase with no bound cannot join a pruned union");

 public:
  template<typename... Args>
  PhraseUnionMember(const SubReader& segment, const TermReader& field,
                    const irs::detail::ScoreArgs& args, Args&&... slots)
    : _slots{std::forward<Args>(slots)...},
      _bound{segment, field, args},
      _fetcher{*args.fetcher} {
    _provider.freq.value = _freqs;
    _score = irs::detail::PrepareScore(args, segment, field.meta(), _provider);
  }

  bool Bounded() const noexcept { return _bound.Bounded(); }

  doc_id_t Seek(doc_id_t target) final {
    if (target <= _doc) {
      return _doc;
    }
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
    while (_doc < max) {
      uint32_t len = 0;
      do {
        _docs[len] = _doc;
        _freqs[len] = _freq;
        ++len;
        Settle(_slots.Next(_doc));
      } while (len != kScoreBlock && _doc < max);
      ScoreBatch(len);
      visit(_docs, len, _scores);
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
    uint32_t slots[kScoreBlock];
    uint32_t hits = 0;
    uint32_t out = 0;
    const auto flush = [&] {
      ScoreBatch(hits);
      irs::detail::MergeHits(required, _docs, slots, _scores, hits, docs,
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
      _docs[hits] = doc;
      _freqs[hits] = _freq;
      slots[hits] = i;
      if (++hits == kScoreBlock) {
        flush();
      }
    }
    if (hits != 0) {
      flush();
    }
    return required ? out : count;
  }

 private:
  doc_id_t Settle(doc_id_t doc) {
    while (!doc_limits::eof(doc) && !_slots.Match(doc)) {
      doc = _slots.Next(doc);
    }
    _freq = _slots.Freq();
    return _doc = doc;
  }

  void ScoreBatch(uint32_t len) {
    FetchAndScore(_fetcher, _score, _docs, _scores, len);
  }

  ABSL_CACHELINE_ALIGNED doc_id_t _docs[kScoreBlock];
  ABSL_CACHELINE_ALIGNED score_t _scores[kScoreBlock];
  ABSL_CACHELINE_ALIGNED uint32_t _freqs[kScoreBlock];
  Slots _slots;
  PhraseBound _bound;
  irs::detail::LeafProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  uint32_t _freq = 0;
};

}  // namespace irs::top
