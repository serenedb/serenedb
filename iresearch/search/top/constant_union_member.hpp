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

#include <algorithm>
#include <utility>

#include "iresearch/search/detail/posting_leaf.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/top/union_member.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Docs>
class ConstantUnionMember : public UnionMember {
 public:
  template<typename... Args>
  explicit ConstantUnionMember(score_t score, Args&&... args)
    : _docs{std::forward<Args>(args)...}, _score{score} {}

  doc_id_t Seek(doc_id_t target) final { return _doc = _docs.Seek(target); }

  doc_id_t AdvanceBlock(doc_id_t) final { return doc_limits::eof(); }

  score_t MaxScore(doc_id_t) final { return _score; }

  doc_id_t SkipNonCompetitive(doc_id_t first, doc_id_t second,
                              score_t threshold) final {
    return _score > threshold ? first : second;
  }

  void ForEachScoredBlock(doc_id_t max, Visit visit) final {
    Batches(max, visit);
  }

  void Fill(doc_id_t min, doc_id_t max, uint64_t* mask, score_t* window) final {
    Batches(
      max, [&](doc_id_t* docs, uint32_t len, score_t* scores) IRS_FORCE_INLINE {
        irs::detail::AddWhole<ScoreMergeType::Sum, false>(
          docs, docs + len, scores, min, nullptr, mask, window);
      });
  }

  uint32_t ScoreCandidates(doc_id_t* docs, score_t* scores, uint32_t count,
                           bool required) final {
    uint32_t out = 0;
    for (uint32_t i = 0; i != count; ++i) {
      const auto doc = docs[i];
      if (_doc < doc) {
        Seek(doc);
      }
      if (!required) {
        if (_doc == doc) {
          scores[i] += _score;
        }
      } else if (_doc == doc) {
        docs[out] = doc;
        scores[out] = scores[i] + _score;
        ++out;
      }
    }
    return required ? out : count;
  }

 private:
  template<typename Visitor>
  IRS_FORCE_INLINE void Batches(doc_id_t max, Visitor&& visit) {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kScoreBlock];
    ABSL_CACHELINE_ALIGNED score_t scores[kScoreBlock];
    while (_doc < max) {
      const auto len = lead::Gather(_docs, _doc, max, docs, kScoreBlock);
      std::fill_n(scores, len, _score);
      visit(docs, len, scores);
    }
  }

  Docs _docs;
  score_t _score;
};

}  // namespace irs::top
