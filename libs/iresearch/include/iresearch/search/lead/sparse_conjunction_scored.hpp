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

#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/search/common/score/make_conjunction.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Head, typename Tail>
class SparseConjunctionScored {
 public:
  SparseConjunctionScored(ScoreMergeType inner, Head&& head, Tail&& tail,
                          score_t absorbed = 0)
    : _head{std::move(head)},
      _tail{std::move(tail)},
      _absorbed{absorbed},
      _inner{inner} {}

  template<typename HeadArgs, typename TailArgs>
  SparseConjunctionScored(std::piecewise_construct_t, ScoreMergeType inner,
                          HeadArgs&& head, TailArgs&& tail,
                          score_t absorbed = 0)
    : _head{std::make_from_tuple<Head>(std::forward<HeadArgs>(head))},
      _tail{std::make_from_tuple<Tail>(std::forward<TailArgs>(tail))},
      _absorbed{absorbed},
      _inner{inner} {}

  SparseConjunctionScored(SparseConjunctionScored&&) = delete;
  SparseConjunctionScored& operator=(SparseConjunctionScored&&) = delete;

  doc_id_t Advance() { return Converge(_head.Advance()); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return Converge(_head.Seek(target));
  }

  doc_id_t Probe(doc_id_t target) { return Seek(target); }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    _head.FetchScoreArgs(slot);
    _tail.FetchScoreArgs(slot);
  }

  ScoreFunction PrepareScore() {
    std::vector<ScoreFunction> scorers;
    search::AppendScorer(scorers, _head.PrepareScore());
    _tail.CollectScorers(scorers);
    return search::MakeConjunctionScore(_inner, std::move(scorers), _absorbed);
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  doc_id_t Converge(doc_id_t doc) {
    while (!doc_limits::eof(doc)) {
      const auto probe = _tail.Probe(doc);
      if (probe == doc) {
        return _doc = doc;
      }
      doc = _head.Seek(probe);
    }
    return _doc = doc;
  }

  Head _head;
  Tail _tail;
  doc_id_t _doc = doc_limits::invalid();
  score_t _absorbed;
  ScoreMergeType _inner;
};

}  // namespace irs::lead
