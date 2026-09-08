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

#include "basics/shared.hpp"
#include "iresearch/search/common/score/make_conjunction.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs::probe {

template<typename Leaves>
class SparseConjunctionScored {
 public:
  SparseConjunctionScored(Leaves&& leaves, ScoreMergeType inner,
                          score_t absorbed = 0) noexcept
    : _leaves{std::move(leaves)}, _absorbed{absorbed}, _inner{inner} {}

  template<typename Args>
  SparseConjunctionScored(std::piecewise_construct_t, Args&& leaves,
                          ScoreMergeType inner, score_t absorbed = 0)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<Args>(leaves))},
      _absorbed{absorbed},
      _inner{inner} {}

  SparseConjunctionScored(SparseConjunctionScored&&) = delete;
  SparseConjunctionScored& operator=(SparseConjunctionScored&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    return _leaves.Probe(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    _leaves.FetchScoreArgs(slot);
  }

  ScoreFunction PrepareScore() {
    std::vector<ScoreFunction> scorers;
    _leaves.CollectScorers(scorers);
    return search::MakeConjunctionScore(_inner, std::move(scorers), _absorbed);
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    if (_absorbed != 0) {
      search::AppendScorer(out, PrepareScore());
      return;
    }
    _leaves.CollectScorers(out);
  }

 private:
  Leaves _leaves;
  score_t _absorbed;
  ScoreMergeType _inner;
};

}  // namespace irs::probe
