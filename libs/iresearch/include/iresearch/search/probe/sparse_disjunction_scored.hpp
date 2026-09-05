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
#include <vector>

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/score/make_probe.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<Type Leaf, size_t N = 0>
class SparseDisjunctionScored {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;
  static_assert(kBatch <= BitsRequired<uint32_t>());

  template<typename Init>
  SparseDisjunctionScored(size_t size, Init&& init, ScoreMergeType inner,
                          score_t absorbed = 0)
    : _leaves{size, std::forward<Init>(init)},
      _held{size},
      _absorbed{absorbed},
      _inner{inner} {
    SDB_ASSERT(_leaves.size() > 1);
  }

  SparseDisjunctionScored(SparseDisjunctionScored&&) = delete;
  SparseDisjunctionScored& operator=(SparseDisjunctionScored&&) = delete;

  doc_id_t Probe(doc_id_t target) {
    auto next = doc_limits::eof();
    for (size_t i = 0, count = _leaves.size(); i != count; ++i) {
      const auto doc = _leaves[i].Probe(target);
      if (doc == target) {
        _doc = target;
        _first = static_cast<uint32_t>(i);
        return target;
      }
      next = std::min(next, doc);
    }
    return next;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    SDB_ASSERT(slot < kBatch);
    SetBit(_held[_first], slot);
    _leaves[_first].FetchScoreArgs(slot);
    for (size_t i = _first + 1, count = _leaves.size(); i != count; ++i) {
      if (_leaves[i].Probe(_doc) != _doc) {
        continue;
      }
      SetBit(_held[i], slot);
      _leaves[i].FetchScoreArgs(slot);
    }
  }

  ScoreFunction PrepareScore() {
    return search::MakeProbeOf(_inner, _leaves, _held, _absorbed);
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  search::RunOf<Leaf, N> _leaves;
  search::RunOf<uint32_t, N> _held;
  score_t _absorbed;
  doc_id_t _doc = doc_limits::invalid();
  uint32_t _first = 0;
  ScoreMergeType _inner;
};

}  // namespace irs::probe
