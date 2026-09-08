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

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/score/make_probe.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<Type Leaf, size_t N = 0>
class SparseThresholdScored {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;
  static_assert(kBatch <= BitsRequired<uint32_t>());

  template<typename Init>
  SparseThresholdScored(size_t size, Init&& init, uint32_t min_match,
                        ScoreMergeType inner, score_t absorbed = 0)
    : _leaves{size, std::forward<Init>(init)},
      _held{size},
      _matched{min_match},
      _absorbed{absorbed},
      _min_match{min_match},
      _inner{inner} {
    SDB_ASSERT(_min_match > 1);
    SDB_ASSERT(_leaves.size() >= _min_match);
  }

  SparseThresholdScored(SparseThresholdScored&&) = delete;
  SparseThresholdScored& operator=(SparseThresholdScored&&) = delete;

  doc_id_t Probe(doc_id_t target) {
    auto next = doc_limits::eof();
    uint32_t hits = 0;
    auto left = static_cast<uint32_t>(_leaves.size());
    for (size_t i = 0, count = _leaves.size(); i != count; ++i) {
      const auto doc = _leaves[i].Probe(target);
      if (doc == target) {
        _matched[hits++] = static_cast<uint32_t>(i);
        if (hits == _min_match) {
          _doc = target;
          return target;
        }
      } else {
        next = std::min(next, doc);
      }
      if (hits + --left < _min_match) {
        return target + 1;
      }
    }
    return next;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    SDB_ASSERT(slot < kBatch);
    for (const auto matched : _matched) {
      SetBit(_held[matched], slot);
      _leaves[matched].FetchScoreArgs(slot);
    }
    for (size_t i = _matched[_min_match - 1] + 1, count = _leaves.size();
         i != count; ++i) {
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
  search::RunOf<uint32_t, N> _matched;
  score_t _absorbed;
  doc_id_t _doc = doc_limits::invalid();
  uint32_t _min_match;
  ScoreMergeType _inner;
};

}  // namespace irs::probe
