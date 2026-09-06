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
class SparseBoostScored {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;
  static_assert(kBatch <= BitsRequired<uint32_t>(),
                "the slots a member held are one word");

  template<typename Init>
  SparseBoostScored(size_t size, Init&& init, ScoreMergeType inner)
    : _leaves{size, std::forward<Init>(init)},
      _held{size, [](uint32_t& word, size_t) noexcept { word = 0; }},
      _inner{inner} {
    SDB_ASSERT(!_leaves.empty());
  }

  SparseBoostScored(SparseBoostScored&&) = delete;
  SparseBoostScored& operator=(SparseBoostScored&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    _doc = target;
    return target;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    SDB_ASSERT(slot < kBatch);
    for (size_t i = 0, count = _leaves.size(); i != count; ++i) {
      if (_leaves[i].Probe(_doc) != _doc) {
        continue;
      }
      SetBit(_held[i], slot);
      _leaves[i].FetchScoreArgs(slot);
    }
  }

  ScoreFunction PrepareScore() {
    return search::MakeProbeOf(_inner, _leaves, _held);
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  search::RunOf<Leaf, N> _leaves;
  search::RunOf<uint32_t, N> _held;
  doc_id_t _doc = doc_limits::invalid();
  ScoreMergeType _inner;
};

}  // namespace irs::probe
