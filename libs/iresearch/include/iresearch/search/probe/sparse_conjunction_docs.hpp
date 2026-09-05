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

#include "basics/shared.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<Type Leaf, size_t N = 0>
class SparseConjunctionDocs {
 public:
  template<typename Init>
  SparseConjunctionDocs(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)} {
    SDB_ASSERT(_leaves.size() > 1);
  }

  template<typename... Args>
  explicit SparseConjunctionDocs(std::piecewise_construct_t, Args&&... args)
    : _leaves{std::piecewise_construct, std::forward<Args>(args)...} {
    static_assert(N > 1);
  }

  SparseConjunctionDocs(SparseConjunctionDocs&&) = delete;
  SparseConjunctionDocs& operator=(SparseConjunctionDocs&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    for (auto& leaf : _leaves) {
      if (const auto probe = leaf.Probe(target); probe != target) {
        return probe;
      }
    }
    return target;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    for (auto& leaf : _leaves) {
      leaf.FetchScoreArgs(slot);
    }
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    for (auto& leaf : _leaves) {
      leaf.CollectScorers(out);
    }
  }

 private:
  search::RunOf<Leaf, N> _leaves;
};

class NoLeaves {
 public:
  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) noexcept { return target; }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t) noexcept {}

  void CollectScorers(std::vector<ScoreFunction>&) const noexcept {}
};

template<typename First, typename Second>
class BothLeaves {
 public:
  BothLeaves(First&& first, Second&& second) noexcept
    : _first{std::move(first)}, _second{std::move(second)} {}

  template<typename FirstArgs, typename SecondArgs>
  BothLeaves(std::piecewise_construct_t, FirstArgs&& first, SecondArgs&& second)
    : _first{std::make_from_tuple<First>(std::forward<FirstArgs>(first))},
      _second{std::make_from_tuple<Second>(std::forward<SecondArgs>(second))} {}

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (const auto probe = _first.Probe(target); probe != target) {
      return probe;
    }
    return _second.Probe(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    _first.FetchScoreArgs(slot);
    _second.FetchScoreArgs(slot);
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    _first.CollectScorers(out);
    _second.CollectScorers(out);
  }

 private:
  First _first;
  Second _second;
};

}  // namespace irs::probe
