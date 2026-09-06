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
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs::probe {

template<typename Include, Type Exclude>
class SparseExclusionScored {
 public:
  SparseExclusionScored(Include&& include, Exclude&& exclude) noexcept
    : _include{std::move(include)}, _exclude{std::move(exclude)} {}

  template<typename IncludeArgs, typename ExcludeArgs>
  SparseExclusionScored(std::piecewise_construct_t, IncludeArgs&& include,
                        ExcludeArgs&& exclude)
    : _include{std::make_from_tuple<Include>(
        std::forward<IncludeArgs>(include))},
      _exclude{
        std::make_from_tuple<Exclude>(std::forward<ExcludeArgs>(exclude))} {}

  SparseExclusionScored(SparseExclusionScored&&) = delete;
  SparseExclusionScored& operator=(SparseExclusionScored&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (const auto probe = _include.Probe(target); probe != target) {
      return probe;
    }
    if (_exclude.Probe(target) == target) {
      return target + 1;
    }
    return target;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    _include.FetchScoreArgs(slot);
  }

  ScoreFunction PrepareScore() { return _include.PrepareScore(); }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    _include.CollectScorers(out);
  }

 private:
  Include _include;
  Exclude _exclude;
};

}  // namespace irs::probe
