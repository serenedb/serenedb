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

#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Include, typename Exclude>
class SparseExclusionScored {
 public:
  template<typename IncludeArgs, typename ExcludeArgs>
  SparseExclusionScored(std::piecewise_construct_t, IncludeArgs&& include,
                        ExcludeArgs&& exclude)
    : _include{std::make_from_tuple<Include>(
        std::forward<IncludeArgs>(include))},
      _exclude{
        std::make_from_tuple<Exclude>(std::forward<ExcludeArgs>(exclude))} {}

  SparseExclusionScored(SparseExclusionScored&&) = delete;
  SparseExclusionScored& operator=(SparseExclusionScored&&) = delete;

  doc_id_t Advance() { return Converge(_include.Advance()); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return Converge(_include.Seek(target));
  }

  doc_id_t Probe(doc_id_t target) { return Seek(target); }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    _include.FetchScoreArgs(slot);
  }

  ScoreFunction PrepareScore() { return _include.PrepareScore(); }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  doc_id_t Converge(doc_id_t doc) {
    while (!doc_limits::eof(doc) && _exclude.Probe(doc) == doc) {
      doc = _include.Advance();
    }
    return _doc = doc;
  }

  Include _include;
  Exclude _exclude;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
