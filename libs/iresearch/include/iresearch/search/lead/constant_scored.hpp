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

#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Leaf>
class ConstantScored {
 public:
  template<typename... Args>
  ConstantScored(score_t constant, Args&&... args)
    : _leaf{std::forward<Args>(args)...}, _constant{constant} {}

  ConstantScored(ConstantScored&&) = delete;
  ConstantScored& operator=(ConstantScored&&) = delete;

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Advance() { return _doc = _leaf.Advance(); }

  doc_id_t Seek(doc_id_t target) { return _doc = _leaf.Seek(target); }

  doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = _leaf.Seek(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t) noexcept {}

  ScoreFunction PrepareScore() { return ScoreFunction::Constant(_constant); }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  Leaf _leaf;
  score_t _constant;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
