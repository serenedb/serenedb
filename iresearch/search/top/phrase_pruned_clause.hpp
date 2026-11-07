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

#include "iresearch/search/probe/two_phase.hpp"
#include "iresearch/search/top/phrase_bound.hpp"

namespace irs::top {

template<typename Slots>
class PhrasePrunedClause : public probe::TwoPhase<Slots, true, true> {
  using Base = probe::TwoPhase<Slots, true, true>;
  using Base::_slots;

 public:
  template<typename... Args>
  PhrasePrunedClause(const SubReader& segment, const TermReader& field,
                     const irs::detail::ScoreArgs& args, Args&&... slots)
    : Base{segment, field, args, std::forward<Args>(slots)...},
      _bound{segment, field, args} {}

  doc_id_t AdvanceBlock(doc_id_t target) {
    return _bound.AdvanceBlock(_slots, target);
  }

  score_t MaxScore(doc_id_t last) { return _bound.MaxScore(_slots, last); }

 private:
  PhraseBound _bound;
};

}  // namespace irs::top
