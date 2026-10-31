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

#include "iresearch/search/detail/two_phase.hpp"
#include "iresearch/search/top/phrase_bound.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Slots>
class PhrasePrunedClause : public irs::detail::TwoPhaseBase<Slots, true> {
  using Base = irs::detail::TwoPhaseBase<Slots, true>;
  using Base::_slots;

 public:
  template<typename... Args>
  PhrasePrunedClause(const SubReader& segment, const TermReader& field,
                     const irs::detail::ScoreArgs& args, Args&&... slots)
    : Base{segment, field, args, std::forward<Args>(slots)...},
      _bound{segment, field, args} {}

  doc_id_t Probe(doc_id_t target) {
    if (target <= _matched) {
      return _matched;
    }
    if (target < _least) {
      return _least;
    }
    if (const auto probe = _slots.Probe(target); probe != target) {
      return _least = probe;
    }
    ++_reads;
    if (!_slots.Match(target)) {
      return _least = target + 1;
    }
    return _matched = target;
  }

  doc_id_t AdvanceBlock(doc_id_t target) {
    return _bound.AdvanceBlock(_slots, target);
  }

  score_t MaxScore(doc_id_t last) { return _bound.MaxScore(_slots, last); }

  uint32_t TakeReads() noexcept { return std::exchange(_reads, 0); }

 private:
  PhraseBound _bound;
  doc_id_t _matched = doc_limits::invalid();
  doc_id_t _least = doc_limits::invalid();
  uint32_t _reads = 0;
};

}  // namespace irs::top
