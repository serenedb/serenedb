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
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<typename Slots, bool Scored, bool CountsReads = false>
class TwoPhase : public detail::TwoPhaseBase<Slots, Scored> {
  using Base = detail::TwoPhaseBase<Slots, Scored>;

 public:
  using Base::Base;

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
    if constexpr (CountsReads) {
      ++_reads;
    }
    if (!_slots.Match(target)) {
      return _least = target + 1;
    }
    return _matched = target;
  }

  uint32_t TakeReads() noexcept
    requires CountsReads
  {
    return std::exchange(_reads, 0);
  }

 protected:
  using Base::_slots;

 private:
  doc_id_t _matched = doc_limits::invalid();
  doc_id_t _least = doc_limits::invalid();
  [[no_unique_address]] utils::Need<CountsReads, uint32_t> _reads{};
};

template<typename Slots>
using TwoPhaseDocs = TwoPhase<Slots, false>;

template<typename Slots>
using TwoPhaseScored = TwoPhase<Slots, true>;

}  // namespace irs::probe
