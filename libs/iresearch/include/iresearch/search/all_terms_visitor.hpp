////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/noncopyable.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/states/multiterm_state.hpp"

namespace irs {

template<typename State>
class AllTermsVisitor : public FilterVisitor, util::Noncopyable {
 public:
  AllTermsVisitor(State& state, ByTermsCollector* collector,
                  uint32_t thread) noexcept
    : _state{state}, _collector{collector}, _thread{thread} {}

  void Prepare(const SubReader&, const TermReader& field,
               TermIterator& terms) noexcept final {
    _state.Prepare(&field);

    _terms = &terms;
  }

  bool Visit(score_t boost) final {
    SDB_ASSERT(_terms);
    const auto& meta = _terms->cookie();
    const byte_type* stats = nullptr;
    if (_collector) {
      _collector->Term(_thread, _stat_index).Collect(meta);
      stats = _collector->Record(_stat_index).stats;
    }

    _state.Push(meta, boost, stats);
    return true;
  }

  void SetIndex(uint32_t term_idx) noexcept { _stat_index = term_idx; }

 private:
  State& _state;
  ByTermsCollector* _collector;
  uint32_t _thread;
  TermIterator* _terms{};
  uint32_t _stat_index = 0;
};

}  // namespace irs
