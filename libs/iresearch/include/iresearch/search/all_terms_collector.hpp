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
#include "iresearch/search/scorer.hpp"

namespace irs {

template<typename States>
class AllTermsCollector : util::Noncopyable {
 public:
  AllTermsCollector(States& states, FieldCollectors& field_stats,
                    TermCollectors& term_stats) noexcept
    : _states(states), _field_stats(field_stats), _term_stats(term_stats) {}

  //////////////////////////////////////////////////////////////////////////////
  /// @brief prepare collector for terms collecting
  /// @param segment segment reader for the current term
  /// @param state state containing this scored term
  /// @param terms segment term-iterator positioned at the current term
  //////////////////////////////////////////////////////////////////////////////
  void Prepare(const SubReader& segment, const TermReader& field,
               const SeekTermIterator& terms) noexcept {
    _field_stats.collect(segment, field);

    auto& state = _states.insert(segment);
    state.reader = &field;

    _state.state = &state;
    _state.segment = &segment;
    _state.field = &field;
    _state.terms = &terms;

    // get term metadata
    auto* meta = irs::get<TermMeta>(terms);
    _state.docs_count = meta ? &meta->docs_count : &_no_docs;
  }

  void Visit(score_t boost) {
    SDB_ASSERT(_state);
    _term_stats.collect(*_state.segment, *_state.state->reader, _stat_index,
                        *_state.terms);

    auto& state = *_state.state;
    state.scored_states.emplace_back(_state.terms->cookie(), _stat_index,
                                     boost);
    state.scored_states_estimation += *_state.docs_count;
  }

  uint32_t stat_index() const noexcept { return _stat_index; }
  void stat_index(uint32_t stat_index) noexcept { _stat_index = stat_index; }

 private:
  struct CollectorState {
    const SubReader* segment{};
    const TermReader* field{};
    const SeekTermIterator* terms{};
    const uint32_t* docs_count{};
    typename States::state_type* state{};

    explicit operator bool() const noexcept {
      return segment && field && terms && state;
    }
  };

  CollectorState _state;
  States& _states;
  FieldCollectors& _field_stats;
  TermCollectors& _term_stats;
  uint32_t _stat_index = 0;
  const decltype(TermMeta::docs_count) _no_docs = 0;
};

}  // namespace irs
