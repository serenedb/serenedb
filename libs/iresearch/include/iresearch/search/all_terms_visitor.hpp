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
#include "iresearch/search/states/multiterm_state.hpp"

namespace irs {

template<typename State>
class AllTermsVisitor : util::Noncopyable {
 public:
  AllTermsVisitor(State& state, FieldCollector& field_stats,
                  TermCollectorsFlat& term_stats) noexcept
    : _state{state}, _field_stats{field_stats}, _term_stats{term_stats} {}

  void Prepare(const SubReader& /*segment*/, const TermReader& field,
               const SeekTermIterator& terms) noexcept {
    _field_stats.Collect(field);
    _state.Prepare(&field);

    _terms = &terms;

    auto* meta = irs::get<TermMeta>(terms);
    _docs_count = meta ? &meta->docs_count : &_no_docs;
  }

  void Visit(score_t boost) {
    SDB_ASSERT(_terms);
    _term_stats.Collect(_stat_index, *_terms);

    _state.Push(typename State::Entry{
      .cookie = _terms->cookie(),
      .docs_count = *_docs_count,
      .boost = boost,
      .stat_offset = _stat_index,
    });
  }

  void SetIndex(uint32_t term_idx) noexcept { _stat_index = term_idx; }

 private:
  State& _state;
  FieldCollector& _field_stats;
  TermCollectorsFlat& _term_stats;
  const SeekTermIterator* _terms{};
  const uint32_t* _docs_count{};
  uint32_t _stat_index = 0;
  const decltype(TermMeta::docs_count) _no_docs = 0;
};

}  // namespace irs
