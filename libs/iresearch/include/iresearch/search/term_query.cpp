////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "term_query.hpp"

#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/score.hpp"

namespace irs {

TermQuery::TermQuery(States&& states, bstring&& stats, score_t boost)
  : _states{std::move(states)}, _stats{std::move(stats)}, _boost{boost} {}

DocIterator::ptr TermQuery::execute(const ExecutionContext& ctx) const {
  const auto& segment = ctx.segment;
  const auto& ord = ctx.scorers;
  // Get term state for the specified reader
  const auto* state = _states.find(segment);

  if (!state) [[unlikely]] {  // Invalid state
    return DocIterator::empty();
  }

  const auto* reader = state->reader;
  SDB_ASSERT(reader);
  DocIterator::ptr docs;
  IteratorOptions options{ctx.wand};

  auto it = reader->Iterator(ord.features(),
                             {
                               .cookie = state->cookie.get(),
                               .stats = _stats.c_str(),
                               .boost = _boost,
                               .field = reader->meta(),
                             },
                             options);
  if (!it) {
    return DocIterator::empty();
  }

  return it;
}

void TermQuery::visit(const SubReader& segment, PreparedStateVisitor& visitor,
                      score_t boost) const {
  if (const auto* state = _states.find(segment); state) {
    visitor.Visit(*this, *state, boost * _boost);
  }
}

}  // namespace irs
