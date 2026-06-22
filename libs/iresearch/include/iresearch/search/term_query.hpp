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

#pragma once

#include "iresearch/search/filter.hpp"
#include "iresearch/search/states/term_state.hpp"

namespace irs {

// Compiled query suitable for filters with a single term like "by_term"
class TermQuery : public QueryBuilder {
 public:
  explicit TermQuery(const SubReader& segment, TermState&& state,
                     score_t boost);

  DocIterator::ptr Execute(const ExecutionContext& ctx,
                           const StatsBuffer& stats) const final;

  void Visit(PreparedStateVisitor&, score_t boost) const final;

  score_t Boost() const noexcept final { return _boost; }

 private:
  TermState _state;
  score_t _boost;
};

}  // namespace irs
