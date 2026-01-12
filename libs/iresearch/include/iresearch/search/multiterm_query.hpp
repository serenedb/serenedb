////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include "iresearch/search/filter.hpp"
#include "iresearch/search/states/multiterm_state.hpp"
#include "iresearch/search/states_cache.hpp"

namespace irs {

// Compiled query suitable for filters with non adjacent set of terms.
class MultiTermQuery : public Filter::Query {
 public:
  using States = StatesCache<MultiTermState>;
  // TODO(mbkkt) block_pool<byte>
  using Stats = ManagedVector<bstring>;

  explicit MultiTermQuery(States&& states, Stats&& stats, score_t boost,
                          ScoreMergeType merge_type, size_t min_match)
    : _states{std::move(states)},
      _stats{std::move(stats)},
      _boost{boost},
      _merge_type{merge_type},
      _min_match{min_match} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const final;

  void visit(const SubReader& segment, PreparedStateVisitor& visitor,
             score_t boost) const final;

  score_t Boost() const noexcept final { return _boost; }

 private:
  States _states;
  Stats _stats;
  score_t _boost;
  ScoreMergeType _merge_type;
  size_t _min_match;
};

}  // namespace irs
