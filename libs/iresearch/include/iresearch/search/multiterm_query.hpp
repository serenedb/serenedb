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

#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/estimate.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/states/multiterm_state.hpp"

namespace irs {

class MultiTermQuery : public QueryBuilderImpl<MultiTermQuery> {
 public:
  explicit MultiTermQuery(const SubReader& segment, IResourceManager& memory,
                          score_t boost, ScoreMergeType merge_type)
    : QueryBuilderImpl{segment, 0, QueryKind::Terms},
      _state{memory},
      _boost{boost},
      _merge_type{merge_type} {}

  MultiTermState& State() noexcept { return _state; }

  static QueryBuilder::ptr Finish(memory::managed_ptr<MultiTermQuery> query,
                                  const PrepareContext& ctx);

  const MultiTermState& State() const noexcept { return _state; }

  ScoreMergeType MergeType() const noexcept { return _merge_type; }

  void Pin() noexcept { _pinned = true; }
  bool Pinned() const noexcept { return _pinned; }

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final;

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  MultiTermState _state;
  score_t _boost;
  ScoreMergeType _merge_type;
  bool _pinned = false;
};

}  // namespace irs
