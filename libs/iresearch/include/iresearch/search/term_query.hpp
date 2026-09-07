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

#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/states/term_state.hpp"

namespace irs {

class TermQuery : public QueryBuilderImpl<TermQuery> {
 public:
  TermQuery(const SubReader& segment, const TermReader* reader,
            const PostingMeta& cookie, score_t boost,
            search::StatsRecord stats);

  void Visit(PreparedStateVisitor&, score_t boost) const final;

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

  const TermState& State() const noexcept { return _state; }

 private:
  TermState _state;
  score_t _boost;
};

QueryBuilder::ptr MakeTermQuery(IResourceManager& memory,
                                const SubReader& segment,
                                const TermReader* reader,
                                const PostingMeta& meta, score_t boost,
                                search::StatsRecord stats = {});

}  // namespace irs
