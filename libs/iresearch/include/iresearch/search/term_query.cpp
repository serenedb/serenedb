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

#include "basics/memory.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_iterator.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

TermQuery::TermQuery(const SubReader& segment, const TermReader* reader,
                     const PostingMeta& cookie, score_t boost)
  : QueryBuilder{segment}, _state{reader, cookie}, _boost{boost} {}

DocIterator::ptr TermQuery::Execute(const ExecutionContext& ctx,
                                    const StatsBuffer& stats) const {
  const auto& segment = _segment;

  if (_state.cookie.docs_count == 0) [[unlikely]] {
    return DocIterator::empty();
  }

  if (!stats.HasScorer() && segment.docs_count() == _state.cookie.docs_count)
    [[unlikely]] {
    return memory::make_managed<AllIterator>(segment.docs_count(),
                                             irs::ScoreSource{}, kNoBoost);
  }

  const auto* reader = _state.reader;
  SDB_ASSERT(reader);
  DocIterator::ptr docs;

  const auto features = GetFeatures(stats.GetScorer());
  auto it = reader->Iterator(
    features,
    {
      .cookie = &_state.cookie,
      .stats = stats.Stats(),
      .boost = _boost,
      .field = reader->meta(),
    },
    {.score_prune = MayScorePrune(ctx, stats), .scorer = stats.GetScorer()});
  if (!it) {
    return DocIterator::empty();
  }

  return it;
}

void TermQuery::Visit(PreparedStateVisitor& visitor, score_t boost) const {
  visitor.Visit(*this, _state, boost * _boost);
}

}  // namespace irs
