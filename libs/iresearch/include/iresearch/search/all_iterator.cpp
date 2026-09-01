////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#include "all_iterator.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs {

AllIterator::AllIterator(uint32_t docs_count, ScoreSource score, score_t boost)
  : _boost{boost}, _score{score}, _max_doc{doc_limits::min() + docs_count - 1} {
  std::get<CostAttr>(_attrs).reset(_max_doc);
}

ScoreFunction AllIterator::PrepareScore(const PrepareScoreContext& ctx) {
  SDB_ASSERT(_score.scorer);
  return _score.scorer->PrepareScorer({
    .segment = *ctx.segment,
    .field = {},
    .doc_attrs = *this,
    .fetcher = ctx.fetcher,
    .stats = _score.stats,
    .boost = _boost,
  });
}

}  // namespace irs
