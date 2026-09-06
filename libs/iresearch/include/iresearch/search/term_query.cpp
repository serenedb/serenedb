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

#include <utility>

#include "basics/memory.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

TermQuery::TermQuery(const SubReader& segment, const TermReader* reader,
                     const PostingMeta& cookie, score_t boost,
                     search::StatsRecord stats)
  : QueryBuilderImpl{segment, cookie.docs_count, QueryKind::Term},
    _state{reader, cookie},
    _boost{boost} {
  SDB_ASSERT(reader != nullptr);
  SDB_ASSERT(cookie.docs_count != 0);
  SetStats(stats);
}

void TermQuery::Visit(PreparedStateVisitor& visitor, score_t boost) const {
  visitor.Visit(_state, boost * _boost);
}

QueryBuilder::ptr MakeTermQuery(IResourceManager& memory,
                                const SubReader& segment,
                                const TermReader* reader,
                                const PostingMeta& meta, score_t boost,
                                search::StatsRecord stats) {
  if (meta.docs_count == 0) {
    return QueryBuilder::Empty();
  }
  return memory::make_tracked<TermQuery>(memory, segment, reader, meta, boost,
                                         stats);
}

}  // namespace irs
