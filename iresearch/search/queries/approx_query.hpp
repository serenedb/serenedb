////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <utility>

#include "iresearch/search/queries/query_builder_impl.hpp"

namespace irs {

class ApproxQuery : public QueryBuilderImpl<ApproxQuery> {
 public:
  ApproxQuery(const SubReader& segment, QueryBuilder::ptr&& approx,
              score_t boost)
    : QueryBuilderImpl{segment, approx->EstimateMax(), QueryKind::Other},
      _approx{std::move(approx)},
      _boost{boost} {
    SDB_ASSERT(!QueryBuilder::IsEmpty(*_approx));
    SDB_ASSERT(!_approx->Scores());
    _estimate_matches = _approx->EstimateMatches();
    _postings = _approx->Postings();
  }

  const QueryBuilder& Approx() const noexcept { return *_approx; }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  QueryBuilder::ptr _approx;
  score_t _boost;
};

}  // namespace irs
