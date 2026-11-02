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

#include "iresearch/search/queries/nested_query.hpp"

#include <utility>

#include "iresearch/search/queries/prepared_state_visitor.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

struct EmptyParentsNode : ParentDocs {
  doc_id_t Next() final { return doc_limits::eof(); }

  doc_id_t Seek(doc_id_t) final { return doc_limits::eof(); }

  doc_id_t Prev() const noexcept final { return doc_limits::invalid(); }
};

ParentDocs::ptr EmptyParentDocs() {
  return memory::make_managed<ParentDocs, EmptyParentsNode>();
}

}  // namespace

ByNestedQuery::ByNestedQuery(const SubReader& segment, ParentProvider parent,
                             QueryBuilder::ptr&& child,
                             ScoreMergeType merge_type,
                             ByNestedOptions::MatchType match,
                             score_t constant) noexcept
  : QueryBuilderImpl{segment, EstimateOf(segment, match, *child),
                     QueryKind::Other},
    _parent{std::move(parent)},
    _child{std::move(child)},
    _match{std::move(match)},
    _merge_type{merge_type},
    _constant{constant} {
  SDB_ASSERT(_parent);
  SDB_ASSERT(_child);
  SDB_ASSERT(IsValidMatch(_match));
}

void ByNestedQuery::Visit(PreparedStateVisitor& visitor, score_t boost) const {
  if (!visitor.Visit(*this, boost)) {
    return;
  }

  SDB_ASSERT(_child);
  _child->Visit(visitor, boost);
}

ParentDocs::ptr ByNestedQuery::Parents() const {
  auto parent = _parent(_segment);
  if (!parent) {
    return EmptyParentDocs();
  }
  return parent;
}

}  // namespace irs
