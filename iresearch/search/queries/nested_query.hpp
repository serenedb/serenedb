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

#include <algorithm>
#include <cstdint>
#include <variant>

#include "iresearch/search/filters/nested_filter.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"

namespace irs {

class ByNestedQuery : public QueryBuilderImpl<ByNestedQuery> {
 public:
  static uint32_t EstimateOf(const SubReader& segment,
                             const ByNestedOptions::MatchType& match,
                             const QueryBuilder& child) noexcept {
    const auto docs = static_cast<uint32_t>(segment.docs_count());
    const auto* const range = std::get_if<Match>(&match);
    if (range == nullptr || range->min == 0) {
      return docs;
    }
    return std::min(docs, child.EstimateMax());
  }

  ByNestedQuery(const SubReader& segment, ParentProvider parent,
                QueryBuilder::ptr&& child, ScoreMergeType merge_type,
                ByNestedOptions::MatchType match, score_t constant) noexcept;

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final;

  score_t Boost() const noexcept final { return kNoBoost; }

  ParentDocs::ptr Parents() const;

  const QueryBuilder& Child() const noexcept { return *_child; }

  const ByNestedOptions::MatchType& MatchKind() const noexcept {
    return _match;
  }

  ScoreMergeType Merge() const noexcept { return _merge_type; }

  score_t Constant() const noexcept { return IsNoneMatch() ? _constant : 0.F; }

  bool ScoresChildren() const noexcept {
    return _merge_type != ScoreMergeType::Noop && !IsNoneMatch() &&
           !QueryBuilder::IsEmpty(*_child);
  }

 private:
  bool IsNoneMatch() const noexcept {
    const auto* range = std::get_if<Match>(&_match);
    return range != nullptr && *range == kMatchNone;
  }

  ParentProvider _parent;
  QueryBuilder::ptr _child;
  ByNestedOptions::MatchType _match;
  ScoreMergeType _merge_type;
  score_t _constant;
};

extern template class QueryBuilderImpl<ByNestedQuery>;

}  // namespace irs
