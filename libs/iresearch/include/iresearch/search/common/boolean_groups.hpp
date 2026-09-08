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

#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "basics/shared.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

struct BooleanGroups {
  std::span<const PostingClause> must;
  std::span<const QueryBuilder::ptr> must_filters;
  std::span<const PostingClause> should;
  std::span<const QueryBuilder::ptr> should_filters;
  std::vector<FillNode::ptr>* should_fills = nullptr;
  std::span<const PostingClause> must_not;
  std::span<const QueryBuilder::ptr> must_not_filters;
};

template<typename Leaves>
class OrGroup {
 public:
  template<typename... Args>
  explicit OrGroup(Args&&... args) : _leaves{std::forward<Args>(args)...} {}

  OrGroup(OrGroup&&) = delete;
  OrGroup& operator=(OrGroup&&) = delete;

  bool Exhausted() const noexcept { return _leaves.Empty(); }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words) {
    return _leaves.Visit(max, [&](auto& leaf) IRS_FORCE_INLINE {
      return leaf.FillOr(min, max, words);
    });
  }

 private:
  Leaves _leaves;
};

}  // namespace irs::search
