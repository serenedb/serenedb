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

#include "boolean_filter.hpp"

#include "conjunction.hpp"
#include "disjunction.hpp"
#include "exclusion.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "prepared_state_visitor.hpp"

namespace irs {

bool BooleanFilter::equals(const Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<BooleanFilter>(rhs);
  const auto eq = [](const auto& lhs, const auto& rhs) { return *lhs == *rhs; };
  return absl::c_equal(_incl, typed_rhs._incl, eq) &&
         absl::c_equal(_excl, typed_rhs._excl, eq);
}

Filter::Query::ptr And::prepare(const PrepareContext& ctx) const {
  if (empty()) {
    return Query::empty();
  }

  SDB_ASSERT(!_incl.empty());

  const PrepareContext sub_ctx = ctx.Boost(Boost());
  auto q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  q->prepare(sub_ctx, MergeType(), _incl, _excl);
  return q;
}

Filter::Query::ptr Or::prepare(const PrepareContext& ctx) const {
  const PrepareContext sub_ctx = ctx.Boost(Boost());

  SDB_ASSERT(0 != _min_match_count);
  SDB_ASSERT(!_incl.empty());
  SDB_ASSERT(_min_match_count <= _incl.size());

  memory::managed_ptr<BooleanQuery> q;
  if (_min_match_count == _incl.size()) {
    q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  } else if (1 == _min_match_count) {
    q = memory::make_tracked<OrQuery>(sub_ctx.memory);
  } else {
    q = memory::make_tracked<MinMatchQuery>(sub_ctx.memory, _min_match_count);
  }

  q->prepare(sub_ctx, MergeType(), _incl, {});
  return q;
}

Filter::Query::ptr Not::prepare(const PrepareContext& ctx) const {
  if (!_filter) {
    return Query::empty();
  }

  const PrepareContext sub_ctx = ctx.Boost(Boost());

  Filter::ptr all_docs = MakeAllDocsFilter(kNoBoost);

  auto q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  q->prepare(sub_ctx, ScoreMergeType::Sum, {&all_docs, 1}, {&_filter, 1});
  return q;
}

bool Not::equals(const irs::Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<Not>(rhs);
  return (!empty() && !typed_rhs.empty() && *_filter == *typed_rhs._filter) ||
         (empty() && typed_rhs.empty());
}

}  // namespace irs
