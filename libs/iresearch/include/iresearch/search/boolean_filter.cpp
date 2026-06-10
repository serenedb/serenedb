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
  return absl::c_equal(*this, typed_rhs, [](const auto& lhs, const auto& rhs) {
    return *lhs == *rhs;
  });
}

Filter::Query::ptr BooleanFilter::PrepareImpl(const PrepareContext& ctx) const {
  SDB_ASSERT(!_filters.empty());

  return PrepareBoolean(_filters, ctx);
}

Filter::Query::ptr And::PrepareBoolean(std::span<const Filter::ptr> filters,
                                       const PrepareContext& ctx) const {
  SDB_ASSERT(!filters.empty());

  PrepareContext sub_ctx = ctx;
  sub_ctx.boost *= Boost();

  if (1 == filters.size()) {
    return filters.front()->prepare(sub_ctx);
  }

  BooleanQuery::queries_t queries{{sub_ctx.memory}};
  queries.reserve(filters.size());
  for (const auto& filter : filters) {
    queries.emplace_back(filter->prepare(sub_ctx));
  }
  auto q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  q->prepare(sub_ctx, merge_type(), std::move(queries));
  return q;
}

Filter::Query::ptr And::prepare(const PrepareContext& ctx) const {
  return BooleanFilter::PrepareImpl(ctx);
}

Filter::Query::ptr Or::prepare(const PrepareContext& ctx) const {
  SDB_ASSERT(_min_match_count != 0);
  return BooleanFilter::PrepareImpl(ctx);
}

Filter::Query::ptr Or::PrepareBoolean(std::span<const Filter::ptr> filters,
                                      const PrepareContext& ctx) const {
  SDB_ASSERT(!filters.empty());
  SDB_ASSERT(_min_match_count != 0);
  SDB_ASSERT(_min_match_count <= filters.size());

  const PrepareContext sub_ctx = ctx.Boost(Boost());

  if (filters.size() == 1) {
    return filters.front()->prepare(sub_ctx);
  }

  BooleanQuery::queries_t queries{{sub_ctx.memory}};
  queries.reserve(filters.size());
  for (const auto& filter : filters) {
    queries.emplace_back(filter->prepare(sub_ctx));
  }

  memory::managed_ptr<BooleanQuery> q;
  if (1 == _min_match_count) {
    q = memory::make_tracked<OrQuery>(sub_ctx.memory);
  } else {
    SDB_ASSERT(_min_match_count < filters.size());
    q = memory::make_tracked<MinMatchQuery>(sub_ctx.memory, _min_match_count);
  }

  q->prepare(sub_ctx, merge_type(), std::move(queries));
  return q;
}

Filter::Query::ptr Exclusion::prepare(const PrepareContext& ctx) const {
  const PrepareContext sub_ctx = ctx.Boost(Boost());

  if (_exclude == nullptr) {
    SDB_ASSERT(_include != nullptr);
    return _include->prepare(sub_ctx);
  }

  if (_include == nullptr) {
    const Filter* inner = _exclude.get();
    bool negated = true;
    while (inner->type() == Type<Exclusion>::id()) {
      const auto& ex = sdb::basics::downCast<Exclusion>(*inner);
      if (ex._include != nullptr || ex._exclude == nullptr) {
        break;
      }
      inner = ex._exclude.get();
      negated = !negated;
    }
    if (!negated) {
      return inner->prepare(sub_ctx);
    }
    return PrepareExclusion(sub_ctx, nullptr, inner);
  }

  return PrepareExclusion(sub_ctx, _include.get(), _exclude.get());
}

bool Exclusion::equals(const irs::Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<Exclusion>(rhs);
  const auto same = [](const Filter::ptr& lhs, const Filter::ptr& rhs) {
    return (lhs == nullptr && rhs == nullptr) ||
           (lhs != nullptr && rhs != nullptr && *lhs == *rhs);
  };
  return same(_include, typed_rhs._include) &&
         same(_exclude, typed_rhs._exclude);
}

Filter::Query::ptr Not::prepare(const PrepareContext&) const {
  SDB_UNREACHABLE();
  return Query::empty();
}

bool Not::equals(const irs::Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<Not>(rhs);
  SDB_ASSERT(_filter != nullptr);
  SDB_ASSERT(typed_rhs._filter != nullptr);
  return *_filter == *typed_rhs._filter;
}

}  // namespace irs
