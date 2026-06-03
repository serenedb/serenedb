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
#include "iresearch/search/exclude_filter.hpp"
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

void BooleanFilter::GroupFilters(std::vector<const Filter*>& incl,
                                 std::vector<const Filter*>& excl) const {
  incl.reserve(size());

  const auto is_and = type() == irs::Type<And>::id();
  for (const auto& filter : *this) {
    if (is_and && irs::Type<Exclude>::id() == filter->type()) {
      const auto& exclude = sdb::basics::downCast<Exclude>(*filter);
      if (!exclude.empty()) {
        excl.push_back(exclude.Child());
      }
      continue;
    }
    incl.push_back(filter.get());
  }
}

Filter::Query::ptr And::prepare(const PrepareContext& ctx) const {
  if (empty()) {
    return Query::empty();
  }

  std::vector<const Filter*> incl;
  std::vector<const Filter*> excl;
  GroupFilters(incl, excl);

  AllDocsProvider::Ptr all_docs;
  if (incl.empty()) {
    if (excl.empty()) {
      return Query::empty();
    }
    all_docs = MakeAllDocsFilter(kNoBoost);
    incl.push_back(all_docs.get());
  }

  const PrepareContext sub_ctx = ctx.Boost(Boost());
  auto q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  q->prepare(sub_ctx, merge_type(), incl, excl);
  return q;
}

Filter::Query::ptr Or::prepare(const PrepareContext& ctx) const {
  const PrepareContext sub_ctx = ctx.Boost(Boost());

  if (0 == _min_match_count) {  // only explicit 0 min match counts!
    return MakeAllDocsFilter(kNoBoost)->prepare(sub_ctx);
  }

  if (empty()) {
    return Query::empty();
  }

  std::vector<const Filter*> incl;
  std::vector<const Filter*> excl;
  GroupFilters(incl, excl);

  if (_min_match_count > incl.size()) {
    return Query::empty();
  }

  memory::managed_ptr<BooleanQuery> q;
  if (_min_match_count == incl.size()) {
    q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  } else if (1 == _min_match_count) {
    q = memory::make_tracked<OrQuery>(sub_ctx.memory);
  } else {
    q = memory::make_tracked<MinMatchQuery>(sub_ctx.memory, _min_match_count);
  }

  q->prepare(sub_ctx, merge_type(), incl, excl);
  return q;
}

Filter::Query::ptr Not::prepare(const PrepareContext& ctx) const {
  if (!_filter) {
    return Query::empty();
  }

  const PrepareContext sub_ctx = ctx.Boost(Boost());

  auto all_docs = MakeAllDocsFilter(kNoBoost);
  const std::array<const irs::Filter*, 1> incl{all_docs.get()};
  const std::array<const irs::Filter*, 1> excl{_filter.get()};

  auto q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  q->prepare(sub_ctx, ScoreMergeType::Sum, incl, excl);
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
