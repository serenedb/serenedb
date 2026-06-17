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

PrepareCollector::ptr And::MakeCollector(const Scorer* scorer) const {
  auto compound = std::make_unique<CompoundCollector>(scorer);
  for (const auto& filter : _filters) {
    compound->Add(filter->MakeCollector(scorer));
  }
  return compound;
}

QueryBuilder::ptr And::PrepareSegment(const SubReader& segment,
                                      const PrepareContext& ctx) const {
  SDB_ASSERT(_filters.size() > 1);
  auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector);
  SDB_ASSERT(compound != nullptr);

  const auto composite_boost = ctx.boost * Boost();

  BooleanQuery::queries_t queries{{ctx.memory}};
  queries.reserve(_filters.size());
  size_t idx = 0;
  for (const auto& filter : _filters) {
    PrepareContext child = ctx;
    child.boost = composite_boost;
    child.collector = &compound->Child(idx++);
    queries.emplace_back(filter->PrepareSegment(segment, child));
  }
  const size_t excl_start = queries.size();
  return memory::make_tracked<AndQuery>(ctx.memory, segment, std::move(queries),
                                        excl_start, merge_type(),
                                        composite_boost);
}

PrepareCollector::ptr Or::MakeCollector(const Scorer* scorer) const {
  auto compound = std::make_unique<CompoundCollector>(scorer);
  for (const auto& filter : _filters) {
    compound->Add(filter->MakeCollector(scorer));
  }
  return compound;
}

QueryBuilder::ptr Or::PrepareSegment(const SubReader& segment,
                                     const PrepareContext& ctx) const {
  SDB_ASSERT(_filters.size() > 1);
  SDB_ASSERT(_min_match_count != 0);
  auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector);
  SDB_ASSERT(compound != nullptr);

  const auto composite_boost = ctx.boost * Boost();

  BooleanQuery::queries_t queries{{ctx.memory}};
  queries.reserve(_filters.size());
  size_t idx = 0;
  for (const auto& filter : _filters) {
    PrepareContext child = ctx;
    child.boost = composite_boost;
    child.collector = &compound->Child(idx++);
    queries.emplace_back(filter->PrepareSegment(segment, child));
  }
  const size_t excl_start = queries.size();
  if (_min_match_count <= 1) {
    return memory::make_tracked<OrQuery>(ctx.memory, segment,
                                         std::move(queries), excl_start,
                                         merge_type(), composite_boost);
  }
  if (_min_match_count >= _filters.size()) {
    return memory::make_tracked<AndQuery>(ctx.memory, segment,
                                          std::move(queries), excl_start,
                                          merge_type(), composite_boost);
  }
  return memory::make_tracked<MinMatchQuery>(
    ctx.memory, segment, std::move(queries), excl_start, merge_type(),
    composite_boost, _min_match_count);
}

PrepareCollector::ptr Exclusion::MakeCollector(const Scorer* scorer) const {
  auto compound = std::make_unique<CompoundCollector>(scorer);
  compound->Add(GetInclude()->MakeCollector(scorer));
  for (const auto& exclude : GetExcludes()) {
    compound->Add(exclude->MakeCollector(nullptr));
  }
  return compound;
}

QueryBuilder::ptr Exclusion::PrepareSegment(const SubReader& segment,
                                            const PrepareContext& ctx) const {
  SDB_ASSERT(!GetExcludes().empty());
  auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector);
  SDB_ASSERT(compound != nullptr);

  const auto child_boost = ctx.boost * Boost();

  BooleanQuery::queries_t queries{{ctx.memory}};
  queries.reserve(_filters.size());
  size_t idx = 0;
  for (const auto& filter : _filters) {
    PrepareContext child = ctx;
    child.boost = child_boost;
    child.collector = &compound->Child(idx++);
    queries.emplace_back(filter->PrepareSegment(segment, child));
  }
  return memory::make_tracked<AndQuery>(ctx.memory, segment, std::move(queries),
                                        size_t{1}, ScoreMergeType::Sum,
                                        child_boost);
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
  return same(GetInclude(), typed_rhs.GetInclude()) &&
         std::ranges::equal(GetExcludes(), typed_rhs.GetExcludes(), same);
}

QueryBuilder::ptr Not::PrepareSegment(const SubReader&,
                                      const PrepareContext&) const {
  SDB_UNREACHABLE();
  return QueryBuilder::Empty();
}

PrepareCollector::ptr Not::MakeCollector(const Scorer*) const {
  SDB_UNREACHABLE();
  return std::make_unique<NoopCollector>();
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
