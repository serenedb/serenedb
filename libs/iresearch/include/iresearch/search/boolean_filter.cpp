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
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
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

Filter::Query::ptr BooleanFilter::PrepareImpl(const PrepareContext& ctx,
                                              uint32_t min_match) const {
  const auto size = _filters.size();

  if (size == 0) [[unlikely]] {
    return Query::empty();
  }

  if (size == 1) {
    auto* filter = _filters.front().get();
    SDB_ASSERT(filter);
    return filter->prepare(ctx.Boost(Boost()));
  }

  if (min_match != 0 && absl::c_all_of(*this, [&](const auto& filter) {
        if (filter->type() != irs::Type<ByTerm>::id()) {
          return false;
        }
        auto& first_term_filter =
          sdb::basics::downCast<ByTerm>(*_filters.front());
        auto& term_filter = sdb::basics::downCast<ByTerm>(*filter);
        return first_term_filter.field_id() == term_filter.field_id();
      })) {
    auto& first_term_filter = sdb::basics::downCast<ByTerm>(*_filters.front());
    ByTermsOptions options;
    options.merge_type = _merge_type;
    bool has_duplicates = false;
    for (const auto& filter : *this) {
      auto& term_filter = sdb::basics::downCast<ByTerm>(*filter);
      auto it =
        options.terms.emplace(term_filter.options().term, term_filter.Boost());
      if (!it.second) {
        const_cast<score_t&>(it.first->boost) *= term_filter.Boost();
        has_duplicates = true;
      }
    }
    if (!has_duplicates || min_match == 1 ||
        min_match == std::numeric_limits<uint32_t>::max()) {
      options.min_match = min_match == std::numeric_limits<uint32_t>::max()
                            ? options.terms.size()
                            : min_match;
      return ByTerms::Prepare(ctx.Boost(Boost()), first_term_filter.field_id(),
                              options);
    }
  }

  return PrepareBoolean(_filters, ctx);
}

Filter::Query::ptr And::PrepareBoolean(std::span<const Filter::ptr> filters,
                                       const PrepareContext& ctx) const {
  if (filters.empty()) {
    return Query::empty();
  }
  for (const auto& filter : filters) {
    if (filter->type() == irs::Type<Empty>::id()) {
      return Query::empty();
    }
  }

  PrepareContext sub_ctx = ctx;

  // single node case
  if (1 == filters.size()) {
    sub_ctx.boost *= Boost();
    return filters.front()->prepare(sub_ctx);
  }

  auto cumulative_all = MakeAllDocsFilter(kNoBoost);
  score_t all_boost{0};
  size_t all_count{0};
  for (const auto& filter : filters) {
    if (*filter == *cumulative_all) {
      all_count++;
      all_boost += sdb::basics::downCast<FilterWithBoost>(*filter).Boost();
    }
  }
  const auto non_all_count = filters.size() - all_count;

  if (all_count != 0 && 1 == non_all_count) {
    const Filter* left = nullptr;
    for (const auto& filter : filters) {
      if (*filter != *cumulative_all) {
        left = filter.get();
        break;
      }
    }
    const auto left_boost = left->BoostImpl();
    if (Boost() != 0 && left_boost != 0 && sub_ctx.scorer) {
      sub_ctx.boost = (sub_ctx.boost * Boost() * all_boost +
                       sub_ctx.boost * Boost() * left_boost) /
                      (left_boost * Boost());
    } else {
      sub_ctx.boost = 0;
    }
    sub_ctx.boost *= this->Boost();
    return left->prepare(sub_ctx);
  }

  sub_ctx.boost *= this->Boost();

  if (all_count != 0 && 0 == non_all_count) {
    cumulative_all->boost(all_boost);
    return cumulative_all->prepare(sub_ctx);
  }

  BooleanQuery::queries_t queries{{sub_ctx.memory}};
  queries.reserve(non_all_count + (all_count != 0 ? 1 : 0));
  for (const auto& filter : filters) {
    if (all_count != 0 && *filter == *cumulative_all) {
      continue;
    }
    queries.emplace_back(filter->prepare(sub_ctx));
  }
  if (all_count != 0) {
    // create new 'all' with boost from all removed
    cumulative_all->boost(all_boost);
    queries.emplace_back(cumulative_all->prepare(sub_ctx));
  }
  auto q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  q->prepare(sub_ctx, merge_type(), std::move(queries));
  return q;
}

Filter::Query::ptr And::prepare(const PrepareContext& ctx) const {
  return BooleanFilter::PrepareImpl(ctx, std::numeric_limits<uint32_t>::max());
}

Filter::Query::ptr Or::prepare(const PrepareContext& ctx) const {
  if (0 == _min_match_count) {  // only explicit 0 min match counts!
    // all conditions are satisfied
    return MakeAllDocsFilter(kNoBoost)->prepare(ctx.Boost(Boost()));
  }

  return BooleanFilter::PrepareImpl(ctx, _min_match_count);
}

Filter::Query::ptr Or::PrepareBoolean(std::span<const Filter::ptr> filters,
                                      const PrepareContext& ctx) const {
  const PrepareContext sub_ctx = ctx.Boost(Boost());

  if (0 == _min_match_count) {  // only explicit 0 min match counts!
    // all conditions are satisfied
    return MakeAllDocsFilter(kNoBoost)->prepare(sub_ctx);
  }

  if (filters.empty()) {
    return Query::empty();
  }

  if (filters.size() == 1) {
    return filters.front()->prepare(sub_ctx);
  }

  auto cumulative_all = MakeAllDocsFilter(kNoBoost);

  score_t all_boost{0};
  size_t all_count{0};
  size_t non_empty_count{0};
  const irs::Filter* all_filter = nullptr;
  for (const auto& filter : filters) {
    if (filter->type() == irs::Type<Empty>::id()) {
      continue;
    }
    ++non_empty_count;
    if (*filter == *cumulative_all) {
      all_count++;
      all_boost += sdb::basics::downCast<FilterWithBoost>(*filter).Boost();
      all_filter = filter.get();
    }
  }

  size_t optimized_match_count = 0;
  bool collapse_all = false;
  bool use_cumulative = false;
  if (all_count != 0) {
    if (!sub_ctx.scorer && _min_match_count <= all_count) {
      // if we have at least one all in include group - all other filters are
      // not necessary in case there is no scoring and 'all' count satisfies
      // min_match
      SDB_ASSERT(all_filter != nullptr);
      collapse_all = true;
    } else {
      // Here Or differs from And. Last All should be left in include group
      use_cumulative = true;
    }
    optimized_match_count = all_count - 1;
  }

  const size_t final_count = collapse_all ? 1
                             : use_cumulative
                               ? (non_empty_count - all_count) + 1
                               : non_empty_count;

  // check strictly less to not roll back to 0 min_match (we`ve handled this
  // case above!) single 'all' left -> it could contain boost we want to
  // preserve
  const auto adjusted_min_match = (optimized_match_count < _min_match_count)
                                    ? _min_match_count - optimized_match_count
                                    : 1;

  if (adjusted_min_match > final_count) {
    // can't satisfy 'min_match_count' conditions
    // having only 'final_count' queries
    return Query::empty();
  }

  if (1 == final_count) {
    // single node case
    if (collapse_all) {
      return all_filter->prepare(sub_ctx);
    }
    if (use_cumulative) {
      // create new 'all' with boost from all removed
      cumulative_all->boost(all_boost);
      return cumulative_all->prepare(sub_ctx);
    }
    for (const auto& filter : filters) {
      if (filter->type() != irs::Type<Empty>::id()) {
        return filter->prepare(sub_ctx);
      }
    }
  }

  SDB_ASSERT(adjusted_min_match > 0 && adjusted_min_match <= final_count);

  BooleanQuery::queries_t queries{{sub_ctx.memory}};
  queries.reserve(final_count);
  for (const auto& filter : filters) {
    if (filter->type() == irs::Type<Empty>::id()) {
      continue;
    }
    if (use_cumulative && *filter == *cumulative_all) {
      continue;
    }
    queries.emplace_back(filter->prepare(sub_ctx));
  }
  if (use_cumulative) {
    // create new 'all' with boost from all removed
    cumulative_all->boost(all_boost);
    queries.emplace_back(cumulative_all->prepare(sub_ctx));
  }

  memory::managed_ptr<BooleanQuery> q;
  if (adjusted_min_match == final_count) {
    q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  } else if (1 == adjusted_min_match) {
    q = memory::make_tracked<OrQuery>(sub_ctx.memory);
  } else {  // min_match_count > 1 && min_match_count < final_count
    q = memory::make_tracked<MinMatchQuery>(sub_ctx.memory, adjusted_min_match);
  }

  q->prepare(sub_ctx, merge_type(), std::move(queries));
  return q;
}

Filter::Query::ptr Exclusion::prepare(const PrepareContext& ctx) const {
  const PrepareContext sub_ctx = ctx.Boost(Boost());

  if (_include == nullptr && _exclude != nullptr) {
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

}  // namespace irs
