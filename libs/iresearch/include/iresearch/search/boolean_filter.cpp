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
        return first_term_filter.field() == term_filter.field();
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
      return ByTerms::Prepare(ctx.Boost(Boost()), first_term_filter.field(),
                              options);
    }
  }

  // determine incl/excl parts
  std::vector<const Filter*> incl;
  std::vector<const Filter*> excl;

  AllDocsProvider::Ptr all_docs_no_boost;

  GroupFilters(incl, excl);

  if (incl.empty() && !excl.empty()) {
    // single negative query case
    all_docs_no_boost = MakeAllDocsFilter(kNoBoost);
    incl.push_back(all_docs_no_boost.get());
  }

  return PrepareBoolean(incl, excl, ctx);
}

void BooleanFilter::GroupFilters(std::vector<const Filter*>& incl,
                                 std::vector<const Filter*>& excl) const {
  incl.reserve(size() / 2);
  excl.reserve(incl.capacity());

  const Filter* empty_filter = nullptr;
  const auto is_and = type() == irs::Type<And>::id();
  for (const auto& filter : *this) {
    if (irs::Type<Empty>::id() == filter->type()) {
      empty_filter = filter.get();
      continue;
    }
    if (is_and && irs::Type<Exclude>::id() == filter->type()) {
      const auto& exclude = sdb::basics::downCast<Exclude>(*filter);
      if (!exclude.empty()) {
        excl.push_back(exclude.Child());
      }
      continue;
    }
    incl.push_back(filter.get());
  }
  if (empty_filter != nullptr) {
    incl.push_back(empty_filter);
  }
}

Filter::Query::ptr And::PrepareBoolean(std::vector<const Filter*>& incl,
                                       std::vector<const Filter*>& excl,
                                       const PrepareContext& ctx) const {
  // optimization step
  //  if include group empty itself or has 'empty' -> this whole conjunction is
  //  empty
  if (incl.empty() || incl.back()->type() == irs::Type<Empty>::id()) {
    return Query::empty();
  }

  PrepareContext sub_ctx = ctx;

  // single node case
  if (1 == incl.size() && excl.empty()) {
    sub_ctx.boost *= Boost();
    return incl.front()->prepare(sub_ctx);
  }

  auto cumulative_all = MakeAllDocsFilter(kNoBoost);
  score_t all_boost{0};
  size_t all_count{0};
  for (auto filter : incl) {
    if (*filter == *cumulative_all) {
      all_count++;
      all_boost += sdb::basics::downCast<FilterWithBoost>(*filter).Boost();
    }
  }
  if (all_count != 0) {
    const auto non_all_count = incl.size() - all_count;
    auto it = std::remove_if(incl.begin(), incl.end(),
                             [&cumulative_all](const irs::Filter* filter) {
                               return *cumulative_all == *filter;
                             });
    incl.erase(it, incl.end());
    // Here And differs from Or. Last 'All' should be left in include group only
    // if there is more than one filter of other type. Otherwise this another
    // filter could be container for boost from 'all' filters
    if (1 == non_all_count) {
      // let this last filter hold boost from all removed ones
      // so we aggregate in external boost values from removed all filters
      // If we will not optimize resulting boost will be:
      //   boost * OR_BOOST * ALL_BOOST + boost * OR_BOOST * LEFT_BOOST
      // We could adjust only 'boost' so we recalculate it as
      // new_boost =  ( boost * OR_BOOST * ALL_BOOST + boost * OR_BOOST *
      // LEFT_BOOST) / (OR_BOOST * LEFT_BOOST) so when filter will be executed
      // resulting boost will be: new_boost * OR_BOOST * LEFT_BOOST. If we
      // substitute new_boost back we will get ( boost * OR_BOOST * ALL_BOOST +
      // boost * OR_BOOST * LEFT_BOOST) - original non-optimized boost value
      auto left_boost = (*incl.begin())->BoostImpl();
      if (Boost() != 0 && left_boost != 0 && sub_ctx.scorer) {
        sub_ctx.boost = (sub_ctx.boost * Boost() * all_boost +
                         sub_ctx.boost * Boost() * left_boost) /
                        (left_boost * Boost());
      } else {
        sub_ctx.boost = 0;
      }
    } else {
      // create new 'all' with boost from all removed
      cumulative_all->boost(all_boost);
      incl.push_back(cumulative_all.get());
    }
  }
  sub_ctx.boost *= this->Boost();
  if (1 == incl.size() && excl.empty()) {
    // single node case
    return incl.front()->prepare(sub_ctx);
  }
  auto q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  q->prepare(sub_ctx, merge_type(), incl, excl);
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

Filter::Query::ptr Or::PrepareBoolean(std::vector<const Filter*>& incl,
                                      std::vector<const Filter*>& excl,
                                      const PrepareContext& ctx) const {
  const PrepareContext sub_ctx = ctx.Boost(Boost());

  if (0 == _min_match_count) {  // only explicit 0 min match counts!
    // all conditions are satisfied
    return MakeAllDocsFilter(kNoBoost)->prepare(sub_ctx);
  }

  if (!incl.empty() && incl.back()->type() == irs::Type<Empty>::id()) {
    incl.pop_back();
  }

  if (incl.empty()) {
    return Query::empty();
  }

  // single node case
  if (1 == incl.size() && excl.empty()) {
    return incl.front()->prepare(sub_ctx);
  }

  auto cumulative_all = MakeAllDocsFilter(kNoBoost);
  size_t optimized_match_count = 0;
  // Optimization steps

  score_t all_boost{0};
  size_t all_count{0};
  const irs::Filter* incl_all{nullptr};
  for (auto filter : incl) {
    if (*filter == *cumulative_all) {
      all_count++;
      all_boost += sdb::basics::downCast<FilterWithBoost>(*filter).Boost();
      incl_all = filter;
    }
  }
  if (all_count != 0) {
    if (!sub_ctx.scorer && incl.size() > 1 && _min_match_count <= all_count) {
      // if we have at least one all in include group - all other filters are
      // not necessary in case there is no scoring and 'all' count satisfies
      // min_match
      SDB_ASSERT(incl_all != nullptr);
      incl.resize(1);
      incl.front() = incl_all;
      optimized_match_count = all_count - 1;
    } else {
      // Here Or differs from And. Last All should be left in include group
      auto it = std::remove_if(incl.begin(), incl.end(),
                               [&cumulative_all](const irs::Filter* filter) {
                                 return *cumulative_all == *filter;
                               });
      incl.erase(it, incl.end());
      // create new 'all' with boost from all removed
      cumulative_all->boost(all_boost);
      incl.push_back(cumulative_all.get());
      optimized_match_count = all_count - 1;
    }
  }
  // check strictly less to not roll back to 0 min_match (we`ve handled this
  // case above!) single 'all' left -> it could contain boost we want to
  // preserve
  const auto adjusted_min_match = (optimized_match_count < _min_match_count)
                                    ? _min_match_count - optimized_match_count
                                    : 1;

  if (adjusted_min_match > incl.size()) {
    // can't satisfy 'min_match_count' conditions
    // having only 'incl.size()' queries
    return Query::empty();
  }

  if (1 == incl.size() && excl.empty()) {
    // single node case
    return incl.front()->prepare(sub_ctx);
  }

  SDB_ASSERT(adjusted_min_match > 0 && adjusted_min_match <= incl.size());

  memory::managed_ptr<BooleanQuery> q;
  if (adjusted_min_match == incl.size()) {
    q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  } else if (1 == adjusted_min_match) {
    q = memory::make_tracked<OrQuery>(sub_ctx.memory);
  } else {  // min_match_count > 1 && min_match_count < incl.size()
    q = memory::make_tracked<MinMatchQuery>(sub_ctx.memory, adjusted_min_match);
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
