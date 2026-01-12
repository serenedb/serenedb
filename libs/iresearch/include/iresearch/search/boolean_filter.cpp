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
#include "min_match_disjunction.hpp"
#include "prepared_state_visitor.hpp"

namespace irs {
namespace {

std::pair<const Filter*, bool> OptimizeNot(const Not& node) {
  bool neg = true;
  const auto* inner = node.filter();
  while (inner != nullptr && inner->type() == Type<Not>::id()) {
    neg = !neg;
    inner = sdb::basics::downCast<Not>(inner)->filter();
  }

  return std::pair{inner, neg};
}

}  // namespace
bool BooleanFilter::equals(const Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<BooleanFilter>(rhs);
  return absl::c_equal(*this, typed_rhs, [](const auto& lhs, const auto& rhs) {
    return *lhs == *rhs;
  });
}

Filter::Query::ptr BooleanFilter::prepare(const PrepareContext& ctx) const {
  const auto size = _filters.size();

  if (size == 0) [[unlikely]] {
    return Query::empty();
  }

  if (size == 1) {
    auto* filter = _filters.front().get();
    SDB_ASSERT(filter);

    // FIXME(gnusi): let Not handle everything?
    if (filter->type() != irs::Type<irs::Not>::id()) {
      return filter->prepare(ctx.Boost(Boost()));
    }
  }

  // determine incl/excl parts
  std::vector<const Filter*> incl;
  std::vector<const Filter*> excl;

  AllDocsProvider::Ptr all_docs_zero_boost;
  AllDocsProvider::Ptr all_docs_no_boost;

  group_filters(all_docs_zero_boost, incl, excl);

  if (incl.empty() && !excl.empty()) {
    // single negative query case
    all_docs_no_boost = MakeAllDocsFilter(kNoBoost);
    incl.push_back(all_docs_no_boost.get());
  }

  return PrepareBoolean(incl, excl, ctx);
}

void BooleanFilter::group_filters(AllDocsProvider::Ptr& all_docs_zero_boost,
                                  std::vector<const Filter*>& incl,
                                  std::vector<const Filter*>& excl) const {
  incl.reserve(size() / 2);
  excl.reserve(incl.capacity());

  const Filter* empty_filter = nullptr;
  const auto is_or = type() == irs::Type<Or>::id();
  for (const auto& filter : *this) {
    if (irs::Type<Empty>::id() == filter->type()) {
      empty_filter = filter.get();
      continue;
    }
    if (irs::Type<Not>::id() == filter->type()) {
      const auto res = OptimizeNot(sdb::basics::downCast<Not>(*filter));

      if (!res.first) {
        continue;
      }

      if (res.second) {
        if (!all_docs_zero_boost) {
          all_docs_zero_boost = MakeAllDocsFilter(0.F);
        }

        if (*all_docs_zero_boost == *res.first) {
          // not all -> empty result
          incl.clear();
          return;
        }
        excl.push_back(res.first);
        if (is_or) {
          // FIXME: this should have same boost as Not filter.
          // But for now we do not boost negation.
          incl.push_back(all_docs_zero_boost.get());
        }
      } else {
        incl.push_back(res.first);
      }
    } else {
      incl.push_back(filter.get());
    }
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
      if (Boost() != 0 && left_boost != 0 && !sub_ctx.scorers.empty()) {
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

Filter::Query::ptr Or::prepare(const PrepareContext& ctx) const {
  if (0 == _min_match_count) {  // only explicit 0 min match counts!
    // all conditions are satisfied
    return MakeAllDocsFilter(kNoBoost)->prepare(ctx.Boost(Boost()));
  }

  return BooleanFilter::prepare(ctx);
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
    if (sub_ctx.scorers.empty() && incl.size() > 1 &&
        _min_match_count <= all_count) {
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
  const auto res = OptimizeNot(*this);

  if (!res.first) {
    return Query::empty();
  }

  const PrepareContext sub_ctx = ctx.Boost(Boost());

  if (res.second) {
    auto all_docs = MakeAllDocsFilter(kNoBoost);
    const std::array<const irs::Filter*, 1> incl{all_docs.get()};
    const std::array<const irs::Filter*, 1> excl{res.first};

    auto q = memory::make_tracked<AndQuery>(sub_ctx.memory);
    q->prepare(sub_ctx, ScoreMergeType::Sum, incl, excl);
    return q;
  }

  // negation has been optimized out
  return res.first->prepare(sub_ctx);
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
