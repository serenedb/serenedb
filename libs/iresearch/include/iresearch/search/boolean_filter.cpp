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

BooleanFilter::ResolvedBoolean MakeEmpty() {
  BooleanFilter::ResolvedBoolean r;
  r.kind = BooleanFilter::ResolvedBoolean::Empty;
  return r;
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

BooleanFilter::ResolvedBoolean BooleanFilter::Resolve(
  const Scorer* scorer, uint32_t min_match) const {
  const auto size = _filters.size();

  if (size == 0) [[unlikely]] {
    return MakeEmpty();
  }

  if (size == 1) {
    auto* filter = _filters.front().get();
    SDB_ASSERT(filter);

    // FIXME(gnusi): let Not handle everything?
    if (filter->type() != irs::Type<irs::Not>::id()) {
      ResolvedBoolean r;
      r.kind = ResolvedBoolean::Delegate;
      r.delegate = filter;
      r.delegate_boost = Boost();
      return r;
    }
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
      ResolvedBoolean r;
      r.kind = ResolvedBoolean::Delegate;
      r.fold_terms = true;
      r.fold_field = first_term_filter.field();
      r.fold_options = std::move(options);
      r.delegate_boost = Boost();
      return r;
    }
  }

  // determine incl/excl parts
  std::vector<const Filter*> incl;
  std::vector<const Filter*> excl;

  ResolvedBoolean resolved;

  GroupFilters(resolved.all_docs_keepalive, incl, excl);

  if (incl.empty() && !excl.empty()) {
    // single negative query case
    resolved.all_docs_keepalive2 = MakeAllDocsFilter(kNoBoost);
    incl.push_back(resolved.all_docs_keepalive2.get());
  }

  ResolveBoolean(resolved, scorer, incl, excl);
  return resolved;
}

void BooleanFilter::GroupFilters(AllDocsProvider::Ptr& all_docs_zero_boost,
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

void And::ResolveBoolean(ResolvedBoolean& resolved, const Scorer* scorer,
                         std::vector<const Filter*>& incl,
                         std::vector<const Filter*>& excl) const {
  // optimization step
  //  if include group empty itself or has 'empty' -> this whole conjunction is
  //  empty
  if (incl.empty() || incl.back()->type() == irs::Type<Empty>::id()) {
    resolved.kind = ResolvedBoolean::Empty;
    return;
  }

  score_t boost = Boost();

  // single node case
  if (1 == incl.size() && excl.empty()) {
    resolved.kind = ResolvedBoolean::Delegate;
    resolved.delegate = incl.front();
    resolved.delegate_boost = boost;
    return;
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
    if (1 == non_all_count) {
      auto left_boost = (*incl.begin())->BoostImpl();
      if (Boost() != 0 && left_boost != 0 && scorer) {
        boost = (boost * Boost() * all_boost + boost * Boost() * left_boost) /
                (left_boost * Boost());
      } else {
        boost = 0;
      }
    } else {
      cumulative_all->boost(all_boost);
      incl.push_back(cumulative_all.get());
      resolved.all_docs_keepalive2 = std::move(cumulative_all);
    }
  }
  boost *= this->Boost();
  if (1 == incl.size() && excl.empty()) {
    // single node case
    resolved.kind = ResolvedBoolean::Delegate;
    resolved.delegate = incl.front();
    resolved.delegate_boost = boost;
    return;
  }

  resolved.kind = ResolvedBoolean::Composite;
  resolved.composite = ResolvedBoolean::kAnd;
  resolved.incl = std::move(incl);
  resolved.excl = std::move(excl);
  resolved.merge = merge_type();
  resolved.boost = boost;
}

PrepareCollector::ptr And::MakeCollector(const Scorer* scorer) const {
  return MakeCollectorImpl(scorer, std::numeric_limits<uint32_t>::max());
}

QueryBuilder::ptr And::PrepareSegment(const SubReader& segment,
                                      const PrepareContext& ctx) const {
  return PrepareSegmentImpl(segment, ctx, std::numeric_limits<uint32_t>::max());
}

PrepareCollector::ptr Or::MakeCollector(const Scorer* scorer) const {
  if (0 == _min_match_count) {
    return MakeAllDocsFilter(kNoBoost)->MakeCollector(scorer);
  }
  return MakeCollectorImpl(scorer, _min_match_count);
}

QueryBuilder::ptr Or::PrepareSegment(const SubReader& segment,
                                     const PrepareContext& ctx) const {
  if (0 == _min_match_count) {
    return MakeAllDocsFilter(kNoBoost)->PrepareSegment(segment,
                                                       ctx.Boost(Boost()));
  }
  return PrepareSegmentImpl(segment, ctx, _min_match_count);
}

void Or::ResolveBoolean(ResolvedBoolean& resolved, const Scorer* scorer,
                        std::vector<const Filter*>& incl,
                        std::vector<const Filter*>& excl) const {
  score_t boost = Boost();

  if (!incl.empty() && incl.back()->type() == irs::Type<Empty>::id()) {
    incl.pop_back();
  }

  if (incl.empty()) {
    resolved.kind = ResolvedBoolean::Empty;
    return;
  }

  // single node case
  if (1 == incl.size() && excl.empty()) {
    resolved.kind = ResolvedBoolean::Delegate;
    resolved.delegate = incl.front();
    resolved.delegate_boost = boost;
    return;
  }

  auto cumulative_all = MakeAllDocsFilter(kNoBoost);
  size_t optimized_match_count = 0;

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
    if (!scorer && incl.size() > 1 && _min_match_count <= all_count) {
      SDB_ASSERT(incl_all != nullptr);
      incl.resize(1);
      incl.front() = incl_all;
      optimized_match_count = all_count - 1;
    } else {
      auto it = std::remove_if(incl.begin(), incl.end(),
                               [&cumulative_all](const irs::Filter* filter) {
                                 return *cumulative_all == *filter;
                               });
      incl.erase(it, incl.end());
      cumulative_all->boost(all_boost);
      incl.push_back(cumulative_all.get());
      resolved.all_docs_keepalive2 = std::move(cumulative_all);
      optimized_match_count = all_count - 1;
    }
  }
  const auto adjusted_min_match = (optimized_match_count < _min_match_count)
                                    ? _min_match_count - optimized_match_count
                                    : 1;

  if (adjusted_min_match > incl.size()) {
    resolved.kind = ResolvedBoolean::Empty;
    return;
  }

  if (1 == incl.size() && excl.empty()) {
    // single node case
    resolved.kind = ResolvedBoolean::Delegate;
    resolved.delegate = incl.front();
    resolved.delegate_boost = boost;
    return;
  }

  SDB_ASSERT(adjusted_min_match > 0 && adjusted_min_match <= incl.size());

  resolved.kind = ResolvedBoolean::Composite;
  if (adjusted_min_match == incl.size()) {
    resolved.composite = ResolvedBoolean::kAnd;
  } else if (1 == adjusted_min_match) {
    resolved.composite = ResolvedBoolean::kOr;
  } else {
    resolved.composite = ResolvedBoolean::kMinMatch;
  }
  resolved.min_match = adjusted_min_match;
  resolved.incl = std::move(incl);
  resolved.excl = std::move(excl);
  resolved.merge = merge_type();
  resolved.boost = boost;
}

PrepareCollector::ptr BooleanFilter::MakeCollectorImpl(
  const Scorer* scorer, uint32_t min_match) const {
  auto resolved = Resolve(scorer, min_match);

  switch (resolved.kind) {
    case ResolvedBoolean::Empty:
      return std::make_unique<NoopCollector>();
    case ResolvedBoolean::Delegate: {
      if (resolved.fold_terms) {
        ByTerms by_terms;
        *by_terms.mutable_field() = std::string{resolved.fold_field};
        *by_terms.mutable_options() = std::move(resolved.fold_options);
        return by_terms.MakeCollector(scorer);
      }
      return resolved.delegate->MakeCollector(scorer);
    }
    case ResolvedBoolean::Composite: {
      auto compound = std::make_unique<CompoundCollector>(scorer);
      for (const auto* filter : resolved.incl) {
        compound->Add(filter->MakeCollector(scorer));
      }
      for (const auto* filter : resolved.excl) {
        compound->Add(filter->MakeCollector(nullptr));
      }
      return compound;
    }
  }
  return std::make_unique<NoopCollector>();
}

QueryBuilder::ptr BooleanFilter::PrepareSegmentImpl(const SubReader& segment,
                                                    const PrepareContext& ctx,
                                                    uint32_t min_match) const {
  const Scorer* scorer = nullptr;
  if (ctx.collector != nullptr) {
    if (auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector)) {
      scorer = compound->GetScorer();
    }
  }

  auto resolved = Resolve(scorer, min_match);

  switch (resolved.kind) {
    case ResolvedBoolean::Empty:
      return QueryBuilder::Empty();
    case ResolvedBoolean::Delegate: {
      auto child_ctx = ctx.Boost(resolved.delegate_boost);
      child_ctx.collector = ctx.collector;
      if (resolved.fold_terms) {
        return ByTerms::PrepareSegment(segment, child_ctx, resolved.fold_field,
                                       resolved.fold_options);
      }
      return resolved.delegate->PrepareSegment(segment, child_ctx);
    }
    case ResolvedBoolean::Composite: {
      auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector);
      SDB_ASSERT(compound != nullptr);

      BooleanQuery::queries_t queries{{ctx.memory}};
      queries.reserve(resolved.incl.size() + resolved.excl.size());

      const auto composite_boost = ctx.boost * resolved.boost;

      size_t idx = 0;
      for (const auto* filter : resolved.incl) {
        PrepareContext child = ctx;
        child.boost = composite_boost;
        child.collector = &compound->Child(idx);
        queries.emplace_back(filter->PrepareSegment(segment, child));
        ++idx;
      }
      for (const auto* filter : resolved.excl) {
        PrepareContext child = ctx;
        child.boost = composite_boost;
        child.collector = &compound->Child(idx);
        queries.emplace_back(filter->PrepareSegment(segment, child));
        ++idx;
      }

      const size_t excl_start = resolved.incl.size();
      switch (resolved.composite) {
        case ResolvedBoolean::kAnd:
          return memory::make_tracked<AndQuery>(
            ctx.memory, segment, std::move(queries), excl_start, resolved.merge,
            composite_boost);
        case ResolvedBoolean::kOr:
          return memory::make_tracked<OrQuery>(ctx.memory, segment,
                                               std::move(queries), excl_start,
                                               resolved.merge, composite_boost);
        case ResolvedBoolean::kMinMatch:
          return memory::make_tracked<MinMatchQuery>(
            ctx.memory, segment, std::move(queries), excl_start, resolved.merge,
            composite_boost, resolved.min_match);
      }
    }
  }
  return QueryBuilder::Empty();
}

QueryBuilder::ptr Not::PrepareSegment(const SubReader& segment,
                                      const PrepareContext& ctx) const {
  const auto res = OptimizeNot(*this);

  if (!res.first) {
    return QueryBuilder::Empty();
  }

  if (res.second) {
    auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector);
    SDB_ASSERT(compound != nullptr);

    auto all_docs = MakeAllDocsFilter(kNoBoost);

    const auto child_boost = ctx.boost * Boost();

    BooleanQuery::queries_t queries{{ctx.memory}};
    queries.reserve(2);
    {
      PrepareContext child = ctx;
      child.boost = child_boost;
      child.collector = &compound->Child(0);
      queries.emplace_back(all_docs->PrepareSegment(segment, child));
    }
    {
      PrepareContext child = ctx;
      child.boost = child_boost;
      child.collector = &compound->Child(1);
      queries.emplace_back(res.first->PrepareSegment(segment, child));
    }

    return memory::make_tracked<AndQuery>(ctx.memory, segment,
                                          std::move(queries), size_t{1},
                                          ScoreMergeType::Sum, child_boost);
  }

  // negation has been optimized out
  return res.first->PrepareSegment(segment, ctx.Boost(Boost()));
}

PrepareCollector::ptr Not::MakeCollector(const Scorer* scorer) const {
  const auto res = OptimizeNot(*this);

  if (!res.first) {
    return std::make_unique<NoopCollector>();
  }

  if (res.second) {
    auto all_docs = MakeAllDocsFilter(kNoBoost);
    auto compound = std::make_unique<CompoundCollector>(scorer);
    compound->Add(all_docs->MakeCollector(scorer));
    compound->Add(res.first->MakeCollector(nullptr));
    return compound;
  }

  return res.first->MakeCollector(scorer);
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
