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

#include <algorithm>
#include <optional>
#include <vector>

#include "all_filter.hpp"
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

namespace {

PrepareCollector::ptr MakeCompoundCollector(
  std::span<const Filter::ptr> filters, const Scorer* scorer) {
  auto compound = std::make_unique<CompoundCollector>(scorer);
  for (const auto& filter : filters) {
    compound->Add(filter->MakeCollector(scorer));
  }
  return compound;
}

BooleanQuery::queries_t PrepareChildren(std::span<const Filter::ptr> filters,
                                        const SubReader& segment,
                                        const PrepareContext& ctx,
                                        score_t composite_boost) {
  auto* compound = ctx.collector
                     ? &sdb::basics::downCast<CompoundCollector>(*ctx.collector)
                     : nullptr;
  BooleanQuery::queries_t queries{{ctx.memory}};
  queries.reserve(filters.size());
  PrepareContext child = ctx;
  child.boost = composite_boost;
  size_t idx = 0;
  for (const auto& filter : filters) {
    child.collector = compound ? &compound->Child(idx++) : nullptr;
    queries.emplace_back(filter->PrepareSegment(segment, child));
  }
  return queries;
}

}  // namespace

PrepareCollector::ptr And::MakeCollector(const Scorer* scorer) const {
  return MakeCompoundCollector(_filters, scorer);
}

QueryBuilder::ptr And::PrepareSegment(const SubReader& segment,
                                      const PrepareContext& ctx) const {
  SDB_ASSERT(_filters.size() > 1);
  const auto composite_boost = ctx.boost * Boost();
  auto queries = PrepareChildren(_filters, segment, ctx, composite_boost);
  return memory::make_tracked<AndQuery>(ctx.memory, segment, std::move(queries),
                                        merge_type(), composite_boost);
}

PrepareCollector::ptr Or::MakeCollector(const Scorer* scorer) const {
  return MakeCompoundCollector(_filters, scorer);
}

QueryBuilder::ptr Or::PrepareSegment(const SubReader& segment,
                                     const PrepareContext& ctx) const {
  SDB_ASSERT(_filters.size() > 1);
  SDB_ASSERT(_min_match_count != 0);
  const auto composite_boost = ctx.boost * Boost();
  auto queries = PrepareChildren(_filters, segment, ctx, composite_boost);
  if (_min_match_count <= 1) {
    return memory::make_tracked<OrQuery>(
      ctx.memory, segment, std::move(queries), merge_type(), composite_boost);
  }
  if (_min_match_count >= _filters.size()) {
    return memory::make_tracked<AndQuery>(
      ctx.memory, segment, std::move(queries), merge_type(), composite_boost);
  }
  return memory::make_tracked<MinMatchQuery>(ctx.memory, segment,
                                             std::move(queries), merge_type(),
                                             composite_boost, _min_match_count);
}

PrepareCollector::ptr Exclusion::MakeCollector(const Scorer* scorer) const {
  auto compound = std::make_unique<CompoundCollector>(scorer);
  const auto& include = GetInclude();
  compound->Add(include ? include->MakeCollector(scorer)
                        : All{}.MakeCollector(scorer));
  for (const auto& exclude : GetExcludes()) {
    compound->Add(exclude->MakeCollector(nullptr));
  }
  return compound;
}

QueryBuilder::ptr Exclusion::PrepareSegment(const SubReader& segment,
                                            const PrepareContext& ctx) const {
  SDB_ASSERT(!GetExcludes().empty());
  auto* compound = ctx.collector
                     ? &sdb::basics::downCast<CompoundCollector>(*ctx.collector)
                     : nullptr;

  const auto child_boost = ctx.boost * Boost();

  PrepareContext incl_ctx = ctx;
  incl_ctx.boost = child_boost;
  incl_ctx.collector = compound ? &compound->Child(0) : nullptr;
  const auto& include_filter = GetInclude();
  auto include = include_filter
                   ? include_filter->PrepareSegment(segment, incl_ctx)
                   : MakeAllQuery(segment, incl_ctx, kNoBoost);

  std::vector<QueryBuilder::ptr> excludes;
  const auto exclude_filters = GetExcludes();
  SDB_ASSERT(absl::c_all_of(exclude_filters,
                            [](const auto& excl) { return excl != nullptr; }));
  excludes.reserve(exclude_filters.size());
  size_t idx = 1;
  for (const auto& filter : exclude_filters) {
    PrepareContext child = ctx;
    child.boost = child_boost;
    child.collector = compound ? &compound->Child(idx++) : nullptr;
    excludes.emplace_back(filter->PrepareSegment(segment, child));
  }
  SDB_ASSERT(
    absl::c_all_of(excludes, [](const auto& excl) { return excl != nullptr; }));
  return memory::make_tracked<ExclusionQuery>(
    ctx.memory, segment, std::move(include), std::move(excludes));
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

namespace {

class AllOfPredicate final : public TermPredicate {
 public:
  explicit AllOfPredicate(std::vector<TermPredicate::ptr>&& preds) noexcept
    : _preds{std::move(preds)} {}

  bool Accepts(bytes_view term) const final {
    return absl::c_all_of(_preds,
                          [&](const auto& p) { return p->Accepts(term); });
  }

 private:
  std::vector<TermPredicate::ptr> _preds;
};

class MinMatchPredicate final : public TermPredicate {
 public:
  MinMatchPredicate(std::vector<TermPredicate::ptr>&& preds,
                    size_t min_match) noexcept
    : _preds{std::move(preds)}, _min_match{min_match} {}

  bool Accepts(bytes_view term) const final {
    size_t matched = 0;
    for (const auto& p : _preds) {
      if (p->Accepts(term) && ++matched == _min_match) {
        return true;
      }
    }
    return false;
  }

 private:
  std::vector<TermPredicate::ptr> _preds;
  size_t _min_match;
};

class NegatedPredicate final : public TermPredicate {
 public:
  explicit NegatedPredicate(TermPredicate::ptr pred) noexcept
    : _pred{std::move(pred)} {}

  bool Accepts(bytes_view term) const final { return !_pred->Accepts(term); }

 private:
  TermPredicate::ptr _pred;
};

TermPredicate::ptr CombinePredicates(std::vector<TermPredicate::ptr>&& preds) {
  if (preds.empty()) {
    return nullptr;
  }
  if (preds.size() == 1) {
    return std::move(preds.front());
  }
  return std::make_unique<AllOfPredicate>(std::move(preds));
}

bool CompileNegatedInto(std::span<const Filter::ptr> excludes,
                        std::vector<TermPredicate::ptr>& preds) {
  return absl::c_all_of(excludes, [&](const auto& excluded) {
    auto pred = excluded->CompileTermPredicate();
    if (!pred) {
      return false;
    }
    preds.push_back(std::make_unique<NegatedPredicate>(std::move(pred)));
    return true;
  });
}

TermIterator::ptr FilterIterator(TermIterator::ptr&& it,
                                 std::vector<TermPredicate::ptr>&& preds) {
  auto predicate = CombinePredicates(std::move(preds));
  if (!predicate) {
    return std::move(it);
  }
  return memory::make_managed<FilteredTermIterator>(std::move(it),
                                                    std::move(predicate));
}

std::optional<std::vector<TermPredicate::ptr>> CompileChildren(
  const BooleanFilter& filter) {
  std::vector<TermPredicate::ptr> preds;
  preds.reserve(filter.size());
  for (const auto& child : filter) {
    auto pred = child->CompileTermPredicate();
    if (!pred) {
      return std::nullopt;
    }
    preds.push_back(std::move(pred));
  }
  return preds;
}

}  // namespace

TermIterator::ptr And::CompileTermIterator(const TermReader& reader) const {
  if (empty()) {
    return nullptr;
  }
  auto it = (*this)[0].CompileTermIterator(reader);
  if (!it) {
    return nullptr;
  }
  std::vector<TermPredicate::ptr> predicates;
  predicates.reserve(size() - 1);
  for (const auto& sibling : std::ranges::subrange{std::next(begin()), end()}) {
    auto predicate = sibling->CompileTermPredicate();
    if (!predicate) {
      return nullptr;
    }
    predicates.push_back(std::move(predicate));
  }
  return FilterIterator(std::move(it), std::move(predicates));
}

TermPredicate::ptr And::CompileTermPredicate() const {
  if (empty()) {
    return nullptr;
  }
  auto preds = CompileChildren(*this);
  if (!preds) {
    return nullptr;
  }
  return CombinePredicates(std::move(*preds));
}

TermPredicate::ptr Or::CompileTermPredicate() const {
  const size_t min_match = min_match_count();
  if (empty() || min_match == 0 || min_match > size()) {
    return nullptr;
  }
  auto preds = CompileChildren(*this);
  if (!preds) {
    return nullptr;
  }
  if (preds->size() == 1) {
    return std::move(preds->front());
  }
  return std::make_unique<MinMatchPredicate>(std::move(*preds), min_match);
}

TermPredicate::ptr Not::CompileTermPredicate() const {
  const auto* child = filter();
  if (!child) {
    return nullptr;
  }
  auto pred = child->CompileTermPredicate();
  if (!pred) {
    return nullptr;
  }
  return std::make_unique<NegatedPredicate>(std::move(pred));
}

TermIterator::ptr Exclusion::CompileTermIterator(
  const TermReader& reader) const {
  const auto& include = GetInclude();
  if (!include) {
    return Filter::CompileTermIterator(reader);
  }
  auto it = include->CompileTermIterator(reader);
  if (!it) {
    return nullptr;
  }
  const auto excludes = GetExcludes();
  if (excludes.empty()) {
    return it;
  }
  std::vector<TermPredicate::ptr> predicates;
  predicates.reserve(excludes.size());
  if (!CompileNegatedInto(excludes, predicates)) {
    return nullptr;
  }
  return FilterIterator(std::move(it), std::move(predicates));
}

TermPredicate::ptr Exclusion::CompileTermPredicate() const {
  const auto& include = GetInclude();
  const auto excludes = GetExcludes();
  std::vector<TermPredicate::ptr> preds;
  preds.reserve(excludes.size() + 1);
  if (include) {
    auto pred = include->CompileTermPredicate();
    if (!pred) {
      return nullptr;
    }
    preds.push_back(std::move(pred));
  } else if (excludes.empty()) {
    return nullptr;
  }
  if (!CompileNegatedInto(excludes, preds)) {
    return nullptr;
  }
  return CombinePredicates(std::move(preds));
}

}  // namespace irs
