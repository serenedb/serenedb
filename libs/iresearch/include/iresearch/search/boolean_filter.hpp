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

#pragma once

#include <span>
#include <vector>

#include "iresearch/search/all_docs_provider.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/iterator.hpp"

namespace irs {

class BooleanFilter;

class MutableFilter {
 public:
  std::vector<Filter::ptr>& Children() const noexcept { return _incl; }
  std::vector<Filter::ptr>& Excludes() const noexcept { return _excl; }
  std::span<Filter::ptr> ChildSlots() const noexcept { return _incl; }
  std::span<Filter::ptr> ExcludeSlots() const noexcept { return _excl; }

  void Erase(size_t to) const {
    SDB_ASSERT(to <= _incl.size());
    _incl.erase(_incl.begin() + to, _incl.end());
  }

 private:
  friend class BooleanFilter;
  MutableFilter(std::vector<Filter::ptr>& incl,
                std::vector<Filter::ptr>& excl) noexcept
    : _incl{incl}, _excl{excl} {}

  std::vector<Filter::ptr>& _incl;
  std::vector<Filter::ptr>& _excl;
};

class FilterMutator;

// Represents user-side boolean filter as the container for other filters
class BooleanFilter : public FilterWithBoost, public AllDocsProvider {
 public:
  auto begin() const { return _incl.begin(); }
  auto end() const { return _incl.end(); }

  ScoreMergeType merge_type() const noexcept { return _merge_type; }

  void merge_type(ScoreMergeType merge_type) noexcept {
    _merge_type = merge_type;
  }

  bool empty() const { return _incl.empty() && _excl.empty(); }
  size_t size() const { return _incl.size(); }
  auto& operator[](this auto& self, size_t i) noexcept {
    SDB_ASSERT(i < self._incl.size());
    return *self._incl[i];
  }

  // Excluded children: filters whose matches are removed from the includes.
  auto ExcludesBegin() const { return _excl.begin(); }
  auto ExcludesEnd() const { return _excl.end(); }
  bool ExcludesEmpty() const { return _excl.empty(); }
  size_t ExcludesSize() const { return _excl.size(); }
  auto& Exclude(this auto& self, size_t i) noexcept {
    SDB_ASSERT(i < self._excl.size());
    return *self._excl[i];
  }

 protected:
  bool equals(const Filter& rhs) const noexcept final;

  // 0..size()-1 included filters; the exclude set is matched separately.
  std::vector<Filter::ptr> _incl;
  std::vector<Filter::ptr> _excl;
  ScoreMergeType _merge_type = ScoreMergeType::Sum;

 private:
  friend class FilterMutator;
  MutableFilter GetMutable() noexcept { return {_incl, _excl}; }
};

// Represents conjunction. Built from a single flat list of filters: a child Not
// contributes its (unwrapped) target to the excludes, everything else to the
// includes. Once constructed it is mutated only by the Optimize pass.
class And final : public BooleanFilter {
 public:
  And() = default;

  explicit And(std::vector<Filter::ptr> filters,
               ScoreMergeType merge_type = ScoreMergeType::Sum);

  Query::ptr prepare(const PrepareContext& ctx) const final;

  TypeInfo::type_id type() const noexcept final { return irs::Type<And>::id(); }
};

// Represents disjunction
class Or final : public BooleanFilter {
 public:
  Or() = default;

  explicit Or(std::vector<Filter::ptr> incl,
              ScoreMergeType merge_type = ScoreMergeType::Sum) {
    _incl = std::move(incl);
    _merge_type = merge_type;
  }

  // Return minimum number of subqueries which must be satisfied
  size_t min_match_count() const { return _min_match_count; }

  // Sets minimum number of subqueries which must be satisfied
  Or& min_match_count(size_t count) {
    _min_match_count = count;
    return *this;
  }

  Query::ptr prepare(const PrepareContext& ctx) const final;

  TypeInfo::type_id type() const noexcept final { return irs::Type<Or>::id(); }

 private:
  uint32_t _min_match_count{1};
};

// Represents negation
class Not : public FilterWithType<Not>, public AllDocsProvider {
 public:
  Not() = default;
  explicit Not(Filter::ptr filter) : _filter{std::move(filter)} {}

  const Filter* filter() const { return _filter.get(); }

  template<typename T>
  const T* filter() const {
    return sdb::basics::downCast<T>(_filter.get());
  }

  bool empty() const { return nullptr == _filter; }

  Query::ptr prepare(const PrepareContext& ctx) const final;

 protected:
  bool equals(const irs::Filter& rhs) const noexcept final;

 private:
  friend class And;
  friend class FilterMutator;
  Filter::ptr& ChildSlot() noexcept { return _filter; }

  Filter::ptr _filter;
};

inline And::And(std::vector<Filter::ptr> filters, ScoreMergeType merge_type) {
  _incl.reserve(filters.size());
  for (auto& filter : filters) {
    if (irs::Type<Not>::id() == filter->type()) {
      auto& not_node = sdb::basics::downCast<Not>(*filter);
      if (!not_node.empty()) {
        _excl.emplace_back(std::move(not_node.ChildSlot()));
        continue;
      }
    }
    _incl.emplace_back(std::move(filter));
  }
  _merge_type = merge_type;
}

}  // namespace irs
