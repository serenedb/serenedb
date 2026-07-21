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
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/utils/iterator.hpp"

namespace irs {

// Represents user-side boolean filter as the container for other filters
class BooleanFilter : public FilterWithBoost, public AllDocsProvider {
 public:
  BooleanFilter() = default;
  BooleanFilter(std::vector<Filter::ptr> filters)
    : _filters{std::move(filters)} {}

  auto begin() const { return _filters.begin(); }
  auto end() const { return _filters.end(); }

  ScoreMergeType merge_type() const noexcept { return _merge_type; }

  void merge_type(ScoreMergeType merge_type) noexcept {
    _merge_type = merge_type;
  }

  template<typename T, typename... Args>
  T& add(Args&&... args) {
    return add(std::make_unique<T>(std::forward<Args>(args)...));
  }

  template<typename T>
  T& add(std::unique_ptr<T>&& filter) {
    SDB_ASSERT(filter);
    return sdb::basics::downCast<T>(*_filters.emplace_back(std::move(filter)));
  }

  void clear() { _filters.clear(); }
  bool empty() const { return _filters.empty(); }
  size_t size() const { return _filters.size(); }
  auto& operator[](this auto& self, size_t i) noexcept {
    SDB_ASSERT(i < self._filters.size());
    return *self._filters[i];
  }

  Filter::ptr PopBack() {
    if (_filters.empty()) {
      return nullptr;
    }
    auto result = std::move(_filters.back());
    _filters.pop_back();
    return result;
  }

  void Erase(size_t to) {
    SDB_ASSERT(to <= _filters.size());
    _filters.erase(_filters.begin() + to, _filters.end());
  }

  std::vector<Filter::ptr>& mutable_filters() noexcept { return _filters; }

  std::span<Filter::ptr> GetChildren() final { return _filters; }

 protected:
  bool equals(const Filter& rhs) const noexcept final;

  std::vector<Filter::ptr> _filters;
  ScoreMergeType _merge_type = ScoreMergeType::Sum;
};

// Represents conjunction
class And final : public BooleanFilter {
 public:
  And() = default;
  And(std::vector<Filter::ptr> filters) : BooleanFilter{std::move(filters)} {}

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  TermPredicate::ptr CompileTermPredicate() const final;

  TermIterator::ptr CompileTermIterator(const TermReader& reader) const final;

  TypeInfo::type_id type() const noexcept final { return irs::Type<And>::id(); }
};

// Represents disjunction
class Or final : public BooleanFilter {
 public:
  Or() = default;
  Or(std::vector<Filter::ptr> filters) : BooleanFilter{std::move(filters)} {}

  // Return minimum number of subqueries which must be satisfied
  size_t min_match_count() const { return _min_match_count; }

  // Sets minimum number of subqueries which must be satisfied
  Or& min_match_count(size_t count) {
    _min_match_count = count;
    return *this;
  }

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  TermPredicate::ptr CompileTermPredicate() const final;

  TypeInfo::type_id type() const noexcept final { return irs::Type<Or>::id(); }

 private:
  uint32_t _min_match_count{1};
};

class Exclusion : public FilterWithType<Exclusion> {
 public:
  const Filter::ptr& GetInclude() const {
    SDB_ASSERT(_filters.size() >= 1);
    return _filters[0];
  }

  template<typename T, typename... Args>
  T& include(Args&&... args) {
    static_assert(std::is_base_of_v<irs::Filter, T>);
    SDB_ASSERT(_filters.size() == 1);
    _filters[0] = std::make_unique<T>(std::forward<Args>(args)...);
    return sdb::basics::downCast<T>(*_filters[0]);
  }

  template<typename T, typename... Args>
  T& exclude(Args&&... args) {
    static_assert(std::is_base_of_v<irs::Filter, T>);
    auto& slot =
      _filters.emplace_back(std::make_unique<T>(std::forward<Args>(args)...));
    return sdb::basics::downCast<T>(*slot);
  }

  Filter& include(Filter::ptr filter) {
    SDB_ASSERT(filter);
    SDB_ASSERT(_filters.size() >= 1);
    _filters[0] = std::move(filter);
    return *_filters[0];
  }

  Filter& exclude(Filter::ptr filter) {
    SDB_ASSERT(filter);
    return *_filters.emplace_back(std::move(filter));
  }

  bool empty() const { return _filters.size() == 1; }

  Filter::ptr& mutable_include() noexcept { return _filters[0]; }
  std::span<Filter::ptr> mutable_excludes() noexcept {
    return GetChildren().subspan(1);
  }

  std::span<const Filter::ptr> GetExcludes() const {
    return std::span{_filters}.subspan(1);
  }

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  TermPredicate::ptr CompileTermPredicate() const final;

  TermIterator::ptr CompileTermIterator(const TermReader& reader) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  std::span<Filter::ptr> GetChildren() final { return std::span{_filters}; }

 protected:
  bool equals(const irs::Filter& rhs) const noexcept final;

 private:
  // [include, excludes[0], excludes[1], ...]
  std::vector<Filter::ptr> _filters{1};
};

class Not final : public FilterWithType<Not> {
 public:
  Not() = default;
  explicit Not(Filter::ptr filter) : _filter{std::move(filter)} {}

  const Filter* filter() const noexcept { return _filter.get(); }
  Filter::ptr& mutable_filter() noexcept { return _filter; }

  template<typename T, typename... Args>
  T& filter(Args&&... args) {
    static_assert(std::is_base_of_v<irs::Filter, T>);
    _filter = std::make_unique<T>(std::forward<Args>(args)...);
    return sdb::basics::downCast<T>(*_filter);
  }

  void clear() { _filter.reset(); }
  bool empty() const { return nullptr == _filter; }

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  TermPredicate::ptr CompileTermPredicate() const final;

  std::span<Filter::ptr> GetChildren() final { return {&_filter, 1}; }

 protected:
  bool equals(const irs::Filter& rhs) const noexcept final;

 private:
  Filter::ptr _filter;
};

}  // namespace irs
