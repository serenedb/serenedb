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

// Represents user-side boolean filter as the container for other filters
class BooleanFilter : public FilterWithBoost, public AllDocsProvider {
 public:
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

  Query::ptr PrepareImpl(const PrepareContext& ctx) const;

 protected:
  bool equals(const Filter& rhs) const noexcept final;

  virtual Query::ptr PrepareBoolean(std::span<const Filter::ptr> filters,
                                    const PrepareContext& ctx) const = 0;

  std::vector<Filter::ptr> _filters;
  ScoreMergeType _merge_type = ScoreMergeType::Sum;
};

// Represents conjunction
class And final : public BooleanFilter {
 public:
  Query::ptr prepare(const PrepareContext& ctx) const final;

  TypeInfo::type_id type() const noexcept final { return irs::Type<And>::id(); }

 protected:
  Query::ptr PrepareBoolean(std::span<const Filter::ptr> filters,
                            const PrepareContext& ctx) const final;
};

// Represents disjunction
class Or final : public BooleanFilter {
 public:
  // Return minimum number of subqueries which must be satisfied
  size_t min_match_count() const { return _min_match_count; }

  // Sets minimum number of subqueries which must be satisfied
  Or& min_match_count(size_t count) {
    _min_match_count = count;
    return *this;
  }

  Query::ptr prepare(const PrepareContext& ctx) const final;

  TypeInfo::type_id type() const noexcept final { return irs::Type<Or>::id(); }

 protected:
  Query::ptr PrepareBoolean(std::span<const Filter::ptr> filters,
                            const PrepareContext& ctx) const final;

 private:
  uint32_t _min_match_count{1};
};

class Exclusion : public FilterWithType<Exclusion> {
 public:
  const Filter* include() const { return _include.get(); }
  const Filter* exclude() const { return _exclude.get(); }

  template<typename T>
  const T* exclude() const {
    return sdb::basics::downCast<T>(_exclude.get());
  }

  template<typename T, typename... Args>
  T& include(Args&&... args) {
    static_assert(std::is_base_of_v<irs::Filter, T>);
    _include = std::make_unique<T>(std::forward<Args>(args)...);
    return sdb::basics::downCast<T>(*_include);
  }

  template<typename T, typename... Args>
  T& exclude(Args&&... args) {
    static_assert(std::is_base_of_v<irs::Filter, T>);
    _exclude = std::make_unique<T>(std::forward<Args>(args)...);
    return sdb::basics::downCast<T>(*_exclude);
  }

  Filter& include(Filter::ptr filter) {
    SDB_ASSERT(filter);
    _include = std::move(filter);
    return *_include;
  }

  Filter& exclude(Filter::ptr filter) {
    SDB_ASSERT(filter);
    _exclude = std::move(filter);
    return *_exclude;
  }

  bool empty() const { return _exclude == nullptr; }

  Filter::ptr& mutable_include() noexcept { return _include; }
  Filter::ptr& mutable_exclude() noexcept { return _exclude; }

  Query::ptr prepare(const PrepareContext& ctx) const final;

 protected:
  bool equals(const irs::Filter& rhs) const noexcept final;

 private:
  Filter::ptr _include;
  Filter::ptr _exclude;
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

  Query::ptr prepare(const PrepareContext& ctx) const final;

 protected:
  bool equals(const irs::Filter& rhs) const noexcept final;

 private:
  Filter::ptr _filter;
};

}  // namespace irs
