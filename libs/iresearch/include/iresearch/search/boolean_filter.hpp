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

  Filter::ptr PopBack() {
    if (_filters.empty()) {
      return nullptr;
    }
    auto result = std::move(_filters.back());
    _filters.pop_back();
    return result;
  }

  Query::ptr prepare(const PrepareContext& ctx) const override;

 protected:
  bool equals(const Filter& rhs) const noexcept final;

  virtual Query::ptr PrepareBoolean(std::vector<const Filter*>& incl,
                                    std::vector<const Filter*>& excl,
                                    const PrepareContext& ctx) const = 0;

 private:
  void group_filters(AllDocsProvider::Ptr& all_docs_zero_boost,
                     std::vector<const Filter*>& incl,
                     std::vector<const Filter*>& excl) const;

  std::vector<Filter::ptr> _filters;
  ScoreMergeType _merge_type{ScoreMergeType::Sum};
};

// Represents conjunction
class And final : public BooleanFilter {
 public:
  TypeInfo::type_id type() const noexcept final { return irs::Type<And>::id(); }

 protected:
  Query::ptr PrepareBoolean(std::vector<const Filter*>& incl,
                            std::vector<const Filter*>& excl,
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
  Query::ptr PrepareBoolean(std::vector<const Filter*>& incl,
                            std::vector<const Filter*>& excl,
                            const PrepareContext& ctx) const final;

 private:
  size_t _min_match_count{1};
};

// Represents negation
class Not : public FilterWithType<Not>, public AllDocsProvider {
 public:
  const Filter* filter() const { return _filter.get(); }

  template<typename T>
  const T* filter() const {
    return sdb::basics::downCast<T>(_filter.get());
  }

  template<typename T, typename... Args>
  T& filter(Args&&... args) {
    static_assert(std::is_base_of_v<irs::Filter, T>);
    _filter = std::make_unique<T>(std::forward<Args>(args)...);
    return sdb::basics::downCast<T>(*_filter);
  }

  void clear() { _filter.reset(); }
  bool empty() const { return nullptr == _filter; }

  Query::ptr prepare(const PrepareContext& ctx) const final;

 protected:
  bool equals(const irs::Filter& rhs) const noexcept final;

 private:
  Filter::ptr _filter;
};

}  // namespace irs
