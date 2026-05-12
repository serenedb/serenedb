////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <span>

#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"

namespace irs {

class FilterRulesConstructor {
 public:
  class FilterRule;
  using ptr = std::unique_ptr<FilterRule>;

  class FilterRule {
   public:
    using TypesMap = absl::flat_hash_map<irs::TypeInfo::type_id,
                                         std::vector<irs::Filter::ptr>>;

    enum class FilterRuleTraversalType : uint8_t {
      Bottom = 0,  // dfs from the bottom to the top
      Top = 1,     // bfs from the top to the bottom
    };

    FilterRule() = default;
    explicit FilterRule(std::span<const TypeInfo::type_id> filter_types)
      : _desired_filter_types(filter_types.begin(), filter_types.end()) {}

    auto SetTraversalType(FilterRuleTraversalType type) -> FilterRule& {
      _traversal_type = type;
      return *this;
    }

    auto traversal_type() const noexcept -> FilterRuleTraversalType {
      return _traversal_type;
    }

    auto AddFilterType(TypeInfo::type_id type_id) -> FilterRule& {
      _desired_filter_types.emplace(type_id);
      return *this;
    }

    [[nodiscard]] auto ApplyWrapper(Filter::ptr filter) const -> Filter::ptr;

    struct FilterRuleOptions {
      // sub-filters outside of desired_filter_types are present in the current
      // node
      bool has_other_filters = false;
    };

    struct [[nodiscard]] FilterRuleResult {
      Filter::ptr filter;
      bool is_applied = false;
    };

    virtual auto ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                              FilterRuleOptions options) const
      -> FilterRuleResult {
      return FilterRuleResult{};
    }

    virtual auto ApplyBoolean(Or& current_filter, TypesMap&& sub_filters,
                              FilterRuleOptions options) const
      -> FilterRuleResult {
      return FilterRuleResult{};
    }

    virtual auto ApplyNot(Not& current_filter, Filter::ptr sub_filter,
                          FilterRuleOptions options) const -> FilterRuleResult {
      return FilterRuleResult{};
    }

    virtual ~FilterRule() = default;

   private:
    FilterRuleTraversalType _traversal_type = FilterRuleTraversalType::Bottom;
    absl::btree_set<TypeInfo::type_id> _desired_filter_types;
    bool _works_with_empty_filters = false;
  };

  FilterRulesConstructor() = default;

  void Add(ptr rule) { _rules.push_back(std::move(rule)); }

  template<typename FilterType, typename... Args>
  void Add(Args&&... args) {
    _rules.push_back(std::make_unique<FilterType>(std::forward<Args>(args)...));
  }

  [[nodiscard]] auto Apply(Filter::ptr filter) const -> Filter::ptr;

 private:
  [[nodiscard]] auto AstTraversalFromBottom(
    Filter::ptr filter, const FilterRule& rule) const -> Filter::ptr;

  std::vector<ptr> _rules;
};

class NotFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit NotFilterRule() { AddFilterType(irs::Type<irs::Not>::id()); }

  auto ApplyNot(Not& current_filter, Filter::ptr sub_filter,
                FilterRuleOptions options) const -> FilterRuleResult override;
};

class AndFlatteningFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit AndFlatteningFilterRule() {
    AddFilterType(irs::Type<irs::And>::id());
  }

  auto ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                    FilterRuleOptions options) const
    -> FilterRuleResult override;
};

class OrFlatteningFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit OrFlatteningFilterRule() { AddFilterType(irs::Type<irs::Or>::id()); }

  auto ApplyBoolean(Or& current_filter, TypesMap&& sub_filters,
                    FilterRuleOptions options) const
    -> FilterRuleResult override;
};

class ByTermsFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit ByTermsFilterRule() { AddFilterType(irs::Type<irs::ByTerm>::id()); }

  auto ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                    FilterRuleOptions options) const
    -> FilterRuleResult override;
  auto ApplyBoolean(Or& current_filter, TypesMap&& sub_filters,
                    FilterRuleOptions options) const
    -> FilterRuleResult override;
};

class LevenshteinPrefixFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit LevenshteinPrefixFilterRule() {
    AddFilterType(irs::Type<irs::ByEditDistance>::id());
    AddFilterType(irs::Type<irs::ByPrefix>::id());
  }

  auto ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                    FilterRuleOptions options) const
    -> FilterRuleResult override;
};

}  // namespace irs
