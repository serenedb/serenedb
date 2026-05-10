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

#include <initializer_list>
#include <unordered_map>

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"

namespace irs {

class FilterRulesConstructor {
 public:
  class FilterRule;
  using ptr = std::shared_ptr<FilterRule>;
  class FilterRule {  // think about managed_ptr like in Filter::Query
   public:
    using Base = FilterRule;
    using TypesMap =
      std::unordered_map<irs::TypeInfo::type_id, std::vector<irs::Filter::ptr>>;
    enum class FilterRuleTraversalType : uint8_t {
      Bottom = 0,  // dfs from the bottom to the top
      Top = 1      // bfs from the top to the bottom
    };

    FilterRule() = default;
    FilterRule(std::initializer_list<TypeInfo::type_id> filter_types)
      : _desired_filter_types(std::move(filter_types)) {}

    FilterRule& SetTraversalType(FilterRuleTraversalType type) {
      _traversal_type = type;
      return *this;
    }

    FilterRuleTraversalType traversal_type() const noexcept {
      return _traversal_type;
    }

    FilterRule& AddFilterType(TypeInfo::type_id type_id) {
      _desired_filter_types.emplace(type_id);
      return *this;
    }

    // filter can be only AND, OR or NOT
    Filter::ptr ApplyWrapper(Filter::ptr filter) const;

    struct FilterRuleOptions {
      bool has_other_filters =
        false;  // for cases when current filter has filters which differ from
                // desired_filter_types
    };

    struct FilterRuleResult {
      Filter::ptr filter =
        nullptr;  // return filter value - could be the same or new
      bool is_new_filter = false;  // signify that new filter was created
      bool is_applied = false;     // show if rule was applied
    };

    // call of these functions depends on current_filter type
    // current_filter can be only AND, OR or NOT
    inline virtual FilterRuleResult ApplyBoolean(
      And& current_filter, TypesMap&& sub_filters,
      FilterRuleOptions options) const {
      return FilterRuleResult{};
    };

    inline virtual FilterRuleResult ApplyBoolean(
      Or& current_filter, TypesMap&& sub_filters,
      FilterRuleOptions options) const {
      return FilterRuleResult{};
    };

    inline virtual FilterRuleResult ApplyNot(Not& current_filter,
                                             Filter::ptr sub_filter,
                                             FilterRuleOptions options) const {
      return FilterRuleResult{};
    };

    virtual ~FilterRule() = default;

   private:
    // parameters like make traversal from top or bottom
    FilterRuleTraversalType _traversal_type = FilterRuleTraversalType::Bottom;
    std::set<TypeInfo::type_id> _desired_filter_types;
    bool _works_with_empty_filters =
      false;  // think about that, maybe want to have rule with zero suitable
              // subfilters
  };

  FilterRulesConstructor() = default;

  void Add(ptr rule) { _rules.push_back(std::move(rule)); }

  template<typename FilterType, typename... Args>
  void Add(Args&&... args) {
    _rules.push_back(std::make_shared<FilterType>(std::forward<Args>(args)...));
  }

  Filter::ptr Apply(Filter::ptr filter) const;

 private:
  Filter::ptr AstTraversalFromBottom(Filter::ptr filter, ptr rule) const;

  std::vector<ptr> _rules;
};

class NotFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  NotFilterRule() : FilterRule({irs::Type<irs::Not>::id()}) {}

  // think about boost in that context
  virtual FilterRuleResult ApplyNot(Not& current_filter, Filter::ptr sub_filter,
                                    FilterRuleOptions options) const override;

 private:
};

class AndFlatteningFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  AndFlatteningFilterRule() : FilterRule({irs::Type<irs::And>::id()}) {}

  // think about boost in that context
  virtual FilterRuleResult ApplyBoolean(
    And& current_filter, TypesMap&& sub_filters,
    FilterRuleOptions options) const override;

 private:
};

class OrFlatteningFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  OrFlatteningFilterRule() : FilterRule({irs::Type<irs::Or>::id()}) {}

  // think about boost in that context
  virtual FilterRuleResult ApplyBoolean(
    Or& current_filter, TypesMap&& sub_filters,
    FilterRuleOptions options) const override;

 private:
};

class ByTermsFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  ByTermsFilterRule() : FilterRule({irs::Type<irs::ByTerm>::id()}) {}

  // think about boost in that context
  virtual FilterRuleResult ApplyBoolean(
    And& current_filter, TypesMap&& sub_filters,
    FilterRuleOptions options) const override;
  // virtual FilterRuleResult ApplyBoolean(Or& current_filter, TypesMap&&
  // sub_filters, FilterRuleOptions options) const override;

 private:
};

}  // namespace irs
