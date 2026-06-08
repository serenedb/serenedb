#pragma once

#include <span>

#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"

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

    [[nodiscard]] Filter::ptr ApplyWrapper(Filter::ptr filter) const;

    struct FilterRuleOptions {
      // sub-filters outside of desired_filter_types are present in the current
      // node
      bool has_other_filters = false;
    };

    struct [[nodiscard]] FilterRuleResult {
      Filter::ptr filter;
      bool is_applied = false;
    };

    virtual FilterRuleResult ApplyBoolean(And& current_filter,
                                          TypesMap&& sub_filters,
                                          FilterRuleOptions options) const {
      return FilterRuleResult{};
    }

    virtual FilterRuleResult ApplyBoolean(Or& current_filter,
                                          TypesMap&& sub_filters,
                                          FilterRuleOptions options) const {
      return FilterRuleResult{};
    }

    virtual FilterRuleResult ApplyNot(Not& current_filter,
                                      Filter::ptr sub_filter,
                                      FilterRuleOptions options) const {
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

  [[nodiscard]] Filter::ptr Apply(Filter::ptr filter) const;

 private:
  [[nodiscard]] Filter::ptr AstTraversalFromBottom(
    Filter::ptr filter, const FilterRule& rule) const;

  std::vector<ptr> _rules;
};

class NotFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit NotFilterRule() { AddFilterType(irs::Type<irs::Not>::id()); }

  FilterRuleResult ApplyNot(Not& current_filter, Filter::ptr sub_filter,
                            FilterRuleOptions options) const override;
};

class AndFlatteningFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit AndFlatteningFilterRule() {
    AddFilterType(irs::Type<irs::And>::id());
  }

  FilterRuleResult ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const override;
};

class OrFlatteningFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit OrFlatteningFilterRule() { AddFilterType(irs::Type<irs::Or>::id()); }

  FilterRuleResult ApplyBoolean(Or& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const override;
};

class ByTermsFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit ByTermsFilterRule() { AddFilterType(irs::Type<irs::ByTerm>::id()); }

  FilterRuleResult ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const override;
  FilterRuleResult ApplyBoolean(Or& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const override;
};

class LevenshteinPrefixFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit LevenshteinPrefixFilterRule() {
    AddFilterType(irs::Type<irs::ByEditDistance>::id());
    AddFilterType(irs::Type<irs::ByPrefix>::id());
  }

  FilterRuleResult ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const override;
};

class AutomatonFilterRule : public FilterRulesConstructor::FilterRule {
 public:
  using FilterRule::ApplyBoolean;
  using FilterRule::ApplyNot;

  explicit AutomatonFilterRule(AutomatonUnionMethod union_method =
                                 AutomatonUnionMethod::RefinedDeterminize)
    : _union_method(union_method) {
    AddFilterType(irs::Type<irs::ByWildcard>::id());
    AddFilterType(irs::Type<irs::ByRegexp>::id());
    AddFilterType(irs::Type<irs::ByAutomaton>::id());
  }

  FilterRuleResult ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const override;
  FilterRuleResult ApplyBoolean(Or& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const override;

 private:
  AutomatonUnionMethod _union_method;
};

}  // namespace irs
