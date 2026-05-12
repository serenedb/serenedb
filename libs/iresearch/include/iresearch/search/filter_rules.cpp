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

#include "filter_rules.hpp"

namespace irs {
namespace {

constexpr std::array<TypeInfo::type_id, 3> kBooleanFiltersIds = {
  Type<And>::id(),
  Type<Or>::id(),
  Type<Not>::id(),
};

auto CheckBooleanNodeType(TypeInfo::type_id current_id) -> bool {
  return absl::c_any_of(kBooleanFiltersIds, [current_id](TypeInfo::type_id id) {
    return current_id == id;
  });
}

void RevertNot(Not& current_filter, Filter::ptr sub_filter) {
  current_filter.assign(std::move(sub_filter));
}

void RevertBoolean(BooleanFilter& current_filter,
                   FilterRulesConstructor::FilterRule::TypesMap&& sub_filters) {
  for (auto&& [_, filters] : sub_filters) {
    for (auto& filter : filters) {
      current_filter.add(std::move(filter));
    }
  }
}

}  // namespace

Filter::ptr FilterRulesConstructor::FilterRule::ApplyWrapper(
  Filter::ptr filter) const {
  auto current_filter_type_id = filter->type();
  SDB_VERIFY(CheckBooleanNodeType(current_filter_type_id),
             "ApplyWrapper called with non-boolean filter node");

  if (current_filter_type_id == Type<Not>::id()) {
    auto& not_filter = sdb::basics::downCast<Not>(*filter);
    auto sub_filter = not_filter.release();
    auto result = this->ApplyNot(not_filter, std::move(sub_filter), {});
    if (result.filter) {
      filter = std::move(result.filter);
    }
  } else {
    auto& boolean_filter = sdb::basics::downCast<BooleanFilter>(*filter);
    auto sub_filters = boolean_filter.release();
    TypesMap incl_sub_filters;
    std::vector<Filter::ptr> excl_sub_filters;
    incl_sub_filters.reserve(sub_filters.size());
    excl_sub_filters.reserve(sub_filters.size());

    for (auto& sub_filter : sub_filters) {
      auto type_id = sub_filter->type();
      if (_desired_filter_types.contains(type_id)) {
        incl_sub_filters.try_emplace(type_id).first->second.push_back(
          std::move(sub_filter));
      } else {
        excl_sub_filters.push_back(std::move(sub_filter));
      }
    }

    if (incl_sub_filters.empty() && !_works_with_empty_filters) {
      boolean_filter.assign(std::move(excl_sub_filters));
      return filter;
    }

    FilterRuleOptions options = {
      .has_other_filters = !excl_sub_filters.empty(),
    };
    FilterRuleResult result;
    if (current_filter_type_id == Type<And>::id()) {
      result = this->ApplyBoolean(sdb::basics::downCast<And>(*filter),
                                  std::move(incl_sub_filters), options);
    } else if (current_filter_type_id == Type<Or>::id()) {
      result = this->ApplyBoolean(sdb::basics::downCast<Or>(*filter),
                                  std::move(incl_sub_filters), options);
    }

    if (result.filter) {
      filter = std::move(result.filter);
    } else {
      for (auto& sub_filter : excl_sub_filters) {
        boolean_filter.add(std::move(sub_filter));
      }
    }
  }

  return filter;
}

Filter::ptr FilterRulesConstructor::Apply(Filter::ptr filter) const {
  for (auto& rule : _rules) {
    if (rule->traversal_type() == FilterRule::FilterRuleTraversalType::Bottom) {
      filter = AstTraversalFromBottom(std::move(filter), *rule);
    }
  }
  return filter;
}

Filter::ptr FilterRulesConstructor::AstTraversalFromBottom(
  Filter::ptr filter, const FilterRule& rule) const {
  auto current_filter_type_id = filter->type();
  if (!CheckBooleanNodeType(current_filter_type_id)) {
    return filter;
  }

  if (current_filter_type_id == Type<Not>::id()) {
    auto& not_filter = sdb::basics::downCast<Not>(*filter);
    auto sub_filter = not_filter.release();
    not_filter.assign(AstTraversalFromBottom(std::move(sub_filter), rule));
  } else {
    auto& boolean_filter = sdb::basics::downCast<BooleanFilter>(*filter);
    std::vector<Filter::ptr> sub_filters;
    sub_filters.reserve(boolean_filter.size());
    while (auto sub_filter = boolean_filter.PopBack()) {
      sub_filters.push_back(
        AstTraversalFromBottom(std::move(sub_filter), rule));
    }
    std::reverse(sub_filters.begin(), sub_filters.end());
    boolean_filter.assign(std::move(sub_filters));
  }

  filter = rule.ApplyWrapper(std::move(filter));
  return filter;
}

FilterRulesConstructor::FilterRule::FilterRuleResult NotFilterRule::ApplyNot(
  Not& current_filter, Filter::ptr sub_filter,
  FilterRuleOptions /*options*/) const {
  if (sub_filter->type() != Type<Not>::id()) {
    RevertNot(current_filter, std::move(sub_filter));
    return FilterRuleResult{};
  }
  auto& not_filter = sdb::basics::downCast<Not>(*sub_filter);
  auto sub_sub_filter = not_filter.release();
  return FilterRuleResult{
    .filter = std::move(sub_sub_filter),
    .is_applied = true,
  };
}

FilterRulesConstructor::FilterRule::FilterRuleResult
AndFlatteningFilterRule::ApplyBoolean(And& current_filter,
                                      TypesMap&& sub_filters,
                                      FilterRuleOptions /*options*/) const {
  for (auto& and_filter_ptr : sub_filters[Type<And>::id()]) {
    auto& and_filter = sdb::basics::downCast<And>(*and_filter_ptr);
    for (auto& sub_and_filter : and_filter.release()) {
      current_filter.add(std::move(sub_and_filter));
    }
  }
  return FilterRuleResult{
    .is_applied = true,
  };
}

FilterRulesConstructor::FilterRule::FilterRuleResult
OrFlatteningFilterRule::ApplyBoolean(Or& current_filter, TypesMap&& sub_filters,
                                     FilterRuleOptions /*options*/) const {
  for (auto& or_filter_ptr : sub_filters[Type<Or>::id()]) {
    auto& or_filter = sdb::basics::downCast<Or>(*or_filter_ptr);
    for (auto& sub_or_filter : or_filter.release()) {
      current_filter.add(std::move(sub_or_filter));
    }
  }
  return FilterRuleResult{
    .is_applied = true,
  };
}

FilterRulesConstructor::FilterRule::FilterRuleResult
ByTermsFilterRule::ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const {
  absl::flat_hash_map<std::string, std::vector<Filter::ptr>> match;
  for (auto& term_filter_ptr : sub_filters[Type<ByTerm>::id()]) {
    auto& term_filter = sdb::basics::downCast<ByTerm>(*term_filter_ptr);
    match.try_emplace(std::string{term_filter.field()})
      .first->second.push_back(std::move(term_filter_ptr));
  }
  for (auto&& [field, terms] : match) {
    if (terms.size() == 1) {
      current_filter.add(std::move(terms.back()));
      continue;
    }
    auto& by_terms = current_filter.add<ByTerms>();
    *by_terms.mutable_field() = field;
    by_terms.mutable_options()->min_match = terms.size();
    for (auto& term_ptr : terms) {
      auto& term = sdb::basics::downCast<ByTerm>(*term_ptr);
      by_terms.mutable_options()->terms.emplace(term.options().term,
                                                term.Boost());
    }
  }
  if (!options.has_other_filters && current_filter.size() == 1) {
    return FilterRuleResult{
      .filter = current_filter.PopBack(),
      .is_applied = true,
    };
  }
  return FilterRuleResult{
    .is_applied = true,
  };
}

FilterRulesConstructor::FilterRule::FilterRuleResult
ByTermsFilterRule::ApplyBoolean(Or& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const {
  absl::flat_hash_map<std::string, std::vector<Filter::ptr>> match;
  for (auto& term_filter_ptr : sub_filters[Type<ByTerm>::id()]) {
    auto& term_filter = sdb::basics::downCast<ByTerm>(*term_filter_ptr);
    match.try_emplace(std::string{term_filter.field()})
      .first->second.push_back(std::move(term_filter_ptr));
  }
  for (auto&& [field, terms] : match) {
    if (terms.size() == 1) {
      current_filter.add(std::move(terms.back()));
      continue;
    }
    auto& by_terms = current_filter.add<ByTerms>();
    *by_terms.mutable_field() = field;
    by_terms.mutable_options()->min_match = 1;
    for (auto& term_ptr : terms) {
      auto& term = sdb::basics::downCast<ByTerm>(*term_ptr);
      by_terms.mutable_options()->terms.emplace(term.options().term,
                                                term.Boost());
    }
  }
  if (!options.has_other_filters && current_filter.size() == 1) {
    return FilterRuleResult{
      .filter = current_filter.PopBack(),
      .is_applied = true,
    };
  }
  return FilterRuleResult{
    .is_applied = true,
  };
}

FilterRulesConstructor::FilterRule::FilterRuleResult
LevenshteinPrefixFilterRule::ApplyBoolean(And& current_filter,
                                          TypesMap&& sub_filters,
                                          FilterRuleOptions options) const {
  if (!sub_filters.contains(Type<ByEditDistance>::id()) ||
      !sub_filters.contains(Type<ByPrefix>::id())) {
    RevertBoolean(current_filter, std::move(sub_filters));
    return FilterRuleResult{};
  }
  auto& levenshteins = sub_filters[Type<ByEditDistance>::id()];
  auto& prefixes = sub_filters[Type<ByPrefix>::id()];
  if (prefixes.size() > 1) {
    RevertBoolean(current_filter, std::move(sub_filters));  // check that case
  } else {
    auto& prefix = sdb::basics::downCast<ByPrefix>(*prefixes.back());
    auto& prefix_term = prefix.options().term;
    size_t found_match = 0;
    for (auto& levenstein_ptr : levenshteins) {
      auto& levenshtein =
        sdb::basics::downCast<ByEditDistance>(*levenstein_ptr);
      if (prefix.field() != levenshtein.field()) {
        continue;
      }
      auto& mutable_levenshtein_options = *levenshtein.mutable_options();
      if (mutable_levenshtein_options.term.starts_with(prefix_term)) {
        auto& mutable_levenshtein_prefix = mutable_levenshtein_options.prefix;
        if (prefix_term.starts_with(mutable_levenshtein_prefix)) {
          mutable_levenshtein_prefix = prefix_term;
        }
        mutable_levenshtein_options.term =
          mutable_levenshtein_options.term.substr(prefix_term.size());
        mutable_levenshtein_options.prefix = prefix_term;
        ++found_match;
      }
    }
    if (found_match != levenshteins.size()) {
      RevertBoolean(current_filter, std::move(sub_filters));
    } else {
      TypesMap map;
      map[Type<ByEditDistance>::id()] = std::move(levenshteins);
      RevertBoolean(current_filter, std::move(map));
      if (!options.has_other_filters && current_filter.size() == 1) {
        return FilterRuleResult{
          .filter = current_filter.PopBack(),
          .is_applied = true,
        };
      }
    }
  }
  return FilterRuleResult{
    .is_applied = true,
  };
}

}  // namespace irs
