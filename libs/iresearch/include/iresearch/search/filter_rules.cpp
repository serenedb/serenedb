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

constexpr std::array<irs::TypeInfo::type_id, 3> kBooleanFiltersIds = {
  irs::Type<irs::And>::id(), irs::Type<irs::Or>::id(),
  irs::Type<irs::Not>::id()};

inline bool CheckBooleanNodeType(irs::TypeInfo::type_id current_id) {
  return absl::c_any_of(
    kBooleanFiltersIds,
    [&current_id](irs::TypeInfo::type_id id) { return current_id == id; });
}

namespace irs {
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

Filter::ptr FilterRulesConstructor::FilterRule::ApplyWrapper(
  Filter::ptr filter) const {
  auto current_filter_type_id = filter->type();
  if (!CheckBooleanNodeType(current_filter_type_id)) {
    // throw error
    return nullptr;
  }

  if (current_filter_type_id == irs::Type<irs::Not>::id()) {
    auto& not_filter = sdb::basics::downCast<Not>(*filter);
    auto sub_filter = not_filter.release();
    auto result = this->ApplyNot(not_filter, std::move(sub_filter), {});
    if (result.is_new_filter) {
      filter = std::move(result.filter);
    }
  } else {
    auto& boolean_filter = sdb::basics::downCast<BooleanFilter>(*filter);
    auto sub_filters = boolean_filter.release();
    TypesMap incl_sub_filters;
    std::vector<Filter::ptr> excl_sub_filters;
    incl_sub_filters.reserve(sub_filters.size());
    excl_sub_filters.reserve(sub_filters.size());

    // find desired filters, have to return excl in vector eventually
    for (auto& sub_filter : sub_filters) {
      auto type_id = sub_filter->type();
      if (_desired_filter_types.contains(type_id)) {
        if (incl_sub_filters.contains(type_id)) {
          incl_sub_filters[type_id].push_back(std::move(sub_filter));
        } else {
          [[maybe_unused]] auto [it, ok] = incl_sub_filters.emplace(
            type_id, std::vector<Filter::ptr>{});  // maybe check
          it->second.push_back(std::move(sub_filter));
        }
      } else {
        excl_sub_filters.push_back(std::move(sub_filter));
      }
    }

    if (incl_sub_filters.empty() && !_works_with_empty_filters) {
      boolean_filter.assign(std::move(excl_sub_filters));
      return filter;
    }

    FilterRuleOptions options = {.has_other_filters =
                                   !excl_sub_filters.empty()};
    FilterRuleResult result;
    if (current_filter_type_id == kBooleanFiltersIds[0]) {
      result = this->ApplyBoolean(sdb::basics::downCast<And>(*filter),
                                  std::move(incl_sub_filters), options);
    } else if (current_filter_type_id == kBooleanFiltersIds[1]) {
      result = this->ApplyBoolean(sdb::basics::downCast<Or>(*filter),
                                  std::move(incl_sub_filters), options);
    }

    if (result.is_new_filter) {
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
      filter = AstTraversalFromBottom(std::move(filter), rule);
    }
  }
  return filter;
}

Filter::ptr FilterRulesConstructor::AstTraversalFromBottom(Filter::ptr filter,
                                                           ptr rule) const {
  auto current_filter_type_id = filter->type();
  if (!CheckBooleanNodeType(current_filter_type_id)) {
    return filter;
  }

  if (current_filter_type_id == irs::Type<irs::Not>::id()) {
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
    boolean_filter.assign(std::move(sub_filters));
  }

  filter = rule->ApplyWrapper(std::move(filter));
  return filter;
}

FilterRulesConstructor::FilterRule::FilterRuleResult NotFilterRule::ApplyNot(
  Not& current_filter, Filter::ptr sub_filter,
  FilterRuleOptions options) const {
  if (sub_filter->type() != irs::Type<irs::Not>::id()) {
    RevertNot(current_filter, std::move(sub_filter));
    return FilterRuleResult{};
  }
  auto& not_filter = sdb::basics::downCast<Not>(*sub_filter);
  auto sub_sub_filter = not_filter.release();
  return FilterRuleResult{.filter = std::move(sub_sub_filter),
                          .is_new_filter = true,
                          .is_applied = true};
}

FilterRulesConstructor::FilterRule::FilterRuleResult
AndFlatteningFilterRule::ApplyBoolean(And& current_filter,
                                      TypesMap&& sub_filters,
                                      FilterRuleOptions options) const {
  // think about merge type in ands
  if (!sub_filters.contains(irs::Type<irs::And>::id())) {
    RevertBoolean(current_filter, std::move(sub_filters));
    return FilterRuleResult{};
  }
  for (auto& and_filter_ptr : sub_filters[irs::Type<irs::And>::id()]) {
    auto& and_filter = sdb::basics::downCast<And>(*and_filter_ptr);
    for (auto& sub_and_filter : and_filter.release()) {
      current_filter.add(std::move(sub_and_filter));
    }
  }
  return FilterRuleResult{.is_applied = true};
}

FilterRulesConstructor::FilterRule::FilterRuleResult
OrFlatteningFilterRule::ApplyBoolean(Or& current_filter, TypesMap&& sub_filters,
                                     FilterRuleOptions options) const {
  // think about merge type in ors
  if (!sub_filters.contains(irs::Type<irs::Or>::id())) {
    RevertBoolean(current_filter, std::move(sub_filters));
    return FilterRuleResult{};
  }
  for (auto& or_filter_ptr : sub_filters[irs::Type<irs::Or>::id()]) {
    auto& or_filter = sdb::basics::downCast<Or>(*or_filter_ptr);
    for (auto& sub_or_filter : or_filter.release()) {
      current_filter.add(std::move(sub_or_filter));
    }
  }
  return FilterRuleResult{.is_applied = true};
}

FilterRulesConstructor::FilterRule::FilterRuleResult
ByTermsFilterRule::ApplyBoolean(And& current_filter, TypesMap&& sub_filters,
                                FilterRuleOptions options) const {
  // think about boost, byTerms could consist of terms with different boosts
  if (!sub_filters.contains(irs::Type<irs::ByTerm>::id())) {
    RevertBoolean(current_filter, std::move(sub_filters));
    return FilterRuleResult{};
  }
  std::unordered_map<std::string,
                     std::unordered_map<score_t, std::vector<irs::Filter::ptr>>>
    match;
  for (auto& term_filter_ptr : sub_filters[irs::Type<irs::ByTerm>::id()]) {
    auto& term_filter = sdb::basics::downCast<ByTerm>(*term_filter_ptr);
    if (match.contains(static_cast<std::string>(term_filter.field()))) {
      auto& boost_map = match[static_cast<std::string>(term_filter.field())];
      if (boost_map.contains(term_filter.Boost())) {
        boost_map[term_filter.Boost()].push_back(std::move(term_filter_ptr));
      } else {
        [[maybe_unused]] auto [it, ok] = boost_map.emplace(
          term_filter.Boost(), std::vector<Filter::ptr>{});  // maybe check
        it->second.push_back(std::move(term_filter_ptr));
      }
    } else {
      [[maybe_unused]] auto [it, ok] = match.emplace(
        term_filter.field(),
        std::unordered_map<score_t,
                           std::vector<irs::Filter::ptr>>{});  // maybe check
      [[maybe_unused]] auto [it2, ok2] = it->second.emplace(
        term_filter.Boost(), std::vector<irs::Filter::ptr>());
      it2->second.push_back(std::move(term_filter_ptr));
    }
  }
  for (auto&& [field, boost_maps] : match) {
    for (auto&& [boost, terms] : boost_maps) {
      if (terms.size() == 1) {
        current_filter.add(std::move(terms.back()));
        continue;
      }
      auto& by_terms = current_filter.add<ByTerms>();
      *by_terms.mutable_field() = static_cast<std::string>(field);
      by_terms.mutable_options()->min_match = terms.size();
      for (auto& term_ptr : terms) {
        auto& term = sdb::basics::downCast<ByTerm>(*term_ptr);
        by_terms.mutable_options()->terms.emplace(term.options().term, boost);
      }
    }
  }
  if (!options.has_other_filters && current_filter.size() == 1) {
    return FilterRuleResult{.filter = current_filter.PopBack(),
                            .is_new_filter = true,
                            .is_applied = true};
  }
  return FilterRuleResult{.is_applied = true};
}

}  // namespace irs
