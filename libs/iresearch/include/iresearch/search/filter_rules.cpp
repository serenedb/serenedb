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
  irs::Type<irs::And>::id(),
  irs::Type<irs::Or>::id(),
  irs::Type<irs::Not>::id()
};

inline bool CheckBooleanNodeType (irs::TypeInfo::type_id current_id) {
  return absl::c_any_of(kBooleanFiltersIds, [&current_id](irs::TypeInfo::type_id id) {
      return current_id == id;
    }
  );
}

namespace irs {

  Filter::ptr FilterRulesConstructor::FilterRule::ApplyWrapper(Filter::ptr filter) const {
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
      std::unordered_map<TypeInfo::type_id, std::vector<Filter::ptr>> incl_sub_filters;
      std::vector<Filter::ptr> excl_sub_filters;
      incl_sub_filters.reserve(sub_filters.size());
      excl_sub_filters.reserve(sub_filters.size());
    
      // find desired filters
      for (auto& sub_filter : sub_filters) {
        auto type_id = sub_filter->type();
        if (_desired_filter_types.contains(type_id)) {
          if (incl_sub_filters.contains(type_id)) {
            incl_sub_filters[type_id].push_back(std::move(sub_filter));
          } else {
            [[maybe_unused]] auto [it, ok] = incl_sub_filters.emplace(type_id, std::vector<Filter::ptr>{}); // maybe check
            it->second.push_back(std::move(sub_filter));
          }
        } else {
          excl_sub_filters.push_back(std::move(sub_filter));
        }
      }
    
      if (incl_sub_filters.empty() && !_works_with_empty_filters) {
        return filter;
      }

      FilterRuleOptions options = {.has_other_filters = !excl_sub_filters.empty()};
      FilterRuleResult result;
      if (current_filter_type_id == kBooleanFiltersIds[0]) {
        result = this->ApplyBoolean(sdb::basics::downCast<And>(*filter), std::move(incl_sub_filters), options);
      } else if (current_filter_type_id == kBooleanFiltersIds[1]) {
        result = this->ApplyBoolean(sdb::basics::downCast<Or>(*filter), std::move(incl_sub_filters), options);
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

  Filter::ptr FilterRulesConstructor::AstTraversalFromBottom(Filter::ptr filter, ptr rule) const {
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
      while(auto sub_filter = boolean_filter.PopBack()) {
        sub_filters.push_back(AstTraversalFromBottom(std::move(sub_filter), rule));
        sub_filter = boolean_filter.PopBack();
      }
      boolean_filter.assign(std::move(sub_filters));
    }

    filter = rule->ApplyWrapper(std::move(filter));
    return filter;
  }

}  // namespace irs
