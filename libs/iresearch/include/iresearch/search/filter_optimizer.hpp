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

#include <array>
#include <concepts>
#include <cstddef>
#include <span>
#include <string_view>

#include "iresearch/search/filter.hpp"

namespace irs {

struct OptimizeContext {
  const Scorer* scorer = nullptr;
};

struct RuleDesc {
  std::string_view name;
  std::span<const TypeInfo::type_id> targets;
  bool (*apply)(Filter::ptr& slot, const OptimizeContext& ctx);
};

template<typename Rule>
concept RuleLike = requires {
  { Rule::kName } -> std::convertible_to<std::string_view>;
  { std::span<const TypeInfo::type_id>{Rule::kTargets} };
  { Rule::kEnable } -> std::convertible_to<bool>;
  {
    &Rule::Apply
  } -> std::convertible_to<bool (*)(Filter::ptr&, const OptimizeContext&)>;
};

template<RuleLike Rule>
constexpr RuleDesc MakeRule() {
  return RuleDesc{Rule::kName, Rule::kTargets, &Rule::Apply};
}

template<RuleLike... Rules>
constexpr auto MakeRuleSet() {
  constexpr std::array<bool, sizeof...(Rules)> kEnabled{Rules::kEnable...};
  constexpr size_t kCount =
    (size_t{0} + ... + (Rules::kEnable ? size_t{1} : size_t{0}));
  std::array<RuleDesc, kCount> result{};
  const std::array<RuleDesc, sizeof...(Rules)> all{MakeRule<Rules>()...};
  size_t index = 0;
  for (size_t i = 0; i < all.size(); ++i) {
    if (kEnabled[i]) {
      result[index++] = all[i];
    }
  }
  return result;
}

extern const std::span<const RuleDesc> kDefaultRules;

void RegisterRule(const RuleDesc& rule);

std::span<const RuleDesc> ActiveRules();

void Optimize(Filter::ptr& root, const OptimizeContext& ctx = {},
              std::span<const RuleDesc> rules = ActiveRules());

}  // namespace irs
