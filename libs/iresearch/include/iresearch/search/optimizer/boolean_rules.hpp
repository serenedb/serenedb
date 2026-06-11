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
#include <string_view>

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"

namespace irs::optimizer {

struct FlattenAnd {
  static constexpr std::string_view kName = "flatten_and";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct FlattenOr {
  static constexpr std::string_view kName = "flatten_or";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct AndExclusionCoalesceRule {
  static constexpr std::string_view kName = "and_exclusion_coalesce";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct AndEmptyRule {
  static constexpr std::string_view kName = "and_empty";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrEmptyRule {
  static constexpr std::string_view kName = "or_empty";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct AndAllFoldRule {
  static constexpr std::string_view kName = "and_all_fold";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrAllFoldRule {
  static constexpr std::string_view kName = "or_all_fold";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct SingleChildRule {
  static constexpr std::string_view kName = "single_child";
  static constexpr std::array kTargets{Type<And>::id(), Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct ByTermsRule {
  static constexpr std::string_view kName = "by_terms";
  static constexpr std::array kTargets{Type<And>::id(), Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrMinMatchZeroRule {
  static constexpr std::string_view kName = "or_min_match_zero";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrUnsatRule {
  static constexpr std::string_view kName = "or_unsat";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrAllRequiredRule {
  static constexpr std::string_view kName = "or_all_required";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct EmptyAndRule {
  static constexpr std::string_view kName = "empty_and";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct MixedDegenerateRule {
  static constexpr std::string_view kName = "mixed_degenerate";
  static constexpr std::array kTargets{Type<MixedBooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

}  // namespace irs::optimizer
