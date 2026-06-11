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

#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/terms_filter.hpp"

namespace irs::optimizer {

struct ByTermsMinMatchZeroRule {
  static constexpr std::string_view kName = "by_terms_min_match_zero";
  static constexpr std::array kTargets{Type<ByTerms>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct ByTermsDegenerateRule {
  static constexpr std::string_view kName = "by_terms_degenerate";
  static constexpr std::array kTargets{Type<ByTerms>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

}  // namespace irs::optimizer
