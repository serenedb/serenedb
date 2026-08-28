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

#include <iresearch/search/scorer_options.hpp>
#include <magic_enum/magic_enum.hpp>

namespace sdb::catalog::persistence {

using irs::ScorerOptions;

}  // namespace sdb::catalog::persistence
namespace magic_enum {

template<>
constexpr customize::customize_t
customize::enum_name<sdb::catalog::persistence::ScorerOptions::DfiMeasure>(
  sdb::catalog::persistence::ScorerOptions::DfiMeasure value) noexcept {
  using DfiMeasure = sdb::catalog::persistence::ScorerOptions::DfiMeasure;
  switch (value) {
    case DfiMeasure::Standardized:
      return "standardized";
    case DfiMeasure::Saturated:
      return "saturated";
    case DfiMeasure::ChiSquared:
      return "chi_squared";
  }
  return invalid_tag;
}

}  // namespace magic_enum
