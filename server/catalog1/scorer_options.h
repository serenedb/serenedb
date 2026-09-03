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

#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "catalog1/entry/inverted_index.h"

namespace duckdb {

class BoundFunctionExpression;
class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

std::unique_ptr<irs::Scorer> MakeScorer(const ScorerOptions& spec);

std::optional<ScorerOptions> ExtractScorerFromBound(
  const duckdb::BoundFunctionExpression& func, std::string_view name);

ScorerOptions ParseScorerExpression(duckdb::ClientContext& context,
                                    std::string input,
                                    std::string_view what = "optimize_top_k");

}  // namespace sdb::catalog
namespace magic_enum {

// The SQL spellings of the DFI measures: enum_cast reads these when dfi()'s
// measure argument is parsed, so they are the wire names, not decoration.
template<>
constexpr customize::customize_t
customize::enum_name<sdb::catalog::ScorerOptions::DfiMeasure>(
  sdb::catalog::ScorerOptions::DfiMeasure value) noexcept {
  using DfiMeasure = sdb::catalog::ScorerOptions::DfiMeasure;
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
