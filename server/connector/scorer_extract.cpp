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

#include "connector/scorer_extract.h"

#include <duckdb/planner/expression/bound_constant_expression.hpp>

#include <cmath>

#include "basics/errors.h"
#include "connector/functions/search.h"

namespace sdb::connector {
namespace {

const duckdb::Value* TryGetConstantValue(const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundConstantExpression>().value;
}

}  // namespace

ResultOr<std::optional<catalog::WandScorer>> ExtractWandScorerFromBound(
  const duckdb::BoundFunctionExpression& func, std::string_view name) {
  using WS = catalog::WandScorer;
  catalog::WandScorer scorer;

  if (name == kBm25) {
    WS::Bm25 p;
    if (func.children.size() == 3) {
      auto* k1v = TryGetConstantValue(*func.children[1]);
      auto* bv = TryGetConstantValue(*func.children[2]);
      if (!k1v || !bv) {
        return std::nullopt;
      }
      p.k1 = static_cast<float>(k1v->GetValue<double>());
      p.b = static_cast<float>(bv->GetValue<double>());
    }
    scorer.params = p;
  } else if (name == kTfidf) {
    WS::Tfidf p;
    if (func.children.size() == 2) {
      auto* cv = TryGetConstantValue(*func.children[1]);
      if (!cv) {
        return std::nullopt;
      }
      p.with_norms = cv->GetValue<bool>();
    }
    scorer.params = p;
  } else if (name == kRawTf) {
    scorer.params = WS::RawTf{};
  } else if (name == kLmJm) {
    WS::LmJm p;
    if (func.children.size() == 2) {
      auto* lv = TryGetConstantValue(*func.children[1]);
      if (!lv) {
        return std::nullopt;
      }
      p.lambda = static_cast<float>(lv->GetValue<double>());
      if (!(p.lambda > 0.0f && p.lambda <= 1.0f)) {
        return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                       "lm_jm lambda must be in (0, 1], got ",
                                       p.lambda};
      }
    }
    scorer.params = p;
  } else if (name == kLmDirichlet) {
    WS::LmDirichlet p;
    if (func.children.size() == 2) {
      auto* mv = TryGetConstantValue(*func.children[1]);
      if (!mv) {
        return std::nullopt;
      }
      p.mu = static_cast<float>(mv->GetValue<double>());
      if (p.mu < 0.0f || !std::isfinite(p.mu)) {
        return std::unexpected<Result>{
          std::in_place, ERROR_BAD_PARAMETER,
          "lm_dirichlet mu must be a non-negative finite value, got ", p.mu};
      }
    }
    scorer.params = p;
  } else if (name == kIndriDirichlet) {
    WS::IndriDirichlet p;
    if (func.children.size() == 2) {
      auto* mv = TryGetConstantValue(*func.children[1]);
      if (!mv) {
        return std::nullopt;
      }
      p.mu = static_cast<float>(mv->GetValue<double>());
      if (p.mu < 0.0f || !std::isfinite(p.mu)) {
        return std::unexpected<Result>{
          std::in_place, ERROR_BAD_PARAMETER,
          "indri_dirichlet mu must be a non-negative finite value, got ",
          p.mu};
      }
    }
    scorer.params = p;
  } else if (name == kDfi) {
    WS::Dfi p;
    if (func.children.size() == 2) {
      auto* mv = TryGetConstantValue(*func.children[1]);
      if (!mv) {
        return std::nullopt;
      }
      auto s = mv->GetValue<std::string>();
      if (s == "standardized") {
        p.measure = WS::DfiMeasure::Standardized;
      } else if (s == "saturated") {
        p.measure = WS::DfiMeasure::Saturated;
      } else if (s == "chi_squared" || s == "chisquared") {
        p.measure = WS::DfiMeasure::ChiSquared;
      } else {
        return std::unexpected<Result>{
          std::in_place, ERROR_BAD_PARAMETER,
          "dfi measure must be one of: standardized, saturated, chi_squared; "
          "got '",
          s, "'"};
      }
    }
    scorer.params = p;
  } else {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER, "Unknown scorer '", name,
      "'. Expected: bm25, tfidf, raw_tf, lm_jm, lm_dirichlet, "
      "indri_dirichlet, dfi"};
  }
  return scorer;
}

}  // namespace sdb::connector
