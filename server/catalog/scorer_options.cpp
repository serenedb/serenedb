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

#include "catalog/scorer_options.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <cmath>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_binder/constant_binder.hpp>
#include <magic_enum/magic_enum.hpp>
#include <string>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

const duckdb::Value* TryGetConstantValue(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundConstantExpression>().value;
}

}  // namespace

std::string ScorerOptions::ToString() const {
  return std::visit(
    [&]<typename P>(const P& p) -> std::string {
      if constexpr (std::is_same_v<P, Bm25>) {
        return absl::StrCat("bm25(k1=", p.k1, ", b=", p.b, ")");
      } else if constexpr (std::is_same_v<P, Tfidf>) {
        return absl::StrCat(
          "tfidf(with_norms=", p.with_norms ? "true" : "false", ")");
      } else if constexpr (std::is_same_v<P, LmJm>) {
        return absl::StrCat("lm_jm(lambda=", p.lambda, ")");
      } else if constexpr (std::is_same_v<P, LmDirichlet>) {
        return absl::StrCat("lm_dirichlet(mu=", p.mu, ")");
      } else if constexpr (std::is_same_v<P, IndriDirichlet>) {
        return absl::StrCat("indri_dirichlet(mu=", p.mu, ")");
      } else if constexpr (std::is_same_v<P, Dfi>) {
        return absl::StrCat("dfi(measure=", magic_enum::enum_name(p.measure),
                            ")");
      } else if constexpr (std::is_same_v<P, RawBoost>) {
        return "raw_boost()";
      } else if constexpr (std::is_same_v<P, RawTf>) {
        return "raw_tf()";
      } else if constexpr (std::is_same_v<P, RawDL>) {
        return "raw_dl()";
      }
    },
    params);
}

std::unique_ptr<irs::Scorer> MakeScorer(const ScorerOptions& spec) {
  return std::visit(
    []<typename P>(const P& p) -> std::unique_ptr<irs::Scorer> {
      if constexpr (std::is_same_v<P, ScorerOptions::Bm25>) {
        return std::make_unique<irs::BM25>(p.k1, p.b);
      } else if constexpr (std::is_same_v<P, ScorerOptions::Tfidf>) {
        return std::make_unique<irs::TFIDF>(p.with_norms);
      } else if constexpr (std::is_same_v<P, ScorerOptions::RawTf>) {
        return std::make_unique<irs::RawTF>();
      } else if constexpr (std::is_same_v<P, ScorerOptions::LmJm>) {
        return std::make_unique<irs::LMJelinekMercer>(p.lambda);
      } else if constexpr (std::is_same_v<P, ScorerOptions::LmDirichlet>) {
        return std::make_unique<irs::LMDirichlet>(p.mu);
      } else if constexpr (std::is_same_v<P, ScorerOptions::IndriDirichlet>) {
        return std::make_unique<irs::IndriDirichlet>(p.mu);
      } else if constexpr (std::is_same_v<P, ScorerOptions::RawDL>) {
        return std::make_unique<irs::RawDL>();
      } else if constexpr (std::is_same_v<P, ScorerOptions::RawBoost>) {
        return std::make_unique<irs::RawBoost>();
      } else if constexpr (std::is_same_v<P, ScorerOptions::Dfi>) {
        irs::DFIMeasure m{};
        switch (p.measure) {
          case ScorerOptions::DfiMeasure::Standardized:
            m = irs::DFIMeasure::Standardized;
            break;
          case ScorerOptions::DfiMeasure::Saturated:
            m = irs::DFIMeasure::Saturated;
            break;
          case ScorerOptions::DfiMeasure::ChiSquared:
            m = irs::DFIMeasure::ChiSquared;
            break;
        }
        return std::make_unique<irs::DFI>(m);
      }
    },
    spec.params);
}

std::optional<ScorerOptions> ExtractScorerFromBound(
  const duckdb::BoundFunctionExpression& func, std::string_view name) {
  using S = ScorerOptions;
  S scorer;

  if (name == S::Bm25::kName) {
    S::Bm25 p;
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
  } else if (name == S::Tfidf::kName) {
    S::Tfidf p;
    if (func.children.size() == 2) {
      auto* cv = TryGetConstantValue(*func.children[1]);
      if (!cv) {
        return std::nullopt;
      }
      p.with_norms = cv->GetValue<bool>();
    }
    scorer.params = p;
  } else if (name == S::LmJm::kName) {
    S::LmJm p;
    if (func.children.size() == 2) {
      auto* lv = TryGetConstantValue(*func.children[1]);
      if (!lv) {
        return std::nullopt;
      }
      p.lambda = static_cast<float>(lv->GetValue<double>());
      if (!(p.lambda > 0.0f && p.lambda <= 1.0f)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("lm_jm lambda must be in (0, 1], got ", p.lambda));
      }
    }
    scorer.params = p;
  } else if (name == S::LmDirichlet::kName) {
    S::LmDirichlet p;
    if (func.children.size() == 2) {
      auto* mv = TryGetConstantValue(*func.children[1]);
      if (!mv) {
        return std::nullopt;
      }
      p.mu = static_cast<float>(mv->GetValue<double>());
      if (p.mu < 0.0f || !std::isfinite(p.mu)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("lm_dirichlet mu must be a non-negative finite value, got ",
                  p.mu));
      }
    }
    scorer.params = p;
  } else if (name == S::IndriDirichlet::kName) {
    S::IndriDirichlet p;
    if (func.children.size() == 2) {
      auto* mv = TryGetConstantValue(*func.children[1]);
      if (!mv) {
        return std::nullopt;
      }
      p.mu = static_cast<float>(mv->GetValue<double>());
      if (p.mu < 0.0f || !std::isfinite(p.mu)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG(
            "indri_dirichlet mu must be a non-negative finite value, got ",
            p.mu));
      }
    }
    scorer.params = p;
  } else if (name == S::Dfi::kName) {
    S::Dfi p;
    if (func.children.size() == 2) {
      auto* mv = TryGetConstantValue(*func.children[1]);
      if (!mv) {
        return std::nullopt;
      }
      auto s = mv->GetValue<std::string>();
      auto parsed =
        magic_enum::enum_cast<S::DfiMeasure>(s, magic_enum::case_insensitive);
      if (!parsed) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("Unknown dfi measure '", s, "'"),
          ERR_HINT(
            "Expected one of: ",
            absl::StrJoin(magic_enum::enum_names<S::DfiMeasure>(), ", ")));
      }
      p.measure = *parsed;
    }
    scorer.params = p;
  } else if (name == S::RawBoost::kName) {
    scorer.params = S::RawBoost{};
  } else if (name == S::RawTf::kName) {
    scorer.params = S::RawTf{};
  } else if (name == S::RawDL::kName) {
    scorer.params = S::RawDL{};
  } else {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Unknown scorer '", name, "'"),
      ERR_HINT("Expected one of: bm25, tfidf, lm_jm, lm_dirichlet, "
               "indri_dirichlet, dfi, raw_boost, raw_tf, raw_dl"));
  }
  return scorer;
}

ScorerOptions ParseScorerExpression(duckdb::ClientContext& context,
                                    std::string input) {
  using namespace duckdb;
  auto exprs = Parser::ParseExpressionList(input);
  if (exprs.size() != 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("'optimize_top_k' must be a single scorer expression, got ",
              exprs.size(), " in '", input, "'"));
  }

  auto fn_expr = std::move(exprs[0]);
  if (fn_expr->GetExpressionType() != ExpressionType::FUNCTION) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("'optimize_top_k' expects a scorer function call, got '", input,
              "'"),
      ERR_HINT("Use e.g. 'tfidf()' or 'bm25(1.2, 0.75)'"));
  }

  // Prepend a tableoid placeholder to match the SQL `BM25(idx.tableoid, ...)`
  // overload that ConstantBinder will resolve.
  auto& fn = fn_expr->Cast<FunctionExpression>();
  fn.children.insert(fn.children.begin(),
                     make_uniq<ConstantExpression>(Value::BIGINT(0)));

  // Capture the name now -- Bind() consumes fn_expr.
  std::string name = fn.function_name;
  absl::AsciiStrToLower(&name);

  auto binder = Binder::CreateBinder(context);
  ConstantBinder cb(*binder, context, "optimize_top_k");
  auto bound = cb.Bind(fn_expr);
  if (!bound ||
      bound->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("'optimize_top_k' did not bind to a scorer function: '", input,
              "'"));
  }

  auto& bound_fn = bound->Cast<BoundFunctionExpression>();
  for (auto& child : bound_fn.children) {
    if (child->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT &&
        child->IsFoldable()) {
      auto val = ExpressionExecutor::EvaluateScalar(context, *child);
      child = make_uniq<BoundConstantExpression>(std::move(val));
    }
  }

  auto extracted = ExtractScorerFromBound(bound_fn, name);
  if (!extracted) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("'optimize_top_k' scorer args must be constants: '", input, "'"));
  }
  return std::move(*extracted);
}

}  // namespace sdb::catalog
