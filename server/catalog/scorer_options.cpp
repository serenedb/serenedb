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

#include <absl/algorithm/container.h>
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
#include <ranges>
#include <span>
#include <string>
#include <vector>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

const duckdb::Value* TryGetConstantValue(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundConstantExpression>().GetValue();
}

std::optional<ScorerOptions> ExtractScorer(
  std::string_view name, std::span<const duckdb::Value* const> args) {
  using S = ScorerOptions;
  S scorer;

  if (name == S::Bm25::Owner::type_name()) {
    S::Bm25 p;
    if (args.size() == 2) {
      auto* k1v = args[0];
      auto* bv = args[1];
      if (!k1v || !bv) {
        return std::nullopt;
      }
      p.k1 = static_cast<float>(k1v->GetValue<double>());
      p.b = static_cast<float>(bv->GetValue<double>());
    }
    scorer.params = p;
  } else if (name == S::Tfidf::Owner::type_name()) {
    S::Tfidf p;
    if (args.size() == 1) {
      auto* cv = args[0];
      if (!cv) {
        return std::nullopt;
      }
      p.with_norms = cv->GetValue<bool>();
    }
    scorer.params = p;
  } else if (name == S::LmJm::Owner::type_name()) {
    S::LmJm p;
    if (args.size() == 1) {
      auto* lv = args[0];
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
  } else if (name == S::LmDirichlet::Owner::type_name()) {
    S::LmDirichlet p;
    if (args.size() == 1) {
      auto* mv = args[0];
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
  } else if (name == S::IndriDirichlet::Owner::type_name()) {
    S::IndriDirichlet p;
    if (args.size() == 1) {
      auto* mv = args[0];
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
  } else if (name == S::Dfi::Owner::type_name()) {
    S::Dfi p;
    if (args.size() == 1) {
      auto* mv = args[0];
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
  } else if (name == S::RawBoost::Owner::type_name()) {
    scorer.params = S::RawBoost{};
  } else if (name == S::RawTf::Owner::type_name()) {
    scorer.params = S::RawTf{};
  } else if (name == S::RawDL::Owner::type_name()) {
    scorer.params = S::RawDL{};
  } else if (name == S::Idf::Owner::type_name()) {
    scorer.params = S::Idf{};
  } else if (name == S::Constant::Owner::type_name()) {
    S::Constant p;
    if (args.size() == 1) {
      auto* vv = args[0];
      if (!vv) {
        return std::nullopt;
      }
      p.value = static_cast<float>(vv->GetValue<double>());
      if (!std::isfinite(p.value)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("constant value must be finite, got ", p.value));
      }
    }
    scorer.params = p;
  } else {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Unknown scorer '", name, "'"),
      ERR_HINT("Expected one of: bm25, tfidf, lm_jm, lm_dirichlet, "
               "indri_dirichlet, dfi, idf, raw_boost, raw_tf, raw_dl"));
  }
  return scorer;
}

}  // namespace

std::unique_ptr<irs::Scorer> MakeScorer(const ScorerOptions& spec) {
  return std::visit(
    []<typename P>(const P& p) -> std::unique_ptr<irs::Scorer> {
      return P::Owner::Make(p);
    },
    spec.params);
}

std::optional<ScorerOptions> ExtractScorerFromBound(
  const duckdb::BoundFunctionExpression& func, std::string_view name) {
  std::vector<const duckdb::Value*> args;
  for (const auto& child : func.GetChildren() | std::views::drop(1)) {
    args.push_back(TryGetConstantValue(*child));
  }
  return ExtractScorer(name, args);
}

ScorerOptions ParseScorerExpression(duckdb::ClientContext* context,
                                    std::string input, std::string_view what) {
  using namespace duckdb;
  auto exprs = Parser::ParseExpressionList(input);
  if (exprs.size() != 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("'", what, "' must be a single scorer expression, got ",
              exprs.size(), " in '", input, "'"));
  }
  unique_ptr<ParsedExpression> fn_expr = std::move(exprs[0]);
  if (fn_expr->GetExpressionType() != ExpressionType::FUNCTION) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("'", what, "' expects a scorer function call, got '", input, "'"),
      ERR_HINT("Use e.g. 'tfidf()' or 'bm25(1.2, 0.75)'"));
  }

  auto& fn = fn_expr->Cast<FunctionExpression>();
  std::string name = fn.FunctionName().GetIdentifierName();
  absl::AsciiStrToLower(&name);

  std::vector<const Value*> literals;
  for (const auto& arg : fn.GetArguments()) {
    const auto& expr = arg.GetExpression();
    if (expr.GetExpressionClass() == ExpressionClass::CONSTANT) {
      literals.push_back(&expr.Cast<ConstantExpression>().GetValue());
    }
  }
  if (literals.size() == fn.GetArguments().size()) {
    return *ExtractScorer(name, literals);
  }
  if (!context) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("'", what, "' scorer args must be constants: '", input, "'"),
      ERR_HINT("Use e.g. 'tfidf()' or 'bm25(1.2, 0.75)'"));
  }

  // Prepend a tableoid placeholder to match the SQL `BM25(idx.tableoid, ...)`
  // overload that ConstantBinder will resolve.
  fn.GetArgumentsMutable().insert(
    fn.GetArgumentsMutable().begin(),
    FunctionArgument{unique_ptr<ParsedExpression>(
      make_uniq<ConstantExpression>(Value::BIGINT(0)))});

  auto binder = Binder::CreateBinder(*context);
  ConstantBinder cb(*binder, *context, std::string{kOptimizeTopKSetting});
  auto bound = cb.Bind(fn_expr);
  if (!bound ||
      bound->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("'", what, "' did not bind to a scorer function: '", input, "'"));
  }

  auto& bound_fn = bound->Cast<BoundFunctionExpression>();
  for (auto& child : bound_fn.GetChildrenMutable()) {
    if (child->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT &&
        child->IsFoldable()) {
      auto val = ExpressionExecutor::EvaluateScalar(*context, *child);
      child = make_uniq<BoundConstantExpression>(std::move(val));
    }
  }

  auto extracted = ExtractScorerFromBound(bound_fn, name);
  if (!extracted) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("'", what, "' scorer args must be constants: '", input, "'"));
  }
  return std::move(*extracted);
}

}  // namespace sdb::catalog
