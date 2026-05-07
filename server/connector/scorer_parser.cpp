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

#include "connector/scorer_parser.h"

#include <absl/strings/ascii.h>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_binder/constant_binder.hpp>

#include <string>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "connector/scorer_extract.h"

namespace sdb::connector {
namespace {

// Wrap a parsed user expression into a scorer-shaped function call by
// prepending a tableoid placeholder constant. Two input shapes:
//
//   bare ident `bm25`          -> ColumnRefExpression  -> bm25(0::BIGINT)
//   call       `bm25(1.5, ..)` -> FunctionExpression   -> bm25(0::BIGINT, ..)
//
// Anything else (literal, binary op, subquery, ...) is rejected. We
// deliberately reuse DuckDB's ParsedExpression types so the result feeds
// straight into ConstantBinder + bm25/tfidf/etc. overload resolution; no
// hand-rolled type coercion needed.
ResultOr<duckdb::unique_ptr<duckdb::ParsedExpression>> WrapWithPlaceholder(
  duckdb::unique_ptr<duckdb::ParsedExpression> expr,
  std::string_view input_for_error) {
  using namespace duckdb;
  auto placeholder = []() {
    return make_uniq<ConstantExpression>(Value::BIGINT(0));
  };

  switch (expr->GetExpressionType()) {
    case ExpressionType::COLUMN_REF: {
      auto& colref = expr->Cast<ColumnRefExpression>();
      if (colref.IsQualified()) {
        return std::unexpected<Result>{
          std::in_place, ERROR_BAD_PARAMETER,
          "'optimize_top_k' scorer name must be unqualified, got '",
          input_for_error, "'"};
      }
      auto name = colref.GetColumnName();
      vector<unique_ptr<ParsedExpression>> children;
      children.emplace_back(placeholder());
      return unique_ptr<ParsedExpression>(
        make_uniq<FunctionExpression>(std::move(name), std::move(children)));
    }
    case ExpressionType::FUNCTION: {
      auto& fn = expr->Cast<FunctionExpression>();
      fn.children.insert(fn.children.begin(), placeholder());
      return std::move(expr);
    }
    default:
      return std::unexpected<Result>{
        std::in_place, ERROR_BAD_PARAMETER,
        "'optimize_top_k' expects a scorer name or function call, got '",
        input_for_error, "'"};
  }
}

}  // namespace

ResultOr<catalog::Scorer> ParseScorerExpression(
  duckdb::ClientContext& context, std::string_view input) {
  using namespace duckdb;
  std::string source(input);

  auto parsed = basics::SafeCallT(
    [&] { return Parser::ParseExpressionList(source); });
  if (!parsed) {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "Cannot parse 'optimize_top_k' scorer expression '", input,
      "': ", parsed.error().errorMessage()};
  }
  auto& exprs = *parsed;
  if (exprs.size() != 1) {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "'optimize_top_k' must be a single scorer expression, got ",
      exprs.size(), " in '", input, "'"};
  }

  auto wrapped = WrapWithPlaceholder(std::move(exprs[0]), input);
  if (!wrapped) {
    return std::unexpected<Result>(std::move(wrapped).error());
  }
  auto fn_expr = std::move(*wrapped);
  // Capture the function name now -- after Bind() the wrapper's identity
  // moves into the BoundFunctionExpression, but we want the user's spelling
  // for error messages either way.
  std::string name = fn_expr->Cast<FunctionExpression>().function_name;
  absl::AsciiStrToLower(&name);

  // ConstantBinder rejects column refs, subqueries, defaults, windows --
  // exactly the shape we want for a scalar scorer-config call.
  auto binder = Binder::CreateBinder(context);
  ConstantBinder cb(*binder, context, "optimize_top_k");

  auto bound_result = basics::SafeCallT([&] { return cb.Bind(fn_expr); });
  if (!bound_result) {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "Cannot bind 'optimize_top_k' scorer '", input,
      "': ", bound_result.error().errorMessage()};
  }
  auto bound = std::move(*bound_result);
  if (!bound ||
      bound->expression_class != ExpressionClass::BOUND_FUNCTION) {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "'optimize_top_k' did not bind to a scorer function: '", input, "'"};
  }
  const auto& bound_fn = bound->Cast<BoundFunctionExpression>();

  auto extracted = ExtractScorerFromBound(bound_fn, name);
  if (!extracted) {
    return std::unexpected<Result>(std::move(extracted).error());
  }
  if (!*extracted) {
    // Non-constant arg -- shouldn't happen because ConstantBinder
    // normalises arg shapes, but report cleanly if it does.
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "'optimize_top_k' scorer args must be constants: '", input, "'"};
  }
  return std::move(**extracted);
}

}  // namespace sdb::connector
