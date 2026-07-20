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

#include "connector/functions/ts_query_codec.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/common/exception.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/expression/type_expression.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_binder/constant_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>

#include "basics/assert.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_common.hpp"

namespace sdb::connector {
namespace {

constexpr size_t kMaxStructuredNodes = 4096;
constexpr size_t kMaxStructuredDepth = 64;
constexpr size_t kMaxTSQueryStructuredTextSize = 64 * 1024;

std::optional<std::string> RenderTSQueryExpression(
  duckdb::ClientContext& context, const duckdb::Expression& expr);
std::optional<std::string> RenderTSQueryCall(
  duckdb::ClientContext& context, std::string_view name,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> children);

bool IsTSQueryFamilyTypeName(std::string_view name) {
  return absl::EqualsIgnoreCase(name, kTSQueryTypeName) ||
         absl::EqualsIgnoreCase(name, kTokenizerTypeName) ||
         absl::EqualsIgnoreCase(name, kBoostTypeName);
}

bool IsNumericTypeId(duckdb::LogicalTypeId id) {
  switch (id) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DECIMAL:
      return true;
    default:
      return false;
  }
}

bool IsWhitelistedTypeName(std::string_view name) {
  return IsTSQueryFamilyTypeName(name) ||
         IsNumericTypeId(
           duckdb::TransformStringToLogicalTypeId(std::string{name}));
}

bool IsWhitelistedCastType(const duckdb::LogicalType& type) {
  if (type.id() != duckdb::LogicalTypeId::UNBOUND || !type.AuxInfo()) {
    return false;
  }
  const auto& expr = duckdb::UnboundType::GetTypeExpression(type);
  if (!expr || expr->GetExpressionClass() != duckdb::ExpressionClass::TYPE) {
    return false;
  }
  const auto& type_expr = expr->Cast<duckdb::TypeExpression>();
  if (!type_expr.GetSchema().empty() || !type_expr.GetCatalog().empty()) {
    return false;
  }
  if (!IsWhitelistedTypeName(type_expr.GetTypeName().GetIdentifierName())) {
    return false;
  }
  return absl::c_all_of(type_expr.GetChildren(), [](const auto& mod) {
    return mod &&
           mod->GetExpressionClass() == duckdb::ExpressionClass::CONSTANT;
  });
}

bool WhitelistWalk(const duckdb::ParsedExpression& expr, size_t depth,
                   size_t& nodes) {
  if (depth > kMaxStructuredDepth || ++nodes > kMaxStructuredNodes) {
    return false;
  }
  switch (expr.GetExpressionClass()) {
    case duckdb::ExpressionClass::CONSTANT:
      return true;
    case duckdb::ExpressionClass::FUNCTION: {
      const auto& func = expr.Cast<duckdb::FunctionExpression>();
      if (func.Distinct() || func.Filter() ||
          (func.OrderBy() && !func.OrderBy()->orders.empty()) ||
          func.ExportState() || !func.GetQualifiedName().Schema().empty() ||
          !func.GetQualifiedName().Catalog().empty()) {
        return false;
      }
      auto name = func.FunctionName().GetIdentifierName();
      absl::AsciiStrToLower(&name);
      if (name != "list_value" && name != "-" && name != "+" &&
          ClassifyTSQueryFunction(name) == TSQueryOp::Unknown) {
        return false;
      }
      return absl::c_all_of(func.GetArguments(), [&](const auto& arg) {
        return !arg.HasName() &&
               WhitelistWalk(arg.GetExpression(), depth + 1, nodes);
      });
    }
    case duckdb::ExpressionClass::CAST: {
      const auto& cast = expr.Cast<duckdb::CastExpression>();
      if (cast.IsTryCast() || !IsWhitelistedCastType(cast.TargetType())) {
        return false;
      }
      return WhitelistWalk(cast.Child(), depth + 1, nodes);
    }
    case duckdb::ExpressionClass::OPERATOR: {
      const auto& op = expr.Cast<duckdb::OperatorExpression>();
      if (op.GetExpressionType() != duckdb::ExpressionType::ARRAY_CONSTRUCTOR) {
        return false;
      }
      return absl::c_all_of(op.GetChildren(), [&](const auto& child) {
        return child && WhitelistWalk(*child, depth + 1, nodes);
      });
    }
    default:
      return false;
  }
}

duckdb::unique_ptr<duckdb::ParsedExpression> ParseWhitelisted(
  std::string_view text) {
  if (text.size() > kMaxTSQueryStructuredTextSize) {
    return nullptr;
  }
  try {
    auto exprs = duckdb::Parser::ParseExpressionList(text);
    if (exprs.size() != 1 || !exprs[0]) {
      return nullptr;
    }
    const auto root_class = exprs[0]->GetExpressionClass();
    if (root_class != duckdb::ExpressionClass::FUNCTION &&
        root_class != duckdb::ExpressionClass::CAST) {
      return nullptr;
    }
    size_t nodes = 0;
    if (!WhitelistWalk(*exprs[0], 0, nodes)) {
      return nullptr;
    }
    return std::move(exprs[0]);
  } catch (const std::exception&) {
    return nullptr;
  }
}

bool IsStructuredText(std::string_view text) {
  return ParseWhitelisted(text) != nullptr;
}

bool IsTSQueryishType(const duckdb::LogicalType& type) {
  if (IsTSQueryStructType(type)) {
    return true;
  }
  if (type.id() == duckdb::LogicalTypeId::LIST) {
    return IsTSQueryStructType(duckdb::ListType::GetChildType(type));
  }
  if (type.id() == duckdb::LogicalTypeId::ARRAY) {
    return IsTSQueryStructType(duckdb::ArrayType::GetChildType(type));
  }
  return false;
}

bool IsRenderableValueType(const duckdb::LogicalType& type) {
  if (IsNumericTypeId(type.id())) {
    return true;
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
    case duckdb::LogicalTypeId::VARCHAR:
      return true;
    case duckdb::LogicalTypeId::LIST:
      return IsRenderableValueType(duckdb::ListType::GetChildType(type));
    case duckdb::LogicalTypeId::ARRAY:
      return IsRenderableValueType(duckdb::ArrayType::GetChildType(type));
    default:
      return false;
  }
}

std::optional<std::string> RenderPlainValue(const duckdb::Value& value) {
  if (value.IsNull()) {
    return "NULL";
  }
  if (!IsRenderableValueType(value.type())) {
    return std::nullopt;
  }
  return value.ToSQLString();
}

std::string BoostOperand(std::string rendered) {
  if (rendered.size() >= 2 && rendered.front() == '\'' &&
      rendered.back() == '\'') {
    rendered += "::tsquery";
  }
  return rendered;
}

std::string RenderBoosted(std::string operand, double factor) {
  return absl::StrCat("(", BoostOperand(std::move(operand)), " ^ ",
                      duckdb::Value::DOUBLE(factor).ToSQLString(), ")");
}

std::string RenderTokenized(std::string operand, std::string_view tokenizer) {
  return absl::StrCat(std::move(operand), "::tokenize(",
                      duckdb::Value(std::string{tokenizer}).ToSQLString(), ")");
}

std::string RenderTSQueryPartsSQL(const TSQueryParts& parts) {
  std::string out;
  if (IsStructuredText(parts.text)) {
    out = absl::StrCat("(", parts.text, ")");
  } else {
    out = duckdb::Value(parts.text).ToSQLString();
  }
  if (!parts.tokenizer.empty()) {
    out = RenderTokenized(std::move(out), parts.tokenizer);
  }
  if (parts.boost != 1.0f) {
    out = RenderBoosted(std::move(out), static_cast<double>(parts.boost));
  }
  return out;
}

std::string RenderTSQueryValueConstant(const duckdb::Value& value) {
  auto parts = TryGetTSQueryParts(value);
  if (!parts) {
    return "NULL";
  }
  return RenderTSQueryPartsSQL(*parts);
}

std::optional<std::string> RenderConstant(const duckdb::Value& value) {
  const auto& type = value.type();
  if (IsTSQueryStructType(type)) {
    return RenderTSQueryValueConstant(value);
  }
  if (value.IsNull()) {
    return "NULL";
  }
  if (IsTSQueryishType(type)) {
    const auto& children = ListOrArrayChildren(value);
    return absl::StrCat(
      "[",
      absl::StrJoin(children, ", ",
                    [](std::string* out, const duckdb::Value& v) {
                      absl::StrAppend(out, RenderTSQueryValueConstant(v));
                    }),
      "]");
  }
  return RenderPlainValue(value);
}

bool IsStringishTypeId(duckdb::LogicalTypeId id) {
  return id == duckdb::LogicalTypeId::VARCHAR ||
         id == duckdb::LogicalTypeId::BLOB ||
         id == duckdb::LogicalTypeId::STRING_LITERAL;
}

std::optional<duckdb::Value> TryEvaluateScalar(duckdb::ClientContext& context,
                                               const duckdb::Expression& expr) {
  duckdb::Value result;
  if (!expr.IsFoldable() ||
      !duckdb::ExpressionExecutor::TryEvaluateScalar(context, expr, result)) {
    return std::nullopt;
  }
  return result;
}

std::optional<std::string> RenderCast(duckdb::ClientContext& context,
                                      const duckdb::BoundCastExpression& cast) {
  const auto& target = cast.GetReturnType();
  const auto& source = cast.Child().GetReturnType();
  if (const auto boost = TryGetBoostModifier(target)) {
    if (*boost < 0.0) {
      return std::nullopt;
    }
    auto child = RenderTSQueryExpression(context, cast.Child());
    if (!child) {
      return std::nullopt;
    }
    return RenderBoosted(std::move(*child), *boost);
  }
  if (const auto tokenizer = TryGetTokenizerModifier(target);
      !tokenizer.empty()) {
    auto child = RenderTSQueryExpression(context, cast.Child());
    if (!child) {
      return std::nullopt;
    }
    return RenderTokenized(std::move(*child), tokenizer);
  }
  if (IsTSQueryStructType(target)) {
    auto child = RenderTSQueryExpression(context, cast.Child());
    if (!child || IsTSQueryStructType(source)) {
      return child;
    }
    return absl::StrCat(*child, "::tsquery");
  }
  if ((IsTSQueryStructType(source) && IsStringishTypeId(target.id())) ||
      (IsTSQueryishType(target) && !IsTSQueryishType(source) &&
       IsRenderableValueType(source))) {
    return RenderTSQueryExpression(context, cast.Child());
  }
  return std::nullopt;
}

std::optional<std::string> RenderJoinedArgs(
  duckdb::ClientContext& context,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> children,
  std::string_view open, std::string_view close) {
  std::string out{open};
  for (size_t i = 0; i < children.size(); ++i) {
    auto rendered = RenderTSQueryExpression(context, *children[i]);
    if (!rendered) {
      return std::nullopt;
    }
    if (i != 0) {
      out += ", ";
    }
    out += *rendered;
  }
  out += close;
  return out;
}

void FoldStructuredConstants(duckdb::ClientContext& context,
                             duckdb::unique_ptr<duckdb::Expression>& expr) {
  if (!expr ||
      expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
    return;
  }
  const auto& type = expr->GetReturnType();
  const bool list_of_tsquery =
    IsTSQueryishType(type) && !IsTSQueryStructType(type) &&
    expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST;
  if (!IsTSQueryishType(type) || list_of_tsquery) {
    if (auto value = TryEvaluateScalar(context, *expr)) {
      expr =
        duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(*value));
      return;
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      FoldStructuredConstants(context, child);
    });
}

std::optional<std::string> RenderTSQueryExpression(
  duckdb::ClientContext& context, const duckdb::Expression& expr) {
  switch (expr.GetExpressionClass()) {
    case duckdb::ExpressionClass::BOUND_CONSTANT:
      return RenderConstant(
        expr.Cast<duckdb::BoundConstantExpression>().GetValue());
    case duckdb::ExpressionClass::BOUND_CAST: {
      const auto& cast = expr.Cast<duckdb::BoundCastExpression>();
      if (IsTSQueryishType(cast.GetReturnType()) ||
          IsTSQueryishType(cast.Child().GetReturnType())) {
        return RenderCast(context, cast);
      }
      break;
    }
    case duckdb::ExpressionClass::BOUND_FUNCTION: {
      const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
      if (IsTSQueryishType(func.GetReturnType())) {
        return RenderTSQueryCall(context,
                                 func.Function().GetName().GetIdentifierName(),
                                 func.GetChildren());
      }
      break;
    }
    default:
      break;
  }
  if (IsTSQueryishType(expr.GetReturnType())) {
    return std::nullopt;
  }
  if (auto value = TryEvaluateScalar(context, expr)) {
    return RenderPlainValue(*value);
  }
  return std::nullopt;
}

std::optional<std::string> RenderTSQueryCall(
  duckdb::ClientContext& context, std::string_view name,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> children) {
  if (name == "list_value" || name == "array_value") {
    return RenderJoinedArgs(context, children, "[", "]");
  }
  switch (ClassifyTSQueryFunction(name)) {
    case TSQueryOp::Unknown:
      return std::nullopt;
    case TSQueryOp::Boost: {
      if (children.size() != 2) {
        return std::nullopt;
      }
      auto inner = RenderTSQueryExpression(context, *children[0]);
      auto factor = TryEvaluateScalar(context, *children[1]);
      if (!inner || !factor || factor->IsNull()) {
        return std::nullopt;
      }
      duckdb::Value coerced;
      if (!factor->DefaultTryCastAs(duckdb::LogicalType::DOUBLE, coerced,
                                    nullptr) ||
          coerced.GetValue<double>() < 0.0) {
        return std::nullopt;
      }
      return RenderBoosted(std::move(*inner), coerced.GetValue<double>());
    }
    case TSQueryOp::Or:
    case TSQueryOp::And:
    case TSQueryOp::PhraseSeq: {
      if (children.size() != 2) {
        return std::nullopt;
      }
      auto lhs = RenderTSQueryExpression(context, *children[0]);
      auto rhs = RenderTSQueryExpression(context, *children[1]);
      if (!lhs || !rhs) {
        return std::nullopt;
      }
      return absl::StrCat("(", *lhs, " ", name, " ", *rhs, ")");
    }
    case TSQueryOp::Not: {
      if (children.size() != 1) {
        return std::nullopt;
      }
      auto inner = RenderTSQueryExpression(context, *children[0]);
      if (!inner) {
        return std::nullopt;
      }
      return absl::StrCat("(!!", *inner, ")");
    }
    default: {
      auto args = RenderJoinedArgs(context, children, "(", ")");
      if (!args) {
        return std::nullopt;
      }
      return absl::StrCat(name, *args);
    }
  }
}

}  // namespace

bool IsTSQueryStructType(const duckdb::LogicalType& type) {
  if (type.id() != duckdb::LogicalTypeId::STRUCT) {
    return false;
  }
  const auto alias = type.GetAlias();
  return alias == kTSQueryTypeName || alias == kModifierTSQueryTypeName;
}

std::optional<TSQueryParts> TryGetTSQueryParts(const duckdb::Value& value) {
  if (!IsTSQueryStructType(value.type()) || value.IsNull()) {
    return std::nullopt;
  }
  const auto& children = duckdb::StructValue::GetChildren(value);
  if (children.size() <= kTSQueryBoostChild ||
      children[kTSQueryTextChild].IsNull()) {
    return std::nullopt;
  }
  TSQueryParts parts;
  parts.text = duckdb::StringValue::Get(children[kTSQueryTextChild]);
  if (!children[kTSQueryTokenizerChild].IsNull()) {
    parts.tokenizer =
      duckdb::StringValue::Get(children[kTSQueryTokenizerChild]);
  }
  if (!children[kTSQueryBoostChild].IsNull()) {
    parts.boost = children[kTSQueryBoostChild].GetValue<float>();
  }
  return parts;
}

duckdb::Value MakeTSQueryValue(const duckdb::LogicalType& type,
                               std::string_view text) {
  SDB_ASSERT(IsTSQueryStructType(type));
  duckdb::vector<duckdb::Value> children;
  children.reserve(3);
  children.emplace_back(std::string{text});
  children.emplace_back(std::string{});
  children.emplace_back(duckdb::Value::FLOAT(1.0f));
  return duckdb::Value::STRUCT(type, std::move(children));
}

std::string RenderTSQuery(std::string_view text, std::string_view tokenizer,
                          float boost) {
  if (tokenizer.empty() && boost == 1.0f) {
    return std::string{text};
  }
  return RenderTSQueryPartsSQL(TSQueryParts{
    .text = std::string{text},
    .tokenizer = std::string{tokenizer},
    .boost = boost,
  });
}

std::optional<duckdb::Value> TryFoldTSQueryCall(
  duckdb::ClientContext& context, std::string_view name,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> children) {
  auto rendered = RenderTSQueryCall(context, name, children);
  if (!rendered || rendered->size() > kMaxTSQueryStructuredTextSize) {
    return std::nullopt;
  }
  return MakeTSQueryValue(MakeTSQueryType(), *rendered);
}

duckdb::unique_ptr<duckdb::Expression> TryParseStructuredTSQueryText(
  std::string_view text, duckdb::ClientContext& context) {
  auto parsed = ParseWhitelisted(text);
  if (!parsed) {
    return nullptr;
  }
  const bool begin_transaction = !context.transaction.HasActiveTransaction();
  if (begin_transaction) {
    try {
      context.transaction.BeginTransaction();
    } catch (const std::exception&) {
      return nullptr;
    }
  }
  duckdb::unique_ptr<duckdb::Expression> bound;
  try {
    auto binder = duckdb::Binder::CreateBinder(context);
    duckdb::ConstantBinder constant_binder(*binder, context, "TSQUERY");
    bound = constant_binder.Bind(parsed);
    if (bound) {
      FoldStructuredConstants(context, bound);
    }
  } catch (const std::exception&) {
    bound = nullptr;
  }
  if (begin_transaction) {
    try {
      context.transaction.Commit();
    } catch (const std::exception&) {
      bound = nullptr;
    }
  }
  return bound;
}

}  // namespace sdb::connector
