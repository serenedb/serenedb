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

#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/utils/string.hpp>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

namespace sdb::connector {

void FromCompound(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_compound([ts_phrase('a')], [], ['b','c'], 1). "
    "Buckets are TSQUERY[] or NULL; min_should_match defaults to 1.";
  SDB_ASSERT(func.GetChildren().size() >= 3 && func.GetChildren().size() <= 4);

  auto extract =
    [](const duckdb::Expression& arg, std::string_view label,
       std::vector<const duckdb::Expression*>& out,
       std::vector<duckdb::unique_ptr<duckdb::Expression>>& synthesised) {
      // NULL bucket-arg -> empty bucket regardless of declared type.
      if (const auto* val = TryGetConstant(arg); val && val->IsNull()) {
        return;
      }
      const auto type_id = arg.GetReturnType().id();
      if (type_id != duckdb::LogicalTypeId::LIST &&
          type_id != duckdb::LogicalTypeId::ARRAY) {
        out.push_back(&arg);
        return;
      }
      // List/array shape: NULL-list -> empty bucket; folded constant
      // -> children values; list_value/array_value call -> children
      // expressions. Mirrors FromAnyAllOf's extraction.
      if (arg.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
        const auto& val =
          arg.Cast<duckdb::BoundConstantExpression>().GetValue();
        if (val.IsNull()) {
          return;
        }
        const auto& children = type_id == duckdb::LogicalTypeId::ARRAY
                                 ? duckdb::ArrayValue::GetChildren(val)
                                 : duckdb::ListValue::GetChildren(val);
        for (const auto& child_val : children) {
          synthesised.push_back(
            duckdb::make_uniq<duckdb::BoundConstantExpression>(child_val));
          out.push_back(synthesised.back().get());
        }
        return;
      }
      if (arg.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
        const auto& fn = arg.Cast<duckdb::BoundFunctionExpression>();
        const auto& fn_name = fn.Function().GetName().GetIdentifierName();
        if (fn_name != "list_value" && fn_name != "array_value") {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("ts_compound ", label,
                    " list arg must be a literal list or array (got: ", fn_name,
                    ")"),
            ERR_HINT("Pass a literal list/array, e.g. ['a', 'b'], or NULL "
                     "for an empty bucket."));
        }
        for (const auto& e : fn.GetChildren()) {
          out.push_back(e.get());
        }
        return;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_compound ", label,
                              " list arg must be a literal list or array"),
                      ERR_HINT("Pass a literal list/array or NULL for an "
                               "empty bucket."));
    };

  std::vector<const duckdb::Expression*> must, must_not, should;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> synthesised;
  extract(*func.GetChildren()[0], "must", must, synthesised);
  extract(*func.GetChildren()[1], "must_not", must_not, synthesised);
  extract(*func.GetChildren()[2], "should", should, synthesised);

  if (must.empty() && must_not.empty() && should.empty()) {
    AddFilter<irs::Empty>(parent);
    return;
  }

  auto& and_filter =
    ctx.negated ? Negate<irs::And>(parent) : AddFilter<irs::And>(parent);
  and_filter.boost(ctx.boost);

  auto inner_ctx = ctx;
  inner_ctx.negated = false;
  inner_ctx.boost = irs::kNoBoost;

  for (const auto* clause : must) {
    BuildTSQuery(and_filter, inner_ctx, column_info, *clause);
  }
  if (!must_not.empty()) {
    auto neg_ctx = inner_ctx;
    neg_ctx.negated = true;
    for (const auto* clause : must_not) {
      BuildTSQuery(and_filter, neg_ctx, column_info, *clause);
    }
  }
  if (!should.empty()) {
    int64_t min_should = 1;
    if (func.GetChildren().size() == 4) {
      if (auto r = GetIntArg(*func.GetChildren()[3],
                             "ts_compound min_should_match", min_should);
          !r.ok()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
      }
      if (min_should < 1 || min_should > static_cast<int64_t>(should.size())) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("ts_compound min_should_match must be in [1, ",
                                should.size(), "], got ", min_should),
                        ERR_HINT(kSyntaxHint));
      }
    }
    auto& or_filter = and_filter.add<irs::Or>();
    or_filter.min_match_count(static_cast<size_t>(min_should));
    for (const auto* clause : should) {
      BuildTSQuery(or_filter, inner_ctx, column_info, *clause);
    }
  } else if (func.GetChildren().size() == 4) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(
        "ts_compound min_should_match makes no sense without should clauses"),
      ERR_HINT(kSyntaxHint));
  }
}

}  // namespace sdb::connector
