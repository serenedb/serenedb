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
#include "tsq_common.hpp"

namespace sdb::connector {

Result FromCompound(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() < 3 || func.children.size() > 4) {
    return {ERROR_BAD_PARAMETER,
            "compound expects (must, must_not, should [, min_should_match]), "
            "got ",
            func.children.size(), " args"};
  }

  auto extract =
    [](const duckdb::Expression& arg, std::string_view label,
       std::vector<const duckdb::Expression*>& out,
       std::vector<duckdb::unique_ptr<duckdb::Expression>>& synthesised)
    -> Result {
    // NULL bucket-arg -> empty bucket regardless of declared type.
    if (const auto* val = TryGetConstant(arg); val && val->IsNull()) {
      return {};
    }
    const auto type_id = arg.return_type.id();
    if (type_id != duckdb::LogicalTypeId::LIST &&
        type_id != duckdb::LogicalTypeId::ARRAY) {
      out.push_back(&arg);
      return {};
    }
    // List/array shape: NULL-list -> empty bucket; folded constant
    // -> children values; list_value/array_value call -> children
    // expressions. Mirrors FromAnyAllOf's extraction.
    if (arg.expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
      const auto& val = arg.Cast<duckdb::BoundConstantExpression>().value;
      if (val.IsNull()) {
        return {};
      }
      const auto& children = type_id == duckdb::LogicalTypeId::ARRAY
                               ? duckdb::ArrayValue::GetChildren(val)
                               : duckdb::ListValue::GetChildren(val);
      for (const auto& child_val : children) {
        synthesised.push_back(
          duckdb::make_uniq<duckdb::BoundConstantExpression>(child_val));
        out.push_back(synthesised.back().get());
      }
      return {};
    }
    if (arg.expression_class == duckdb::ExpressionClass::BOUND_FUNCTION) {
      const auto& fn = arg.Cast<duckdb::BoundFunctionExpression>();
      if (fn.function.name != "list_value" &&
          fn.function.name != "array_value") {
        return {ERROR_BAD_PARAMETER,
                "compound ",
                label,
                " list arg must be a literal list or array (got: ",
                fn.function.name,
                ")"};
      }
      for (const auto& e : fn.children) {
        out.push_back(e.get());
      }
      return {};
    }
    return {ERROR_BAD_PARAMETER, "compound ", label,
            " list arg must be a literal list or array"};
  };

  std::vector<const duckdb::Expression*> must, must_not, should;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> synthesised;
  if (auto r = extract(*func.children[0], "must", must, synthesised); !r.ok()) {
    return r;
  }
  if (auto r = extract(*func.children[1], "must_not", must_not, synthesised);
      !r.ok()) {
    return r;
  }
  if (auto r = extract(*func.children[2], "should", should, synthesised);
      !r.ok()) {
    return r;
  }

  if (must.empty() && must_not.empty() && should.empty()) {
    AddFilter<irs::Empty>(parent);
    return {};
  }

  auto& and_filter =
    ctx.negated ? Negate<irs::And>(parent) : AddFilter<irs::And>(parent);
  and_filter.boost(ctx.boost);

  auto inner_ctx = ctx;
  inner_ctx.negated = false;
  inner_ctx.boost = irs::kNoBoost;

  for (const auto* clause : must) {
    if (auto r = BuildTSQuery(and_filter, inner_ctx, column_info, *clause);
        !r.ok()) {
      return r;
    }
  }
  if (!must_not.empty()) {
    auto neg_ctx = inner_ctx;
    neg_ctx.negated = true;
    for (const auto* clause : must_not) {
      if (auto r = BuildTSQuery(and_filter, neg_ctx, column_info, *clause);
          !r.ok()) {
        return r;
      }
    }
  }
  if (!should.empty()) {
    int64_t min_should = 1;
    if (func.children.size() == 4) {
      if (auto r = GetIntArg(*func.children[3], "compound min_should_match",
                             min_should);
          !r.ok()) {
        return r;
      }
      if (min_should < 1 || min_should > static_cast<int64_t>(should.size())) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("compound min_should_match must be in [1, ",
                                should.size(), "], got ", min_should));
      }
    }
    auto& or_filter = and_filter.add<irs::Or>();
    or_filter.min_match_count(static_cast<size_t>(min_should));
    for (const auto* clause : should) {
      if (auto r = BuildTSQuery(or_filter, inner_ctx, column_info, *clause);
          !r.ok()) {
        return r;
      }
    }
  } else if (func.children.size() == 4) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(
        "compound min_should_match makes no sense without should clauses"));
  }
  return {};
}

}  // namespace sdb::connector
