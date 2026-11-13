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
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/search/filters/range_filter.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string.hpp>
#include <vector>

#include "ts_common.hpp"

namespace sdb::connector {
namespace {

// Copy the term before the next bound reuses the tokenizer's buffers.
// No tokens means no match, which is distinct from an unbounded NULL.
bool AnalyzeRangeBound(irs::analysis::Tokenizer& analyzer,
                       const duckdb::Value& value, std::string_view label,
                       irs::bstring& out) {
  const auto& text = duckdb::StringValue::Get(value);
  irs::ValueAnalyzer value_analyzer;
  irs::ValueTokens tokens;
  if (!value_analyzer.Analyze(analyzer, text, tokens)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(label, " failed to analyse '", text, "'"),
      ERR_HINT("The column's analyzer rejected the bound value."));
  }
  const auto terms = tokens.terms();
  if (terms.empty()) {
    return false;
  }
  if (terms.size() > 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(label,
              " produced multiple tokens; range comparison "
              "requires a single token"),
      ERR_HINT("Use a bound that produces a single token with the column's "
               "analyzer."));
  }
  out.assign(irs::AsBytesView(terms[0]));
  return true;
}

bool IsText(duckdb::LogicalTypeId id) noexcept {
  return id == duckdb::LogicalTypeId::VARCHAR ||
         id == duckdb::LogicalTypeId::BLOB;
}

void AddRange(BoolTarget parent, const FilterContext& ctx,
              const SearchColumnInfo& column_info, const RangeArgs& args,
              std::string_view label) {
  SDB_ASSERT(args.min || args.max);
  const auto col_type = column_info.logical_type.id();
  const auto& bound = args.min ? *args.min : *args.max;
  const auto val_type = bound.type().id();
  bool compatible = false;
  if (IsText(col_type)) {
    compatible = IsText(val_type);
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    compatible = val_type == duckdb::LogicalTypeId::BOOLEAN;
  } else if (term_dict::IsNumeric(col_type)) {
    compatible = term_dict::IsNumeric(val_type);
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(label, ": unsupported column type ",
                            column_info.logical_type.ToString()),
                    ERR_HINT("Range comparisons are supported on VARCHAR, "
                             "BLOB, BOOLEAN and numeric columns."));
  }
  if (!compatible) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(label, " bound type ", bound.type().ToString(),
                            " is incompatible with column type ",
                            column_info.logical_type.ToString()),
                    ERR_HINT("Bounds must match the column's type family "
                             "(VARCHAR / BLOB / BOOLEAN / numeric)."));
  }

  if (IsText(col_type)) {
    irs::ByRangeOptions options;
    const auto analyze = [&](const duckdb::Value* value, bool inclusive,
                             irs::bstring& term, irs::BoundType& type) {
      if (!value) {
        return true;
      }
      type = inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
      return AnalyzeRangeBound(ctx.tokenizer, *value, label, term);
    };
    const bool has_min = analyze(args.min, args.min_incl, options.range.min,
                                 options.range.min_type);
    const bool has_max = analyze(args.max, args.max_incl, options.range.max,
                                 options.range.max_type);
    if (!has_min || !has_max) {
      AddMaybeNegated<irs::Empty>(parent, ctx, column_info);
      return;
    }
    auto& range = AddMaybeNegated<irs::ByRange>(parent, ctx, column_info);
    SetField(range, column_info);
    range.SetBoost(ctx.boost);
    SetLeafScorer(range, column_info);
    *range.mutable_options() = std::move(options);
    return;
  }

  if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    const auto accepts = [&](bool value) {
      if (args.min) {
        const bool min = args.min->GetValue<bool>();
        if (args.min_incl ? value < min : value <= min) {
          return false;
        }
      }
      if (args.max) {
        const bool max = args.max->GetValue<bool>();
        if (args.max_incl ? value > max : value >= max) {
          return false;
        }
      }
      return true;
    };
    AddBools(MaybeNegated(parent, ctx, column_info), column_info,
             accepts(false), accepts(true), ctx.boost);
    return;
  }

  AddNumericRange(MaybeNegated(parent, ctx, column_info), ctx, column_info,
                  args.min, !args.min_incl, args.max, !args.max_incl);
}

}  // namespace

RangeArgs ParseRangeArgs(const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_between('a', 'z', true, false). NULL bound = unbounded.";
  SDB_ASSERT(func.GetChildren().size() == 4);
  RangeArgs out;
  for (size_t i = 0; i < 2; ++i) {
    const auto* val = TryGetConstant(*func.GetChildren()[i]);
    if (!val) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_between bound ", i, " must be a constant"),
                      ERR_HINT(kSyntaxHint));
    }
    if (!val->IsNull()) {
      (i == 0 ? out.min : out.max) = val;
    }
  }
  GetBoolArg(*func.GetChildren()[2], out.min_incl,
             {"ts_between min_incl", kSyntaxHint});
  GetBoolArg(*func.GetChildren()[3], out.max_incl,
             {"ts_between max_incl", kSyntaxHint});
  if (out.min && out.max && out.min->type().id() != out.max->type().id()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_between bounds have mismatched types: ",
              out.min->type().ToString(), " vs ", out.max->type().ToString()),
      ERR_HINT("Both bounds must share the same type family."));
  }
  return out;
}

void FillByRangeOptionsVarchar(const RangeArgs& args,
                               irs::ByRangeOptions& out) {
  if (args.min) {
    auto sv = duckdb::StringValue::Get(*args.min);
    out.range.min.assign(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
    out.range.min_type =
      args.min_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  if (args.max) {
    auto sv = duckdb::StringValue::Get(*args.max);
    out.range.max.assign(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
    out.range.max_type =
      args.max_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
}

void FromHalfRange(BoolTarget parent, const FilterContext& ctx,
                   const SearchColumnInfo& column_info,
                   const duckdb::BoundFunctionExpression& func,
                   std::string_view label, bool is_lower, bool inclusive) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_lt('m') or ts_ge(42). Bound must be non-null; "
    "use ts_between(NULL, ...) for unbounded.";
  SDB_ASSERT(func.GetChildren().size() == 1);
  const auto* bound_val = TryGetConstant(*func.GetChildren()[0]);
  if (!bound_val) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(label, " bound must be a constant"),
                    ERR_HINT(kSyntaxHint));
  }
  if (bound_val->IsNull()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(label, " bound must be non-null"),
      ERR_HINT("Use ts_between(NULL, max, true, false) (or similar) "
               "for unbounded semantics."));
  }

  RangeArgs args;
  (is_lower ? args.min : args.max) = bound_val;
  (is_lower ? args.min_incl : args.max_incl) = inclusive;
  AddRange(parent, ctx, column_info, args, label);
}

void FromBetween(BoolTarget parent, const FilterContext& ctx,
                 const SearchColumnInfo& column_info,
                 const duckdb::BoundFunctionExpression& func) {
  const auto args = ParseRangeArgs(func);
  if (!args.min && !args.max) {
    if (ctx.negated) {
      AddFilter<irs::Empty>(parent);
    } else {
      AddFilter<irs::All>(parent).SetBoost(ctx.boost);
    }
    return;
  }
  AddRange(parent, ctx, column_info, args, "ts_between");
}

}  // namespace sdb::connector
