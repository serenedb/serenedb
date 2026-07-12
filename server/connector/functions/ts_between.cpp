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
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "basics/exceptions.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

namespace sdb::connector {

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

void FromHalfRange(irs::BooleanFilter& parent, const FilterContext& ctx,
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

  const auto col_type = column_info.logical_type.id();
  const auto val_type = bound_val->type().id();
  auto type_mismatch = [&]() {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(label, " bound type ", bound_val->type().ToString(),
              " is incompatible with column type ",
              column_info.logical_type.ToString()),
      ERR_HINT("The bound's type must match the column's type family "
               "(VARCHAR / BOOLEAN / numeric)."));
  };
  if (col_type == duckdb::LogicalTypeId::VARCHAR ||
      col_type == duckdb::LogicalTypeId::BLOB) {
    if (val_type != duckdb::LogicalTypeId::VARCHAR &&
        val_type != duckdb::LogicalTypeId::BLOB) {
      type_mismatch();
    }
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    if (val_type != duckdb::LogicalTypeId::BOOLEAN) {
      type_mismatch();
    }
  } else if (IsNumericTypeId(col_type)) {
    if (!IsRangeNumericValueType(val_type)) {
      type_mismatch();
    }
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(label, ": unsupported column type ",
                            column_info.logical_type.ToString()),
                    ERR_HINT("Range comparisons are supported on VARCHAR, "
                             "BOOLEAN and numeric columns."));
  }

  ValidateFilterType(col_type);
  const auto bound_type =
    inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;

  if (col_type == duckdb::LogicalTypeId::VARCHAR ||
      col_type == duckdb::LogicalTypeId::BLOB) {
    // VARCHAR / BLOB: tokenise the bound through the ambient analyzer; the
    // (single) token becomes the bound's bytes. BLOB shares
    // PhysicalType::VARCHAR storage, so GetValue<string>() returns the raw
    // bytes either way.
    auto text = duckdb::StringValue::Get(*bound_val);
    auto& analyzer = ctx.tokenizer;
    if (!analyzer.reset(std::string_view{text})) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(label, " failed to analyse '", text, "'"),
        ERR_HINT("The column's analyzer rejected the bound value."));
    }
    const auto* token = irs::get<irs::TermAttr>(analyzer);
    if (!analyzer.next()) {
      // Zero tokens (e.g. all-stopword input) -> the comparison can't
      // match anything in the term dictionary.
      AddFilter<irs::Empty>(parent);
      return;
    }
    auto& range = ctx.negated ? Negate<irs::ByRange>(parent)
                              : AddFilter<irs::ByRange>(parent);
    *range.mutable_field_id() = PickPerKindFieldId(column_info, col_type);
    range.boost(ctx.boost);
    auto* options = range.mutable_options();
    options->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = options->range;
    if (is_lower) {
      rng.min.assign(token->value);
      rng.min_type = bound_type;
    } else {
      rng.max.assign(token->value);
      rng.max_type = bound_type;
    }
    if (analyzer.next()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(label,
                " produced multiple tokens; range comparison "
                "requires a single token"),
        ERR_HINT("Use ts_between(min, max, ...) for multi-component bounds."));
    }
    return;
  }

  if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    auto& range = ctx.negated ? Negate<irs::ByRange>(parent)
                              : AddFilter<irs::ByRange>(parent);
    *range.mutable_field_id() = PickPerKindFieldId(column_info, col_type);
    range.boost(ctx.boost);
    auto* options = range.mutable_options();
    options->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = options->range;
    auto bytes = irs::ViewCast<irs::byte_type>(
      irs::BooleanTokenizer::value(bound_val->GetValue<bool>()));
    if (is_lower) {
      rng.min.assign(bytes);
      rng.min_type = bound_type;
    } else {
      rng.max.assign(bytes);
      rng.max_type = bound_type;
    }
    return;
  }

  auto& range = ctx.negated ? Negate<irs::ByGranularRange>(parent)
                            : AddFilter<irs::ByGranularRange>(parent);
  *range.mutable_field_id() = PickPerKindFieldId(column_info, col_type);
  range.boost(ctx.boost);
  auto* options = range.mutable_options();
  options->scored_terms_limit = ctx.scored_terms_limit;
  auto& rng = options->range;
  auto cast = bound_val->DefaultCastAs(column_info.logical_type);
  irs::NumericTokenizer stream;
  ResetNumericStream(stream, col_type, cast);
  if (is_lower) {
    irs::SetGranularTerm(rng.min, stream);
    rng.min_type = bound_type;
  } else {
    irs::SetGranularTerm(rng.max, stream);
    rng.max_type = bound_type;
  }
}

void FromBetween(irs::BooleanFilter& parent, const FilterContext& ctx,
                 const SearchColumnInfo& column_info,
                 const duckdb::BoundFunctionExpression& func) {
  auto args = ParseRangeArgs(func);
  // Both bounds NULL -> unbounded on both sides; matches every doc.
  if (!args.min && !args.max) {
    if (ctx.negated) {
      AddFilter<irs::Empty>(parent);
    } else {
      AddFilter<irs::All>(parent).boost(ctx.boost);
    }
    return;
  }

  const auto col_type = column_info.logical_type.id();
  const auto* val_sample = args.min ? args.min : args.max;
  const auto val_type = val_sample->type().id();

  // Validate value type matches column type family.
  auto type_mismatch = [&]() {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_between bound type ", val_sample->type().ToString(),
              " is incompatible with column type ",
              column_info.logical_type.ToString()),
      ERR_HINT("Both bounds must match the column's type "
               "family (VARCHAR / BOOLEAN / numeric)."));
  };
  if (col_type == duckdb::LogicalTypeId::VARCHAR ||
      col_type == duckdb::LogicalTypeId::BLOB) {
    if (val_type != duckdb::LogicalTypeId::VARCHAR &&
        val_type != duckdb::LogicalTypeId::BLOB) {
      type_mismatch();
    }
    if (column_info.tokenizer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_between on VARCHAR field requires keyword-analyzed column"),
        ERR_HINT("Recreate the inverted index with the keyword tokenizer "
                 "for this column, or use ts_lt/ts_le/ts_gt/ts_ge for "
                 "analyzed-text bounds."));
    }
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    if (val_type != duckdb::LogicalTypeId::BOOLEAN) {
      type_mismatch();
    }
  } else if (IsNumericTypeId(col_type)) {
    if (!IsRangeNumericValueType(val_type)) {
      type_mismatch();
    }
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_between: unsupported column type ",
                            column_info.logical_type.ToString()),
                    ERR_HINT("ts_between is supported on VARCHAR (identity "
                             "analyzer), BOOLEAN and numeric columns."));
  }

  ValidateFilterType(col_type);

  if (col_type == duckdb::LogicalTypeId::VARCHAR ||
      col_type == duckdb::LogicalTypeId::BLOB) {
    auto& range = ctx.negated ? Negate<irs::ByRange>(parent)
                              : AddFilter<irs::ByRange>(parent);
    *range.mutable_field_id() = PickPerKindFieldId(column_info, col_type);
    range.boost(ctx.boost);
    auto* options = range.mutable_options();
    options->scored_terms_limit = ctx.scored_terms_limit;
    FillByRangeOptionsVarchar(args, *options);
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    auto& range = ctx.negated ? Negate<irs::ByRange>(parent)
                              : AddFilter<irs::ByRange>(parent);
    *range.mutable_field_id() = PickPerKindFieldId(column_info, col_type);
    range.boost(ctx.boost);
    auto* options = range.mutable_options();
    options->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = options->range;
    if (args.min) {
      rng.min.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args.min->GetValue<bool>())));
      rng.min_type =
        args.min_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    if (args.max) {
      rng.max.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args.max->GetValue<bool>())));
      rng.max_type =
        args.max_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
  } else {
    // Numeric. Cast each bound to the column's logical type before
    // tokenising so the indexed and queried representations match.
    auto& range = ctx.negated ? Negate<irs::ByGranularRange>(parent)
                              : AddFilter<irs::ByGranularRange>(parent);
    *range.mutable_field_id() = PickPerKindFieldId(column_info, col_type);
    range.boost(ctx.boost);
    auto* range_opts = range.mutable_options();
    range_opts->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = range_opts->range;
    auto emit_bound = [&](const duckdb::Value& v,
                          irs::ByGranularRangeOptions::terms& boundary,
                          irs::BoundType& bt, bool incl) {
      auto cast = v.DefaultCastAs(column_info.logical_type);
      irs::NumericTokenizer stream;
      ResetNumericStream(stream, col_type, cast);
      irs::SetGranularTerm(boundary, stream);
      bt = incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    };
    if (args.min) {
      emit_bound(*args.min, rng.min, rng.min_type, args.min_incl);
    }
    if (args.max) {
      emit_bound(*args.max, rng.max, rng.max_type, args.max_incl);
    }
  }
}

}  // namespace sdb::connector
