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

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "tsq_common.hpp"

namespace sdb::connector {

RangeArgs ParseRangeArgs(const duckdb::BoundFunctionExpression& func) {
  constexpr auto kSyntaxHint =
    "Example: RANGE('a', 'z', true, false). NULL on either bound means "
    "unbounded; min_incl/max_incl select inclusive vs exclusive.";
  if (func.children.size() != 4) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("RANGE expects 4 arguments (min, max, min_incl, max_incl), "
              "got ",
              func.children.size()),
      ERR_HINT(kSyntaxHint));
  }
  RangeArgs out;
  for (size_t i = 0; i < 2; ++i) {
    const auto* val = TryGetConstant(*func.children[i]);
    if (!val) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("RANGE bound ", i, " must be a constant"),
                      ERR_HINT(kSyntaxHint));
    }
    if (!val->IsNull()) {
      (i == 0 ? out.min_val : out.max_val) = val;
    }
  }
  if (auto r = GetBoolArg(*func.children[2], "RANGE min_incl", out.min_incl);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
  }
  if (auto r = GetBoolArg(*func.children[3], "RANGE max_incl", out.max_incl);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
  }
  if (out.min_val && out.max_val &&
      out.min_val->type().id() != out.max_val->type().id()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("RANGE bounds have mismatched types: ",
                            out.min_val->type().ToString(), " vs ",
                            out.max_val->type().ToString()),
                    ERR_HINT("Both bounds must share the same type family."));
  }
  return out;
}

void FillByRangeOptionsVarchar(const RangeArgs& args,
                               irs::ByRangeOptions& out) {
  if (args.min_val) {
    auto sv = args.min_val->GetValue<std::string>();
    out.range.min.assign(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
    out.range.min_type =
      args.min_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  if (args.max_val) {
    auto sv = args.max_val->GetValue<std::string>();
    out.range.max.assign(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
    out.range.max_type =
      args.max_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
}

void FromTSQRangeOne(irs::BooleanFilter& parent, const FilterContext& ctx,
                     const SearchColumnInfo& column_info,
                     const duckdb::BoundFunctionExpression& func,
                     std::string_view label, bool is_lower, bool inclusive) {
  constexpr auto kSyntaxHint =
    "Example: LESS('m'), GREATER_EQ(42). The bound must be a non-null "
    "constant; for unbounded comparisons use RANGE(NULL, max, ...).";
  if (func.children.size() != 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(label, " expects 1 argument (bound), got ", func.children.size()),
      ERR_HINT(kSyntaxHint));
  }
  const auto* bound_val = TryGetConstant(*func.children[0]);
  if (!bound_val) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(label, " bound must be a constant"),
                    ERR_HINT(kSyntaxHint));
  }
  if (bound_val->IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(label, " bound must be non-null"),
                    ERR_HINT("Use RANGE(NULL, max, true, false) (or similar) "
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
  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    if (val_type != duckdb::LogicalTypeId::VARCHAR) {
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

  std::string field_name;
  MakeFieldName(column_info, field_name);
  if (auto r = MangleForType(col_type, field_name); !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()));
  }
  const auto bound_type =
    inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;

  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    // VARCHAR: tokenise the bound through the ambient analyzer; the
    // (single) token becomes the bound's bytes.
    auto text = bound_val->GetValue<std::string>();
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
    *range.mutable_field() = std::move(field_name);
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
        ERR_HINT("Use RANGE(min, max, ...) for multi-component bounds."));
    }
    return;
  }

  if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    auto& range = ctx.negated ? Negate<irs::ByRange>(parent)
                              : AddFilter<irs::ByRange>(parent);
    *range.mutable_field() = std::move(field_name);
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

  // Numeric: cast the bound to the column's logical type, then run
  // NumericTokenizer + SetGranularTerm (mirrors FromRange's numeric
  // path so DECIMAL bounds on a DOUBLE/INT/BIGINT column work
  // cleanly).
  auto& range = ctx.negated ? Negate<irs::ByGranularRange>(parent)
                            : AddFilter<irs::ByGranularRange>(parent);
  *range.mutable_field() = std::move(field_name);
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

void FromRange(irs::BooleanFilter& parent, const FilterContext& ctx,
               const SearchColumnInfo& column_info,
               const duckdb::BoundFunctionExpression& func) {
  auto args = ParseRangeArgs(func);
  // Both bounds NULL -> unbounded on both sides; matches every doc.
  if (!args.min_val && !args.max_val) {
    if (ctx.negated) {
      AddFilter<irs::Empty>(parent);
    } else {
      AddFilter<irs::All>(parent).boost(ctx.boost);
    }
    return;
  }

  const auto col_type = column_info.logical_type.id();
  const auto* val_sample = args.min_val ? args.min_val : args.max_val;
  const auto val_type = val_sample->type().id();

  // Validate value type matches column type family.
  auto type_mismatch = [&]() {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("RANGE bound type ", val_sample->type().ToString(),
                            " is incompatible with column type ",
                            column_info.logical_type.ToString()),
                    ERR_HINT("Both bounds must match the column's type "
                             "family (VARCHAR / BOOLEAN / numeric)."));
  };
  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    if (val_type != duckdb::LogicalTypeId::VARCHAR) {
      type_mismatch();
    }
    if (column_info.tokenizer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("RANGE on VARCHAR field requires identity-analyzed column"),
        ERR_HINT("Recreate the inverted index with the identity tokenizer "
                 "for this column, or use LESS/LESS_EQ/GREATER/GREATER_EQ "
                 "for analyzed-text bounds."));
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
                    ERR_MSG("RANGE: unsupported column type ",
                            column_info.logical_type.ToString()),
                    ERR_HINT("RANGE is supported on VARCHAR (identity "
                             "analyzer), BOOLEAN and numeric columns."));
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  if (auto r = MangleForType(col_type, field_name); !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()));
  }

  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    auto& range = ctx.negated ? Negate<irs::ByRange>(parent)
                              : AddFilter<irs::ByRange>(parent);
    *range.mutable_field() = std::move(field_name);
    range.boost(ctx.boost);
    auto* options = range.mutable_options();
    options->scored_terms_limit = ctx.scored_terms_limit;
    FillByRangeOptionsVarchar(args, *options);
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    auto& range = ctx.negated ? Negate<irs::ByRange>(parent)
                              : AddFilter<irs::ByRange>(parent);
    *range.mutable_field() = std::move(field_name);
    range.boost(ctx.boost);
    auto* options = range.mutable_options();
    options->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = options->range;
    if (args.min_val) {
      rng.min.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args.min_val->GetValue<bool>())));
      rng.min_type =
        args.min_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    if (args.max_val) {
      rng.max.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args.max_val->GetValue<bool>())));
      rng.max_type =
        args.max_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
  } else {
    // Numeric. Cast each bound to the column's logical type before
    // tokenising so the indexed and queried representations match.
    auto& range = ctx.negated ? Negate<irs::ByGranularRange>(parent)
                              : AddFilter<irs::ByGranularRange>(parent);
    *range.mutable_field() = std::move(field_name);
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
    if (args.min_val) {
      emit_bound(*args.min_val, rng.min, rng.min_type, args.min_incl);
    }
    if (args.max_val) {
      emit_bound(*args.max_val, rng.max, rng.max_type, args.max_incl);
    }
  }
}

}  // namespace sdb::connector
