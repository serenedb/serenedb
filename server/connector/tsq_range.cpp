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

#include "tsq_common.hpp"

namespace sdb::connector {

ResultOr<RangeArgs> ParseRangeArgs(
  const duckdb::BoundFunctionExpression& func) {
  auto err = [](auto&&... args) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   std::forward<decltype(args)>(args)...};
  };
  if (func.children.size() != 4) {
    return err(
      "RANGE expects 4 arguments "
      "(min, max, min_incl, max_incl), got ",
      func.children.size());
  }
  RangeArgs out;
  for (size_t i = 0; i < 2; ++i) {
    const auto* val = TryGetConstant(*func.children[i]);
    if (!val) {
      return err("RANGE bound ", i, " must be a constant");
    }
    if (!val->IsNull()) {
      (i == 0 ? out.min_val : out.max_val) = val;
    }
  }
  if (auto r = GetBoolArg(*func.children[2], "RANGE min_incl", out.min_incl);
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (auto r = GetBoolArg(*func.children[3], "RANGE max_incl", out.max_incl);
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (out.min_val && out.max_val &&
      out.min_val->type().id() != out.max_val->type().id()) {
    throw duckdb::InvalidInputException(
      "RANGE bounds have mismatched types: %s vs %s",
      out.min_val->type().ToString(), out.max_val->type().ToString());
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

Result FromTSQRangeOne(irs::BooleanFilter& parent, const FilterContext& ctx,
                       const SearchColumnInfo& column_info,
                       const duckdb::BoundFunctionExpression& func,
                       std::string_view label, bool is_lower, bool inclusive) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, label, " expects 1 argument (bound), got ",
            func.children.size()};
  }
  const auto* bound_val = TryGetConstant(*func.children[0]);
  if (!bound_val) {
    return {ERROR_BAD_PARAMETER, label, " bound must be a constant"};
  }
  if (bound_val->IsNull()) {
    throw duckdb::InvalidInputException(
      "%s bound must be non-null (use RANGE(NULL, ..., ...) for unbounded "
      "semantics)",
      std::string{label});
  }

  const auto col_type = column_info.logical_type.id();
  const auto val_type = bound_val->type().id();
  auto type_mismatch = [&]() {
    throw duckdb::InvalidInputException(
      "%s bound type %s is incompatible with column type %s",
      std::string{label}, bound_val->type().ToString(),
      column_info.logical_type.ToString());
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
    throw duckdb::InvalidInputException("%s: unsupported column type %s",
                                        std::string{label},
                                        column_info.logical_type.ToString());
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  if (auto r = MangleForType(col_type, field_name); !r.ok()) {
    return r;
  }
  const auto bound_type =
    inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;

  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    // VARCHAR: tokenise the bound through the ambient analyzer; the
    // (single) token becomes the bound's bytes.
    auto text = bound_val->GetValue<std::string>();
    auto& analyzer = ctx.tokenizer;
    if (!analyzer.reset(std::string_view{text})) {
      return {ERROR_BAD_PARAMETER, label, " failed to analyse '", text, "'"};
    }
    const auto* token = irs::get<irs::TermAttr>(analyzer);
    if (!analyzer.next()) {
      // Zero tokens (e.g. all-stopword input) -> the comparison can't
      // match anything in the term dictionary.
      AddFilter<irs::Empty>(parent);
      return {};
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
      return {ERROR_BAD_PARAMETER, label,
              " produced multiple tokens; range comparison requires a "
              "single token (use RANGE for multi-component bounds)"};
    }
    return {};
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
    return {};
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
  return {};
}

// plainto_tsquery(text): tokenise via the column analyzer and AND all
Result FromRange(irs::BooleanFilter& parent, const FilterContext& ctx,
                 const SearchColumnInfo& column_info,
                 const duckdb::BoundFunctionExpression& func) {
  auto args = ParseRangeArgs(func);
  if (!args) {
    return std::move(args.error());
  }
  // Both bounds NULL -> unbounded on both sides; matches every doc.
  if (!args->min_val && !args->max_val) {
    if (ctx.negated) {
      AddFilter<irs::Empty>(parent);
    } else {
      AddFilter<irs::All>(parent).boost(ctx.boost);
    }
    return {};
  }

  const auto col_type = column_info.logical_type.id();
  const auto* val_sample = args->min_val ? args->min_val : args->max_val;
  const auto val_type = val_sample->type().id();

  // Validate value type matches column type family.
  auto type_mismatch = [&]() {
    throw duckdb::InvalidInputException(
      "RANGE bound type %s is incompatible with column type %s",
      val_sample->type().ToString(), column_info.logical_type.ToString());
  };
  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    if (val_type != duckdb::LogicalTypeId::VARCHAR) {
      type_mismatch();
    }
    if (column_info.tokenizer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
      throw duckdb::InvalidInputException(
        "RANGE on VARCHAR field requires identity-analyzed column");
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
    throw duckdb::InvalidInputException("RANGE: unsupported column type %s",
                                        column_info.logical_type.ToString());
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  if (auto r = MangleForType(col_type, field_name); !r.ok()) {
    return r;
  }

  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    auto& range = ctx.negated ? Negate<irs::ByRange>(parent)
                              : AddFilter<irs::ByRange>(parent);
    *range.mutable_field() = std::move(field_name);
    range.boost(ctx.boost);
    auto* options = range.mutable_options();
    options->scored_terms_limit = ctx.scored_terms_limit;
    FillByRangeOptionsVarchar(*args, *options);
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    auto& range = ctx.negated ? Negate<irs::ByRange>(parent)
                              : AddFilter<irs::ByRange>(parent);
    *range.mutable_field() = std::move(field_name);
    range.boost(ctx.boost);
    auto* options = range.mutable_options();
    options->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = options->range;
    if (args->min_val) {
      rng.min.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args->min_val->GetValue<bool>())));
      rng.min_type =
        args->min_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    if (args->max_val) {
      rng.max.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args->max_val->GetValue<bool>())));
      rng.max_type =
        args->max_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
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
    if (args->min_val) {
      emit_bound(*args->min_val, rng.min, rng.min_type, args->min_incl);
    }
    if (args->max_val) {
      emit_bound(*args->max_val, rng.max, rng.max_type, args->max_incl);
    }
  }
  return {};
}

}  // namespace sdb::connector
