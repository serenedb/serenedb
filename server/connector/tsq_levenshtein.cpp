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
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "catalog/mangling.h"
#include "tsq_common.hpp"

namespace sdb::connector {

ResultOr<LevenshteinArgs> ParseLevenshteinArgs(
  const duckdb::BoundFunctionExpression& func) {
  auto err = [](auto&&... args) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   std::forward<decltype(args)>(args)...};
  };
  if (func.children.empty() || func.children.size() > 3) {
    return err(
      "LEVENSHTEIN expects 1 to 3 arguments "
      "(text, distance?, transpositions?), got ",
      func.children.size());
  }
  LevenshteinArgs out;
  if (auto r = GetVarcharArg(*func.children[0], "LEVENSHTEIN text", out.text);
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (func.children.size() >= 2) {
    if (auto r =
          GetIntArg(*func.children[1], "LEVENSHTEIN distance", out.distance);
        !r.ok()) {
      return std::unexpected<Result>{std::in_place, std::move(r)};
    }
  }
  if (out.distance < 0 || out.distance > 4) {
    return err("LEVENSHTEIN distance must be between 0 and 4, got ",
               out.distance);
  }
  if (func.children.size() >= 3) {
    if (auto r = GetBoolArg(*func.children[2], "LEVENSHTEIN transpositions",
                            out.with_transpositions);
        !r.ok()) {
      return std::unexpected<Result>{std::in_place, std::move(r)};
    }
  }
  if (out.with_transpositions && out.distance > 3) {
    return err(
      "LEVENSHTEIN distance must be between 0 and 3 when "
      "transpositions is true, got ",
      out.distance);
  }
  return out;
}

void FillByEditDistanceOptions(const LevenshteinArgs& args,
                               irs::ByEditDistanceOptions& out) {
  out.term.assign(irs::ViewCast<irs::byte_type>(std::string_view{args.text}));
  out.max_distance = static_cast<uint8_t>(args.distance);
  out.with_transpositions = args.with_transpositions;
  out.max_terms = 64;
}

// Tokenises `text` via the column analyzer and pushes ByTermOptions
// parts into `options`. The FIRST token gets `base_gap` -- the gap
// from the previous phrase part. Subsequent tokens are strictly
// adjacent ({1, 1}). Errors if the analyzer produces no tokens. Shared
// between BuildFtsPhrase (called with PhraseGap{}) and EmitPhraseSeq's
Result FromLevenshtein(irs::BooleanFilter& filter, const FilterContext& ctx,
                       const SearchColumnInfo& column_info,
                       const duckdb::BoundFunctionExpression& func) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "LEVENSHTEIN field is not VARCHAR"};
  }
  if (func.children.empty() || func.children.size() > 3) {
    return {ERROR_BAD_PARAMETER,
            "LEVENSHTEIN expects 1 to 3 arguments "
            "(text, distance?, transpositions?), got ",
            func.children.size()};
  }

  auto args = ParseLevenshteinArgs(func);
  if (!args) {
    return std::move(args.error());
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& edit_filter = ctx.negated ? Negate<irs::ByEditDistance>(filter)
                                  : AddFilter<irs::ByEditDistance>(filter);
  edit_filter.boost(ctx.boost);
  *edit_filter.mutable_field() = field_name;
  auto& edit_opts = *edit_filter.mutable_options();
  FillByEditDistanceOptions(*args, edit_opts);
  edit_opts.max_terms = ctx.scored_terms_limit;
  return {};
}

}  // namespace sdb::connector
