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
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

namespace sdb::connector {

LevenshteinArgs ParseLevenshteinArgs(
  const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_levenshtein('test', 1, true, 'pre'). Distance must be 0-4 "
    "(0-3 with transpositions). Optional `prefix` matches exactly. "
    "If distance is omitted (ts_levenshtein('test')), it is picked "
    "automatically from the term length (0 for <=2 chars, 1 for 3-5, "
    "2 for >=6).";
  SDB_ASSERT(func.children.size() >= 1 && func.children.size() <= 4);
  LevenshteinArgs out;
  if (auto r =
        GetVarcharArg(*func.children[0], "ts_levenshtein text", out.text);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
  }
  if (func.children.size() >= 2) {
    if (auto r =
          GetIntArg(*func.children[1], "ts_levenshtein distance", out.distance);
        !r.ok()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
    }
  } else {
    // No explicit distance: pick by term length. Keeps short queries from
    // matching unrelated tokens that happen to be within 2 edits.
    const auto n = out.text.size();
    out.distance = (n <= 2) ? 0 : (n <= 5) ? 1 : 2;
  }
  if (out.distance < 0 || out.distance > 4) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_levenshtein distance must be between 0 and 4, got ",
              out.distance),
      ERR_HINT(kSyntaxHint));
  }
  if (func.children.size() >= 3) {
    if (auto r = GetBoolArg(*func.children[2], "ts_levenshtein transpositions",
                            out.with_transpositions);
        !r.ok()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
    }
  }
  if (out.with_transpositions && out.distance > 3) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_levenshtein distance must be between 0 and 3 when "
              "transpositions is true, got ",
              out.distance),
      ERR_HINT(kSyntaxHint));
  }
  if (func.children.size() >= 4) {
    if (auto r =
          GetVarcharArg(*func.children[3], "ts_levenshtein prefix", out.prefix);
        !r.ok()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
    }
  }
  return out;
}

void FillByEditDistanceOptions(const LevenshteinArgs& args,
                               irs::ByEditDistanceOptions& out) {
  out.set_term(irs::ViewCast<irs::byte_type>(std::string_view{args.text}));
  out.set_prefix(irs::ViewCast<irs::byte_type>(std::string_view{args.prefix}));
  out.max_distance = static_cast<uint8_t>(args.distance);
  out.with_transpositions = args.with_transpositions;
  out.max_terms = 64;
}

// Tokenises `text` via the column analyzer and pushes ByTermOptions
// parts into `options`. The FIRST token gets `base_gap` -- the gap
// from the previous phrase part. Subsequent tokens are strictly
// adjacent ({1, 1}). Errors if the analyzer produces no tokens. Shared
// between BuildFtsPhrase (called with PhraseGap{}) and EmitPhraseSeq's
void FromLevenshtein(irs::BooleanFilter& filter, const FilterContext& ctx,
                     const SearchColumnInfo& column_info,
                     const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_levenshtein('test', 1, true, 'pre'). Distance must be 0-4 "
    "(0-3 with transpositions). Optional `prefix` matches exactly. "
    "If distance is omitted (ts_levenshtein('test')), it is picked "
    "automatically from the term length (0 for <=2 chars, 1 for 3-5, "
    "2 for >=6).";
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_levenshtein field is not VARCHAR"),
                    ERR_HINT(kSyntaxHint));
  }
  auto args = ParseLevenshteinArgs(func);

  std::string field_name;
  MakeFieldName(column_info.field_id, field_name);
  search::mangling::MangleString(field_name);

  auto& edit_filter = ctx.negated ? Negate<irs::ByEditDistance>(filter)
                                  : AddFilter<irs::ByEditDistance>(filter);
  edit_filter.boost(ctx.boost);
  *edit_filter.mutable_field() = field_name;
  auto& edit_opts = *edit_filter.mutable_options();
  FillByEditDistanceOptions(args, edit_opts);
  edit_opts.max_terms = ctx.scored_terms_limit;
}

}  // namespace sdb::connector
