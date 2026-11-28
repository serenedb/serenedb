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
#include <iresearch/search/filters/levenshtein_filter.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string.hpp>

#include "ts_common.hpp"

namespace sdb::connector {
namespace {

constexpr std::string_view kSyntaxHint =
  "Example: ts_levenshtein('test', 1, prefix := 'pre'). Distance must be 0-4 "
  "(0-3 with transpositions). `transpositions := false` counts swapping two "
  "adjacent characters as two edits, `prefix` must match exactly, and "
  "`starts_with := true` matches the terms that start within the distance "
  "of the text; `transpositions` and `prefix` may also follow the distance "
  "by position. If distance is omitted (ts_levenshtein('test')), it is "
  "picked automatically from the term length (0 for <=2 chars, 1 for 3-5, "
  "2 for >=6).";

[[noreturn]] void ThrowGivenTwice(std::string_view name) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("ts_levenshtein ", name, " is given both by position and by name"),
    ERR_HINT(kSyntaxHint));
}

}  // namespace

LevenshteinArgs ParseLevenshteinArgs(
  const duckdb::BoundFunctionExpression& func, const SearchColumnInfo& column) {
  const auto& children = func.GetChildren();
  const auto positional = PositionalArgumentCount(func);
  SDB_ASSERT(positional >= 1 && positional <= 4);
  LevenshteinArgs out;
  out.bytes = ComparesBytes(column, func);
  GetVarcharArg(*children[0], out.text, {"ts_levenshtein text", kSyntaxHint});
  if (positional >= 2) {
    GetIntArg(*children[1], out.distance,
              {"ts_levenshtein distance", kSyntaxHint});
  }
  if (positional >= 3) {
    GetBoolArg(*children[2], out.with_transpositions,
               {"ts_levenshtein transpositions", kSyntaxHint});
  }
  if (positional >= 4) {
    GetVarcharArg(*children[3], out.prefix,
                  {"ts_levenshtein prefix", kSyntaxHint});
  }
  const auto& names = func.Function().GetNamedArguments();
  for (size_t i = 0; i < names.size(); ++i) {
    const auto& alias = names[i];
    const auto& named = children[positional + i];
    if (alias == "transpositions") {
      if (positional >= 3) {
        ThrowGivenTwice("transpositions");
      }
      GetBoolArg(*named, out.with_transpositions,
                 {"ts_levenshtein transpositions", kSyntaxHint});
    } else if (alias == "prefix") {
      if (positional >= 4) {
        ThrowGivenTwice("prefix");
      }
      GetVarcharArg(*named, out.prefix, {"ts_levenshtein prefix", kSyntaxHint});
      out.bytes = out.bytes || IsBlob(*named);
    } else if (alias == "starts_with") {
      GetBoolArg(*named, out.starts_with,
                 {"ts_levenshtein starts_with", kSyntaxHint});
    } else {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_levenshtein has no argument named ",
                              alias.GetIdentifierName()),
                      ERR_HINT(kSyntaxHint));
    }
  }
  if (positional < 2) {
    // No explicit distance: pick by term length. Keeps short queries from
    // matching unrelated tokens that happen to be within 2 edits.
    const auto n = irs::LevenshteinAcceptor::Length(
      irs::ViewCast<irs::byte_type>(std::string_view{out.text}), out.bytes);
    out.distance = (n <= 2) ? 0 : (n <= 5) ? 1 : 2;
  }
  if (out.distance < 0 || out.distance > 4) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_levenshtein distance must be between 0 and 4, got ",
              out.distance),
      ERR_HINT(kSyntaxHint));
  }
  if (out.with_transpositions && out.distance > 3) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_levenshtein distance must be between 0 and 3 when "
              "transpositions is true, got ",
              out.distance),
      ERR_HINT(kSyntaxHint));
  }
  return out;
}

void FillByEditDistanceOptions(const LevenshteinArgs& args,
                               irs::ByEditDistanceOptions& out,
                               size_t max_terms) {
  out.term.assign(irs::ViewCast<irs::byte_type>(std::string_view{args.text}));
  out.prefix.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{args.prefix}));
  out.max_distance = static_cast<uint8_t>(args.distance);
  out.with_transpositions = args.with_transpositions;
  out.starts_with = args.starts_with;
  out.bytes = args.bytes;
  out.max_terms = max_terms;
}

void FromLevenshtein(BoolTarget filter, const FilterContext& ctx,
                     const SearchColumnInfo& column_info,
                     const duckdb::BoundFunctionExpression& func) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_levenshtein field is not VARCHAR"),
                    ERR_HINT(kSyntaxHint));
  }
  auto args = ParseLevenshteinArgs(func, column_info);

  auto& edit_filter =
    AddMaybeNegated<irs::ByEditDistance>(filter, ctx, column_info);
  edit_filter.SetBoost(ctx.boost);
  SetField(edit_filter, column_info);
  FillByEditDistanceOptions(
    args, *edit_filter.mutable_options(),
    column_info.levenshtein_max_terms.value_or(ctx.levenshtein_max_terms));
}

}  // namespace sdb::connector
