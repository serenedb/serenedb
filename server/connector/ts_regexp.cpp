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
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "catalog/mangling.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

// magic_enum customisation for irs::RegexpSyntax: maps the surface
// names accepted by ts_regexp(pattern, syntax) ('perl', 'posix') to the
// underlying enum values. The specialisation must be visible at
// every enum_cast / enum_names<irs::RegexpSyntax> instantiation; only
// this TU calls them, so the customisation lives here.
namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<irs::RegexpSyntax>(irs::RegexpSyntax value) noexcept {
  switch (value) {
    using enum irs::RegexpSyntax;
    case Perl:
      return "perl";
    case PosixEre:
      return "posix";
    default:
      return invalid_tag;
  }
}

}  // namespace magic_enum
namespace sdb::connector {
namespace {

void BuildFtsRegexp(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    std::string_view pattern, irs::RegexpSyntax syntax) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    throw duckdb::InvalidInputException("ts_regexp field is not VARCHAR");
  }
  std::string field_name;
  MakeFieldName(column_info.column_id, field_name);
  search::mangling::MangleString(field_name);
  auto& filter = ctx.negated ? Negate<irs::ByRegexp>(parent)
                             : AddFilter<irs::ByRegexp>(parent);
  filter.boost(ctx.boost);
  *filter.mutable_field() = field_name;
  auto* opts = filter.mutable_options();
  opts->scored_terms_limit = ctx.scored_terms_limit;
  opts->pattern.assign(irs::ViewCast<irs::byte_type>(pattern));
  opts->syntax = syntax;
}

}  // namespace

void FromRegexp(irs::BooleanFilter& parent, const FilterContext& ctx,
                const SearchColumnInfo& column_info,
                const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_regexp('abc.*') or ts_regexp('foo', 'posix'). "
    "Syntax is 'perl' (default) or 'posix'.";
  if (func.children.empty() || func.children.size() > 2) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_regexp expects 1 or 2 arguments (pattern[, syntax]), got ",
              func.children.size()),
      ERR_HINT(kSyntaxHint));
  }
  std::string pattern;
  if (auto r = GetVarcharArg(*func.children[0], "ts_regexp pattern", pattern);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
  }
  auto syntax = irs::RegexpSyntax::Perl;
  if (func.children.size() == 2) {
    std::string syntax_name;
    if (auto r =
          GetVarcharArg(*func.children[1], "ts_regexp syntax", syntax_name);
        !r.ok()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
    }
    auto parsed = magic_enum::enum_cast<irs::RegexpSyntax>(
      syntax_name, magic_enum::case_insensitive);
    if (!parsed) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_regexp syntax must be one of [",
                absl::StrJoin(magic_enum::enum_names<irs::RegexpSyntax>(), ", ",
                              [](std::string* out, std::string_view name) {
                                absl::StrAppend(out, "'", name, "'");
                              }),
                "], got '", syntax_name, "'"),
        ERR_HINT(kSyntaxHint));
    }
    syntax = *parsed;
  }
  BuildFtsRegexp(parent, ctx, column_info, pattern, syntax);
}

}  // namespace sdb::connector
