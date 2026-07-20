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

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

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

void FromRegexp(irs::BooleanFilter& parent, const FilterContext& ctx,
                const SearchColumnInfo& column_info,
                const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_regexp('abc.*') or ts_regexp('foo', 'posix'). "
    "Syntax is 'perl' (default) or 'posix'.";
  SDB_ASSERT(func.GetChildren().size() >= 1 && func.GetChildren().size() <= 2);
  std::string pattern;
  GetVarcharArg(*func.GetChildren()[0], pattern,
                {"ts_regexp pattern", kSyntaxHint});
  auto syntax = irs::RegexpSyntax::Perl;
  if (func.GetChildren().size() == 2) {
    std::string syntax_name;
    GetVarcharArg(*func.GetChildren()[1], syntax_name,
                  {"ts_regexp syntax", kSyntaxHint});
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
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                    ERR_MSG("ts_regexp field is not VARCHAR"));
  }
  auto regexp = irs::CreateByRegexp(
    PickPerKindFieldId(column_info, duckdb::LogicalTypeId::VARCHAR),
    irs::ViewCast<irs::byte_type>(std::string_view{pattern}), syntax,
    ctx.scored_terms_limit, ctx.boost);
  if (!ctx.negated) {
    parent.add(std::move(regexp));
    return;
  }
  AddNegated(parent, column_info, std::move(regexp));
}

}  // namespace sdb::connector
