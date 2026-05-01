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
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "catalog/mangling.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

namespace sdb::connector {
namespace {

void BuildFtsPrefix(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    std::string_view prefix) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_starts_with field is not VARCHAR"),
                    ERR_HINT("Example: ts_starts_with('pre'). ts_starts_with "
                             "requires a VARCHAR column."));
  }
  std::string field_name;
  MakeFieldName(column_info.column_id, field_name);
  search::mangling::MangleString(field_name);
  auto& filter = ctx.negated ? Negate<irs::ByPrefix>(parent)
                             : AddFilter<irs::ByPrefix>(parent);
  filter.boost(ctx.boost);
  *filter.mutable_field() = field_name;
  auto& pf_opts = *filter.mutable_options();
  pf_opts.scored_terms_limit = ctx.scored_terms_limit;
  pf_opts.term.assign(irs::ViewCast<irs::byte_type>(prefix));
}

}  // namespace

void FromPrefix(irs::BooleanFilter& parent, const FilterContext& ctx,
                const SearchColumnInfo& column_info,
                const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_starts_with expects 1 argument (text), got ",
              func.children.size()),
      ERR_HINT("Example: ts_starts_with('pre'). For mid-string wildcards use "
               "ts_like('foo%bar')."));
  }
  std::string prefix;
  if (auto r = GetVarcharArg(*func.children[0], "ts_starts_with text", prefix);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()),
                    ERR_HINT("Example: ts_starts_with('pre')."));
  }
  BuildFtsPrefix(parent, ctx, column_info, prefix);
}

Result FromFuncPrefix(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, "prefix expects 2 arguments, got ",
            func.children.size()};
  }
  const auto* column_ref = TryGetColumnRef(*func.children[0]);
  if (!column_ref) {
    return {ERROR_BAD_PARAMETER, "prefix input is not a column reference"};
  }
  const auto* literal_val = TryGetConstant(*func.children[1]);
  if (!literal_val ||
      literal_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "prefix literal must be a VARCHAR constant"};
  }
  const auto* column_info = FindColumnInfo(ctx, *column_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column is not indexed"};
  }
  if (column_info->tokenizer.analyzer->type() !=
      irs::Type<irs::StringTokenizer>::id()) {
    return {ERROR_BAD_PARAMETER,
            "prefix requires an identity-analyzed column. Use "
            "`col @@ ts_starts_with('pre')` for non-identity analyzers."};
  }
  BuildFtsPrefix(filter, ctx, *column_info,
                 literal_val->GetValue<std::string>());
  return {};
}

}  // namespace sdb::connector
