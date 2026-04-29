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
#include <iresearch/utils/string.hpp>

#include "catalog/mangling.h"
#include "tsq_common.hpp"

namespace sdb::connector {

void EmitLikeFilter(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info, std::string field_name,
                    std::string_view raw_pattern, char escape_char = '\\');

namespace {

Result BuildFtsLike(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    std::string_view like_pattern) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "LIKE field is not VARCHAR"};
  }
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  EmitLikeFilter(parent, ctx, column_info, std::move(field_name), like_pattern);
  return {};
}

}  // namespace

Result FromTSQLike(irs::BooleanFilter& parent, const FilterContext& ctx,
                   const SearchColumnInfo& column_info,
                   const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "LIKE expects 1 argument (pattern), got ",
            func.children.size()};
  }
  std::string pat;
  if (auto r = GetVarcharArg(*func.children[0], "LIKE pattern", pat); !r.ok()) {
    return r;
  }
  return BuildFtsLike(parent, ctx, column_info, pat);
}

}  // namespace sdb::connector
