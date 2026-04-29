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
#include "tsq_common.hpp"

namespace sdb::connector {
namespace {

Result BuildFtsPrefix(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view prefix) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "PREFIX field is not VARCHAR"};
  }
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  auto& filter = ctx.negated ? Negate<irs::ByPrefix>(parent)
                             : AddFilter<irs::ByPrefix>(parent);
  filter.boost(ctx.boost);
  *filter.mutable_field() = field_name;
  auto& pf_opts = *filter.mutable_options();
  pf_opts.scored_terms_limit = ctx.scored_terms_limit;
  pf_opts.term.assign(irs::ViewCast<irs::byte_type>(prefix));
  return {};
}

}  // namespace

Result FromPrefix(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "PREFIX expects 1 argument (text), got ",
            func.children.size()};
  }
  std::string prefix;
  if (auto r = GetVarcharArg(*func.children[0], "PREFIX text", prefix);
      !r.ok()) {
    return r;
  }
  return BuildFtsPrefix(parent, ctx, column_info, prefix);
}

}  // namespace sdb::connector
