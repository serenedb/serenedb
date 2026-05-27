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
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/search/wildcard_ngram_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "basics/down_cast.h"
#include "catalog/mangling.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

namespace sdb::connector {

void FromLike(irs::BooleanFilter& parent, const FilterContext& ctx,
              const SearchColumnInfo& column_info,
              const duckdb::BoundFunctionExpression& func) {
  SDB_ASSERT(func.children.size() == 1);

  std::string pattern;
  if (auto r = GetVarcharArg(*func.children[0], "ts_like pattern", pattern);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()),
                    ERR_HINT("Example: ts_like('foo%bar')."));
  }
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_like field is not VARCHAR"),
      ERR_HINT("ts_like requires a VARCHAR column with identity or wildcard "
               "analyzer."));
  }
  std::string field_name;
  MakeFieldName(column_info.field_id, field_name);
  search::mangling::MangleString(field_name);

  if (column_info.tokenizer.analyzer->type() ==
      irs::Type<irs::analysis::WildcardAnalyzer>::id()) {
    auto& wf = ctx.negated ? Negate<irs::ByWildcardNgram>(parent)
                           : AddFilter<irs::ByWildcardNgram>(parent);
    wf.boost(ctx.boost);
    *wf.mutable_field() = std::move(field_name);
    auto* opts = wf.mutable_options();
    *opts = {
      pattern,
      basics::downCast<irs::analysis::WildcardAnalyzer>(
        *column_info.tokenizer.analyzer.get()),
      (column_info.tokenizer.features & irs::IndexFeatures::Pos) ==
        irs::IndexFeatures::Pos,
    };
    SDB_ASSERT(column_info.tokenizer.tokenizer_column);
    opts->store_field_id = *column_info.tokenizer.tokenizer_column;
    return;
  }
  auto& filter = ctx.negated ? Negate<irs::ByWildcard>(parent)
                             : AddFilter<irs::ByWildcard>(parent);
  filter.boost(ctx.boost);
  *filter.mutable_field() = std::move(field_name);
  auto& wild_opts = *filter.mutable_options();
  wild_opts.scored_terms_limit = ctx.scored_terms_limit;
  wild_opts.set_term(irs::ViewCast<irs::byte_type>(std::string_view{pattern}));
}

}  // namespace sdb::connector
