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
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/ngram_similarity_query.hpp>
#include <iresearch/utils/string.hpp>

#include "catalog/mangling.h"
#include "tsq_common.hpp"

namespace sdb::connector {

Result FromNgram(irs::BooleanFilter& filter, const FilterContext& ctx,
                 const SearchColumnInfo& column_info,
                 const duckdb::BoundFunctionExpression& func) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "NGRAM field is not VARCHAR"};
  }
  if (func.children.empty() || func.children.size() > 2) {
    return {ERROR_BAD_PARAMETER,
            "NGRAM expects 1 or 2 arguments (text[, threshold]), got ",
            func.children.size()};
  }

  std::string target;
  if (auto r = GetVarcharArg(*func.children[0], "NGRAM text", target);
      !r.ok()) {
    return r;
  }

  float threshold = 0.7f;
  if (func.children.size() == 2) {
    double thr;
    if (auto r = GetDoubleArg(*func.children[1], "NGRAM threshold", thr);
        !r.ok()) {
      return r;
    }
    threshold = static_cast<float>(thr);
  }
  if (threshold < 0.f || threshold > 1.f) {
    return {ERROR_BAD_PARAMETER, "NGRAM threshold must be between 0 and 1"};
  }

  if ((column_info.tokenizer.features &
       irs::NGramSimilarityQuery::kRequiredFeatures) !=
      irs::NGramSimilarityQuery::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "NGRAM field should have Positions and Frequency features "
            "enabled"};
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& ngram = ctx.negated ? Negate<irs::ByNGramSimilarity>(filter)
                            : AddFilter<irs::ByNGramSimilarity>(filter);
  ngram.boost(ctx.boost);
  *ngram.mutable_field() = field_name;
  ngram.mutable_options()->threshold = threshold;
  auto& analyzer = ctx.tokenizer;
  analyzer.reset(std::string_view{target});
  const irs::TermAttr* token = irs::get<irs::TermAttr>(analyzer);
  while (analyzer.next()) {
    ngram.mutable_options()->ngrams.emplace_back(token->value);
  }
  return {};
}

}  // namespace sdb::connector
