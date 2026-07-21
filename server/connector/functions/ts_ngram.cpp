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
#include <iresearch/analysis/batch/token_sinks.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/ngram_similarity_query.hpp>
#include <iresearch/utils/string.hpp>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

namespace sdb::connector {

void FromNgram(irs::BooleanFilter& filter, const FilterContext& ctx,
               const SearchColumnInfo& column_info,
               const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_ngram('hello', 0.7). Threshold is 0.0-1.0 (default 0.7).";
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_ngram field is not VARCHAR"),
                    ERR_HINT(kSyntaxHint));
  }
  SDB_ASSERT(func.GetChildren().size() >= 1 && func.GetChildren().size() <= 2);

  std::string target;
  GetVarcharArg(*func.GetChildren()[0], target, {"ts_ngram text", kSyntaxHint});

  float threshold = 0.7f;
  if (func.GetChildren().size() == 2) {
    double thr;
    GetDoubleArg(*func.GetChildren()[1], thr,
                 {"ts_ngram threshold", kSyntaxHint});
    threshold = static_cast<float>(thr);
  }
  if (threshold < 0.f || threshold > 1.f) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_ngram threshold must be between 0 and 1, got ", threshold),
      ERR_HINT(kSyntaxHint));
  }

  if ((column_info.tokenizer.features &
       irs::NGramSimilarityQuery::kRequiredFeatures) !=
      irs::NGramSimilarityQuery::kRequiredFeatures) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(
        "ts_ngram field should have Positions and Frequency features enabled"),
      ERR_HINT("Recreate the inverted index with both `Positions` and "
               "`Frequency` features attached to the column."));
  }

  auto& ngram = ctx.negated ? Negate<irs::ByNGramSimilarity>(filter)
                            : AddFilter<irs::ByNGramSimilarity>(filter);
  ngram.boost(ctx.boost);
  *ngram.mutable_field_id() =
    PickPerKindFieldId(column_info, duckdb::LogicalTypeId::VARCHAR);
  ngram.mutable_options()->threshold = threshold;
  auto& analyzer = ctx.tokenizer;
  irs::TermVectorSink sink{ngram.mutable_options()->ngrams};
  analyzer.Fill(std::string_view{target}, sink.writer, irs::TokenLayout::Terms);
  sink.writer.Finish();
}

}  // namespace sdb::connector
