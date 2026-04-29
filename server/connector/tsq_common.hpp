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

// Cross-TU surface for the search_filter_builder family. The main TU
// (search_filter_builder.cpp) holds the entry point + non-TSQUERY
// dispatch; each TSQUERY query type lives in its own tsq_<name>.cpp.
// All shared types and helpers live here.

#pragma once

#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/wildcard_utils.hpp>
#include <magic_enum/magic_enum.hpp>

#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"
#include "basics/result_or.h"
#include "search_filter_builder.hpp"

namespace sdb::connector {

struct FilterContext {
  bool negated = false;
  irs::score_t boost = irs::kNoBoost;
  const ColumnGetter& column_getter;
  containers::FlatHashMap<catalog::Column::Id, SearchColumnInfo>& column_cache;
  irs::analysis::Analyzer& identity;
  irs::analysis::Analyzer& tokenizer;
  duckdb::ClientContext& client_context;
  size_t scored_terms_limit = 1024;

  FilterContext WithTokenizer(irs::analysis::Analyzer& tokenizer) const {
    return {.negated = negated,
            .boost = boost,
            .column_getter = column_getter,
            .column_cache = column_cache,
            .identity = identity,
            .tokenizer = tokenizer,
            .client_context = client_context,
            .scored_terms_limit = scored_terms_limit};
  }

  FilterContext WithBoost(irs::score_t factor) const {
    return {.negated = negated,
            .boost = boost * factor,
            .column_getter = column_getter,
            .column_cache = column_cache,
            .identity = identity,
            .tokenizer = tokenizer,
            .client_context = client_context,
            .scored_terms_limit = scored_terms_limit};
  }
};

template<typename Filter, typename Source>
auto& AddFilter(Source& parent) {
  if constexpr (std::is_same_v<Filter, irs::All>) {
    static_assert(std::is_base_of_v<irs::BooleanFilter, Source>);
    return parent.add(std::make_unique<irs::All>());
  } else {
    if constexpr (std::is_same_v<irs::Not, Source>) {
      return parent.template filter<Filter>();
    } else {
      return parent.template add<Filter>();
    }
  }
}

template<typename Filter, typename Source>
Filter& Negate(Source& parent) {
  return AddFilter<Filter>(AddFilter<irs::Not>(
    parent.type() == irs::Type<irs::Or>::id() ? AddFilter<irs::And>(parent)
                                              : parent));
}

const duckdb::Value* TryGetConstant(const duckdb::Expression& expr);

const duckdb::Expression& UnwrapTSQueryCast(const duckdb::Expression& expr);

void MakeFieldName(catalog::Column::Id column_id, std::string& field_name);
void MakeFieldName(const SearchColumnInfo& column, std::string& field_name);
Result MangleForType(duckdb::LogicalTypeId type_id, std::string& field_name);

bool IsNumericTypeId(duckdb::LogicalTypeId id);
bool IsRangeNumericValueType(duckdb::LogicalTypeId id);

Result GetVarcharArg(const duckdb::Expression& expr, std::string_view label,
                     std::string& out);
Result GetIntArg(const duckdb::Expression& expr, std::string_view label,
                 int64_t& out);
Result GetBoolArg(const duckdb::Expression& expr, std::string_view label,
                  bool& out);
Result GetDoubleArg(const duckdb::Expression& expr, std::string_view label,
                    double& out);

void ResetNumericStream(irs::NumericTokenizer& stream,
                        duckdb::LogicalTypeId type_id,
                        const duckdb::Value& value);

// TSQUERY tree walker. Per-type FromXxx entry points are forward-
// declared in search_filter_builder.cpp where the dispatch lives;
// they don't need to be on the cross-TU surface.
Result BuildTSQuery(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::Expression& expr);

// Shared FTS leaf builders (called from the dispatch in main and from
// other per-type cpps -- to_tsquery, tokenize, term).
Result BuildFtsPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view text);
Result BuildFtsTerm(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::Value& value);
Result BuildFtsTokens(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view text, bool require_all);

// Shared between RANGE constructor and phrase-seq's RANGE-as-part
// dispatch. Pointers reference constants in the bound expression tree;
// nullptr means an unbounded side (NULL).
struct RangeArgs {
  const duckdb::Value* min_val = nullptr;
  const duckdb::Value* max_val = nullptr;
  bool min_incl = false;
  bool max_incl = false;
};
ResultOr<RangeArgs> ParseRangeArgs(const duckdb::BoundFunctionExpression& func);
void FillByRangeOptionsVarchar(const RangeArgs& args, irs::ByRangeOptions& out);

// Shared between LEVENSHTEIN constructor and phrase-seq's
// LEVENSHTEIN-as-part dispatch.
struct LevenshteinArgs {
  std::string text;
  int64_t distance = 1;
  bool with_transpositions = true;
};
ResultOr<LevenshteinArgs> ParseLevenshteinArgs(
  const duckdb::BoundFunctionExpression& func);
void FillByEditDistanceOptions(const LevenshteinArgs& args,
                               irs::ByEditDistanceOptions& out);

// ANY_OF/ALL_OF arg unpacker: handles single TSQUERY, TSQUERY[]
// (extracts elements), and the optional min_should_match suffix.
// `synthesised` collects any temporary expressions the unpacker
// constructs (so their lifetime extends past return).
Result ExtractAnyAllOfArgs(
  const duckdb::BoundFunctionExpression& func, bool is_any,
  std::vector<const duckdb::Expression*>& args,
  std::vector<duckdb::unique_ptr<duckdb::Expression>>& synthesised,
  std::optional<size_t>& min_match);

// Phrase-sequence representation, shared between FromTSQueryPhraseSeq
// (the `##` operator) and tsquery_phrase
struct PhraseGap {
  size_t offs_min{0};
  size_t offs_max{0};
};

struct PhraseSeq {
  std::vector<const duckdb::Expression*> parts;
  std::vector<PhraseGap> gaps;
  std::optional<PhraseGap> pending;
};
ResultOr<PhraseGap> ParsePhraseSeqGap(const duckdb::Expression& expr);
Result FlattenPhraseSeq(const duckdb::Expression& expr, PhraseSeq& seq);
Result AttachPart(PhraseSeq& seq, const duckdb::Expression& next);
Result EmitPhraseSeq(irs::BooleanFilter& parent, const FilterContext& ctx,
                     const SearchColumnInfo& column_info, const PhraseSeq& seq);

enum class TSQueryOp {
  Unknown,
  Phrase,
  Term,
  Like,
  Prefix,
  Ngram,
  Fuzzy,
  AnyOf,
  AllOf,
  Range,
  Regexp,
  Less,
  LessEq,
  Greater,
  GreaterEq,
  Tokenize,
  PlainToTsquery,
  PhraseToTsquery,
  WebsearchToTsquery,
  TsqueryPhrase,
  Or,
  And,
  Not,
  Boost,
  PhraseSeq,
  ToTSQuery,
  Compound,
};

}  // namespace sdb::connector
