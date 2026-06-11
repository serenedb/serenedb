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

#pragma once

#include <duckdb/planner/column_binding_map.hpp>
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

#include "basics/containers/node_hash_map.h"
#include "basics/result.h"
#include "catalog/tokenizer.h"
#include "connector/search_filter_builder.hpp"

namespace sdb::catalog {

struct Snapshot;

}  // namespace sdb::catalog
namespace sdb::connector {

struct FilterContext {
  bool negated = false;
  irs::score_t boost = irs::kNoBoost;
  const ColumnGetter& column_getter;
  const ExpressionGetter* expr_getter = nullptr;
  duckdb::column_binding_map_t<SearchColumnInfo>& column_cache;
  containers::NodeHashMap<irs::field_id, SearchColumnInfo>& expr_cache;
  irs::analysis::Analyzer& identity;
  irs::analysis::Analyzer& tokenizer;
  duckdb::ClientContext& client_context;
  size_t scored_terms_limit = 1024;

  FilterContext WithTokenizer(irs::analysis::Analyzer& tokenizer) const {
    return {
      .negated = negated,
      .boost = boost,
      .column_getter = column_getter,
      .expr_getter = expr_getter,
      .column_cache = column_cache,
      .expr_cache = expr_cache,
      .identity = identity,
      .tokenizer = tokenizer,
      .client_context = client_context,
      .scored_terms_limit = scored_terms_limit,
    };
  }

  FilterContext WithBoost(irs::score_t factor) const {
    return {
      .negated = negated,
      .boost = boost * factor,
      .column_getter = column_getter,
      .expr_getter = expr_getter,
      .column_cache = column_cache,
      .expr_cache = expr_cache,
      .identity = identity,
      .tokenizer = tokenizer,
      .client_context = client_context,
      .scored_terms_limit = scored_terms_limit,
    };
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

Result ValidateFilterType(duckdb::LogicalTypeId type_id);

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

// All throw THROW_SQL_ERROR on any failure.
void BuildTSQuery(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::Expression& expr);

void BuildFtsPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info, std::string_view text);
void BuildFtsTerm(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::Value& value);
void BuildFtsTokens(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info, std::string_view text,
                    bool require_all);

const SearchColumnInfo* FindColumnInfoForExpr(const FilterContext& ctx,
                                              const duckdb::Expression& expr);

// Pointers reference constants in the bound expression tree;
// nullptr means an unbounded side (NULL).
struct RangeArgs {
  const duckdb::Value* min = nullptr;
  const duckdb::Value* max = nullptr;
  bool min_incl = false;
  bool max_incl = false;
};
// All Parse*/Fill*/Extract*/Flatten*/Attach*/Emit* helpers below throw
// THROW_SQL_ERROR on any failure -- callers don't need to wrap.
RangeArgs ParseRangeArgs(const duckdb::BoundFunctionExpression& func);
void FillByRangeOptionsVarchar(const RangeArgs& args, irs::ByRangeOptions& out);

// ts_levenshtein-as-part dispatch.
struct LevenshteinArgs {
  std::string text;
  int64_t distance = 1;
  bool with_transpositions = true;
  // Literal prefix that must match exactly; only the suffix beyond it
  // participates in edit-distance computation. Empty by default.
  std::string prefix;
};
LevenshteinArgs ParseLevenshteinArgs(
  const duckdb::BoundFunctionExpression& func);
void FillByEditDistanceOptions(const LevenshteinArgs& args,
                               irs::ByEditDistanceOptions& out);

// ts_any/ts_all arg unpacker: handles single TSQUERY, TSQUERY[]
// (extracts elements), and the optional min_should_match suffix.
// `synthesised` collects any temporary expressions the unpacker
// constructs (so their lifetime extends past return).
void ExtractAnyAllOfArgs(
  const duckdb::BoundFunctionExpression& func, bool is_any,
  std::vector<const duckdb::Expression*>& args,
  std::vector<duckdb::unique_ptr<duckdb::Expression>>& synthesised,
  std::optional<size_t>& min_match);

// Phrase-sequence representation, shared between FromTSQueryPhraseSeq
// (the `##` operator) and tsquery_phrase
struct PhraseGap {
  size_t min = 0;
  size_t max = 0;
};

struct PhraseSeq {
  std::vector<const duckdb::Expression*> parts;
  std::vector<PhraseGap> gaps;
  std::optional<PhraseGap> pending;
};
PhraseGap ParsePhraseSeqGap(const duckdb::Expression& expr);
void FlattenPhraseSeq(const duckdb::Expression& expr, PhraseSeq& seq);
void AttachPart(PhraseSeq& seq, const duckdb::Expression& next);
void EmitPhraseSeq(irs::BooleanFilter& parent, const FilterContext& ctx,
                   const SearchColumnInfo& column_info, const PhraseSeq& seq);

enum class TSQueryOp {
  Unknown,
  Phrase,
  Term,
  Like,
  Prefix,
  Ngram,
  Fuzzy,
  Any,
  All,
  Between,
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
  IsNull,
  IsNotNull,
};

}  // namespace sdb::connector
