////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "search_filter_builder.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_join.h>
#include <iresearch/parser/parser.h>

#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/mixed_boolean_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/ngram_similarity_query.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/phrase_query.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/search/wildcard_ngram_filter.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/wildcard_utils.hpp>
#include <magic_enum/magic_enum.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/result_or.h"
#include "basics/string_utils.h"
#include "catalog/mangling.h"
#include "functions/search.h"
#include "functions/string.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb_filter.hpp"

namespace sdb::connector {
namespace {

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
};

}  // namespace
}  // namespace sdb::connector
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

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::connector::TSQueryOp>(
  sdb::connector::TSQueryOp value) noexcept {
  using enum sdb::connector::TSQueryOp;
  switch (value) {
    case Phrase:
      return sdb::connector::kTSQPhrase;
    case Like:
      return sdb::connector::kTSQLike;
    case Prefix:
      return sdb::connector::kTSQPrefix;
    case Ngram:
      return sdb::connector::kTSQNgram;
    case Fuzzy:
      return sdb::connector::kTSQLevenshtein;
    case AnyOf:
      return sdb::connector::kTSQAnyOf;
    case AllOf:
      return sdb::connector::kTSQAllOf;
    case Range:
      return sdb::connector::kTSQRange;
    case Regexp:
      return sdb::connector::kTSQRegexp;
    case Less:
      return sdb::connector::kTSQLess;
    case LessEq:
      return sdb::connector::kTSQLessEq;
    case Greater:
      return sdb::connector::kTSQGreater;
    case GreaterEq:
      return sdb::connector::kTSQGreaterEq;
    case Tokenize:
      return sdb::connector::kTSQTokenize;
    case PlainToTsquery:
      return sdb::connector::kPlainToTsquery;
    case PhraseToTsquery:
      return sdb::connector::kPhraseToTsquery;
    case WebsearchToTsquery:
      return sdb::connector::kWebsearchToTsquery;
    case TsqueryPhrase:
      return sdb::connector::kTsqueryPhrase;
    case Or:
      return sdb::connector::kTSQueryOr;
    case And:
      return sdb::connector::kTSQueryAnd;
    case Not:
      return sdb::connector::kTSQueryNot;
    case Boost:
      return sdb::connector::kTSQueryBoost;
    case PhraseSeq:
      return sdb::connector::kTSQueryPhraseSeq;
    case ToTSQuery:
      return sdb::connector::kToTsquery;
    case Unknown:
    case Term:
      return invalid_tag;
  }
  return invalid_tag;
}

}  // namespace magic_enum
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
    return {
      .negated = negated,
      .boost = boost,
      .column_getter = column_getter,
      .column_cache = column_cache,
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
      .column_cache = column_cache,
      .identity = identity,
      .tokenizer = tokenizer,
      .client_context = client_context,
      .scored_terms_limit = scored_terms_limit,
    };
  }
};

Result FromExpression(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& expr);

namespace {

const duckdb::BoundColumnRefExpression* TryGetColumnRef(
  const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundColumnRefExpression>();
}

const duckdb::Value* TryGetConstant(const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundConstantExpression>().value;
}

const SearchColumnInfo* FindColumnInfo(
  const FilterContext& ctx, const duckdb::BoundColumnRefExpression& ref) {
  // Try cache first -- keyed on column_id from a previous resolution.
  // We do a two-step lookup: first resolve via column_getter to get the
  // column_id, then check cache.  The column_getter itself may be cheap
  // (just a span lookup), but caching avoids repeated analyzer copies.

  // We cannot cache by binding alone (table_index + column_index) because
  // different bindings may map to the same catalog column_id.  Instead
  // we resolve first, then cache by column_id.

  auto info = ctx.column_getter(ref);
  if (!info) {
    return nullptr;
  }

  auto cache_it = ctx.column_cache.find(info->column_id);
  if (cache_it != ctx.column_cache.end()) {
    SDB_ASSERT(cache_it->second.logical_type.id() !=
                 duckdb::LogicalTypeId::VARCHAR ||
               cache_it->second.tokenizer.analyzer);
    return &cache_it->second;
  }

  auto column_id = info->column_id;
  return &ctx.column_cache.emplace(column_id, std::move(info.value()))
            .first->second;
}

}  // namespace

void MakeFieldName(catalog::Column::Id column_id, std::string& field_name) {
  basics::StrResize(field_name, sizeof(column_id));
  absl::big_endian::Store(field_name.data(), column_id);
}

void MakeFieldName(const SearchColumnInfo& column, std::string& field_name) {
  MakeFieldName(column.column_id, field_name);
}

namespace {

Result MangleForType(duckdb::LogicalTypeId type_id, std::string& field_name) {
  switch (type_id) {
    case duckdb::LogicalTypeId::VARCHAR:
      search::mangling::MangleString(field_name);
      return {};
    case duckdb::LogicalTypeId::BOOLEAN:
      search::mangling::MangleBool(field_name);
      return {};
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DATE:
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      search::mangling::MangleNumeric(field_name);
      return {};
    default:
      return {ERROR_NOT_IMPLEMENTED, "Unsupported type id ",
              static_cast<int>(type_id), " for field mangling"};
  }
}

void ResetNumericStream(irs::NumericTokenizer& stream,
                        duckdb::LogicalTypeId type_id,
                        const duckdb::Value& value) {
  switch (type_id) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::DATE:
      stream.reset(value.GetValue<int32_t>());
      break;
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      stream.reset(value.GetValue<int64_t>());
      break;
    case duckdb::LogicalTypeId::FLOAT:
      stream.reset(value.GetValue<float>());
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      stream.reset(value.GetValue<double>());
      break;
    default:
      SDB_ASSERT(false, "ResetNumericStream called with non-numeric type");
  }
}

bool IsNumericTypeId(duckdb::LogicalTypeId id) {
  switch (id) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DATE:
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return true;
    default:
      return false;
  }
}

// Looser numeric check used by RANGE / LESS / LESS_EQ / GREATER /
// GREATER_EQ bound validation: accepts the same set as
// IsNumericTypeId plus DECIMAL. The TSQUERY range constructors cast
// bound values to the column's logical type before tokenising, so
// DECIMAL bounds on a DOUBLE/INT/BIGINT column work as expected. We
// don't fold DECIMAL into IsNumericTypeId itself because the legacy
// FromComparison / SetupTermFilter paths feed `type_id` directly into
// ResetNumericStream, which doesn't handle DECIMAL.
bool IsRangeNumericValueType(duckdb::LogicalTypeId id) {
  return IsNumericTypeId(id) || id == duckdb::LogicalTypeId::DECIMAL;
}

// Returns the raw byte content of a Value whose physical type is
// VARCHAR (covers both LogicalType::VARCHAR and LogicalType::BLOB) as
// an irs::bytes_view ready for term-dictionary use. Use this instead
// of GetValue<std::string>() at sites where the constant may arrive
// as BLOB: DuckDB's regex_range_filter optimizer rewrites
// regexp_full_match(col, pat) into
//   col >= BLOB_RAW(min) AND col <= BLOB_RAW(max)
// so the comparison constant against a VARCHAR column may be BLOB
// even though the column is VARCHAR. GetValue<std::string>() on a
// BLOB returns the textual display form (e.g. "\xF4\xBF\xBF\xC0" as
// 24 ASCII chars), which is wrong as a byte-wise term-dictionary
// bound. StringValue::Get returns the raw stored bytes for both
// types because they share PhysicalType::VARCHAR.
irs::bytes_view AsRawBytes(const duckdb::Value& value) {
  return irs::ViewCast<irs::byte_type>(
    std::string_view{duckdb::StringValue::Get(value)});
}

// Forward declaration: definition is below alongside the leaf-builder
// helpers it shares with the phrase-part path.
void EmitLikeFilter(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info, std::string field_name,
                    std::string_view raw_pattern, char escape_char = '\\');

// Sets up a ByTerm filter for a single constant value against a column.
Result SetupTermFilter(irs::ByTerm& filter, std::string& field_name,
                       const SearchColumnInfo& column_info,
                       const duckdb::Value& value) {
  SDB_ASSERT(!value.IsNull(),
             "UNKNOWN and Nulls should be handled as part of IS NULL operator. "
             "For regular filter it should be just irs::Empty!");

  auto type_id = column_info.logical_type.id();

  if (auto r = MangleForType(type_id, field_name); !r.ok()) {
    return r;
  }

  if (type_id == duckdb::LogicalTypeId::VARCHAR) {
    filter.mutable_options()->term.assign(AsRawBytes(value));
  } else if (type_id == duckdb::LogicalTypeId::BOOLEAN) {
    filter.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(
      irs::BooleanTokenizer::value(value.GetValue<bool>())));
  } else if (IsNumericTypeId(type_id)) {
    irs::NumericTokenizer stream;
    const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
    ResetNumericStream(stream, type_id, value);
    stream.next();
    filter.mutable_options()->term.assign(token->value);
  } else {
    return {ERROR_NOT_IMPLEMENTED,
            "Unsupported type for term filter: ", static_cast<int>(type_id)};
  }

  *filter.mutable_field() = field_name;
  return {};
}

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

ComparisonOp GetComparisonOp(duckdb::ExpressionType type) {
  switch (type) {
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return ComparisonOp::Le;
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      return ComparisonOp::Lt;
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      return ComparisonOp::Ge;
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return ComparisonOp::Gt;
    default:
      return ComparisonOp::None;
  }
}

ComparisonOp InvertComparisonOp(ComparisonOp op) {
  switch (op) {
    case ComparisonOp::Le:
      return ComparisonOp::Gt;
    case ComparisonOp::Ge:
      return ComparisonOp::Lt;
    case ComparisonOp::Gt:
      return ComparisonOp::Le;
    case ComparisonOp::Lt:
      return ComparisonOp::Ge;
    case ComparisonOp::None:
      return ComparisonOp::None;
  }
  SDB_UNREACHABLE();
}

bool IsComparisonExpr(const duckdb::Expression& expr) {
  return expr.expression_class == duckdb::ExpressionClass::BOUND_COMPARISON &&
         GetComparisonOp(expr.type) != ComparisonOp::None;
}

// Unwraps reinterpret casts between VARCHAR / TSQUERY / TOKENIZED_TSQUERY
// so the filter builder sees through both directions of implicit
// promotion:
//   - VARCHAR -> TSQUERY  (bare string literals into TSQUERY contexts)
//   - TSQUERY -> VARCHAR  (TSQUERY-typed children flowing into VARCHAR
//     mirror overloads of `##`, e.g. PHRASE('a') ## 1, where DuckDB
//     wraps the LHS in BOUND_CAST<VARCHAR>)
//   - TOK <-> TSQ / VARCHAR transit casts that DON'T carry a tokenize
//     modifier on the cast's return_type. Modifier-bearing casts are
//     preserved here so the BuildTSQuery walker can read the override
//     before continuing to dispatch the inner expression.
// Iterative because casts can chain (e.g. PHRASE('x')::tokenize('y')
// inside @@ becomes BoundCast<TSQ>(BoundCast<TOK-mod-y>(PHRASE)) -- we
// peel the outer transit cast and stop at the modifier-bearing cast).
const duckdb::Expression& UnwrapTSQueryCast(const duckdb::Expression& expr) {
  const duckdb::Expression* cur = &expr;
  while (cur->expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast = cur->Cast<duckdb::BoundCastExpression>();
    if (!cast.child) {
      break;
    }
    const auto& target = cast.return_type;
    const auto& source = cast.child->return_type;
    // Stop at modifier-bearing casts; the walker needs to see them.
    if (!TryGetTokenizerModifier(target).name.empty() ||
        TryGetBoostModifier(target).factor) {
      break;
    }
    // Peel only transit casts within the {VARCHAR, TSQUERY,
    // TOKENIZED_TSQUERY} family: both sides VARCHAR-backed, with at
    // least one carrying a TSQUERY alias (otherwise it's a plain
    // VARCHAR->VARCHAR cast we shouldn't strip).
    if (target.id() != duckdb::LogicalTypeId::VARCHAR ||
        source.id() != duckdb::LogicalTypeId::VARCHAR) {
      break;
    }
    if (!IsAnyTSQueryType(target) && !IsAnyTSQueryType(source)) {
      break;
    }
    cur = cast.child.get();
  }
  return *cur;
}

Result GetVarcharArg(const duckdb::Expression& expr, std::string_view label,
                     std::string& out) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null VARCHAR constant"};
  }
  if (val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, label, " must be a VARCHAR constant"};
  }
  out = val->GetValue<std::string>();
  return {};
}

Result GetIntArg(const duckdb::Expression& expr, std::string_view label,
                 int64_t& out) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null INTEGER constant"};
  }
  switch (val->type().id()) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
      out = val->GetValue<int64_t>();
      return {};
    default:
      return {ERROR_BAD_PARAMETER, label, " must be an INTEGER constant"};
  }
}

Result GetBoolArg(const duckdb::Expression& expr, std::string_view label,
                  bool& out) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null BOOLEAN constant"};
  }
  if (val->type().id() != duckdb::LogicalTypeId::BOOLEAN) {
    return {ERROR_BAD_PARAMETER, label, " must be a BOOLEAN constant"};
  }
  out = val->GetValue<bool>();
  return {};
}

Result GetDoubleArg(const duckdb::Expression& expr, std::string_view label,
                    double& out) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null numeric constant"};
  }
  switch (val->type().id()) {
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DECIMAL:
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
      out = val->GetValue<double>();
      return {};
    default:
      return {ERROR_BAD_PARAMETER, label, " must be a numeric constant"};
  }
}

template<typename Filter>
Result MakeGroup(irs::BooleanFilter& parent, const FilterContext& ctx,
                 const duckdb::BoundConjunctionExpression& conj) {
  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  irs::BooleanFilter* group_root;
  if (ctx.negated && absl::c_all_of(conj.children, [](const auto& child) {
        SDB_ASSERT(child);
        return IsComparisonExpr(*child);
      })) {
    // De Morgan's law: if we negate a group of comparisons, comparisons
    // consume negation by inversion so we can reduce NOT filters.
    group_root =
      irs::Type<Filter>::id() == irs::Type<irs::And>::id()
        ? static_cast<irs::BooleanFilter*>(&AddFilter<irs::Or>(parent))
        : static_cast<irs::BooleanFilter*>(&AddFilter<irs::And>(parent));
  } else {
    group_root =
      ctx.negated
        ? static_cast<irs::BooleanFilter*>(&Negate<Filter>(parent))
        : static_cast<irs::BooleanFilter*>(&AddFilter<Filter>(parent));
    sub_ctx.negated = false;
  }
  group_root->boost(ctx.boost);
  for (const auto& child : conj.children) {
    auto result = FromExpression(*group_root, sub_ctx, *child);
    if (!result.ok()) {
      return result;
    }
  }
  return {};
}

Result FromIsNull(irs::BooleanFilter& filter, const FilterContext& ctx,
                  const duckdb::BoundOperatorExpression& op_expr) {
  if (op_expr.children.size() != 1) {
    return {ERROR_NOT_IMPLEMENTED, "IS NULL has ", op_expr.children.size(),
            " inputs but 1 expected"};
  }
  const auto* column_ref = TryGetColumnRef(*op_expr.children[0]);
  if (!column_ref) {
    return {ERROR_BAD_PARAMETER, "Input is not a column reference"};
  }

  const auto* column_info = FindColumnInfo(ctx, *column_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column was not found"};
  }
  std::string field_name;
  MakeFieldName(*column_info, field_name);
  search::mangling::MangleNull(field_name);
  auto& term_filter =
    ctx.negated ? Negate<irs::ByTerm>(filter) : AddFilter<irs::ByTerm>(filter);
  term_filter.boost(ctx.boost);
  *term_filter.mutable_field() = field_name;
  term_filter.mutable_options()->term.assign(
    irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  return {};
}

template<bool GenericVersion>
Result FromBinaryEq(irs::BooleanFilter& filter, const FilterContext& ctx,
                    const duckdb::Expression& left_expr,
                    const duckdb::Expression& right_expr, bool not_equal) {
  const auto* column_ref = TryGetColumnRef(left_expr);
  const auto* const_val = TryGetConstant(right_expr);

  if (!column_ref || !const_val) {
    return {ERROR_BAD_PARAMETER,
            "Expected column reference on the left and constant on the right"};
  }

  if (const_val->IsNull()) {
    // foo == NULL is always false and foo != NULL is false too.
    AddFilter<irs::Empty>(filter);
    return {};
  }

  const auto* column_info = FindColumnInfo(ctx, *column_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column was not found"};
  }
  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
        column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Use `col @@ "
              "'value'` (tokenised) or `col @@ "
              "'value'::tokenize('identity')` (raw)."};
    }
  }

  auto& term_filter = (ctx.negated != not_equal)
                        ? Negate<irs::ByTerm>(filter)
                        : AddFilter<irs::ByTerm>(filter);

  term_filter.boost(ctx.boost);
  std::string field_name;
  MakeFieldName(*column_info, field_name);
  return SetupTermFilter(term_filter, field_name, *column_info, *const_val);
}

template<bool GenericVersion>
Result FromComparison(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& field_expr,
                      const duckdb::Expression& value_expr, ComparisonOp op) {
  if (ctx.negated) {
    op = InvertComparisonOp(op);
  }

  const auto* column_ref = TryGetColumnRef(field_expr);
  const auto* const_val = TryGetConstant(value_expr);

  if (!column_ref || !const_val) {
    return {ERROR_BAD_PARAMETER,
            "Expected column reference and constant for comparison"};
  }

  if (const_val->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return {};
  }

  const auto* column_info = FindColumnInfo(ctx, *column_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column was not found"};
  }
  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
        column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Range predicates "
              "(<, <=, >, >=, BETWEEN) require an identity-analyzed column. "
              "Use `col @@ LESS('value')` / `LESS_EQ` / `GREATER` / "
              "`GREATER_EQ` / `RANGE(min, max, ...)` (tokenised through the "
              "column's analyzer) instead."};
    }
  }

  std::string field_name;
  MakeFieldName(*column_info, field_name);

  auto type_id = column_info->logical_type.id();

  auto setup_base_filter = [&](auto& range_filter,
                               std::string&& fn) -> decltype(auto) {
    *range_filter.mutable_field() = std::move(fn);
    range_filter.boost(ctx.boost);
    switch (op) {
      case ComparisonOp::Lt:
        range_filter.mutable_options()->range.max_type =
          irs::BoundType::Exclusive;
        return (range_filter.mutable_options()->range.max);
      case ComparisonOp::Le:
        range_filter.mutable_options()->range.max_type =
          irs::BoundType::Inclusive;
        return (range_filter.mutable_options()->range.max);
      case ComparisonOp::Gt:
        range_filter.mutable_options()->range.min_type =
          irs::BoundType::Exclusive;
        return (range_filter.mutable_options()->range.min);
      case ComparisonOp::Ge:
        range_filter.mutable_options()->range.min_type =
          irs::BoundType::Inclusive;
        return (range_filter.mutable_options()->range.min);
      default:
        SDB_ASSERT(false, "Not all comparison operations implemented");
    }
    SDB_UNREACHABLE();
  };

  if (auto r = MangleForType(type_id, field_name); !r.ok()) {
    return r;
  }

  if (type_id == duckdb::LogicalTypeId::VARCHAR) {
    auto& range_filter = AddFilter<irs::ByRange>(filter);
    range_filter.mutable_options()->scored_terms_limit = ctx.scored_terms_limit;
    setup_base_filter(range_filter, std::move(field_name))
      .assign(AsRawBytes(*const_val));
  } else if (type_id == duckdb::LogicalTypeId::BOOLEAN) {
    auto& range_filter = AddFilter<irs::ByRange>(filter);
    range_filter.mutable_options()->scored_terms_limit = ctx.scored_terms_limit;
    setup_base_filter(range_filter, std::move(field_name))
      .assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(const_val->GetValue<bool>())));
  } else if (IsNumericTypeId(type_id)) {
    auto& range_filter = AddFilter<irs::ByGranularRange>(filter);
    range_filter.mutable_options()->scored_terms_limit = ctx.scored_terms_limit;
    irs::NumericTokenizer stream;
    ResetNumericStream(stream, type_id, *const_val);
    irs::SetGranularTerm(setup_base_filter(range_filter, std::move(field_name)),
                         stream);
  } else {
    return {ERROR_NOT_IMPLEMENTED, "Unsupported type for range comparison: ",
            static_cast<int>(type_id)};
  }
  return {};
}

Result FromBetween(irs::BooleanFilter& filter, const FilterContext& ctx,
                   const duckdb::BoundBetweenExpression& between) {
  // Decompose BETWEEN into conjunction of two range comparisons.
  // BETWEEN a AND b  =>  field >= a (or >) AND field <= b (or <)
  // NOT BETWEEN       =>  field < a (or <=) OR field > b (or >=)

  const auto* column_ref = TryGetColumnRef(*between.input);
  if (!column_ref) {
    return {ERROR_BAD_PARAMETER, "BETWEEN input is not a column reference"};
  }
  const auto* lower_val = TryGetConstant(*between.lower);
  const auto* upper_val = TryGetConstant(*between.upper);
  if (!lower_val || !upper_val) {
    return {ERROR_BAD_PARAMETER, "BETWEEN bounds must be constants"};
  }

  if (!ctx.negated) {
    // field >= lower AND field <= upper (with inclusivity flags)
    auto lower = between.lower_inclusive ? ComparisonOp::Ge : ComparisonOp::Gt;
    auto upper = between.upper_inclusive ? ComparisonOp::Le : ComparisonOp::Lt;

    auto& group = AddFilter<irs::And>(filter);
    group.boost(ctx.boost);

    // Sub-context: not negated, no extra boost (already on group)
    FilterContext sub_ctx = ctx;
    sub_ctx.negated = false;
    sub_ctx.boost = irs::kNoBoost;

    auto r = FromComparison<true>(group, sub_ctx, *between.input,
                                  *between.lower, lower);
    if (!r.ok()) {
      return r;
    }
    return FromComparison<true>(group, sub_ctx, *between.input, *between.upper,
                                upper);
  }

  // NOT BETWEEN: De Morgan -> field < lower OR field > upper
  auto lower = between.lower_inclusive ? ComparisonOp::Lt : ComparisonOp::Le;
  auto upper = between.upper_inclusive ? ComparisonOp::Gt : ComparisonOp::Ge;

  auto& group = AddFilter<irs::Or>(filter);
  group.boost(ctx.boost);

  FilterContext sub_ctx = ctx;
  sub_ctx.negated = false;
  sub_ctx.boost = irs::kNoBoost;

  auto r =
    FromComparison<true>(group, sub_ctx, *between.input, *between.lower, lower);
  if (!r.ok()) {
    return r;
  }
  return FromComparison<true>(group, sub_ctx, *between.input, *between.upper,
                              upper);
}

template<bool GenericVersion>
Result FromIn(irs::BooleanFilter& filter, const FilterContext& ctx,
              const duckdb::BoundOperatorExpression& op_expr) {
  if (op_expr.children.size() < 2) {
    return {ERROR_NOT_IMPLEMENTED, "IN has ", op_expr.children.size(),
            " inputs but at least 2 expected"};
  }

  const auto* column_ref = TryGetColumnRef(*op_expr.children[0]);
  if (!column_ref) {
    return {ERROR_BAD_PARAMETER, "Input is not a column reference"};
  }

  const auto* column_info = FindColumnInfo(ctx, *column_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column was not found"};
  }

  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
        column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Use `col @@ "
              "ANY_OF('a', 'b', ...)` (tokenised) or `col @@ ANY_OF("
              "'a'::tokenize('identity'), ...)` (raw)."};
    }
  }

  // Collect constant values from children[1..]
  std::vector<const duckdb::Value*> values;
  values.reserve(op_expr.children.size() - 1);
  for (size_t i = 1; i < op_expr.children.size(); ++i) {
    const auto* val = TryGetConstant(*op_expr.children[i]);
    if (!val) {
      return {ERROR_BAD_PARAMETER, "Failed to evaluate IN value as constant"};
    }
    if (!val->IsNull()) {
      values.push_back(val);
    }
  }

  if (values.empty()) {
    AddFilter<irs::Empty>(filter);
    return {};
  }

  std::string field_name;
  MakeFieldName(*column_info, field_name);

  auto type_id = column_info->logical_type.id();
  auto r = MangleForType(type_id, field_name);
  if (!r.ok()) {
    return r;
  }

  auto& terms_filter = ctx.negated ? Negate<irs::ByTerms>(filter)
                                   : AddFilter<irs::ByTerms>(filter);
  terms_filter.boost(ctx.boost);
  *terms_filter.mutable_field() = field_name;
  auto& opts = *terms_filter.mutable_options();

  for (const auto* value : values) {
    if (type_id == duckdb::LogicalTypeId::VARCHAR) {
      opts.terms.emplace(AsRawBytes(*value));
    } else if (type_id == duckdb::LogicalTypeId::BOOLEAN) {
      opts.terms.emplace(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(value->GetValue<bool>())));
    } else if (IsNumericTypeId(type_id)) {
      irs::NumericTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      ResetNumericStream(stream, type_id, *value);
      stream.next();
      opts.terms.emplace(token->value);
    } else {
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported type for IN filter: ", static_cast<int>(type_id)};
    }
  }
  return {};
}

// `generic_version` selects the call-site contract:
//  - true: SQL `b LIKE 'pat'` operator -- the column may be any
//    indexed type; the function returns a Result so the optimizer
//    can leave the filter unclaimed when the column rejects the
//    LIKE shape (non-VARCHAR, non-identity / non-wildcard analyzer).
//  - false: TSQUERY-surface entry where the binder has already
//    constrained the column to VARCHAR. Validate via SDB_ASSERT
//    instead of returning a Result -- the failure mode is a bind-
//    time programmer error, not a user-recoverable predicate
//    mismatch.
Result FromLike(irs::BooleanFilter& filter, const FilterContext& ctx,
                const duckdb::Expression& field_expr,
                const duckdb::Expression& pattern_expr, bool generic_version,
                char escape_char = '\\') {
  const auto* column_ref = TryGetColumnRef(field_expr);
  if (!column_ref) {
    return {ERROR_BAD_PARAMETER, "Input is not a column reference"};
  }

  const auto* const_val = TryGetConstant(pattern_expr);
  if (!const_val) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate LIKE pattern as constant"};
  }

  if (const_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate LIKE pattern as VARCHAR"};
  }

  const auto* column_info = FindColumnInfo(ctx, *column_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column is not indexed"};
  }

  std::string field_name;
  MakeFieldName(*column_info, field_name);

  if (generic_version) {
    if (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
      return {ERROR_BAD_PARAMETER, "LIKE field is not VARCHAR"};
    }
    const auto analyzer_type = column_info->tokenizer.analyzer->type();
    if (analyzer_type != irs::Type<irs::StringTokenizer>::id() &&
        analyzer_type != irs::Type<irs::analysis::WildcardAnalyzer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity or wildcard analyzer. Use "
              "`col @@ LIKE('pattern')`."};
    }
  } else {
    SDB_ASSERT(column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR,
               ERROR_BAD_PARAMETER, "LIKE field is not VARCHAR");
  }

  search::mangling::MangleString(field_name);
  EmitLikeFilter(filter, ctx, *column_info, std::move(field_name),
                 const_val->GetValue<std::string>(), escape_char);
  return {};
}

// Phrase-gap representation, shared between PHRASE (variadic
// text/INTEGER/INTEGER[] grammar) and the `##` operator. Offsets are
// already +1-adjusted to raw iresearch positions: a gap of N
// "intervening words" maps to offs_min = offs_max = N + 1, and a 2-
// element interval [lo, hi] maps to offs_min = lo + 1, offs_max = hi + 1.
struct PhraseGap {
  size_t offs_min{0};
  size_t offs_max{0};
};

// True iff `id` is one of the integral types we accept as a phrase-gap
// scalar (any signed integer narrower than or equal to BIGINT). Values
// in this set need a sign check at parse time.
bool IsPhraseGapIntegralTypeId(duckdb::LogicalTypeId id) {
  return id == duckdb::LogicalTypeId::TINYINT ||
         id == duckdb::LogicalTypeId::SMALLINT ||
         id == duckdb::LogicalTypeId::INTEGER ||
         id == duckdb::LogicalTypeId::BIGINT;
}

// Parses a constant Value into a PhraseGap. Accepts a non-negative
// integral scalar for an exact gap, or a 2-element LIST/ARRAY of
// non-negative integers for an interval gap (min <= max). The returned
// PhraseGap has offsets already +1-adjusted; callers can hand it
// directly to ByPhraseOptions::push_back(offs_min, offs_max). On error,
// returns a Result whose message is prefixed with `label` (e.g.
// "PHRASE", "##") so the call site is identifiable.
ResultOr<PhraseGap> ParsePhraseGap(const duckdb::Value& val,
                                   std::string_view label) {
  auto err = [&](auto&&... args) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   std::forward<decltype(args)>(args)...};
  };
  if (val.IsNull()) {
    return err(label, " gap must be a non-null constant");
  }
  const auto id = val.type().id();
  if (IsPhraseGapIntegralTypeId(id)) {
    auto raw = val.GetValue<int64_t>();
    if (raw < 0) {
      return err(label, " gap must be >= 0, got ", raw);
    }
    return PhraseGap{.offs_min = static_cast<size_t>(raw) + 1,
                     .offs_max = static_cast<size_t>(raw) + 1};
  }
  if (id == duckdb::LogicalTypeId::LIST || id == duckdb::LogicalTypeId::ARRAY) {
    const auto& children = id == duckdb::LogicalTypeId::ARRAY
                             ? duckdb::ArrayValue::GetChildren(val)
                             : duckdb::ListValue::GetChildren(val);
    if (children.size() != 2) {
      return err(label,
                 " interval gap must be a 2-element list [min, max], got ",
                 children.size(), " elements");
    }
    if (!IsPhraseGapIntegralTypeId(children[0].type().id()) ||
        !IsPhraseGapIntegralTypeId(children[1].type().id()) ||
        children[0].IsNull() || children[1].IsNull()) {
      return err(label, " interval gap elements must be non-negative integers");
    }
    auto lo = children[0].GetValue<int64_t>();
    auto hi = children[1].GetValue<int64_t>();
    if (lo < 0 || hi < 0 || lo > hi) {
      return err(label, " interval gap must satisfy 0 <= min <= max, got [", lo,
                 ", ", hi, "]");
    }
    return PhraseGap{.offs_min = static_cast<size_t>(lo) + 1,
                     .offs_max = static_cast<size_t>(hi) + 1};
  }
  return err(label, " gap has unsupported type ", val.type().ToString(),
             "; expected non-negative INTEGER or 2-element INTEGER[] for an "
             "interval gap");
}

Result FromPhrase(irs::BooleanFilter& filter, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func) {
  // PHRASE is registered with at least one VARCHAR arg (plus variadic
  // ANY tail), so DuckDB's function resolver rejects empty calls at
  // bind time before we get here.
  SDB_ASSERT(!func.children.empty());

  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "PHRASE field is not VARCHAR"};
  }

  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "PHRASE field should have Positions and Frequency features "
            "enabled"};
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(filter)
                             : AddFilter<irs::ByPhrase>(filter);
  phrase.boost(ctx.boost);
  *phrase.mutable_field() = field_name;
  auto* opts = phrase.mutable_options();
  auto& analyzer = ctx.tokenizer;
  const irs::TermAttr* token = irs::get<irs::TermAttr>(analyzer);

  std::optional<PhraseGap> pending_gap;

  // A non-constant argument is an index-only restriction -- a future
  // full-scan PHRASE executor could handle it -- so we return Result
  // and let the caller roll back any partially-built phrase filter so
  // this predicate falls through to regular execution. All OTHER
  // violations below are gap-grammar errors: the PHRASE call is
  // malformed and no executor can satisfy it, so they throw with a
  // specific message rather than letting SearchStubFn surface its
  // generic "outside inverted index context" error.
  for (size_t i = 0; i < func.children.size(); ++i) {
    const auto* const_val = TryGetConstant(*func.children[i]);
    if (!const_val) {
      return {ERROR_BAD_PARAMETER, "PHRASE argument ", i,
              " must be a constant"};
    }
    if (const_val->type().id() == duckdb::LogicalTypeId::VARCHAR) {
      auto text = const_val->GetValue<std::string>();
      analyzer.reset(std::string_view{text});
      while (analyzer.next()) {
        if (pending_gap) {
          // First token of a new text pattern: apply pending gap.
          opts
            ->push_back<irs::ByTermOptions>(pending_gap->offs_min,
                                            pending_gap->offs_max)
            .term.assign(token->value);
        } else {
          // No pending gap: first term or adjacent token within same
          // pattern.
          opts->push_back<irs::ByTermOptions>().term.assign(token->value);
        }
        pending_gap.reset();
      }
      continue;
    }
    auto gap = ParsePhraseGap(*const_val, "PHRASE");
    if (!gap) {
      THROW_SQL_ERROR(
        ERR_CODE(ERROR_BAD_PARAMETER),
        ERR_MSG(gap.error().errorMessage(), " (argument ", i, ")"));
    }
    if (opts->empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERROR_BAD_PARAMETER),
                      ERR_MSG("PHRASE gap at argument ", i,
                              " must be preceded by a text pattern"));
    }
    if (pending_gap) {
      THROW_SQL_ERROR(ERR_CODE(ERROR_BAD_PARAMETER),
                      ERR_MSG("PHRASE has consecutive gaps at argument ", i));
    }
    pending_gap = *gap;
  }

  if (pending_gap) {
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_BAD_PARAMETER),
      ERR_MSG("PHRASE ends with a gap; a text pattern must follow each gap"));
  }
  if (opts->empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_BAD_PARAMETER),
      ERR_MSG("PHRASE text arguments produced no searchable terms"));
  }
  return {};
}

// Emits a LIKE-shaped filter against `column_info`. Translates the
// SQL LIKE `raw_pattern` into iresearch wildcard bytes via
// LikeEscapePattern (escape_char selects which character escapes
// `%`/`_`; SQL `LIKE 'p' ESCAPE 'x'` flows through here, the
// TSQUERY-surface entry uses the default `\\`).
// Picks ByWildcardNgram for WildcardAnalyzer-indexed columns -- those
// columns ngram-tokenise terms at index time, so the pattern matches
// through the inverted index instead of a brute-force term-dictionary
// scan -- and ByWildcard otherwise.
void EmitLikeFilter(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info, std::string field_name,
                    std::string_view raw_pattern, char escape_char) {
  auto pattern = LikeEscapePattern(raw_pattern, escape_char);
  if (column_info.tokenizer.analyzer->type() ==
      irs::Type<irs::analysis::WildcardAnalyzer>::id()) {
    auto& wf = ctx.negated ? Negate<irs::ByWildcardNgram>(parent)
                           : AddFilter<irs::ByWildcardNgram>(parent);
    wf.boost(ctx.boost);
    *wf.mutable_field() = std::move(field_name);
    *wf.mutable_options() = {
      pattern,
      basics::downCast<irs::analysis::WildcardAnalyzer>(
        *column_info.tokenizer.analyzer.get()),
      (column_info.tokenizer.features & irs::IndexFeatures::Pos) ==
        irs::IndexFeatures::Pos};
    return;
  }
  auto& wild = ctx.negated ? Negate<irs::ByWildcard>(parent)
                           : AddFilter<irs::ByWildcard>(parent);
  wild.boost(ctx.boost);
  *wild.mutable_field() = std::move(field_name);
  auto& wild_opts = *wild.mutable_options();
  wild_opts.scored_terms_limit = ctx.scored_terms_limit;
  wild_opts.term.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{pattern}));
}

// LEVENSHTEIN argument parsing + range validation. Used identically by
// the standalone TSQUERY-surface emitter (`FromLevenshteinMatch`) and
// the phrase-part emitter inside EmitPhraseSeq.
struct LevenshteinArgs {
  std::string text;
  int64_t distance = 1;
  bool with_transpositions = true;
};

ResultOr<LevenshteinArgs> ParseLevenshteinArgs(
  const duckdb::BoundFunctionExpression& func) {
  auto err = [](auto&&... args) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   std::forward<decltype(args)>(args)...};
  };
  if (func.children.empty() || func.children.size() > 3) {
    return err(
      "LEVENSHTEIN expects 1 to 3 arguments "
      "(text, distance?, transpositions?), got ",
      func.children.size());
  }
  LevenshteinArgs out;
  if (auto r = GetVarcharArg(*func.children[0], "LEVENSHTEIN text", out.text);
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (func.children.size() >= 2) {
    if (auto r =
          GetIntArg(*func.children[1], "LEVENSHTEIN distance", out.distance);
        !r.ok()) {
      return std::unexpected<Result>{std::in_place, std::move(r)};
    }
  }
  if (out.distance < 0 || out.distance > 4) {
    return err("LEVENSHTEIN distance must be between 0 and 4, got ",
               out.distance);
  }
  if (func.children.size() >= 3) {
    if (auto r = GetBoolArg(*func.children[2], "LEVENSHTEIN transpositions",
                            out.with_transpositions);
        !r.ok()) {
      return std::unexpected<Result>{std::in_place, std::move(r)};
    }
  }
  if (out.with_transpositions && out.distance > 3) {
    return err(
      "LEVENSHTEIN distance must be between 0 and 3 when "
      "transpositions is true, got ",
      out.distance);
  }
  return out;
}

void FillByEditDistanceOptions(const LevenshteinArgs& args,
                               irs::ByEditDistanceOptions& out) {
  out.term.assign(irs::ViewCast<irs::byte_type>(std::string_view{args.text}));
  out.max_distance = static_cast<uint8_t>(args.distance);
  out.with_transpositions = args.with_transpositions;
  out.max_terms = 64;
}

// Tokenises `text` via the column analyzer and pushes ByTermOptions
// parts into `options`. The FIRST token gets `base_gap` -- the gap
// from the previous phrase part. Subsequent tokens are strictly
// adjacent ({1, 1}). Errors if the analyzer produces no tokens. Shared
// between BuildFtsPhrase (called with PhraseGap{}) and EmitPhraseSeq's
// nested-PHRASE phrase-part branch.
Result EmitPhraseTokens(irs::ByPhraseOptions& options, const FilterContext& ctx,
                        const SearchColumnInfo& column_info,
                        std::string_view text, PhraseGap base_gap) {
  auto& analyzer = ctx.tokenizer;
  if (!analyzer.reset(text)) {
    return {ERROR_BAD_PARAMETER, "PHRASE failed to analyse '", text, "'"};
  }
  const auto* token = irs::get<irs::TermAttr>(analyzer);
  bool first = true;
  while (analyzer.next()) {
    const PhraseGap g = first ? base_gap : PhraseGap{1, 1};
    auto& part = options.push_back<irs::ByTermOptions>(g.offs_min, g.offs_max);
    part.term.assign(token->value);
    first = false;
  }
  if (first) {
    return {ERROR_BAD_PARAMETER, "PHRASE('", text,
            "') produced no tokens after analysis"};
  }
  return {};
}

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

Result FromLevenshtein(irs::BooleanFilter& filter, const FilterContext& ctx,
                       const SearchColumnInfo& column_info,
                       const duckdb::BoundFunctionExpression& func) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "LEVENSHTEIN field is not VARCHAR"};
  }
  if (func.children.empty() || func.children.size() > 3) {
    return {ERROR_BAD_PARAMETER,
            "LEVENSHTEIN expects 1 to 3 arguments "
            "(text, distance?, transpositions?), got ",
            func.children.size()};
  }

  auto args = ParseLevenshteinArgs(func);
  if (!args) {
    return std::move(args.error());
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& edit_filter = ctx.negated ? Negate<irs::ByEditDistance>(filter)
                                  : AddFilter<irs::ByEditDistance>(filter);
  edit_filter.boost(ctx.boost);
  *edit_filter.mutable_field() = field_name;
  auto& edit_opts = *edit_filter.mutable_options();
  FillByEditDistanceOptions(*args, edit_opts);
  edit_opts.max_terms = ctx.scored_terms_limit;
  return {};
}

// ---------------------------------------------------------------------------
// TSQUERY dispatch (`@@`, `||`, `&&`, `!!`, `^`, leaf constructors)
//
// Leaf logic below lives alongside the legacy FromPhrase/FromLike/... above
// rather than sharing code with them -- the legacy entries extract a column
// ref from child[0], whereas TSQUERY leaves carry only string literals and
// the column comes from the enclosing @@ match.
// ---------------------------------------------------------------------------

TSQueryOp ClassifyTSQueryFunction(std::string_view name) {
  return magic_enum::enum_cast<TSQueryOp>(name).value_or(TSQueryOp::Unknown);
}

Result BuildFtsPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view text) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "PHRASE field is not VARCHAR"};
  }
  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "PHRASE field should have Positions and Frequency features "
            "enabled"};
  }
  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(parent)
                             : AddFilter<irs::ByPhrase>(parent);
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  *phrase.mutable_field() = field_name;
  phrase.boost(ctx.boost);
  return EmitPhraseTokens(*phrase.mutable_options(), ctx, column_info, text,
                          PhraseGap{});
}

Result BuildFtsTerm(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::Value& value) {
  if (value.IsNull()) {
    AddFilter<irs::Empty>(parent);
    return {};
  }
  auto& term =
    ctx.negated ? Negate<irs::ByTerm>(parent) : AddFilter<irs::ByTerm>(parent);
  term.boost(ctx.boost);
  std::string field_name;
  MakeFieldName(column_info, field_name);
  return SetupTermFilter(term, field_name, column_info, value);
}

// Tokenises `text` via the column's analyzer and emits an iresearch
// filter matching any (min_match=1) or all (min_match=token_count) of
// the resulting tokens. For non-VARCHAR columns falls back to a raw
// ByTerm. For 0 tokens emits irs::Empty; for 1 token emits ByTerm.
// Used by the bare-string VARCHAR path, TOKENIZE(text), and
// plainto_tsquery(text).
Result BuildFtsTokens(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view text, bool require_all) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return BuildFtsTerm(parent, ctx, column_info,
                        duckdb::Value(std::string{text}));
  }
  auto& analyzer = ctx.tokenizer;
  std::vector<irs::bstring> tokens;
  if (!analyzer.reset(text)) {
    return {ERROR_BAD_PARAMETER, "Failed to analyse '", text, "'"};
  }
  const auto* tok_attr = irs::get<irs::TermAttr>(analyzer);
  while (analyzer.next()) {
    tokens.emplace_back(tok_attr->value.begin(), tok_attr->value.end());
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  if (tokens.empty()) {
    AddFilter<irs::Empty>(parent);
    return {};
  }
  if (tokens.size() == 1) {
    auto& term = ctx.negated ? Negate<irs::ByTerm>(parent)
                             : AddFilter<irs::ByTerm>(parent);
    term.boost(ctx.boost);
    *term.mutable_field() = field_name;
    term.mutable_options()->term.assign(tokens[0]);
    return {};
  }
  // Multi-token: ByTerms with min_match=1 (OR) or N (AND).
  auto& terms = ctx.negated ? Negate<irs::ByTerms>(parent)
                            : AddFilter<irs::ByTerms>(parent);
  terms.boost(ctx.boost);
  *terms.mutable_field() = field_name;
  auto& opts = *terms.mutable_options();
  opts.min_match = require_all ? tokens.size() : 1;
  for (auto& t : tokens) {
    opts.terms.emplace(std::move(t));
  }
  return {};
}

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

void BuildFtsRegexp(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    std::string_view pattern, irs::RegexpSyntax syntax) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    throw duckdb::InvalidInputException("REGEXP field is not VARCHAR");
  }
  std::string field_name;
  MakeFieldName(column_info, field_name);
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

// ---------------------------------------------------------------------------
// Phrase sequence (`##`): variable-distance phrase composition
//
// `a ## 2 ## b` parses left-assoc as `## ( ## (a, 2), b )`. The walker
// flattens any ##-tree into (parts[], gaps[]) where
//   gaps.size() == parts.size() - 1
// and each gap is either an exact `size_t N` or an interval
// {min, max}. Gap `N` means N intervening words, which lands as a
// push_back offset of N (iresearch adds the implicit +1 for the part
// itself). Interval {lo, hi} -> push_back(part, lo + 1, hi + 1).
// ---------------------------------------------------------------------------

struct PhraseSeq {
  // Parts are stored as raw Expression pointers; resolved into irs
  // phrase parts (ByTermOptions / ByPrefixOptions / ...) at emission
  // time. The expressions are owned by the bound DuckDB tree, which
  // outlives the filter-builder pass.
  std::vector<const duckdb::Expression*> parts;
  std::vector<PhraseGap> gaps;       // length == parts.size() - 1
  std::optional<PhraseGap> pending;  // trailing `## N` with no following part
};

// Expression-level wrapper for `##`: extracts the constant Value from
// `expr` (unwrapping any TSQUERY casts) and delegates to ParsePhraseGap.
ResultOr<PhraseGap> ParsePhraseSeqGap(const duckdb::Expression& expr) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   "## gap must be a constant"};
  }
  return ParsePhraseGap(*val, "##");
}

// True iff `expr`'s return_type indicates a gap operand (bare INTEGER
// or INTEGER[]) rather than a TSQUERY part. Used to distinguish the
// RHS of a `##` binary: if INTEGER/INTEGER[], treat as gap; else as
// next phrase part.
bool IsPhraseSeqGapType(const duckdb::Expression& expr) {
  const auto id = expr.return_type.id();
  return IsPhraseGapIntegralTypeId(id) || id == duckdb::LogicalTypeId::LIST ||
         id == duckdb::LogicalTypeId::ARRAY;
}

// True iff `expr` is a ## FunctionExpression.
bool IsPhraseSeqNode(const duckdb::Expression& expr) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  if (unwrapped.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& f = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  return f.function.name == kTSQueryPhraseSeq;
}

Result FlattenPhraseSeq(const duckdb::Expression& expr, PhraseSeq& seq);

// Attaches `next` (a part OR a gap-bearing sub-expression) to `seq`,
// using the `pending` gap if any, defaulting to "adjacent" otherwise.
Result AttachPart(PhraseSeq& seq, const duckdb::Expression& next) {
  if (IsPhraseSeqNode(next)) {
    // Delegate to the sub-walker; it will merge its flattened parts into
    // whatever the caller wanted next.
    PhraseSeq sub;
    if (auto r = FlattenPhraseSeq(next, sub); !r.ok()) {
      return r;
    }
    if (sub.parts.empty()) {
      return {ERROR_BAD_PARAMETER, "## produced no parts"};
    }
    // Splice sub into seq with the pending gap between seq.last and sub[0].
    for (size_t i = 0; i < sub.parts.size(); ++i) {
      if (!seq.parts.empty() && i == 0) {
        seq.gaps.push_back(seq.pending.value_or(PhraseGap{1, 1}));
        seq.pending.reset();
      } else if (i > 0) {
        seq.gaps.push_back(sub.gaps[i - 1]);
      }
      seq.parts.push_back(sub.parts[i]);
    }
    seq.pending = sub.pending;
    return {};
  }
  // Leaf part.
  if (!seq.parts.empty()) {
    seq.gaps.push_back(seq.pending.value_or(PhraseGap{1, 1}));
    seq.pending.reset();
  }
  seq.parts.push_back(&next);
  return {};
}

Result FlattenPhraseSeq(const duckdb::Expression& expr, PhraseSeq& seq) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  if (!IsPhraseSeqNode(unwrapped)) {
    // Bare leaf part. A lone INTEGER / INTEGER[] here is a user error
    // (a gap without parts around it).
    if (IsPhraseSeqGapType(unwrapped)) {
      return {ERROR_BAD_PARAMETER,
              "## gap must appear between two phrase parts"};
    }
    seq.parts.push_back(&unwrapped);
    return {};
  }
  const auto& f = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  if (f.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, "## expects 2 arguments (lhs ## rhs), got ",
            f.children.size()};
  }

  // Walk left first so parts arrive in source order.
  if (auto r = FlattenPhraseSeq(*f.children[0], seq); !r.ok()) {
    return r;
  }

  // Right child is either an INTEGER / INTEGER[] gap operand or a
  // TSQUERY phrase part.
  const auto& right = *f.children[1];
  if (IsPhraseSeqGapType(right)) {
    if (seq.pending) {
      return {ERROR_BAD_PARAMETER, "## gap must be followed by a phrase part"};
    }
    auto gap = ParsePhraseSeqGap(right);
    if (!gap) {
      return std::move(gap.error());
    }
    seq.pending = *gap;
    return {};
  }
  return AttachPart(seq, right);
}

// RANGE argument structure -- shared by the standalone FromInRange
// emitter and the ## phrase-part dispatcher. Pointers reference the
// bound function's constant children; nullptr means a NULL operand
// (unbounded that side).
struct RangeArgs {
  const duckdb::Value* min_val = nullptr;
  const duckdb::Value* max_val = nullptr;
  bool min_incl = false;
  bool max_incl = false;
};

ResultOr<RangeArgs> ParseRangeArgs(
  const duckdb::BoundFunctionExpression& func) {
  auto err = [](auto&&... args) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   std::forward<decltype(args)>(args)...};
  };
  if (func.children.size() != 4) {
    return err(
      "RANGE expects 4 arguments "
      "(min, max, min_incl, max_incl), got ",
      func.children.size());
  }
  RangeArgs out;
  for (size_t i = 0; i < 2; ++i) {
    const auto* val = TryGetConstant(*func.children[i]);
    if (!val) {
      return err("RANGE bound ", i, " must be a constant");
    }
    if (!val->IsNull()) {
      (i == 0 ? out.min_val : out.max_val) = val;
    }
  }
  if (auto r = GetBoolArg(*func.children[2], "RANGE min_incl", out.min_incl);
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (auto r = GetBoolArg(*func.children[3], "RANGE max_incl", out.max_incl);
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (out.min_val && out.max_val &&
      out.min_val->type().id() != out.max_val->type().id()) {
    throw duckdb::InvalidInputException(
      "RANGE bounds have mismatched types: %s vs %s",
      out.min_val->type().ToString(), out.max_val->type().ToString());
  }
  return out;
}

// Fills the VARCHAR-form ByRangeOptions::range from parsed RANGE args.
// Shared by FromRange (standalone VARCHAR path) and the ## phrase-part
// dispatcher (only the VARCHAR variant is meaningful inside a phrase).
void FillByRangeOptionsVarchar(const RangeArgs& args,
                               irs::ByRangeOptions& out) {
  if (args.min_val) {
    auto sv = args.min_val->GetValue<std::string>();
    out.range.min.assign(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
    out.range.min_type =
      args.min_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  if (args.max_val) {
    auto sv = args.max_val->GetValue<std::string>();
    out.range.max.assign(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
    out.range.max_type =
      args.max_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
}

// Parses an `any_of` / `all_of` call into (args, synthesised, min_match).
// `synthesised` owns BoundConstantExpression wrappers when the list was
// constant-folded; the caller must keep it alive for the duration of `args`
// use. `min_match` is empty for `all_of` and for `any_of` when not provided.
Result ExtractAnyAllOfArgs(
  const duckdb::BoundFunctionExpression& func, bool is_any,
  std::vector<const duckdb::Expression*>& args,
  std::vector<duckdb::unique_ptr<duckdb::Expression>>& synthesised,
  std::optional<size_t>& min_match) {
  if (func.children.empty() || func.children.size() > 2) {
    return {ERROR_BAD_PARAMETER,
            is_any ? "any_of takes ([list]) or ([list], min_match)"
                   : "all_of takes ([list])"};
  }
  if (!is_any && func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "all_of takes a single list argument"};
  }

  // DuckDB constant-folds `['a', 'b']` into a BOUND_CONSTANT holding a
  // LIST/ARRAY Value rather than a `list_value`/`array_value` function
  // call. Support both shapes (and both LIST and fixed-length ARRAY):
  // synthesised BoundConstantExpression wrappers per child Value in the
  // folded case, raw child expression pointers otherwise.
  const auto& list_expr = *func.children[0];
  const auto list_type_id = list_expr.return_type.id();
  if (list_type_id != duckdb::LogicalTypeId::LIST &&
      list_type_id != duckdb::LogicalTypeId::ARRAY) {
    return {ERROR_BAD_PARAMETER,
            "any_of/all_of first argument must be a list or array"};
  }
  if (list_expr.expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& val = list_expr.Cast<duckdb::BoundConstantExpression>().value;
    if (val.IsNull()) {
      return {ERROR_BAD_PARAMETER, "list arg must not be NULL"};
    }
    const auto& children = list_type_id == duckdb::LogicalTypeId::ARRAY
                             ? duckdb::ArrayValue::GetChildren(val)
                             : duckdb::ListValue::GetChildren(val);
    for (const auto& child_val : children) {
      synthesised.push_back(
        duckdb::make_uniq<duckdb::BoundConstantExpression>(child_val));
      args.push_back(synthesised.back().get());
    }
  } else if (list_expr.expression_class ==
             duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& list_fn = list_expr.Cast<duckdb::BoundFunctionExpression>();
    if (list_fn.function.name != "list_value" &&
        list_fn.function.name != "array_value") {
      return {ERROR_BAD_PARAMETER,
              "list arg must be a literal list or array (got: ",
              list_fn.function.name, ")"};
    }
    for (const auto& e : list_fn.children) {
      args.push_back(e.get());
    }
  } else {
    return {ERROR_BAD_PARAMETER, "list arg must be a literal list or array"};
  }

  if (func.children.size() == 2) {
    int64_t m;
    if (auto r = GetIntArg(*func.children[1], "any_of min_match", m); !r.ok()) {
      return r;
    }
    if (m < 1) {
      return {ERROR_BAD_PARAMETER, "any_of min_match must be >= 1, got ", m};
    }
    min_match = static_cast<size_t>(m);
  }

  if (args.empty()) {
    return {ERROR_BAD_PARAMETER, is_any
                                   ? "any_of requires at least one argument"
                                   : "all_of requires at least one argument"};
  }
  if (min_match && *min_match > args.size()) {
    return {ERROR_BAD_PARAMETER, "any_of min_match (",
            *min_match,          ") exceeds number of arguments (",
            args.size(),         ")"};
  }
  return {};
}

// Emits the flattened phrase sequence as an irs::ByPhrase under `parent`.
Result EmitPhraseSeq(irs::BooleanFilter& parent, const FilterContext& ctx,
                     const SearchColumnInfo& column_info,
                     const PhraseSeq& seq) {
  if (seq.parts.empty()) {
    return {ERROR_BAD_PARAMETER, "## phrase has no parts"};
  }
  if (seq.pending) {
    return {ERROR_BAD_PARAMETER,
            "## trailing gap must be followed by a phrase part"};
  }
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "## field is not VARCHAR"};
  }
  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "## field should have Positions and Frequency features enabled"};
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(parent)
                             : AddFilter<irs::ByPhrase>(parent);
  *phrase.mutable_field() = field_name;
  phrase.boost(ctx.boost);

  auto* options = phrase.mutable_options();

  // Emit one phrase part per input, using the gap offsets. First part
  // is always at offset 0 (iresearch normalises this internally). Each
  // case in the switch is self-contained: it parses the part's args,
  // pushes a phrase-part Options slot at (gap.offs_min, gap.offs_max),
  // and breaks. The push_back overload with (offs_min, offs_max) is
  // the interval form; we always use it (rather than the single-arg
  // shorthand) to keep offset semantics consistent between exact and
  // range gaps.
  for (size_t i = 0; i < seq.parts.size(); ++i) {
    const auto& part_expr_ref = UnwrapTSQueryCast(*seq.parts[i]);
    const PhraseGap gap = i > 0 ? seq.gaps[i - 1] : PhraseGap{};

    // Classify the part. Bare VARCHAR constants (via implicit cast to
    // TSQUERY) are treated as TERM-shaped parts; otherwise dispatch on
    // the function name.
    TSQueryOp leaf_op;
    const duckdb::BoundFunctionExpression* f = nullptr;
    std::string bare_text;
    if (part_expr_ref.expression_class ==
        duckdb::ExpressionClass::BOUND_CONSTANT) {
      const auto& val =
        part_expr_ref.Cast<duckdb::BoundConstantExpression>().value;
      if (val.IsNull() || val.type().id() != duckdb::LogicalTypeId::VARCHAR) {
        return {ERROR_BAD_PARAMETER, "## part must be a VARCHAR constant"};
      }
      bare_text = val.GetValue<std::string>();
      leaf_op = TSQueryOp::Term;
    } else if (part_expr_ref.expression_class ==
               duckdb::ExpressionClass::BOUND_FUNCTION) {
      f = &part_expr_ref.Cast<duckdb::BoundFunctionExpression>();
      leaf_op = ClassifyTSQueryFunction(f->function.name);
    } else {
      return {ERROR_NOT_IMPLEMENTED, "## part expression class: ",
              static_cast<int>(part_expr_ref.expression_class)};
    }

    // Helper for the simple "1 VARCHAR arg" pattern (Term/Like/Prefix).
    // For a bare VARCHAR constant, returns the constant value; for a
    // function, validates arity and extracts its single child.
    auto get_text_arg = [&](std::string& out) -> Result {
      if (!f) {
        out = bare_text;
        return {};
      }
      if (f->children.size() != 1) {
        return {ERROR_BAD_PARAMETER, "## ", f->function.name,
                " phrase part expects 1 argument, got ", f->children.size()};
      }
      return GetVarcharArg(*f->children[0], "## phrase part text", out);
    };

    switch (leaf_op) {
      case TSQueryOp::Term: {
        std::string text;
        if (auto r = get_text_arg(text); !r.ok()) {
          return r;
        }
        options->push_back<irs::ByTermOptions>(gap.offs_min, gap.offs_max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      }
      case TSQueryOp::Prefix: {
        std::string text;
        if (auto r = get_text_arg(text); !r.ok()) {
          return r;
        }
        options->push_back<irs::ByPrefixOptions>(gap.offs_min, gap.offs_max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      }
      case TSQueryOp::Like: {
        std::string text;
        if (auto r = get_text_arg(text); !r.ok()) {
          return r;
        }
        auto pattern = LikeEscapePattern(text, '\\');
        options->push_back<irs::ByWildcardOptions>(gap.offs_min, gap.offs_max)
          .term.assign(
            irs::ViewCast<irs::byte_type>(std::string_view{pattern}));
        break;
      }
      case TSQueryOp::Fuzzy: {
        auto args = ParseLevenshteinArgs(*f);
        if (!args) {
          return std::move(args.error());
        }
        FillByEditDistanceOptions(
          *args, options->push_back<irs::ByEditDistanceOptions>(gap.offs_min,
                                                                gap.offs_max));
        break;
      }
      case TSQueryOp::Phrase: {
        // Nested PHRASE('x y z') -> tokenise via column analyzer and
        // emit one term part per token. The FIRST token uses the
        // incoming gap; subsequent tokens are strictly adjacent. Shared
        // with BuildFtsPhrase via EmitPhraseTokens.
        if (f->children.empty() || f->children.size() > 2) {
          return {ERROR_BAD_PARAMETER,
                  "## PHRASE phrase part expects 1 or 2 arguments "
                  "(text[, slop]), got ",
                  f->children.size()};
        }
        std::string phrase_text;
        if (auto r =
              GetVarcharArg(*f->children[0], "## PHRASE text", phrase_text);
            !r.ok()) {
          return r;
        }
        if (auto r =
              EmitPhraseTokens(*options, ctx, column_info, phrase_text, gap);
            !r.ok()) {
          return r;
        }
        break;
      }
      case TSQueryOp::AnyOf: {
        // ANY_OF as a phrase part -> ByTermsOptions slot with the
        // listed terms as alternatives at this phrase position. Only
        // `ANY_OF([list])` and `ANY_OF([list], 1)` are accepted:
        // iresearch's phrase filter ignores min_match for a
        // ByTermsOptions slot (a single position holds at most one
        // token, so min_match > 1 is unsatisfiable).
        std::vector<const duckdb::Expression*> sub_args;
        std::vector<duckdb::unique_ptr<duckdb::Expression>> sub_synth;
        std::optional<size_t> sub_min_match;
        if (auto r = ExtractAnyAllOfArgs(*f, /*is_any=*/true, sub_args,
                                         sub_synth, sub_min_match);
            !r.ok()) {
          return r;
        }
        if (sub_min_match && *sub_min_match != 1) {
          throw duckdb::InvalidInputException(
            "## ANY_OF phrase part requires min_match=1 (got %d); a "
            "phrase position can match only one token",
            static_cast<int>(*sub_min_match));
        }
        auto& terms_opts =
          options->push_back<irs::ByTermsOptions>(gap.offs_min, gap.offs_max);
        terms_opts.min_match = 1;
        for (const auto* arg : sub_args) {
          std::string term_text;
          if (auto r = GetVarcharArg(UnwrapTSQueryCast(*arg),
                                     "## ANY_OF phrase part term", term_text);
              !r.ok()) {
            return r;
          }
          terms_opts.terms.emplace(
            irs::ViewCast<irs::byte_type>(std::string_view{term_text}));
        }
        break;
      }
      case TSQueryOp::AllOf:
        // ALL_OF rejected for the same reason min_match > 1 is rejected
        // for ANY_OF: a phrase position can match only one token.
        throw duckdb::InvalidInputException(
          "## ALL_OF phrase part is not supported (a phrase position "
          "can match only one token; use ANY_OF instead)");
      case TSQueryOp::Range: {
        // RANGE as a phrase part -> ByRangeOptions slot. Only the
        // VARCHAR variant is meaningful here: phrases live on the
        // analyzed text field, so numeric / boolean ranges (which would
        // target separate fields) make no sense at a phrase position.
        auto args = ParseRangeArgs(*f);
        if (!args) {
          return std::move(args.error());
        }
        if ((args->min_val &&
             args->min_val->type().id() != duckdb::LogicalTypeId::VARCHAR) ||
            (args->max_val &&
             args->max_val->type().id() != duckdb::LogicalTypeId::VARCHAR)) {
          throw duckdb::InvalidInputException(
            "## RANGE phrase part requires VARCHAR bounds");
        }
        FillByRangeOptionsVarchar(
          *args,
          options->push_back<irs::ByRangeOptions>(gap.offs_min, gap.offs_max));
        break;
      }
      default:
        return {ERROR_NOT_IMPLEMENTED, "## part type not supported yet: ",
                f ? f->function.name : "<bare-const>"};
    }
  }
  return {};
}

// ---------------------------------------------------------------------------
// websearch_to_tsquery parser
//
// PG-compatible web-search syntax over a single VARCHAR input:
//   - Quoted substrings ("...") are phrases (ordered token sequence).
//   - Bare words are conjuncted (implicit AND) at the top level.
//   - The keyword `OR` (case-insensitive) between two adjacent tokens
//     forms a disjunction; OR binds tighter than implicit AND.
//   - `-` prefix on an atom means NOT (exclusion).
//   - Parens are NOT honored (per PG docs).
//
// Implementation is two-pass: a hand-rolled lexer (LexWebsearch) produces
// a flat token list, then GroupWebsearch collects OR-chained atoms into
// groups. ParseWebsearchQuery emits the resulting iresearch filter tree
// (AND of groups, each group an atom or OR of atoms), reusing
// BuildFtsPhrase / BuildFtsTokens for the leaves so tokenisation goes
// through the column's ambient analyzer consistently with the rest of
// the TSQUERY surface.
// ---------------------------------------------------------------------------

enum class WsTokKind { Word, Phrase, Or };

struct WsToken {
  WsTokKind kind;
  std::string text;
  bool negated = false;
};

std::vector<WsToken> LexWebsearch(std::string_view text) {
  std::vector<WsToken> out;
  size_t i = 0;
  const size_t n = text.size();
  while (i < n) {
    while (i < n && std::isspace(static_cast<unsigned char>(text[i]))) {
      ++i;
    }
    if (i >= n) {
      break;
    }

    bool neg = false;
    if (text[i] == '-') {
      neg = true;
      ++i;
      if (i >= n || std::isspace(static_cast<unsigned char>(text[i]))) {
        // lone '-' -- ignore
        continue;
      }
    }

    if (text[i] == '"') {
      ++i;
      size_t start = i;
      while (i < n && text[i] != '"') {
        ++i;
      }
      out.push_back(
        {WsTokKind::Phrase, std::string{text.substr(start, i - start)}, neg});
      if (i < n) {
        ++i;  // consume closing quote
      }
      continue;
    }

    size_t start = i;
    while (i < n && !std::isspace(static_cast<unsigned char>(text[i])) &&
           text[i] != '"') {
      ++i;
    }
    std::string word{text.substr(start, i - start)};
    // `OR` keyword: case-insensitive, exactly 2 chars, and only when it
    // isn't negated (the user wrote `-OR` or similar).
    if (!neg && word.size() == 2 && (word[0] == 'o' || word[0] == 'O') &&
        (word[1] == 'r' || word[1] == 'R')) {
      out.push_back({WsTokKind::Or, {}, false});
    } else {
      out.push_back({WsTokKind::Word, std::move(word), neg});
    }
  }
  return out;
}

// Groups OR-chained atoms. Stray OR tokens (at start / end / between
// other ORs) are silently dropped.
std::vector<std::vector<WsToken>> GroupWebsearch(
  const std::vector<WsToken>& tokens) {
  std::vector<std::vector<WsToken>> groups;
  size_t i = 0;
  while (i < tokens.size()) {
    if (tokens[i].kind == WsTokKind::Or) {
      ++i;
      continue;
    }
    std::vector<WsToken> group;
    group.push_back(tokens[i]);
    ++i;
    while (i + 1 < tokens.size() && tokens[i].kind == WsTokKind::Or &&
           tokens[i + 1].kind != WsTokKind::Or) {
      group.push_back(tokens[i + 1]);
      i += 2;
    }
    groups.push_back(std::move(group));
  }
  return groups;
}

Result ParseWebsearchQuery(std::string_view text,
                           const SearchColumnInfo& column_info,
                           const FilterContext& ctx,
                           irs::BooleanFilter& parent);

Result BuildTSQuery(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::Expression& expr);

std::optional<Result> TryDispatchBoostCast(irs::BooleanFilter& parent,
                                           const FilterContext& ctx,
                                           const SearchColumnInfo& column_info,
                                           const duckdb::Expression& peeled);

std::optional<Result> TryDispatchTokenizeCast(
  irs::BooleanFilter& parent, const FilterContext& ctx,
  const SearchColumnInfo& column_info, const duckdb::Expression& peeled);

Result ParseWebsearchQuery(std::string_view text,
                           const SearchColumnInfo& column_info,
                           const FilterContext& ctx,
                           irs::BooleanFilter& parent) {
  const auto groups = GroupWebsearch(LexWebsearch(text));
  if (groups.empty()) {
    AddFilter<irs::Empty>(parent);
    return {};
  }

  auto emit_atom = [&](const WsToken& tok, irs::BooleanFilter& into,
                       const FilterContext& c) -> Result {
    auto ac = c;
    ac.negated = c.negated ^ tok.negated;
    if (tok.kind == WsTokKind::Phrase) {
      return BuildFtsPhrase(into, ac, column_info, tok.text);
    }
    return BuildFtsTokens(into, ac, column_info, tok.text,
                          /*require_all=*/false);
  };

  auto emit_group = [&](const std::vector<WsToken>& group,
                        irs::BooleanFilter& into,
                        const FilterContext& c) -> Result {
    if (group.size() == 1) {
      return emit_atom(group[0], into, c);
    }
    auto& or_group =
      c.negated ? Negate<irs::Or>(into) : AddFilter<irs::Or>(into);
    or_group.boost(c.boost);
    auto inner = c;
    inner.negated = false;
    inner.boost = irs::kNoBoost;
    for (const auto& tok : group) {
      if (auto r = emit_atom(tok, or_group, inner); !r.ok()) {
        return r;
      }
    }
    return {};
  };

  if (groups.size() == 1) {
    return emit_group(groups[0], parent, ctx);
  }

  auto& and_group =
    ctx.negated ? Negate<irs::And>(parent) : AddFilter<irs::And>(parent);
  and_group.boost(ctx.boost);
  auto inner = ctx;
  inner.negated = false;
  inner.boost = irs::kNoBoost;
  for (const auto& group : groups) {
    if (auto r = emit_group(group, and_group, inner); !r.ok()) {
      return r;
    }
  }
  return {};
}

// TERM(text): emit an irs::ByTerm with the raw text (no tokenisation).
Result FromTerm(irs::BooleanFilter& parent, const FilterContext& ctx,
                const SearchColumnInfo& column_info,
                const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "TERM expects 1 argument (text), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "TERM text", text); !r.ok()) {
    return r;
  }
  return BuildFtsTerm(parent, ctx, column_info, duckdb::Value(text));
}

// LIKE(pattern): emit an irs::ByWildcard with SQL-LIKE wildcard
// semantics (escape `%`/`_` etc.).
Result FromLike(irs::BooleanFilter& parent, const FilterContext& ctx,
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

// PREFIX(text): emit an irs::ByPrefix
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

// phraseto_tsquery(text): identical to a single-text PHRASE call --
// just delegate to FromPhrase, whose variadic-text loop handles the
// 1-arg case correctly.
Result FromPhraseToTsquery(irs::BooleanFilter& parent, const FilterContext& ctx,
                           const SearchColumnInfo& column_info,
                           const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER,
            "phraseto_tsquery expects 1 argument (text), got ",
            func.children.size()};
  }
  return FromPhrase(parent, ctx, column_info, func);
}

// TOKENIZE(text [, analyzer]): 1-arg uses the ambient (column)
// analyzer with OR semantics; 2-arg accepts only `'identity'` for v1
// (bypasses tokenisation, emits a raw ByTerm). Catalog-registered
// analyzers require snapshot access not yet plumbed through the
// filter builder.
Result FromTokenize(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::BoundFunctionExpression& func) {
  if (func.children.empty() || func.children.size() > 2) {
    return {ERROR_BAD_PARAMETER,
            "TOKENIZE expects 1 or 2 arguments (text[, analyzer]), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "TOKENIZE text", text);
      !r.ok()) {
    return r;
  }
  if (func.children.size() == 1) {
    return BuildFtsTokens(parent, ctx, column_info, text,
                          /*require_all=*/false);
  }
  std::string analyzer_name;
  if (auto r = GetVarcharArg(*func.children[1], "TOKENIZE analyzer name",
                             analyzer_name);
      !r.ok()) {
    return r;
  }
  if (analyzer_name == irs::StringTokenizer::type_name()) {
    return BuildFtsTerm(parent, ctx, column_info, duckdb::Value(text));
  }
  // Hold the wrapper as a stack local for the duration of this call;
  // when it drops the analyzer goes back to the catalog Tokenizer's
  // pool.
  auto wrapper = ResolveTokenizerAnalyzer(ctx.client_context, analyzer_name);
  if (!wrapper) {
    // Throw rather than return Result-error: returning would leave the
    // predicate unclaimed and DuckDB would fall back to running the
    // TSQUERY stub at runtime with a confusing "outside @@" message.
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("TOKENIZE(text, '", analyzer_name,
                            "'): tokenizer not found in catalog"),
                    ERR_HINT("Create it via CREATE TEXT SEARCH DICTIONARY "
                             "or use 'identity' for raw bytes."));
  }
  auto sub_ctx = ctx.WithTokenizer(*wrapper);
  return BuildFtsTokens(parent, sub_ctx, column_info, text,
                        /*require_all=*/false);
}

// EQ(text) -- analyzer-aware equality. Tokenises `text` via the
// ambient analyzer and emits ByTerm (single token) or ByTerms with
// LESS / LESS_EQ / GREATER / GREATER_EQ (bound) -- single-bound
// range constructor. Mirrors RANGE's per-type dispatch:
//   * VARCHAR column + VARCHAR bound -> tokenise via ambient analyzer
//     (single-token requirement) and emit irs::ByRange.
//   * BOOLEAN column + BOOLEAN bound -> emit irs::ByRange via
//     BooleanTokenizer.
//   * Numeric column + numeric bound -> emit irs::ByGranularRange via
//     NumericTokenizer + SetGranularTerm (DECIMAL bounds are cast to
//     the column's logical type first).
// Bound vs column type mismatch is a bind-time error (same shape as
// FromRange's `type_mismatch` lambda). NULL bound is also a bind-time
// error -- use RANGE(NULL, ...) for unbounded semantics.
Result FromTSQRangeOne(irs::BooleanFilter& parent, const FilterContext& ctx,
                       const SearchColumnInfo& column_info,
                       const duckdb::BoundFunctionExpression& func,
                       std::string_view label, bool is_lower, bool inclusive) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, label, " expects 1 argument (bound), got ",
            func.children.size()};
  }
  const auto* bound_val = TryGetConstant(*func.children[0]);
  if (!bound_val) {
    return {ERROR_BAD_PARAMETER, label, " bound must be a constant"};
  }
  if (bound_val->IsNull()) {
    throw duckdb::InvalidInputException(
      "%s bound must be non-null (use RANGE(NULL, ..., ...) for unbounded "
      "semantics)",
      std::string{label});
  }

  const auto col_type = column_info.logical_type.id();
  const auto val_type = bound_val->type().id();
  auto type_mismatch = [&]() {
    throw duckdb::InvalidInputException(
      "%s bound type %s is incompatible with column type %s",
      std::string{label}, bound_val->type().ToString(),
      column_info.logical_type.ToString());
  };
  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    if (val_type != duckdb::LogicalTypeId::VARCHAR) {
      type_mismatch();
    }
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    if (val_type != duckdb::LogicalTypeId::BOOLEAN) {
      type_mismatch();
    }
  } else if (IsNumericTypeId(col_type)) {
    if (!IsRangeNumericValueType(val_type)) {
      type_mismatch();
    }
  } else {
    throw duckdb::InvalidInputException("%s: unsupported column type %s",
                                        std::string{label},
                                        column_info.logical_type.ToString());
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  if (auto r = MangleForType(col_type, field_name); !r.ok()) {
    return r;
  }
  const auto bound_type =
    inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;

  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    // VARCHAR: tokenise the bound through the ambient analyzer; the
    // (single) token becomes the bound's bytes.
    auto text = bound_val->GetValue<std::string>();
    auto& analyzer = ctx.tokenizer;
    if (!analyzer.reset(std::string_view{text})) {
      return {ERROR_BAD_PARAMETER, label, " failed to analyse '", text, "'"};
    }
    const auto* token = irs::get<irs::TermAttr>(analyzer);
    if (!analyzer.next()) {
      // Zero tokens (e.g. all-stopword input) -> the comparison can't
      // match anything in the term dictionary.
      AddFilter<irs::Empty>(parent);
      return {};
    }
    auto& rf = ctx.negated ? Negate<irs::ByRange>(parent)
                           : AddFilter<irs::ByRange>(parent);
    *rf.mutable_field() = std::move(field_name);
    rf.boost(ctx.boost);
    auto* rf_opts = rf.mutable_options();
    rf_opts->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = rf_opts->range;
    if (is_lower) {
      rng.min.assign(token->value);
      rng.min_type = bound_type;
    } else {
      rng.max.assign(token->value);
      rng.max_type = bound_type;
    }
    if (analyzer.next()) {
      return {ERROR_BAD_PARAMETER, label,
              " produced multiple tokens; range comparison requires a "
              "single token (use RANGE for multi-component bounds)"};
    }
    return {};
  }

  if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    auto& rf = ctx.negated ? Negate<irs::ByRange>(parent)
                           : AddFilter<irs::ByRange>(parent);
    *rf.mutable_field() = std::move(field_name);
    rf.boost(ctx.boost);
    auto* rf_opts = rf.mutable_options();
    rf_opts->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = rf_opts->range;
    auto bytes = irs::ViewCast<irs::byte_type>(
      irs::BooleanTokenizer::value(bound_val->GetValue<bool>()));
    if (is_lower) {
      rng.min.assign(bytes);
      rng.min_type = bound_type;
    } else {
      rng.max.assign(bytes);
      rng.max_type = bound_type;
    }
    return {};
  }

  // Numeric: cast the bound to the column's logical type, then run
  // NumericTokenizer + SetGranularTerm (mirrors FromRange's numeric
  // path so DECIMAL bounds on a DOUBLE/INT/BIGINT column work
  // cleanly).
  auto& rf = ctx.negated ? Negate<irs::ByGranularRange>(parent)
                         : AddFilter<irs::ByGranularRange>(parent);
  *rf.mutable_field() = std::move(field_name);
  rf.boost(ctx.boost);
  auto* rf_opts = rf.mutable_options();
  rf_opts->scored_terms_limit = ctx.scored_terms_limit;
  auto& rng = rf_opts->range;
  auto cast = bound_val->DefaultCastAs(column_info.logical_type);
  irs::NumericTokenizer stream;
  ResetNumericStream(stream, col_type, cast);
  if (is_lower) {
    irs::SetGranularTerm(rng.min, stream);
    rng.min_type = bound_type;
  } else {
    irs::SetGranularTerm(rng.max, stream);
    rng.max_type = bound_type;
  }
  return {};
}

// plainto_tsquery(text): tokenise via the column analyzer and AND all
// resulting tokens (PG semantics; every token must match).
Result FromPlainToTsquery(irs::BooleanFilter& parent, const FilterContext& ctx,
                          const SearchColumnInfo& column_info,
                          const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER,
            "plainto_tsquery expects 1 argument (text), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "plainto_tsquery text", text);
      !r.ok()) {
    return r;
  }
  return BuildFtsTokens(parent, ctx, column_info, text, /*require_all=*/true);
}

// websearch_to_tsquery(text): PG-style web-search syntax (quoted
// phrases, OR keyword, leading `-` for NOT). Delegates to the
// websearch parser in this file.
Result FromWebsearchToTsquery(irs::BooleanFilter& parent,
                              const FilterContext& ctx,
                              const SearchColumnInfo& column_info,
                              const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER,
            "websearch_to_tsquery expects 1 argument (text), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r =
        GetVarcharArg(*func.children[0], "websearch_to_tsquery text", text);
      !r.ok()) {
    return r;
  }
  return ParseWebsearchQuery(text, column_info, ctx, parent);
}

// tsquery_phrase(q1, q2 [, distance]): function form of `##`. Wraps
// the args into a synthetic PhraseSeq (q1, optional gap, q2) and
// emits via the shared phrase-seq emitter.
Result FromTsqueryPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                         const SearchColumnInfo& column_info,
                         const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() < 2 || func.children.size() > 3) {
    return {ERROR_BAD_PARAMETER,
            "tsquery_phrase expects 2 or 3 arguments "
            "(q1, q2[, distance]), got ",
            func.children.size()};
  }
  PhraseSeq seq;
  if (auto r = FlattenPhraseSeq(*func.children[0], seq); !r.ok()) {
    return r;
  }
  if (func.children.size() == 3) {
    auto gap = ParsePhraseSeqGap(*func.children[2]);
    if (!gap) {
      return std::move(gap.error());
    }
    seq.pending = *gap;
  }
  if (auto r = AttachPart(seq, *func.children[1]); !r.ok()) {
    return r;
  }
  return EmitPhraseSeq(parent, ctx, column_info, seq);
}

// to_tsquery(<lucene-string>): delegate to the iresearch Lucene parser
// (full Lucene grammar: TERM, PHRASE, AND/OR/NOT, +/-, prefix/wildcard/
// regex, ranges, boost, fuzzy, slop). Writes filters into a freshly-
// added MixedBooleanFilter using the column's analyzer for phrase
// tokenisation.
Result FromToTsquery(irs::BooleanFilter& parent, const FilterContext& ctx,
                     const SearchColumnInfo& column_info,
                     const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER,
            "to_tsquery expects 1 argument (lucene-string), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "to_tsquery text", text);
      !r.ok()) {
    return r;
  }
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  auto& mixed = ctx.negated ? Negate<irs::MixedBooleanFilter>(parent)
                            : AddFilter<irs::MixedBooleanFilter>(parent);
  mixed.boost(ctx.boost);
  sdb::ParserContext parser_ctx{mixed, field_name, ctx.tokenizer};
  // The column is already pinned by the enclosing @@; reject any
  // `field:term` prefix the user might write inside the Lucene string.
  parser_ctx.strict_field = true;
  if (auto r = sdb::ParseQuery(parser_ctx, text); !r.ok()) {
    return {ERROR_BAD_PARAMETER, "to_tsquery parse error: ", r.errorMessage()};
  }
  return {};
}

// TSQUERY `&&` / `||` -- 2-operand conjunction or disjunction. `is_and`
// selects irs::And vs irs::Or. Sub-context resets boost (so a
// containing ^ doesn't double-apply) and negation (the parent already
// applied any negation when adding the And/Or to the parent).
Result FromTSQueryConjunction(irs::BooleanFilter& parent,
                              const FilterContext& ctx,
                              const SearchColumnInfo& column_info,
                              const duckdb::BoundFunctionExpression& func,
                              bool is_and) {
  if (func.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, "TSQUERY ", is_and ? "&&" : "||",
            " expects 2 operands, got ", func.children.size()};
  }
  irs::BooleanFilter* group;
  if (is_and) {
    group =
      ctx.negated ? &Negate<irs::And>(parent) : &AddFilter<irs::And>(parent);
  } else {
    group =
      ctx.negated ? &Negate<irs::Or>(parent) : &AddFilter<irs::Or>(parent);
  }
  group->boost(ctx.boost);
  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  sub_ctx.negated = false;
  for (const auto& child : func.children) {
    if (auto r = BuildTSQuery(*group, sub_ctx, column_info, *child); !r.ok()) {
      return r;
    }
  }
  return {};
}

// TSQUERY `!!` -- prefix NOT. Flips ctx.negated and recurses; no new
// filter node is added at this level (the inner expression's emitter
// will wrap itself in irs::Not when ctx.negated is true).
Result FromTSQueryNot(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "TSQUERY !! expects 1 operand, got ",
            func.children.size()};
  }
  auto neg = ctx;
  neg.negated = !ctx.negated;
  return BuildTSQuery(parent, neg, column_info, *func.children[0]);
}

// TSQUERY `^` -- boost. Multiplies the inherited ctx.boost by the
// factor (defaulting to factor when ctx.boost is the unset sentinel)
// and recurses into the inner expression.
Result FromTSQueryBoost(irs::BooleanFilter& parent, const FilterContext& ctx,
                        const SearchColumnInfo& column_info,
                        const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 2) {
    return {ERROR_BAD_PARAMETER,
            "TSQUERY ^ expects 2 operands (query ^ factor), got ",
            func.children.size()};
  }
  double factor_d;
  if (auto r = GetDoubleArg(*func.children[1], "boost factor", factor_d);
      !r.ok()) {
    return r;
  }
  const auto factor = static_cast<irs::score_t>(factor_d);
  if (factor < 0.0f) {
    return {ERROR_BAD_PARAMETER, "boost factor must be >= 0, got ", factor};
  }
  return BuildTSQuery(parent, ctx.WithBoost(factor), column_info,
                      *func.children[0]);
}

// TSQUERY `##` -- phrase sequence. Flattens the (possibly nested) ##
// tree into a (parts, gaps) sequence then emits an irs::ByPhrase via
// the shared phrase-seq emitter.
Result FromTSQueryPhraseSeq(irs::BooleanFilter& parent,
                            const FilterContext& ctx,
                            const SearchColumnInfo& column_info,
                            const duckdb::BoundFunctionExpression& func) {
  PhraseSeq seq;
  if (auto r = FlattenPhraseSeq(func, seq); !r.ok()) {
    return r;
  }
  return EmitPhraseSeq(parent, ctx, column_info, seq);
}

// ANY_OF / ALL_OF emitter. Both share the list-extraction + recursion
// shape; `is_any` selects between irs::Or (with optional min_match) and
// irs::And. Forms:
//   ANY_OF([list])
//   ANY_OF([list], min_match)
//   ALL_OF([list])
// Variadic / non-list forms are intentionally not supported (DuckDB's
// STRING_LITERAL->INTEGER cost-table preference would absorb 2-arg
// variadic bare-string calls into a (LIST, INTEGER) overload).
// True iff `expr` is a `TOKENIZE(text_array [, analyzer])` call whose
// return type is `LIST(TSQUERY)`. Filters out the scalar overloads.
bool IsTokenizeListCall(const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& f = expr.Cast<duckdb::BoundFunctionExpression>();
  if (f.function.name != kTSQTokenize) {
    return false;
  }
  return f.return_type.id() == duckdb::LogicalTypeId::LIST &&
         IsTSQueryType(duckdb::ListType::GetChildType(f.return_type));
}

// Handles `ANY_OF(TOKENIZE([...] [, name]) [, min_match])` and the
// matching ALL_OF form. Tokenises every element through the chosen
// analyzer (column's ambient or 'identity' override) and emits a single
// ByTerms filter aggregating every produced token. min_match selects
// between OR (1) / AND (count) / explicit. Named (non-identity)
// analyzer is deferred to mirror the scalar TOKENIZE(text, name)
// limitation at FromTokenize.
Result FromTokenizeListInAnyAllOf(
  irs::BooleanFilter& parent, const FilterContext& ctx,
  const SearchColumnInfo& column_info,
  const duckdb::BoundFunctionExpression& outer,
  const duckdb::BoundFunctionExpression& tokenize_call, bool is_any) {
  if (!is_any && outer.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "all_of takes a single argument"};
  }
  std::optional<size_t> min_match;
  if (is_any && outer.children.size() == 2) {
    int64_t m;
    if (auto r = GetIntArg(*outer.children[1], "any_of min_match", m);
        !r.ok()) {
      return r;
    }
    if (m < 1) {
      return {ERROR_BAD_PARAMETER, "any_of min_match must be >= 1, got ", m};
    }
    min_match = static_cast<size_t>(m);
  }
  if (tokenize_call.children.empty() || tokenize_call.children.size() > 2) {
    return {ERROR_BAD_PARAMETER,
            "TOKENIZE(text_array[, analyzer]) expects 1 or 2 arguments, got ",
            tokenize_call.children.size()};
  }
  // Inner list -- v1 requires a constant LIST(VARCHAR).
  const auto* list_const = TryGetConstant(*tokenize_call.children[0]);
  if (!list_const) {
    return {ERROR_BAD_PARAMETER,
            "TOKENIZE array form requires a constant text array"};
  }
  if (list_const->IsNull()) {
    return {ERROR_BAD_PARAMETER, "TOKENIZE text array must not be NULL"};
  }
  const auto list_const_id = list_const->type().id();
  if (list_const_id != duckdb::LogicalTypeId::LIST &&
      list_const_id != duckdb::LogicalTypeId::ARRAY) {
    return {ERROR_BAD_PARAMETER,
            "TOKENIZE array form: first arg must be a list or array, got ",
            list_const->type().ToString()};
  }
  // Resolve the analyzer choice:
  //   1-arg form               -> ambient column analyzer
  //   2-arg with 'identity'    -> raw bytes per element (no analysis)
  //   2-arg with named name    -> resolve via catalog at filter-build time
  bool use_identity = false;
  // Hold the wrapper as a stack local so its raw pointer (used by
  // `analyzer` below) stays valid for the loop. Drops at function
  // return -> analyzer goes back to the catalog Tokenizer's pool.
  catalog::Tokenizer::TokenizerWrapper override_wrapper;
  if (tokenize_call.children.size() == 2) {
    std::string analyzer_name;
    if (auto r = GetVarcharArg(*tokenize_call.children[1],
                               "TOKENIZE analyzer name", analyzer_name);
        !r.ok()) {
      return r;
    }
    if (analyzer_name == irs::StringTokenizer::type_name()) {
      use_identity = true;
    } else {
      override_wrapper =
        ResolveTokenizerAnalyzer(ctx.client_context, analyzer_name);
      if (!override_wrapper) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
          ERR_MSG("TOKENIZE(text_array, '", analyzer_name,
                  "'): tokenizer not found in catalog"),
          ERR_HINT("Create it via CREATE TEXT SEARCH DICTIONARY or use "
                   "'",
                   irs::StringTokenizer::type_name(),
                   "' for raw bytes per element."));
      }
    }
  }

  // Walk every element, tokenise it (or take it raw for identity), and
  // accumulate produced tokens. Empty inputs / NULL elements are skipped
  // -- they contribute no terms.
  auto* analyzer = override_wrapper ? override_wrapper.get() : &ctx.tokenizer;
  if (!use_identity &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER,
            "TOKENIZE array form requires a VARCHAR-indexed column"};
  }
  std::vector<irs::bstring> tokens;
  const auto& elems = list_const_id == duckdb::LogicalTypeId::ARRAY
                        ? duckdb::ArrayValue::GetChildren(*list_const)
                        : duckdb::ListValue::GetChildren(*list_const);
  for (const auto& elem : elems) {
    if (elem.IsNull()) {
      continue;
    }
    if (elem.type().id() != duckdb::LogicalTypeId::VARCHAR) {
      return {ERROR_BAD_PARAMETER,
              "TOKENIZE text array elements must be VARCHAR, got ",
              elem.type().ToString()};
    }
    auto raw = duckdb::StringValue::Get(elem);
    if (use_identity) {
      auto bytes = irs::ViewCast<irs::byte_type>(std::string_view{raw});
      tokens.emplace_back(bytes.begin(), bytes.end());
      continue;
    }
    if (!analyzer->reset(raw)) {
      return {ERROR_BAD_PARAMETER, "Failed to analyse '", raw, "'"};
    }
    const auto* tok_attr = irs::get<irs::TermAttr>(*analyzer);
    while (analyzer->next()) {
      tokens.emplace_back(tok_attr->value.begin(), tok_attr->value.end());
    }
  }

  if (tokens.empty()) {
    AddFilter<irs::Empty>(parent);
    return {};
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  // Single-token short-circuit -> ByTerm.
  if (tokens.size() == 1) {
    auto& term = ctx.negated ? Negate<irs::ByTerm>(parent)
                             : AddFilter<irs::ByTerm>(parent);
    term.boost(ctx.boost);
    *term.mutable_field() = field_name;
    term.mutable_options()->term.assign(tokens[0]);
    return {};
  }

  // Aggregate as ByTerms with the min_match policy:
  //   ANY_OF without min_match -> 1
  //   ANY_OF(min_match=N) -> N (capped at tokens.size())
  //   ALL_OF -> tokens.size()
  size_t mm = 1;
  if (!is_any) {
    mm = tokens.size();
  } else if (min_match) {
    mm = std::min<size_t>(*min_match, tokens.size());
  }
  auto& terms = ctx.negated ? Negate<irs::ByTerms>(parent)
                            : AddFilter<irs::ByTerms>(parent);
  terms.boost(ctx.boost);
  *terms.mutable_field() = std::move(field_name);
  auto& opts = *terms.mutable_options();
  opts.min_match = mm;
  for (auto& t : tokens) {
    opts.terms.emplace(std::move(t));
  }
  return {};
}

Result FromAnyAllOf(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::BoundFunctionExpression& func, bool is_any) {
  // Special case: ANY_OF/ALL_OF wrapping a TOKENIZE(text_array[, name])
  // call. Tokenise every element, flatten into a single ByTerms with the
  // appropriate min_match. Bypasses the per-arg BuildTSQuery loop so we
  // can emit one aggregated filter rather than N individual leaves.
  if (!func.children.empty() && IsTokenizeListCall(*func.children[0])) {
    return FromTokenizeListInAnyAllOf(
      parent, ctx, column_info, func,
      func.children[0]->Cast<duckdb::BoundFunctionExpression>(), is_any);
  }
  std::vector<const duckdb::Expression*> args;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> synthesised;
  std::optional<size_t> min_match;
  if (auto r = ExtractAnyAllOfArgs(func, is_any, args, synthesised, min_match);
      !r.ok()) {
    return r;
  }

  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  sub_ctx.negated = false;

  irs::BooleanFilter* group;
  if (is_any) {
    auto& or_group =
      ctx.negated ? Negate<irs::Or>(parent) : AddFilter<irs::Or>(parent);
    if (min_match && *min_match > 1) {
      or_group.min_match_count(*min_match);
    }
    group = &or_group;
  } else {
    group =
      ctx.negated ? &Negate<irs::And>(parent) : &AddFilter<irs::And>(parent);
  }
  group->boost(ctx.boost);
  for (const auto* arg : args) {
    if (auto r = BuildTSQuery(*group, sub_ctx, column_info, *arg); !r.ok()) {
      return r;
    }
  }
  return {};
}

// REGEXP(pattern [, syntax]) -- TSQUERY regex constructor.
// `syntax` is 'perl' (default, full RE2) or 'posix' (POSIX ERE only).
// The pattern is matched against indexed terms; on a non-identity-
// analyzed column it matches the analyzed token form, which is
// usually only meaningful for explicit ranges (lowercased, etc).
Result FromRegexp(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func) {
  if (func.children.empty() || func.children.size() > 2) {
    return {ERROR_BAD_PARAMETER,
            "REGEXP expects 1 or 2 arguments (pattern[, syntax]), got ",
            func.children.size()};
  }
  std::string pattern;
  if (auto r = GetVarcharArg(*func.children[0], "REGEXP pattern", pattern);
      !r.ok()) {
    return r;
  }
  auto syntax = irs::RegexpSyntax::Perl;
  if (func.children.size() == 2) {
    std::string syntax_name;
    if (auto r = GetVarcharArg(*func.children[1], "REGEXP syntax", syntax_name);
        !r.ok()) {
      return r;
    }
    // Surface names are mapped to the iresearch RegexpSyntax enum via
    // the customize::enum_name specialization above; unknown names
    // throw an InvalidInputException at bind time. The valid-value
    // list in the error message is generated from the enum so it
    // stays in sync if a new dialect is added.
    auto parsed = magic_enum::enum_cast<irs::RegexpSyntax>(
      syntax_name, magic_enum::case_insensitive);
    if (!parsed) {
      throw duckdb::InvalidInputException(
        "REGEXP syntax must be one of [%s], got '%s'",
        absl::StrJoin(magic_enum::enum_names<irs::RegexpSyntax>(), ", ",
                      [](std::string* out, std::string_view name) {
                        absl::StrAppend(out, "'", name, "'");
                      }),
        syntax_name);
    }
    syntax = *parsed;
  }
  BuildFtsRegexp(parent, ctx, column_info, pattern, syntax);
  return {};
}

// RANGE(min, max, min_incl, max_incl) -- TSQUERY range constructor.
// Mirrors SQL BETWEEN with explicit inclusivity. Either bound may be
// NULL (unbounded that side). VARCHAR / BOOLEAN columns emit irs::ByRange,
// numeric columns emit irs::ByGranularRange.

Result FromRange(irs::BooleanFilter& parent, const FilterContext& ctx,
                 const SearchColumnInfo& column_info,
                 const duckdb::BoundFunctionExpression& func) {
  auto args = ParseRangeArgs(func);
  if (!args) {
    return std::move(args.error());
  }
  // Both bounds NULL -> unbounded on both sides; matches every doc.
  if (!args->min_val && !args->max_val) {
    if (ctx.negated) {
      AddFilter<irs::Empty>(parent);
    } else {
      AddFilter<irs::All>(parent).boost(ctx.boost);
    }
    return {};
  }

  const auto col_type = column_info.logical_type.id();
  const auto* val_sample = args->min_val ? args->min_val : args->max_val;
  const auto val_type = val_sample->type().id();

  // Validate value type matches column type family.
  auto type_mismatch = [&]() {
    throw duckdb::InvalidInputException(
      "RANGE bound type %s is incompatible with column type %s",
      val_sample->type().ToString(), column_info.logical_type.ToString());
  };
  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    if (val_type != duckdb::LogicalTypeId::VARCHAR) {
      type_mismatch();
    }
    if (column_info.tokenizer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
      throw duckdb::InvalidInputException(
        "RANGE on VARCHAR field requires identity-analyzed column");
    }
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    if (val_type != duckdb::LogicalTypeId::BOOLEAN) {
      type_mismatch();
    }
  } else if (IsNumericTypeId(col_type)) {
    if (!IsRangeNumericValueType(val_type)) {
      type_mismatch();
    }
  } else {
    throw duckdb::InvalidInputException("RANGE: unsupported column type %s",
                                        column_info.logical_type.ToString());
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  if (auto r = MangleForType(col_type, field_name); !r.ok()) {
    return r;
  }

  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    auto& rf = ctx.negated ? Negate<irs::ByRange>(parent)
                           : AddFilter<irs::ByRange>(parent);
    *rf.mutable_field() = std::move(field_name);
    rf.boost(ctx.boost);
    auto* rf_opts = rf.mutable_options();
    rf_opts->scored_terms_limit = ctx.scored_terms_limit;
    FillByRangeOptionsVarchar(*args, *rf_opts);
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    auto& rf = ctx.negated ? Negate<irs::ByRange>(parent)
                           : AddFilter<irs::ByRange>(parent);
    *rf.mutable_field() = std::move(field_name);
    rf.boost(ctx.boost);
    auto* rf_opts = rf.mutable_options();
    rf_opts->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = rf_opts->range;
    if (args->min_val) {
      rng.min.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args->min_val->GetValue<bool>())));
      rng.min_type =
        args->min_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    if (args->max_val) {
      rng.max.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args->max_val->GetValue<bool>())));
      rng.max_type =
        args->max_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
  } else {
    // Numeric. Cast each bound to the column's logical type before
    // tokenising so the indexed and queried representations match.
    auto& range = ctx.negated ? Negate<irs::ByGranularRange>(parent)
                              : AddFilter<irs::ByGranularRange>(parent);
    *range.mutable_field() = std::move(field_name);
    range.boost(ctx.boost);
    auto* range_opts = range.mutable_options();
    range_opts->scored_terms_limit = ctx.scored_terms_limit;
    auto& rng = range_opts->range;
    auto emit_bound = [&](const duckdb::Value& v,
                          irs::ByGranularRangeOptions::terms& boundary,
                          irs::BoundType& bt, bool incl) {
      auto cast = v.DefaultCastAs(column_info.logical_type);
      irs::NumericTokenizer stream;
      ResetNumericStream(stream, col_type, cast);
      irs::SetGranularTerm(boundary, stream);
      bt = incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    };
    if (args->min_val) {
      emit_bound(*args->min_val, rng.min, rng.min_type, args->min_incl);
    }
    if (args->max_val) {
      emit_bound(*args->max_val, rng.max, rng.max_type, args->max_incl);
    }
  }
  return {};
}

// `(...)::boost(K)` -- multiplies ctx.boost by the modifier's factor
// and recurses on the inner. Returns nullopt if `peeled` carries no
// boost modifier.
std::optional<Result> TryDispatchBoostCast(irs::BooleanFilter& parent,
                                           const FilterContext& ctx,
                                           const SearchColumnInfo& column_info,
                                           const duckdb::Expression& peeled) {
  if (peeled.expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast_expr = peeled.Cast<duckdb::BoundCastExpression>();
    auto mod = TryGetBoostModifier(cast_expr.return_type);
    if (!mod.factor || !cast_expr.child) {
      return std::nullopt;
    }
    return BuildTSQuery(parent,
                        ctx.WithBoost(static_cast<irs::score_t>(*mod.factor)),
                        column_info, *cast_expr.child);
  }
  if (peeled.expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& cv = peeled.Cast<duckdb::BoundConstantExpression>().value;
    auto mod = TryGetBoostModifier(cv.type());
    if (!mod.factor) {
      return std::nullopt;
    }
    // Strip the BOOSTED alias before recursing, otherwise we re-enter
    // this branch on the same value.
    duckdb::Value cleaned = cv;
    cleaned.Reinterpret(MakeTSQueryType());
    duckdb::BoundConstantExpression cleaned_expr(std::move(cleaned));
    return BuildTSQuery(parent,
                        ctx.WithBoost(static_cast<irs::score_t>(*mod.factor)),
                        column_info, cleaned_expr);
  }
  return std::nullopt;
}

// `(...)::tokenize('<name>')` -- 'identity' bypasses tokenisation;
// any other name resolves via the catalog. Returns nullopt if
// `peeled` carries no tokenize modifier.
std::optional<Result> TryDispatchTokenizeCast(
  irs::BooleanFilter& parent, const FilterContext& ctx,
  const SearchColumnInfo& column_info, const duckdb::Expression& peeled) {
  TokenizerModifier mod;
  const duckdb::Expression* expr = nullptr;
  const duckdb::Value* val = nullptr;
  if (peeled.expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast_expr = peeled.Cast<duckdb::BoundCastExpression>();
    mod = TryGetTokenizerModifier(cast_expr.return_type);
    if (!mod.name.empty() && cast_expr.child) {
      expr = cast_expr.child.get();
      val = TryGetConstant(UnwrapTSQueryCast(*expr));
    }
  } else if (peeled.expression_class ==
             duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& cv = peeled.Cast<duckdb::BoundConstantExpression>().value;
    mod = TryGetTokenizerModifier(cv.type());
    if (!mod.name.empty()) {
      val = &cv;
    }
  }
  if (mod.name.empty()) {
    return std::nullopt;
  }
  if (mod.name == irs::StringTokenizer::type_name()) {
    if (val && !val->IsNull() &&
        val->type().id() == duckdb::LogicalTypeId::VARCHAR) {
      return BuildFtsTerm(parent, ctx, column_info, *val);
    }
    if (expr) {
      return BuildTSQuery(parent, ctx.WithTokenizer(ctx.identity), column_info,
                          *expr);
    }
    return Result{ERROR_NOT_IMPLEMENTED,
                  "::tokenize('identity'): inner expression has unsupported "
                  "shape"};
  }
  // Wrapper lives on this stack frame; releases the analyzer back to
  // the Tokenizer's pool when the scope exits.
  auto wrapper = ResolveTokenizerAnalyzer(ctx.client_context, mod.name);
  if (!wrapper) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("::tokenize('", mod.name, "'): tokenizer not found in catalog"),
      ERR_HINT("Create it via CREATE TEXT SEARCH DICTIONARY "
               "or use 'identity' for raw bytes."));
  }
  auto sub_ctx = ctx.WithTokenizer(*wrapper);
  if (val) {
    // Cannot recurse on a folded constant -- its type still carries
    // the modifier and would re-enter this branch.
    if (val->IsNull() || val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
      return Result{ERROR_BAD_PARAMETER,
                    "::tokenize(<name>): inner value must be VARCHAR"};
    }
    return BuildFtsTokens(parent, sub_ctx, column_info,
                          val->GetValue<std::string>(),
                          /*require_all=*/false);
  }
  return BuildTSQuery(parent, sub_ctx, column_info, *expr);
}

Result BuildTSQuery(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::Expression& expr) {
  const duckdb::Expression& unwrapped = UnwrapTSQueryCast(expr);

  // Trivial-constant short-circuit: NULL -> Empty, true -> All,
  // false -> Empty. Surfaces as either a NULL TSQUERY constant or a
  // BoundCast<TSQUERY> wrapping a BOOLEAN constant. Works at any
  // TSQUERY position thanks to the recursive walker.
  if (unwrapped.expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast = unwrapped.Cast<duckdb::BoundCastExpression>();
    if (cast.child &&
        cast.child->return_type.id() == duckdb::LogicalTypeId::BOOLEAN) {
      const auto* val = TryGetConstant(*cast.child);
      if (!val) {
        return {ERROR_BAD_PARAMETER,
                "BOOLEAN inside TSQUERY must be a constant"};
      }
      if (val->IsNull() || !val->GetValue<bool>()) {
        AddFilter<irs::Empty>(parent);
      } else {
        AddFilter<irs::All>(parent);
      }
      return {};
    }
  }
  if (const auto* val = TryGetConstant(unwrapped); val && val->IsNull()) {
    AddFilter<irs::Empty>(parent);
    return {};
  }

  if (auto r = TryDispatchBoostCast(parent, ctx, column_info, unwrapped)) {
    return std::move(*r);
  }

  if (auto r = TryDispatchTokenizeCast(parent, ctx, column_info, unwrapped)) {
    return std::move(*r);
  }

  // Bare string (promoted via VARCHAR -> TSQUERY cast) -> tokenize via
  // the ambient (column) analyzer. Multi-token input composes with OR
  // (min_match=1) per the plan's "col @@ 'Quick Fox' ≡ ANY_OF(tokens)"
  // rule. Non-VARCHAR / analyzer-less paths fall back to raw ByTerm.
  if (unwrapped.expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& val = unwrapped.Cast<duckdb::BoundConstantExpression>().value;
    if (val.IsNull()) {
      AddFilter<irs::Empty>(parent);
      return {};
    }
    if (val.type().id() == duckdb::LogicalTypeId::VARCHAR) {
      return BuildFtsTokens(parent, ctx, column_info,
                            val.GetValue<std::string>(),
                            /*require_all=*/false);
    }
    return BuildFtsTerm(parent, ctx, column_info, val);
  }

  if (unwrapped.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return {ERROR_NOT_IMPLEMENTED, "Unsupported TSQUERY expression class: ",
            static_cast<int>(unwrapped.expression_class)};
  }

  const auto& func = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  const auto op = ClassifyTSQueryFunction(func.function.name);

  switch (op) {
    case TSQueryOp::Phrase:
      return FromPhrase(parent, ctx, column_info, func);
    case TSQueryOp::Term:
      return FromTerm(parent, ctx, column_info, func);
    case TSQueryOp::Like:
      return FromLike(parent, ctx, column_info, func);
    case TSQueryOp::Prefix:
      return FromPrefix(parent, ctx, column_info, func);
    case TSQueryOp::Ngram:
      return FromNgram(parent, ctx, column_info, func);
    case TSQueryOp::Fuzzy:
      return FromLevenshtein(parent, ctx, column_info, func);
    case TSQueryOp::Or:
      return FromTSQueryConjunction(parent, ctx, column_info, func,
                                    /*is_and=*/false);
    case TSQueryOp::And:
      return FromTSQueryConjunction(parent, ctx, column_info, func,
                                    /*is_and=*/true);
    case TSQueryOp::Not:
      return FromTSQueryNot(parent, ctx, column_info, func);
    case TSQueryOp::Boost:
      return FromTSQueryBoost(parent, ctx, column_info, func);
    case TSQueryOp::PhraseSeq:
      return FromTSQueryPhraseSeq(parent, ctx, column_info, func);
    case TSQueryOp::PhraseToTsquery:
      return FromPhraseToTsquery(parent, ctx, column_info, func);
    case TSQueryOp::AnyOf:
      return FromAnyAllOf(parent, ctx, column_info, func, /*is_any=*/true);
    case TSQueryOp::AllOf:
      return FromAnyAllOf(parent, ctx, column_info, func, /*is_any=*/false);
    case TSQueryOp::Range:
      return FromRange(parent, ctx, column_info, func);
    case TSQueryOp::Regexp:
      return FromRegexp(parent, ctx, column_info, func);
    case TSQueryOp::Less:
      return FromTSQRangeOne(parent, ctx, column_info, func, "LESS",
                             /*is_lower=*/false, /*inclusive=*/false);
    case TSQueryOp::LessEq:
      return FromTSQRangeOne(parent, ctx, column_info, func, "LESS_EQ",
                             /*is_lower=*/false, /*inclusive=*/true);
    case TSQueryOp::Greater:
      return FromTSQRangeOne(parent, ctx, column_info, func, "GREATER",
                             /*is_lower=*/true, /*inclusive=*/false);
    case TSQueryOp::GreaterEq:
      return FromTSQRangeOne(parent, ctx, column_info, func, "GREATER_EQ",
                             /*is_lower=*/true, /*inclusive=*/true);
    case TSQueryOp::Tokenize:
      return FromTokenize(parent, ctx, column_info, func);
    case TSQueryOp::PlainToTsquery:
      return FromPlainToTsquery(parent, ctx, column_info, func);
    case TSQueryOp::WebsearchToTsquery:
      return FromWebsearchToTsquery(parent, ctx, column_info, func);
    case TSQueryOp::TsqueryPhrase:
      return FromTsqueryPhrase(parent, ctx, column_info, func);
    case TSQueryOp::ToTSQuery:
      return FromToTsquery(parent, ctx, column_info, func);
    case TSQueryOp::Unknown:
      return {ERROR_NOT_IMPLEMENTED,
              "Not a TSQUERY-producing function: ", func.function.name};
  }
  SDB_UNREACHABLE();
}

Result FromTSQueryMatch(irs::BooleanFilter& filter, const FilterContext& ctx,
                        const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, "@@ has ", func.children.size(),
            " inputs but 2 expected"};
  }
  // @@ is commutative: either side can be the column. Try LHS first,
  // then the cast-stripped RHS. Matches PG `doc @@ q` / `q @@ doc`.
  const auto* left_col = TryGetColumnRef(UnwrapTSQueryCast(*func.children[0]));
  const auto* right_col = TryGetColumnRef(UnwrapTSQueryCast(*func.children[1]));
  const duckdb::BoundColumnRefExpression* column_ref = nullptr;
  const duckdb::Expression* tsquery_expr = nullptr;
  if (left_col && right_col) {
    // Both sides resolve to column refs -- only error if BOTH are
    // indexed inverted columns; otherwise prefer the indexed side.
    const auto* left_info = FindColumnInfo(ctx, *left_col);
    const auto* right_info = FindColumnInfo(ctx, *right_col);
    if (left_info && right_info) {
      return {ERROR_BAD_PARAMETER,
              "@@ has column references on both sides; disambiguate by "
              "wrapping the non-column side as a TSQUERY (e.g. ::TSQUERY "
              "cast or PHRASE/LIKE/PREFIX/LEVENSHTEIN constructor)"};
    }
    column_ref = left_info ? left_col : right_col;
    tsquery_expr = left_info ? func.children[1].get() : func.children[0].get();
  } else if (left_col) {
    column_ref = left_col;
    tsquery_expr = func.children[1].get();
  } else if (right_col) {
    column_ref = right_col;
    tsquery_expr = func.children[0].get();
  } else {
    return {ERROR_BAD_PARAMETER, "@@ must have a column reference on one side"};
  }
  const auto* column_info = FindColumnInfo(ctx, *column_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "@@ column not found in inverted index"};
  }
  auto* analyzer = column_info->tokenizer.analyzer.get();
  if (!analyzer) {
    return {ERROR_BAD_PARAMETER,
            "@@ column has no analyzer (not a text-indexed column)"};
  }
  auto sub_ctx = ctx.WithTokenizer(*analyzer);
  return BuildTSQuery(filter, sub_ctx, *column_info, *tsquery_expr);
}

Result FromFunctionExpression(irs::BooleanFilter& filter,
                              const FilterContext& ctx,
                              const duckdb::BoundFunctionExpression& func) {
  const auto& name = func.function.name;

  if (name == kTSQueryMatch) {
    return FromTSQueryMatch(filter, ctx, func);
  }

  // DuckDB turns LIKE into a BoundFunctionExpression with function.name
  // "~~" or "like_escape".  Handle it as generic LIKE.
  //
  // We deliberately do NOT add a `regexp_full_match` claimer here even
  // though Postgres `~ / ~* / !~ / !~*` rewrite to it. DuckDB's own
  // regex_range_filter optimizer runs first and wraps any
  // `regexp_full_match(col, pat)` call in a sibling LogicalFilter that
  // adds `col >= range_min AND col <= range_max` (computed from the
  // pattern's literal prefix). After our walker recursively claims
  // the inner range filter and rewrites the LogicalGet into an
  // iresearch scan, the outer filter (still holding the original
  // regexp_full_match) no longer matches our claim shape -- the Get
  // has scan_source.Kind() != FullTable -- so the regexp_full_match
  // would silently fall back to the DuckDB regex executor anyway.
  if (name == "~~" || name == "like_escape") {
    if (func.children.size() < 2) {
      return {ERROR_BAD_PARAMETER, "LIKE has ", func.children.size(),
              " inputs but at least 2 expected"};
    }
    char escape_char = '\\';
    if (name == "like_escape" && func.children.size() >= 3) {
      const auto* esc_val = TryGetConstant(*func.children[2]);
      if (!esc_val || esc_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
        return {ERROR_BAD_PARAMETER, "LIKE ESCAPE must be a VARCHAR constant"};
      }
      auto esc_str = esc_val->GetValue<std::string>();
      if (esc_str.size() != 1) {
        return {ERROR_BAD_PARAMETER, "LIKE ESCAPE must be a single character"};
      }
      escape_char = esc_str[0];
    }
    return FromLike(filter, ctx, *func.children[0], *func.children[1],
                    /*generic_version=*/true, escape_char);
  }

  return {ERROR_NOT_IMPLEMENTED, "Unsupported function: ", name};
}

Result FromComparisonExpression(irs::BooleanFilter& filter,
                                const FilterContext& ctx,
                                const duckdb::BoundComparisonExpression& cmp) {
  switch (cmp.type) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
      return FromBinaryEq<true>(filter, ctx, *cmp.left, *cmp.right, false);
    case duckdb::ExpressionType::COMPARE_NOTEQUAL:
      return FromBinaryEq<true>(filter, ctx, *cmp.left, *cmp.right, true);
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
      auto op = GetComparisonOp(cmp.type);
      return FromComparison<true>(filter, ctx, *cmp.left, *cmp.right, op);
    }
    default:
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported comparison type: ", static_cast<int>(cmp.type)};
  }
}

Result FromOperatorExpression(irs::BooleanFilter& filter,
                              const FilterContext& ctx,
                              const duckdb::BoundOperatorExpression& op_expr) {
  switch (op_expr.type) {
    case duckdb::ExpressionType::OPERATOR_NOT: {
      SDB_ASSERT(op_expr.children.size() == 1);
      auto negated_ctx = ctx;
      negated_ctx.negated = !ctx.negated;
      return FromExpression(filter, negated_ctx, *op_expr.children[0]);
    }
    case duckdb::ExpressionType::OPERATOR_IS_NULL:
      return FromIsNull(filter, ctx, op_expr);
    case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL: {
      FilterContext sub_ctx = ctx;
      sub_ctx.negated = !ctx.negated;
      return FromIsNull(filter, sub_ctx, op_expr);
    }
    case duckdb::ExpressionType::COMPARE_IN:
      return FromIn<true>(filter, ctx, op_expr);
    case duckdb::ExpressionType::COMPARE_NOT_IN: {
      FilterContext sub_ctx = ctx;
      sub_ctx.negated = !ctx.negated;
      return FromIn<true>(filter, sub_ctx, op_expr);
    }
    default:
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported operator type: ", static_cast<int>(op_expr.type)};
  }
}

}  // namespace

Result FromExpression(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& expr) {
  switch (expr.expression_class) {
    case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
      const auto& conj = expr.Cast<duckdb::BoundConjunctionExpression>();
      if (conj.type == duckdb::ExpressionType::CONJUNCTION_AND) {
        return MakeGroup<irs::And>(filter, ctx, conj);
      }
      if (conj.type == duckdb::ExpressionType::CONJUNCTION_OR) {
        return MakeGroup<irs::Or>(filter, ctx, conj);
      }
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported conjunction type: ", static_cast<int>(conj.type)};
    }
    case duckdb::ExpressionClass::BOUND_COMPARISON:
      return FromComparisonExpression(
        filter, ctx, expr.Cast<duckdb::BoundComparisonExpression>());
    case duckdb::ExpressionClass::BOUND_OPERATOR:
      return FromOperatorExpression(
        filter, ctx, expr.Cast<duckdb::BoundOperatorExpression>());
    case duckdb::ExpressionClass::BOUND_FUNCTION:
      return FromFunctionExpression(
        filter, ctx, expr.Cast<duckdb::BoundFunctionExpression>());
    case duckdb::ExpressionClass::BOUND_BETWEEN:
      return FromBetween(filter, ctx,
                         expr.Cast<duckdb::BoundBetweenExpression>());
    default:
      return {ERROR_NOT_IMPLEMENTED, "Unsupported expression class: ",
              static_cast<int>(expr.expression_class)};
  }
}

Result MakeSearchFilter(
  irs::And& root,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  const ColumnGetter& column_getter, const SearchFilterOptions& options) {
  irs::StringTokenizer identity;
  containers::FlatHashMap<catalog::Column::Id, SearchColumnInfo> column_cache;

  FilterContext ctx{
    .negated = false,
    .column_getter = column_getter,
    .column_cache = column_cache,
    .identity = identity,
    .tokenizer = identity,
    .client_context = options.client_context,
    .scored_terms_limit = options.scored_terms_limit,
  };

  for (const auto& expr : conjuncts) {
    SDB_ASSERT(expr);

    auto r = FromExpression(root, ctx, *expr);
    if (!r.ok()) {
      return r;
    }
  }
  return {};
}

}  // namespace sdb::connector
