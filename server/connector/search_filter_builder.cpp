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
#include <iresearch/search/geo_filter.h>
#include <vpack/parser.h>

#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/types/geometry_crs.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
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
#include "basics/string_utils.h"
#include "catalog/geo_validate.h"
#include "catalog/mangling.h"
#include "functions/search.h"
#include "functions/string.h"
#include "geo/coding.h"
#include "geo/geo_json.h"
#include "geo/shape_container.h"
#include "geo/wkb.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb_filter.hpp"
#include "ts_common.hpp"

namespace magic_enum {

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
    case Any:
      return sdb::connector::kTSQAnyOf;
    case All:
      return sdb::connector::kTSQAllOf;
    case Between:
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
    case Compound:
      return sdb::connector::kTSQCompound;
    case Unknown:
    case Term:
      return invalid_tag;
  }
  return invalid_tag;
}

}  // namespace magic_enum
namespace sdb::connector {

// True iff `type` is one of TSQUERY / TOKENIZED_TSQUERY /
// BOOSTED_TSQUERY -- all valid carriers of TSQUERY-shaped values
// inside the filter builder.
bool IsAnyTSQueryType(const duckdb::LogicalType& type) {
  if (type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return false;
  }
  const auto alias = type.GetAlias();
  return alias == kTSQueryTypeName || alias == kTokenizedTSQueryTypeName ||
         alias == kBoostedTSQueryTypeName;
}

// If `type` is a TSQUERY annotated with a `tokenize(name)` modifier,
// returns the tokenizer name. Resolution to a live analyzer happens
// at filter-build time via ResolveTokenizerAnalyzer below -- the bind
// callback intentionally does NOT pre-resolve, because the analyzer
// is stateful (one tokenization stream per use) and shouldn't be
// shared across queries.
std::string_view TryGetTokenizerModifier(const duckdb::LogicalType& type) {
  if (!IsAnyTSQueryType(type) || !type.HasExtensionInfo()) {
    return {};
  }
  const auto* ext = type.GetExtensionInfo().get();
  const auto& mods = ext->modifiers;
  if (mods.empty() || mods[0].value.IsNull() ||
      mods[0].value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return {};
  }
  return duckdb::StringValue::Get(mods[0].value);
}

// If `type` carries a `boost(K)` modifier, returns the factor.
// Distinguished from TokenizerModifier by the modifier's value type
// (DOUBLE vs VARCHAR) so the two never alias each other.
std::optional<double> TryGetBoostModifier(const duckdb::LogicalType& type) {
  if (!IsAnyTSQueryType(type) || !type.HasExtensionInfo()) {
    return {};
  }
  const auto* ext = type.GetExtensionInfo().get();
  const auto& mods = ext->modifiers;
  if (mods.empty() || mods[0].value.IsNull() ||
      mods[0].value.type().id() != duckdb::LogicalTypeId::DOUBLE) {
    return {};
  }
  return mods[0].value.GetValue<double>();
}

const duckdb::BoundColumnRefExpression* TryGetColumnRef(
  const duckdb::Expression& expr) {
  // Peel a BoundCastExpression wrapper that DuckDB inserts when a function
  // signature's parameter type differs from the column's declared type only
  // in auxiliary metadata (e.g. GEOMETRY('OGC:CRS84') vs GEOMETRY() -- both
  // share LogicalTypeId::GEOMETRY and the cast is a no-op reinterpret at
  // runtime). Restricted to same-id, non-nested casts so genuine type
  // conversions (int -> varchar, struct field reshuffles, etc.) still
  // surface as opaque expressions and don't get misidentified as columns.
  const duckdb::Expression* inner = &expr;
  while (inner->expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& c = inner->Cast<duckdb::BoundCastExpression>();
    if (c.return_type.id() != c.child->return_type.id() ||
        c.return_type.IsNested()) {
      break;
    }
    inner = c.child.get();
  }
  if (inner->expression_class != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &inner->Cast<duckdb::BoundColumnRefExpression>();
}

const duckdb::Value* TryGetConstant(const duckdb::Expression& expr) {
  // Peel a BoundCastExpression wrapping a constant when the cast is a no-op
  // metadata-only reinterpret (same LogicalTypeId, non-nested) -- e.g. a
  // GEOMETRY('OGC:CRS84') literal narrowed to a GEOMETRY() function signature.
  // Real conversions across LogicalTypeIds (varchar -> geometry, int -> double,
  // ...) are left wrapped so the caller treats them as opaque, since peeling
  // would skip the conversion's effect on the value.
  const duckdb::Expression* inner = &expr;
  while (inner->expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& c = inner->Cast<duckdb::BoundCastExpression>();
    if (c.return_type.id() != c.child->return_type.id() ||
        c.return_type.IsNested()) {
      break;
    }
    inner = c.child.get();
  }
  if (inner->expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &inner->Cast<duckdb::BoundConstantExpression>().value;
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

void MakeFieldName(catalog::Column::Id column_id, std::string& field_name) {
  basics::StrResize(field_name, sizeof(column_id));
  absl::big_endian::Store(field_name.data(), column_id);
}

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
//     mirror overloads of `##`, e.g. ts_phrase('a') ## 1, where DuckDB
//     wraps the LHS in BOUND_CAST<VARCHAR>)
//   - TOK <-> TSQ / VARCHAR transit casts that DON'T carry a tokenize
//     modifier on the cast's return_type. Modifier-bearing casts are
//     preserved here so the BuildTSQuery walker can read the override
//     before continuing to dispatch the inner expression.
// Iterative because casts can chain (e.g. ts_phrase('x')::tokenize('y')
// inside @@ becomes BoundCast<TSQ>(BoundCast<TOK-mod-y>(PHRASE)) -- we
// peel the outer transit cast and stop at the modifier-bearing cast).
// Peels the BOOSTED_TSQUERY -> BOOLEAN coercion that the WhereBinder
// inserts when a `(predicate)::boost(K)` cast appears at the WHERE
// root. Returns the inner cast (whose return_type carries the boost
// modifier) so the SQL-surface peel can read the factor. If `expr`
// isn't that exact shape, returns `expr` unchanged.
const duckdb::Expression& UnwrapBoostBoolCoercion(
  const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_CAST) {
    return expr;
  }
  const auto& cast = expr.Cast<duckdb::BoundCastExpression>();
  if (!cast.child) {
    return expr;
  }
  if (cast.return_type.id() != duckdb::LogicalTypeId::BOOLEAN) {
    return expr;
  }
  if (!TryGetBoostModifier(cast.child->return_type)) {
    return expr;
  }
  return *cast.child;
}

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
    if (!TryGetTokenizerModifier(target).empty() ||
        TryGetBoostModifier(target)) {
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

Result FromExpression(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& expr);
void FromTSQueryMatch(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::BoundFunctionExpression& func);

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
  MakeFieldName(column_info->column_id, field_name);
  search::mangling::MangleNull(field_name);
  auto& term_filter =
    ctx.negated ? Negate<irs::ByTerm>(filter) : AddFilter<irs::ByTerm>(filter);
  term_filter.boost(ctx.boost);
  *term_filter.mutable_field() = field_name;
  term_filter.mutable_options()->term.assign(
    irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  return {};
}

// Forward declarations for geo-distance comparison rewriting (defined later
// alongside the rest of the geo helpers but called from FromBinaryEq /
// FromComparison below).
const duckdb::BoundFunctionExpression* TryGetGeoDistanceCall(
  const duckdb::Expression& expr);
Result FromGeoDistanceBinaryEq(irs::BooleanFilter& filter,
                               const FilterContext& ctx,
                               const duckdb::BoundFunctionExpression& geo_call,
                               const duckdb::Expression& dist_expr);
Result FromGeoDistanceComparison(
  irs::BooleanFilter& filter, const FilterContext& ctx,
  const duckdb::BoundFunctionExpression& geo_call,
  const duckdb::Expression& dist_expr, ComparisonOp op);

template<bool GenericVersion>
Result FromBinaryEq(irs::BooleanFilter& filter, const FilterContext& ctx,
                    const duckdb::Expression& left_expr,
                    const duckdb::Expression& right_expr, bool not_equal) {
  // ST_Distance_Centroid(field, centroid) = / != distance  --  rewrite to
  // range.
  if constexpr (GenericVersion) {
    if (const auto* geo_call = TryGetGeoDistanceCall(left_expr)) {
      FilterContext geo_ctx = ctx;
      geo_ctx.negated = (ctx.negated != not_equal);
      return FromGeoDistanceBinaryEq(filter, geo_ctx, *geo_call, right_expr);
    }
  }

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
              "Field is not indexed by keyword analyzer. Use `col @@ "
              "'value'` (tokenised) or `col @@ "
              "'value'::tokenize('keyword')` (raw)."};
    }
  }

  auto& term_filter = (ctx.negated != not_equal)
                        ? Negate<irs::ByTerm>(filter)
                        : AddFilter<irs::ByTerm>(filter);

  term_filter.boost(ctx.boost);
  std::string field_name;
  MakeFieldName(column_info->column_id, field_name);
  return SetupTermFilter(term_filter, field_name, *column_info, *const_val);
}

template<bool GenericVersion>
Result FromComparison(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& field_expr,
                      const duckdb::Expression& value_expr, ComparisonOp op) {
  if (ctx.negated) {
    op = InvertComparisonOp(op);
  }

  // ST_Distance_Centroid(field, centroid) </<=/>/>= distance  --  rewrite to
  // range.
  if constexpr (GenericVersion) {
    if (const auto* geo_call = TryGetGeoDistanceCall(field_expr)) {
      return FromGeoDistanceComparison(filter, ctx, *geo_call, value_expr, op);
    }
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
      return {
        ERROR_BAD_PARAMETER,
        "Field is not indexed by keyword analyzer. Range predicates "
        "(<, <=, >, >=, BETWEEN) require an keyword-analyzed column. "
        "Use `col @@ ts_lt('value')` / `LESS_EQ` / `GREATER` / "
        "`GREATER_EQ` / `ts_between(min, max, ...)` (tokenised through the "
        "column's analyzer) instead."};
    }
  }

  std::string field_name;
  MakeFieldName(column_info->column_id, field_name);

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
              "Field is not indexed by keyword analyzer. Use `col @@ "
              "ts_any('a', 'b', ...)` (tokenised) or `col @@ ts_any("
              "'a'::tokenize('keyword'), ...)` (raw)."};
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
  MakeFieldName(column_info->column_id, field_name);

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
//    LIKE shape (non-VARCHAR, non-keyword / non-wildcard analyzer).
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
  MakeFieldName(column_info->column_id, field_name);

  if (generic_version) {
    if (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
      return {ERROR_BAD_PARAMETER, "LIKE field is not VARCHAR"};
    }
    const auto analyzer_type = column_info->tokenizer.analyzer->type();
    if (analyzer_type != irs::Type<irs::StringTokenizer>::id() &&
        analyzer_type != irs::Type<irs::analysis::WildcardAnalyzer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity or wildcard analyzer. Use "
              "`col @@ ts_like('pattern')`."};
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

// ---------------------------------------------------------------------------
// Geo filter helpers
// ---------------------------------------------------------------------------

// Returns the inner expression as a ST_Distance_Centroid(field, centroid) call
// when it matches that exact shape, or nullptr otherwise. Used to rewrite the
// pattern `ST_Distance_Centroid(...) OP <const>` into an iresearch
// GeoDistanceFilter at filter-build time.
const duckdb::BoundFunctionExpression* TryGetGeoDistanceCall(
  const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return nullptr;
  }
  const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
  if (func.function.name != kGeoDistance || func.children.size() != 2) {
    return nullptr;
  }
  return &func;
}

// Populate the iresearch geo filter base options from the column's geo
// analyzer. Calls into GeoAnalyzer::prepare which fills in the indexer
// terms-prefix, S2 indexer options, and the analyzer's stored-form coding.
Result SetupGeoFilter(const irs::analysis::Analyzer& a,
                      irs::GeoFilterOptionsBase& options) {
  const auto type_id = a.type();
  if (type_id != irs::Type<irs::analysis::GeoJsonAnalyzer>::id() &&
      type_id != irs::Type<irs::analysis::GeoPointAnalyzer>::id()) {
    return {ERROR_BAD_PARAMETER, "Analyzer for field is not a geo analyzer"};
  }
  basics::downCast<irs::analysis::GeoAnalyzer>(a).prepare(options);
  return {};
}

// Parse a constant geo argument (centroid / target) into a ShapeContainer.
// JSON / VARCHAR-typed string literal: GeoJSON text via the
//   JSON->vpack->ParseShape pipeline (LogicalTypeId::VARCHAR catches both
//   the JSON alias and bare string literals the user types inline).
// GEOMETRY: raw WKB bytes via ParseShapeWKB (parser also re-validates CRS84
//   when the bytes carry an EWKB SRID).
Result ParseGeoConstant(const duckdb::Value& value,
                        sdb::geo::coding::Options coding,
                        sdb::geo::ShapeContainer& shape) {
  std::vector<S2LatLng> cache;
  switch (value.type().id()) {
    case duckdb::LogicalTypeId::VARCHAR: {
      // StringValue::Get returns the raw stored bytes; for VARCHAR that's the
      // GeoJSON text directly. (Value::GetValue<string>() goes through
      // ToString() which is fine for VARCHAR but not for GEOMETRY -- see
      // below -- so we use the same accessor here for consistency.)
      const auto& json_str = duckdb::StringValue::Get(value);
      vpack::Builder vpack;
      try {
        vpack::Parser parser{vpack};
        parser.parse(json_str);
      } catch (...) {
        return {ERROR_BAD_PARAMETER, "Geo argument is not valid JSON"};
      }
      if (!sdb::geo::ParseShape<sdb::geo::Parsing::GeoJson>(
            vpack.slice(), shape, cache, coding, nullptr)) {
        return {ERROR_BAD_PARAMETER, "Geo argument is not valid GeoJSON"};
      }
      return {};
    }
    case duckdb::LogicalTypeId::GEOMETRY: {
      // Mirror the column-side rule from ValidateGeoAnalyzerColumn: a
      // GEOMETRY constant must declare a CRS84-compatible CRS. The function
      // signatures are registered with bare GEOMETRY so DuckDB binds any-CRS
      // values, and our cast peeling in TryGetConstant exposes them; without
      // this check the bytes would be silently interpreted as CRS84.
      if (auto r = sdb::catalog::ValidateGeometryCRS84(value.type());
          r.fail()) {
        return {ERROR_BAD_PARAMETER, "GEOMETRY constant: ", r.errorMessage()};
      }
      // GEOMETRY values store WKB bytes internally. Value::GetValue<string>()
      // would call Geometry::ToString() and return the WKT text form, which
      // ParseShapeWKB can't read; StringValue::Get bypasses that and yields
      // the raw bytes the cast / serializer wrote.
      const auto& wkb_str = duckdb::StringValue::Get(value);
      if (auto r = sdb::geo::ParseShapeWKB(wkb_str, shape, cache); r.fail()) {
        return r;
      }
      return {};
    }
    default:
      return {ERROR_BAD_PARAMETER,
              "Geo argument must be JSON (GeoJSON) or GEOMETRY (WKB)"};
  }
}

// ---------------------------------------------------------------------------
// Set up GeoDistanceFilter (field + origin + analyzer-derived options) from
// a ST_Distance_Centroid(field, centroid) call and a constant distance
// expression. Range bounds are left to the caller. Returns the filter and the
// parsed distance value on success.
// ---------------------------------------------------------------------------

ResultOr<std::pair<irs::GeoDistanceFilter*, double>> PrepareGeoDistanceFilter(
  irs::BooleanFilter& parent, const FilterContext& ctx,
  const duckdb::BoundFunctionExpression& geo_call,
  const duckdb::Expression& dist_expr) {
  SDB_ASSERT(geo_call.children.size() == 2);

  const auto* col_ref = TryGetColumnRef(*geo_call.children[0]);
  if (!col_ref) {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "ST_Distance_Centroid first input must be a column"};
  }

  const auto* centroid_val = TryGetConstant(*geo_call.children[1]);
  if (!centroid_val) {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "ST_Distance_Centroid centroid must be a constant"};
  }

  const auto* dist_val = TryGetConstant(dist_expr);
  if (!dist_val || dist_val->type().id() != duckdb::LogicalTypeId::DOUBLE) {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "ST_Distance_Centroid comparison value must be a constant DOUBLE"};
  }
  const double distance = dist_val->GetValue<double>();

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info ||
      (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
       column_info->logical_type.id() != duckdb::LogicalTypeId::GEOMETRY)) {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "ST_Distance_Centroid field must be JSON (GeoJSON) or GEOMETRY"};
  }
  if (!column_info->tokenizer.analyzer) {
    return std::unexpected<Result>{
      std::in_place, ERROR_BAD_PARAMETER,
      "ST_Distance_Centroid field has no analyzer attached"};
  }

  std::string field_name;
  MakeFieldName(column_info->column_id, field_name);
  search::mangling::MangleString(field_name);

  auto& geo_filter = ctx.negated ? Negate<irs::GeoDistanceFilter>(parent)
                                 : AddFilter<irs::GeoDistanceFilter>(parent);
  geo_filter.boost(ctx.boost);
  *geo_filter.mutable_field() = std::move(field_name);

  auto* options = geo_filter.mutable_options();
  if (auto r = SetupGeoFilter(*column_info->tokenizer.analyzer, *options);
      r.fail()) {
    return std::unexpected<Result>(std::move(r));
  }

  sdb::geo::ShapeContainer centroid_shape;
  if (auto r = ParseGeoConstant(*centroid_val, options->coding, centroid_shape);
      r.fail()) {
    return std::unexpected<Result>(std::move(r));
  }
  options->origin = centroid_shape.centroid();

  return std::pair{&geo_filter, distance};
}

// ST_Distance_Centroid(field, centroid) OP distance  --  range one-sided.
Result FromGeoDistanceComparison(
  irs::BooleanFilter& filter, const FilterContext& ctx,
  const duckdb::BoundFunctionExpression& geo_call,
  const duckdb::Expression& dist_expr, ComparisonOp op) {
  auto setup = PrepareGeoDistanceFilter(filter, ctx, geo_call, dist_expr);
  if (!setup) {
    return std::move(setup.error());
  }
  auto* options = setup->first->mutable_options();
  switch (op) {
    case ComparisonOp::Lt:
      options->range.max = setup->second;
      options->range.max_type = irs::BoundType::Exclusive;
      break;
    case ComparisonOp::Le:
      options->range.max = setup->second;
      options->range.max_type = irs::BoundType::Inclusive;
      break;
    case ComparisonOp::Gt:
      options->range.min = setup->second;
      options->range.min_type = irs::BoundType::Exclusive;
      break;
    case ComparisonOp::Ge:
      options->range.min = setup->second;
      options->range.min_type = irs::BoundType::Inclusive;
      break;
    default:
      return {ERROR_BAD_PARAMETER,
              "ST_Distance_Centroid: unsupported comparison op"};
  }
  return {};
}

// ST_Distance_Centroid(field, centroid) = distance  --  point range [d, d].
Result FromGeoDistanceBinaryEq(irs::BooleanFilter& filter,
                               const FilterContext& ctx,
                               const duckdb::BoundFunctionExpression& geo_call,
                               const duckdb::Expression& dist_expr) {
  auto setup = PrepareGeoDistanceFilter(filter, ctx, geo_call, dist_expr);
  if (!setup) {
    return std::move(setup.error());
  }
  auto* options = setup->first->mutable_options();
  options->range.min = setup->second;
  options->range.min_type = irs::BoundType::Inclusive;
  options->range.max = setup->second;
  options->range.max_type = irs::BoundType::Inclusive;
  return {};
}

// ---------------------------------------------------------------------------
// ST_Distance_Between(field, centroid, min_distance, max_distance,
//                     [include_min, [include_max]]) -> bool
//
// `field` is a column reference (JSON GeoJSON, or GEOMETRY).
// `centroid` is a constant -- JSON GeoJSON, or a GEOMETRY value (WKB).
// Builds an iresearch GeoDistanceFilter that matches indexed
// values whose geodesic distance from `centroid` falls in [min, max].
// `include_min` / `include_max` toggle each endpoint's inclusivity, both
// default to inclusive.
// ---------------------------------------------------------------------------

Result FromGeoInRange(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::BoundFunctionExpression& func) {
  const auto num_inputs = func.children.size();
  if (num_inputs < 4 || num_inputs > 6) {
    return {ERROR_BAD_PARAMETER, "ST_Distance_Between has ", num_inputs,
            " inputs but 4 to 6 expected"};
  }

  const auto* col_ref = TryGetColumnRef(*func.children[0]);
  if (!col_ref) {
    return {ERROR_BAD_PARAMETER, "ST_Distance_Between first input must be a column"};
  }

  const auto* centroid_val = TryGetConstant(*func.children[1]);
  if (!centroid_val) {
    return {ERROR_BAD_PARAMETER, "ST_Distance_Between centroid must be a constant"};
  }

  const auto* min_val = TryGetConstant(*func.children[2]);
  if (!min_val || min_val->type().id() != duckdb::LogicalTypeId::DOUBLE) {
    return {ERROR_BAD_PARAMETER,
            "ST_Distance_Between min_distance must be a constant DOUBLE"};
  }
  const double min_distance = min_val->GetValue<double>();

  const auto* max_val = TryGetConstant(*func.children[3]);
  if (!max_val || max_val->type().id() != duckdb::LogicalTypeId::DOUBLE) {
    return {ERROR_BAD_PARAMETER,
            "ST_Distance_Between max_distance must be a constant DOUBLE"};
  }
  const double max_distance = max_val->GetValue<double>();

  bool include_min = true;
  if (num_inputs >= 5) {
    const auto* v = TryGetConstant(*func.children[4]);
    if (!v || v->type().id() != duckdb::LogicalTypeId::BOOLEAN) {
      return {ERROR_BAD_PARAMETER,
              "ST_Distance_Between include_min must be a constant BOOLEAN"};
    }
    include_min = v->GetValue<bool>();
  }
  bool include_max = true;
  if (num_inputs >= 6) {
    const auto* v = TryGetConstant(*func.children[5]);
    if (!v || v->type().id() != duckdb::LogicalTypeId::BOOLEAN) {
      return {ERROR_BAD_PARAMETER,
              "ST_Distance_Between include_max must be a constant BOOLEAN"};
    }
    include_max = v->GetValue<bool>();
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info ||
      (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
       column_info->logical_type.id() != duckdb::LogicalTypeId::GEOMETRY)) {
    return {ERROR_BAD_PARAMETER,
            "ST_Distance_Between field must be JSON (GeoJSON) or GEOMETRY"};
  }
  if (!column_info->tokenizer.analyzer) {
    return {ERROR_BAD_PARAMETER, "ST_Distance_Between field has no analyzer attached"};
  }

  std::string field_name;
  MakeFieldName(column_info->column_id, field_name);
  search::mangling::MangleString(field_name);

  auto& geo_filter = ctx.negated ? Negate<irs::GeoDistanceFilter>(filter)
                                 : AddFilter<irs::GeoDistanceFilter>(filter);
  geo_filter.boost(ctx.boost);
  *geo_filter.mutable_field() = std::move(field_name);

  auto* options = geo_filter.mutable_options();
  if (auto r = SetupGeoFilter(*column_info->tokenizer.analyzer, *options);
      r.fail()) {
    return r;
  }

  sdb::geo::ShapeContainer centroid_shape;
  if (auto r = ParseGeoConstant(*centroid_val, options->coding, centroid_shape);
      r.fail()) {
    return r;
  }
  options->origin = centroid_shape.centroid();

  if (min_distance != 0.) {
    options->range.min = min_distance;
    options->range.min_type =
      include_min ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  options->range.max = max_distance;
  options->range.max_type =
    include_max ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;

  return {};
}

// ---------------------------------------------------------------------------
// ST_Intersects(field, shape) / ST_Intersects(shape, field) -> bool
// ST_Contains(field, shape)    -> bool   indexed ⊇ shape  (IsContained)
// ST_Contains(shape, field)    -> bool   shape ⊇ indexed  (Contains)
//
// Both predicates accept JSON (GeoJSON) or GEOMETRY (WKB) on either
// side. ST_Intersects is commutative; ST_Contains' two argument orders
// pick different GeoFilterType values.
// ---------------------------------------------------------------------------

Result FromGeoFilter(irs::BooleanFilter& filter, const FilterContext& ctx,
                     const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, func.function.name, " has ",
            func.children.size(), " inputs but 2 expected"};
  }

  // Either argument can be the column reference; the other must be constant.
  size_t field_idx = 0;
  size_t shape_idx = 1;
  const auto* col_ref = TryGetColumnRef(*func.children[0]);
  if (!col_ref) {
    col_ref = TryGetColumnRef(*func.children[1]);
    if (!col_ref) {
      return {ERROR_BAD_PARAMETER, func.function.name,
              ": one argument must be a column reference"};
    }
    field_idx = 1;
    shape_idx = 0;
  }

  const auto* shape_val = TryGetConstant(*func.children[shape_idx]);
  if (!shape_val) {
    return {ERROR_BAD_PARAMETER, func.function.name,
            ": shape argument must be a constant"};
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info ||
      (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
       column_info->logical_type.id() != duckdb::LogicalTypeId::GEOMETRY)) {
    return {ERROR_BAD_PARAMETER, func.function.name,
            ": field must be JSON (GeoJSON) or GEOMETRY"};
  }
  if (!column_info->tokenizer.analyzer) {
    return {ERROR_BAD_PARAMETER, func.function.name,
            ": field has no analyzer attached"};
  }

  std::string field_name;
  MakeFieldName(column_info->column_id, field_name);
  search::mangling::MangleString(field_name);

  auto& geo_filter = ctx.negated ? Negate<irs::GeoFilter>(filter)
                                 : AddFilter<irs::GeoFilter>(filter);
  geo_filter.boost(ctx.boost);
  *geo_filter.mutable_field() = std::move(field_name);

  auto* options = geo_filter.mutable_options();
  if (auto r = SetupGeoFilter(*column_info->tokenizer.analyzer, *options);
      r.fail()) {
    return r;
  }

  sdb::geo::ShapeContainer shape;
  if (auto r = ParseGeoConstant(*shape_val, options->coding, shape); r.fail()) {
    return r;
  }
  options->shape = std::move(shape);

  if (func.function.name == kGeoIntersects) {
    options->type = irs::GeoFilterType::Intersects;
  } else {
    SDB_ASSERT(func.function.name == kGeoContains);
    // ST_Contains(field, shape): indexed contains shape -> filter type
    //   IsContained ("the filter shape is contained within indexed data").
    // ST_Contains(shape, field): shape contains indexed -> filter type
    //   Contains ("the filter shape contains indexed data").
    options->type = field_idx == 0 ? irs::GeoFilterType::IsContained
                                   : irs::GeoFilterType::Contains;
  }
  return {};
}
Result FromFunctionExpression(irs::BooleanFilter& filter,
                              const FilterContext& ctx,
                              const duckdb::BoundFunctionExpression& func) {
  const auto& name = func.function.name;
  if (name == kTSQueryMatch) {
    // Anything that fails inside `@@` would otherwise fall through to
    // the runtime stub and surface the generic "TSQUERY expression
    // evaluated outside @@" error -- losing the specific cause. Throw
    // at this boundary so users see the actual reason + a hint.
    FromTSQueryMatch(filter, ctx, func);
    return {};
  }
  if (name == kGeoInRange) {
    return FromGeoInRange(filter, ctx, func);
  }
  if (name == kGeoIntersects || name == kGeoContains) {
    return FromGeoFilter(filter, ctx, func);
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

  if (name == "prefix") {
    return FromFuncPrefix(filter, ctx, func);
  }

  return {ERROR_NOT_IMPLEMENTED, "Unsupported function: ", name};
}

// Per-type TSQUERY entry points -- each defined in ts_<name>.cpp and
// dispatched to from BuildTSQuery's switch below. All throw
// THROW_SQL_ERROR on any failure (with operator-specific hints).
void FromPhrase(irs::BooleanFilter&, const FilterContext&,
                const SearchColumnInfo&,
                const duckdb::BoundFunctionExpression&);
void FromNgram(irs::BooleanFilter&, const FilterContext&,
               const SearchColumnInfo&, const duckdb::BoundFunctionExpression&);
void FromLevenshtein(irs::BooleanFilter&, const FilterContext&,
                     const SearchColumnInfo&,
                     const duckdb::BoundFunctionExpression&);
void FromTerm(irs::BooleanFilter&, const FilterContext&,
              const SearchColumnInfo&, const duckdb::BoundFunctionExpression&);
void FromTSQLike(irs::BooleanFilter&, const FilterContext&,
                 const SearchColumnInfo&,
                 const duckdb::BoundFunctionExpression&);
void FromPrefix(irs::BooleanFilter&, const FilterContext&,
                const SearchColumnInfo&,
                const duckdb::BoundFunctionExpression&);
void FromTokenize(irs::BooleanFilter&, const FilterContext&,
                  const SearchColumnInfo&,
                  const duckdb::BoundFunctionExpression&);
void FromHalfRange(irs::BooleanFilter&, const FilterContext&,
                   const SearchColumnInfo&,
                   const duckdb::BoundFunctionExpression&,
                   std::string_view label, bool is_lower, bool inclusive);
void FromRegexp(irs::BooleanFilter&, const FilterContext&,
                const SearchColumnInfo&,
                const duckdb::BoundFunctionExpression&);
void FromBetween(irs::BooleanFilter&, const FilterContext&,
                 const SearchColumnInfo&,
                 const duckdb::BoundFunctionExpression&);
void FromCompound(irs::BooleanFilter&, const FilterContext&,
                  const SearchColumnInfo&,
                  const duckdb::BoundFunctionExpression&);
void FromAnyAllOf(irs::BooleanFilter&, const FilterContext&,
                  const SearchColumnInfo&,
                  const duckdb::BoundFunctionExpression&, bool is_any);
void FromPlainToTsquery(irs::BooleanFilter&, const FilterContext&,
                        const SearchColumnInfo&,
                        const duckdb::BoundFunctionExpression&);
void FromPhraseToTsquery(irs::BooleanFilter&, const FilterContext&,
                         const SearchColumnInfo&,
                         const duckdb::BoundFunctionExpression&);
void FromTsqueryPhrase(irs::BooleanFilter&, const FilterContext&,
                       const SearchColumnInfo&,
                       const duckdb::BoundFunctionExpression&);
void FromToTsquery(irs::BooleanFilter&, const FilterContext&,
                   const SearchColumnInfo&,
                   const duckdb::BoundFunctionExpression&);
void FromWebsearchToTsquery(irs::BooleanFilter&, const FilterContext&,
                            const SearchColumnInfo&,
                            const duckdb::BoundFunctionExpression&);
void FromTSQueryPhraseSeq(irs::BooleanFilter&, const FilterContext&,
                          const SearchColumnInfo&,
                          const duckdb::BoundFunctionExpression&);

TSQueryOp ClassifyTSQueryFunction(std::string_view name) {
  return magic_enum::enum_cast<TSQueryOp>(name).value_or(TSQueryOp::Unknown);
}

void FromTSQueryConjunction(irs::BooleanFilter& parent,
                            const FilterContext& ctx,
                            const SearchColumnInfo& column_info,
                            const duckdb::BoundFunctionExpression& func,
                            bool is_and) {
  if (func.children.size() != 2) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("TSQUERY ", is_and ? "&&" : "||",
                            " expects 2 operands, got ", func.children.size()),
                    ERR_HINT("Example: ts_phrase('a') && 'b'."));
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
    BuildTSQuery(*group, sub_ctx, column_info, *child);
  }
}

// TSQUERY `!!` -- prefix NOT. Flips ctx.negated and recurses; no new
// filter node is added at this level (the inner expression's emitter
// will wrap itself in irs::Not when ctx.negated is true).
void FromTSQueryNot(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("TSQUERY !! expects 1 operand, got ", func.children.size()),
      ERR_HINT("Example: !!ts_phrase('text')."));
  }
  auto neg = ctx;
  neg.negated = !ctx.negated;
  BuildTSQuery(parent, neg, column_info, *func.children[0]);
}

// TSQUERY `^` -- boost. Multiplies the inherited ctx.boost by the
// factor and recurses into the inner expression.
void FromTSQueryBoost(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_phrase('text') ^ 2.0. Factor must be >= 0; "
    "for composable boost use ::boost(K).";
  if (func.children.size() != 2) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("TSQUERY ^ expects 2 operands (query ^ factor), got ",
              func.children.size()),
      ERR_HINT(kSyntaxHint));
  }
  double factor_d;
  if (auto r = GetDoubleArg(*func.children[1], "boost factor", factor_d);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
  }
  const auto factor = static_cast<irs::score_t>(factor_d);
  if (factor < 0.0f) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("boost factor must be >= 0, got ", factor),
                    ERR_HINT(kSyntaxHint));
  }
  BuildTSQuery(parent, ctx.WithBoost(factor), column_info, *func.children[0]);
}

// `(...)::boost(K)` -- multiplies ctx.boost by the modifier's factor
// and recurses on the inner. Returns false if `peeled` carries no
// boost modifier; true if it claimed and dispatched the cast (any
// dispatch failure throws via the inner BuildTSQuery / BuildFts*).
bool TryDispatchBoostCast(irs::BooleanFilter& parent, const FilterContext& ctx,
                          const SearchColumnInfo& column_info,
                          const duckdb::Expression& peeled) {
  if (peeled.expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast_expr = peeled.Cast<duckdb::BoundCastExpression>();
    const auto boost = TryGetBoostModifier(cast_expr.return_type);
    if (!boost || !cast_expr.child) {
      return false;
    }
    BuildTSQuery(parent, ctx.WithBoost(static_cast<irs::score_t>(*boost)),
                 column_info, *cast_expr.child);
    return true;
  }
  if (peeled.expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& cv = peeled.Cast<duckdb::BoundConstantExpression>().value;
    const auto boost = TryGetBoostModifier(cv.type());
    if (!boost) {
      return false;
    }
    // Strip the BOOSTED alias before recursing, otherwise we re-enter
    // this branch on the same value.
    duckdb::Value cleaned = cv;
    cleaned.Reinterpret(MakeTSQueryType());
    duckdb::BoundConstantExpression cleaned_expr(std::move(cleaned));
    BuildTSQuery(parent, ctx.WithBoost(static_cast<irs::score_t>(*boost)),
                 column_info, cleaned_expr);
    return true;
  }
  return false;
}

bool TryDispatchSqlBoostCast(irs::BooleanFilter& filter,
                             const FilterContext& ctx,
                             const duckdb::Expression& peeled) {
  if (peeled.expression_class != duckdb::ExpressionClass::BOUND_CAST) {
    return false;
  }
  const auto& cast_expr = peeled.Cast<duckdb::BoundCastExpression>();
  const auto boost = TryGetBoostModifier(cast_expr.return_type);
  if (!boost || !cast_expr.child) {
    return false;
  }
  auto r = FromExpression(
    filter, ctx.WithBoost(static_cast<irs::score_t>(*boost)), *cast_expr.child);
  if (!r.ok()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("::boost(K) used on a predicate the inverted index could not "
              "claim: ",
              r.errorMessage()),
      ERR_HINT("boost is only meaningful inside an inverted-index match. "
               "Move the boost into an `@@` match or remove it."));
  }
  return true;
}

// `(...)::tokenize('<name>')` -- 'keyword' bypasses tokenisation;
// any other name resolves via the catalog. Returns false if `peeled`
// carries no tokenize modifier.
bool TryDispatchTokenizeCast(irs::BooleanFilter& parent,
                             const FilterContext& ctx,
                             const SearchColumnInfo& column_info,
                             const duckdb::Expression& peeled) {
  std::string_view tokenizer;
  const duckdb::Expression* expr = nullptr;
  const duckdb::Value* val = nullptr;
  if (peeled.expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast_expr = peeled.Cast<duckdb::BoundCastExpression>();
    tokenizer = TryGetTokenizerModifier(cast_expr.return_type);
    if (!tokenizer.empty() && cast_expr.child) {
      expr = cast_expr.child.get();
      val = TryGetConstant(UnwrapTSQueryCast(*expr));
    }
  } else if (peeled.expression_class ==
             duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& cv = peeled.Cast<duckdb::BoundConstantExpression>().value;
    tokenizer = TryGetTokenizerModifier(cv.type());
    if (!tokenizer.empty()) {
      val = &cv;
    }
  }
  if (tokenizer.empty()) {
    return false;
  }
  if (tokenizer == irs::StringTokenizer::type_name()) {
    if (val && !val->IsNull() &&
        val->type().id() == duckdb::LogicalTypeId::VARCHAR) {
      BuildFtsTerm(parent, ctx, column_info, *val);
      return true;
    }
    if (expr) {
      BuildTSQuery(parent, ctx.WithTokenizer(ctx.identity), column_info, *expr);
      return true;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("::tokenize('keyword'): inner expression has unsupported "
              "shape"));
  }
  // Wrapper lives on this stack frame; releases the analyzer back to
  // the Tokenizer's pool when the scope exits.
  auto wrapper = ResolveTokenizerAnalyzer(ctx.client_context, tokenizer);
  if (!wrapper) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("::tokenize('", tokenizer, "'): tokenizer not found in catalog"),
      ERR_HINT("Create it via CREATE TEXT SEARCH DICTIONARY "
               "or use 'keyword' for raw bytes."));
  }
  auto sub_ctx = ctx.WithTokenizer(*wrapper);
  if (val) {
    // Cannot recurse on a folded constant -- its type still carries
    // the modifier and would re-enter this branch.
    if (val->IsNull() || val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("::tokenize(<name>): inner value must be "
                              "VARCHAR"));
    }
    BuildFtsTokens(parent, sub_ctx, column_info, val->GetValue<std::string>(),
                   /*require_all=*/false);
    return true;
  }
  BuildTSQuery(parent, sub_ctx, column_info, *expr);
  return true;
}

void BuildTSQuery(irs::BooleanFilter& parent, const FilterContext& ctx,
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
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("BOOLEAN inside TSQUERY must be a constant"),
                        ERR_HINT("Use a literal true / false / NULL."));
      }
      if (val->IsNull() || !val->GetValue<bool>()) {
        AddFilter<irs::Empty>(parent);
      } else {
        AddFilter<irs::All>(parent);
      }
      return;
    }
  }
  if (const auto* val = TryGetConstant(unwrapped); val && val->IsNull()) {
    AddFilter<irs::Empty>(parent);
    return;
  }

  if (TryDispatchBoostCast(parent, ctx, column_info, unwrapped)) {
    return;
  }

  if (TryDispatchTokenizeCast(parent, ctx, column_info, unwrapped)) {
    return;
  }

  // Bare string (promoted via VARCHAR -> TSQUERY cast) -> tokenize via
  // the ambient (column) analyzer. Multi-token input composes with OR
  // (min_match=1) per the plan's "col @@ 'Quick Fox' ≡ ANY_OF(tokens)"
  // rule. Non-VARCHAR / analyzer-less paths fall back to raw ByTerm.
  if (unwrapped.expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& val = unwrapped.Cast<duckdb::BoundConstantExpression>().value;
    if (val.IsNull()) {
      AddFilter<irs::Empty>(parent);
      return;
    }
    if (val.type().id() == duckdb::LogicalTypeId::VARCHAR) {
      BuildFtsTokens(parent, ctx, column_info, val.GetValue<std::string>(),
                     /*require_all=*/false);
      return;
    }
    BuildFtsTerm(parent, ctx, column_info, val);
    return;
  }

  if (unwrapped.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Unsupported TSQUERY expression class: ",
                            static_cast<int>(unwrapped.expression_class)),
                    ERR_HINT("Use a TSQUERY constructor (ts_phrase, ts_like, "
                             "...) or 'literal'::TSQUERY."));
  }

  const auto& func = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  const auto op = ClassifyTSQueryFunction(func.function.name);

  switch (op) {
    case TSQueryOp::Phrase:
      return FromPhrase(parent, ctx, column_info, func);
    case TSQueryOp::Term:
      return FromTerm(parent, ctx, column_info, func);
    case TSQueryOp::Like:
      return FromTSQLike(parent, ctx, column_info, func);
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
    case TSQueryOp::Any:
      return FromAnyAllOf(parent, ctx, column_info, func, /*is_any=*/true);
    case TSQueryOp::All:
      return FromAnyAllOf(parent, ctx, column_info, func, /*is_any=*/false);
    case TSQueryOp::Compound:
      return FromCompound(parent, ctx, column_info, func);
    case TSQueryOp::Between:
      return FromBetween(parent, ctx, column_info, func);
    case TSQueryOp::Regexp:
      return FromRegexp(parent, ctx, column_info, func);
    case TSQueryOp::Less:
      return FromHalfRange(parent, ctx, column_info, func, "ts_lt",
                           /*is_lower=*/false, /*inclusive=*/false);
    case TSQueryOp::LessEq:
      return FromHalfRange(parent, ctx, column_info, func, "ts_le",
                           /*is_lower=*/false, /*inclusive=*/true);
    case TSQueryOp::Greater:
      return FromHalfRange(parent, ctx, column_info, func, "ts_gt",
                           /*is_lower=*/true, /*inclusive=*/false);
    case TSQueryOp::GreaterEq:
      return FromHalfRange(parent, ctx, column_info, func, "ts_ge",
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
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Not a TSQUERY-producing function: ", func.function.name),
        ERR_HINT("Use a TSQUERY constructor (ts_phrase, ts_like, ts_between, "
                 "ts_ngram, ts_levenshtein, ts_regexp, ts_any, ts_all, "
                 "ts_compound, ...) or 'literal'::TSQUERY."));
  }
  SDB_UNREACHABLE();
}

void FromTSQueryMatch(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::BoundFunctionExpression& func) {
  SDB_ASSERT(func.children.size() == 2);
  // @@ is commutative: either side can be the column. Try LHS first,
  // then the cast-stripped RHS. Matches PG `doc @@ q` / `q @@ doc`.
  const auto* left_col = TryGetColumnRef(UnwrapTSQueryCast(*func.children[0]));
  const auto* right_col = TryGetColumnRef(UnwrapTSQueryCast(*func.children[1]));
  const duckdb::BoundColumnRefExpression* column = nullptr;
  const duckdb::Expression* expr = nullptr;
  if (left_col && right_col) {
    const auto* left_info = FindColumnInfo(ctx, *left_col);
    const auto* right_info = FindColumnInfo(ctx, *right_col);
    if (left_info && right_info) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("@@ has column references on both sides"),
        ERR_HINT("Wrap one side in 'word'::TSQUERY or a constructor "
                 "(ts_phrase, ts_like, ...)."));
    }
    column = left_info ? left_col : right_col;
    expr = left_info ? func.children[1].get() : func.children[0].get();
  } else if (left_col) {
    column = left_col;
    expr = func.children[1].get();
  } else if (right_col) {
    column = right_col;
    expr = func.children[0].get();
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("@@ requires a column reference on one side"),
                    ERR_HINT("Use: <col> @@ <tsquery_expr>."));
  }
  const auto* column_info = FindColumnInfo(ctx, *column);
  if (!column_info) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("@@ column not found in inverted index"),
                    ERR_HINT("CREATE INDEX ... USING inverted(<col>)."));
  }
  auto* tokenizer = column_info->tokenizer.analyzer.get();
  if (!tokenizer) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("@@ column has no analyzer (not a text-indexed column)"),
      ERR_HINT("Reindex the VARCHAR column with a text-search analyzer."));
  }
  auto sub_ctx = ctx.WithTokenizer(*tokenizer);
  // BuildTSQuery throws THROW_SQL_ERROR with the standard hint on any
  // dispatch failure, so we don't wrap its result here.
  BuildTSQuery(filter, sub_ctx, *column_info, *expr);
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

Result FromExpression(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& expr) {
  // Peel the BOOSTED_TSQUERY -> BOOLEAN coercion the WHERE-binder
  // inserts when a `(predicate)::boost(K)` cast appears at the
  // predicate root, then dispatch the boost cast itself. If the
  // inner predicate can't be claimed, this throws.
  if (TryDispatchSqlBoostCast(filter, ctx, UnwrapBoostBoolCoercion(expr))) {
    return {};
  }
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
