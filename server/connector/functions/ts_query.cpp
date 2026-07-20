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

#include "connector/functions/ts_query.h"

#include <duckdb/common/exception.hpp>
#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/cast/bound_cast_data.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <iresearch/analysis/tokenizers.hpp>

#include "connector/functions/search.h"
#include "connector/functions/ts_common.hpp"
#include "connector/functions/ts_query_codec.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

duckdb::LogicalType MakeTSQueryStructType(std::string_view alias) {
  duckdb::child_list_t<duckdb::LogicalType> children;
  children.emplace_back("text", duckdb::LogicalType::VARCHAR);
  children.emplace_back("tokenizer", duckdb::LogicalType::VARCHAR);
  children.emplace_back("boost", duckdb::LogicalType::FLOAT);
  auto type = duckdb::LogicalType::STRUCT(std::move(children));
  type.SetAlias(std::string{alias});
  return type;
}

duckdb::LogicalType MakeModifierTSQueryType() {
  return MakeTSQueryStructType(kModifierTSQueryTypeName);
}

struct TSQueryCastData final : duckdb::BoundCastData {
  float boost = 1.0f;

  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const override {
    return duckdb::make_uniq<TSQueryCastData>(*this);
  }
};

const duckdb::Value* TryGetTypeModifier(const duckdb::LogicalType& type) {
  if (type.GetAlias() != kModifierTSQueryTypeName || !type.HasExtensionInfo()) {
    return nullptr;
  }
  const auto& mods = type.GetExtensionInfo()->modifiers;
  if (mods.empty() || mods[0].value.IsNull()) {
    return nullptr;
  }
  return &mods[0].value;
}

bool HasTokenizerModifier(const duckdb::LogicalType& type) {
  const auto* mod = TryGetTypeModifier(type);
  return mod && mod->type().id() == duckdb::LogicalTypeId::VARCHAR;
}

TSQueryCastData ReadTargetBoost(const duckdb::LogicalType& target) {
  TSQueryCastData data;
  const auto* mod = TryGetTypeModifier(target);
  if (mod && mod->type().id() == duckdb::LogicalTypeId::DOUBLE) {
    data.boost = static_cast<float>(mod->GetValue<double>());
  }
  return data;
}

bool ThrowingTokenizeCast(duckdb::Vector&, duckdb::Vector&, duckdb::idx_t,
                          duckdb::CastParameters&) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("::tokenize(...) is only meaningful inside an `@@` match "
            "against an inverted-indexed column."));
}

struct TSQueryStructWriter {
  duckdb::Vector& text;
  duckdb::Vector& tokenizer;
  duckdb::Vector& boost;

  explicit TSQueryStructWriter(duckdb::Vector& result)
    : text{duckdb::StructVector::GetEntries(result)[kTSQueryTextChild]},
      tokenizer{
        duckdb::StructVector::GetEntries(result)[kTSQueryTokenizerChild]},
      boost{duckdb::StructVector::GetEntries(result)[kTSQueryBoostChild]} {}

  void Write(duckdb::idx_t row, std::string_view text_value,
             std::string_view tokenizer_value, float boost_value) {
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(text)[row] =
      duckdb::StringVector::AddString(text, text_value.data(),
                                      text_value.size());
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(tokenizer)[row] =
      duckdb::StringVector::AddString(tokenizer, tokenizer_value.data(),
                                      tokenizer_value.size());
    duckdb::FlatVector::GetDataMutable<float>(boost)[row] = boost_value;
  }
};

struct TSQueryRowView {
  std::string_view text;
  std::string_view tokenizer;
  float boost = 1.0f;
};

std::optional<TSQueryRowView> ReadTSQueryRow(const duckdb::Vector& vec,
                                             duckdb::idx_t row) {
  if (duckdb::FlatVector::IsNull(vec, row)) {
    return std::nullopt;
  }
  auto& entries = duckdb::StructVector::GetEntries(vec);
  auto& text_vec = entries[kTSQueryTextChild];
  if (duckdb::FlatVector::IsNull(text_vec, row)) {
    return std::nullopt;
  }
  TSQueryRowView parts;
  const auto& text =
    duckdb::FlatVector::GetData<duckdb::string_t>(text_vec)[row];
  parts.text = {text.GetData(), text.GetSize()};
  auto& tok_vec = entries[kTSQueryTokenizerChild];
  if (!duckdb::FlatVector::IsNull(tok_vec, row)) {
    const auto& tok =
      duckdb::FlatVector::GetData<duckdb::string_t>(tok_vec)[row];
    parts.tokenizer = {tok.GetData(), tok.GetSize()};
  }
  auto& boost_vec = entries[kTSQueryBoostChild];
  if (!duckdb::FlatVector::IsNull(boost_vec, row)) {
    parts.boost = duckdb::FlatVector::GetData<float>(boost_vec)[row];
  }
  return parts;
}

bool TSQueryFromStringCast(duckdb::Vector& source, duckdb::Vector& result,
                           duckdb::idx_t count,
                           duckdb::CastParameters& params) {
  const auto& data = params.cast_data->Cast<TSQueryCastData>();
  duckdb::UnifiedVectorFormat fmt;
  source.ToUnifiedFormat(count, fmt);
  const auto* src = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  TSQueryStructWriter writer{result};
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (!fmt.validity.RowIsValid(idx)) {
      duckdb::FlatVector::SetNull(result, i, true);
      continue;
    }
    writer.Write(i, {src[idx].GetData(), src[idx].GetSize()}, {}, data.boost);
  }
  return true;
}

duckdb::BoundCastInfo BindTSQueryFromStringCast(
  duckdb::BindCastInput&, const duckdb::LogicalType&,
  const duckdb::LogicalType& target) {
  if (HasTokenizerModifier(target)) {
    return duckdb::BoundCastInfo(ThrowingTokenizeCast);
  }
  return {TSQueryFromStringCast,
          duckdb::make_uniq<TSQueryCastData>(ReadTargetBoost(target))};
}

bool TSQueryBoostCast(duckdb::Vector& source, duckdb::Vector& result,
                      duckdb::idx_t count, duckdb::CastParameters& params) {
  const auto& data = params.cast_data->Cast<TSQueryCastData>();
  source.Flatten(count);
  TSQueryStructWriter writer{result};
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto parts = ReadTSQueryRow(source, i);
    if (!parts) {
      duckdb::FlatVector::SetNull(result, i, true);
      continue;
    }
    writer.Write(i, parts->text, parts->tokenizer, parts->boost * data.boost);
  }
  return true;
}

duckdb::BoundCastInfo BindTSQueryBoostCast(duckdb::BindCastInput&,
                                           const duckdb::LogicalType& source,
                                           const duckdb::LogicalType& target) {
  if (HasTokenizerModifier(source) || HasTokenizerModifier(target)) {
    return duckdb::BoundCastInfo(ThrowingTokenizeCast);
  }
  return {TSQueryBoostCast,
          duckdb::make_uniq<TSQueryCastData>(ReadTargetBoost(target))};
}

bool TSQueryToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                          duckdb::idx_t count, duckdb::CastParameters&) {
  source.Flatten(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto parts = ReadTSQueryRow(source, i);
    if (!parts) {
      duckdb::FlatVector::SetNull(result, i, true);
      continue;
    }
    const auto rendered =
      RenderTSQuery(parts->text, parts->tokenizer, parts->boost);
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result)[i] =
      duckdb::StringVector::AddString(result, rendered);
  }
  return true;
}

duckdb::BoundCastInfo BindTSQueryToVarcharCast(
  duckdb::BindCastInput&, const duckdb::LogicalType& source,
  const duckdb::LogicalType&) {
  if (HasTokenizerModifier(source)) {
    return duckdb::BoundCastInfo(ThrowingTokenizeCast);
  }
  return duckdb::BoundCastInfo(TSQueryToVarcharCast);
}

void TSQueryBoostFn(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  const auto count = args.size();
  args.Flatten();
  const auto* factors = duckdb::FlatVector::GetData<double>(args.data[1]);
  TSQueryStructWriter writer{result};
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto parts = ReadTSQueryRow(args.data[0], i);
    if (!parts || duckdb::FlatVector::IsNull(args.data[1], i)) {
      duckdb::FlatVector::SetNull(result, i, true);
      continue;
    }
    const auto factor = factors[i];
    if (factor < 0.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("boost factor must be >= 0, got ", factor),
                      ERR_HINT("Example: ts_phrase('text') ^ 2.0."));
    }
    writer.Write(i, parts->text, parts->tokenizer,
                 parts->boost * static_cast<float>(factor));
  }
}

// Folds ts_* constructor/operator/parser calls into TSQUERY constants whose
// text is the rendered SQL form. Registered as a pre-optimize hook so it runs
// before the anchored iresearch claim regardless of which built-in optimizers
// are enabled. Folds bottom-up (children first) -- the order that defines the
// canonical text pinned in tsquery_type.test. `^` (TSQueryBoostFn) is a real
// value function: excluded here, folded by the expression rewriter instead.
class TSQueryFoldVisitor final : public duckdb::LogicalOperatorVisitor {
 public:
  explicit TSQueryFoldVisitor(duckdb::ClientContext& context)
    : _context{context} {}

  void VisitExpression(duckdb::unique_ptr<duckdb::Expression>* slot) final {
    VisitExpressionChildren(**slot);
    const auto& expr = **slot;
    if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
      return;
    }
    const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
    if (!IsTSQueryStructType(func.GetReturnType())) {
      return;
    }
    const auto name = func.Function().GetName().GetIdentifierName();
    const auto op = ClassifyTSQueryFunction(name);
    if (op == TSQueryOp::Unknown || op == TSQueryOp::Boost) {
      return;
    }
    auto folded = TryFoldTSQueryCall(_context, name, func.GetChildren());
    if (!folded) {
      return;
    }
    *slot =
      duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(*folded));
  }

 private:
  duckdb::ClientContext& _context;
};

void FoldTSQueryConstants(duckdb::OptimizerExtensionInput& input,
                          duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan) {
    TSQueryFoldVisitor{input.context}.VisitOperator(*plan);
  }
}

void TSQueryStubFn(duckdb::DataChunk& /*args*/,
                   duckdb::ExpressionState& /*state*/,
                   duckdb::Vector& /*result*/) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("TSQUERY expression evaluated outside an `@@` match against an "
            "inverted-indexed column."));
}

duckdb::ScalarFunction TSQConstructor(
  duckdb::vector<duckdb::LogicalType> args) {
  return duckdb::ScalarFunction(std::move(args), MakeTSQueryType(),
                                TSQueryStubFn);
}

duckdb::ScalarFunction TSQConstructor(
  duckdb::Identifier name, duckdb::vector<duckdb::LogicalType> args) {
  return duckdb::ScalarFunction(std::move(name), std::move(args),
                                MakeTSQueryType(), TSQueryStubFn);
}

void RegisterTSQueryTypes(duckdb::ExtensionLoader& loader) {
  loader.RegisterType(std::string{kTSQueryTypeName}, MakeTSQueryType());

  // `tokenize(<analyzer-name>)` is a parameterized type registered as
  // a TSQUERY variant. The bind function consumes a single VARCHAR
  // modifier and stores it in ExtensionTypeInfo so the filter builder
  // can route the inner expression through the named analyzer instead
  // of the ambient one. The modifier travels with the LogicalType into
  // BoundCastExpression.return_type; every tokenize-modifier cast
  // throws at runtime, so the cast expression always survives binding
  // and constant folding for the walker to dispatch at `@@`.
  loader.RegisterType(
    std::string{kTokenizerTypeName}, MakeTSQueryType(),
    +[](duckdb::BindLogicalTypeInput& input) -> duckdb::LogicalType {
      const auto& modifiers = input.modifiers;
      if (modifiers.size() != 1) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("tokenize(<analyzer-name>) requires exactly "
                                "one VARCHAR argument"));
      }
      const auto& v = modifiers[0].GetValue();
      if (!v.IsNull() && v.type().id() != duckdb::LogicalTypeId::VARCHAR) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("tokenize() argument must be a VARCHAR "
                                "analyzer name or NULL"));
      }
      // NULL is sugar for 'keyword' -- both mean "no tokenisation,
      // treat the bytes as a single raw term". Normalize to a literal
      // 'keyword' so the filter builder's existing identity branch
      // handles it without a separate null-check path.
      auto resolved =
        v.IsNull()
          ? duckdb::Value(std::string{irs::StringTokenizer::type_name()})
          : v;
      // Return TSQUERY_MODIFIER (distinct alias from TSQUERY) so a
      // `<TSQ-typed expr>::tokenize(...)` cast doesn't get short-
      // circuited by DuckDB's same-alias cast elision. Modifier types
      // with different modifier VALUES (or value types -- DOUBLE for
      // boost, VARCHAR for tokenize) compare unequal, so chained
      // `::boost(K)::tokenize(n)` casts survive too. The cost-0 casts
      // registered below let modifier-typed expressions flow back into
      // TSQUERY-expecting overloads (`@@`, `||`, `&&`, `##`, ...)
      // transparently.
      auto type = MakeModifierTSQueryType();
      auto info = duckdb::make_uniq<duckdb::ExtensionTypeInfo>();
      info->modifiers.emplace_back(resolved);
      // Note: the bind callback only stashes the analyzer NAME. The
      // analyzer is stateful (one tokenization stream per use), so it
      // would be unsafe to share a single resolved pointer across all
      // queries / threads that use this type. Resolution happens at
      // filter-build time (search_filter_builder.cpp) where each call
      // gets its own AnalyzerWrapper from the catalog Tokenizer's pool
      // and releases it back when the call returns.
      type.SetExtensionInfo(std::move(info));
      return type;
    });

  // `boost(<factor>)` parameterised type: parallel to tokenize, with
  // a DOUBLE modifier instead of VARCHAR. The modifier value type is
  // what distinguishes boost from tokenize on the shared
  // TSQUERY_MODIFIER alias.
  loader.RegisterType(
    std::string{kBoostTypeName}, MakeTSQueryType(),
    +[](duckdb::BindLogicalTypeInput& input) -> duckdb::LogicalType {
      const auto& modifiers = input.modifiers;
      if (modifiers.size() != 1) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("boost(<factor>) requires exactly one numeric argument"));
      }
      auto factor =
        modifiers[0].GetValue().DefaultCastAs(duckdb::LogicalType::DOUBLE);
      if (factor.IsNull()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("boost() factor must be non-null"));
      }
      if (factor.GetValue<double>() < 0.0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("boost() factor must be >= 0, got ",
                                factor.GetValue<double>()));
      }
      auto type = MakeModifierTSQueryType();
      auto info = duckdb::make_uniq<duckdb::ExtensionTypeInfo>();
      info->modifiers.emplace_back(std::move(factor));
      type.SetExtensionInfo(std::move(info));
      return type;
    });
}

void RegisterTSQueryAliasCasts(duckdb::ExtensionLoader& loader) {
  const auto tsq = MakeTSQueryType();
  const auto modifier = MakeModifierTSQueryType();

  for (const auto& target : {tsq, modifier}) {
    loader.RegisterCastFunction(duckdb::LogicalType::VARCHAR, target,
                                BindTSQueryFromStringCast, 50);
    loader.RegisterCastFunction(duckdb::LogicalType::BLOB, target,
                                BindTSQueryFromStringCast, 50);
    loader.RegisterCastFunction(
      duckdb::LogicalType::SQLNULL, target,
      duckdb::BoundCastInfo(duckdb::DefaultCasts::TryVectorNullCast), 149);
    loader.RegisterCastFunction(target, duckdb::LogicalType::VARCHAR,
                                BindTSQueryToVarcharCast, 100);
  }
  loader.RegisterCastFunction(tsq, modifier, BindTSQueryBoostCast, 0);
  loader.RegisterCastFunction(modifier, tsq, BindTSQueryBoostCast, 0);
  loader.RegisterCastFunction(modifier, modifier, BindTSQueryBoostCast, 0);
}

void RegisterTSQueryBoolCasts(duckdb::ExtensionLoader& loader) {
  // BOOLEAN -> TSQUERY: lets `true` / `false` flow into any TSQUERY
  // position. The runtime function throws -- which makes the cast
  // non-foldable, so the BoundCastExpression survives in the bound
  // tree. The walker intercepts it and emits irs::All / irs::Empty.
  auto bool_cast_bind =
    +[](duckdb::BindCastInput&, const duckdb::LogicalType&,
        const duckdb::LogicalType&) -> duckdb::BoundCastInfo {
    return duckdb::BoundCastInfo(+[](duckdb::Vector&, duckdb::Vector&,
                                     duckdb::idx_t,
                                     duckdb::CastParameters&) -> bool {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("BOOLEAN -> TSQUERY: only meaningful inside TSQUERY context"));
    });
  };
  loader.RegisterCastFunction(duckdb::LogicalType::BOOLEAN, MakeTSQueryType(),
                              bool_cast_bind,
                              /*implicit_cast_cost=*/0);

  // BOOLEAN <-> TSQUERY_MODIFIER: lets `(predicate)::boost(K)` apply
  // to plain SQL conditions outside `@@`. Both directions throw at
  // runtime -- the optimizer extension claims the boost cast at
  // bind time when the inner predicate is index-claimable; if it
  // isn't, the throwing stub fires with a specific message
  // pointing the user back to `@@`.
  auto boost_bool_cast_bind =
    +[](duckdb::BindCastInput&, const duckdb::LogicalType&,
        const duckdb::LogicalType&) -> duckdb::BoundCastInfo {
    return duckdb::BoundCastInfo(+[](duckdb::Vector&, duckdb::Vector&,
                                     duckdb::idx_t,
                                     duckdb::CastParameters&) -> bool {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("::boost(K) used on a predicate the inverted index could not "
                "claim -- boost is only meaningful inside an inverted-index "
                "match. Move the boost into an `@@` match or remove it."));
    });
  };
  // Cost 50 mirrors VARCHAR -> TSQUERY_MODIFIER (above): keeps plain
  // BOOLEAN operands from sweeping into TSQUERY overloads when no
  // boost was actually requested.
  loader.RegisterCastFunction(duckdb::LogicalType::BOOLEAN,
                              MakeModifierTSQueryType(), boost_bool_cast_bind,
                              /*implicit_cast_cost=*/50);
  // Cost 0: this is the WHERE-clause coercion DuckDB inserts when a
  // `(predicate)::boost(K)` cast appears at the predicate root (the
  // WhereBinder unconditionally adds a cast to BOOLEAN).
  loader.RegisterCastFunction(MakeModifierTSQueryType(),
                              duckdb::LogicalType::BOOLEAN,
                              boost_bool_cast_bind,
                              /*implicit_cast_cost=*/0);
}

void RegisterTSQueryListCast(duckdb::ExtensionLoader& loader) {
  loader.RegisterCastFunction(
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
    duckdb::LogicalType::LIST(MakeTSQueryType()),
    +[](duckdb::BindCastInput& input, const duckdb::LogicalType& source,
        const duckdb::LogicalType& target) -> duckdb::BoundCastInfo {
      return duckdb::BoundCastInfo(
        duckdb::ListCast::ListToListCast,
        duckdb::ListBoundCastData::BindListToListCast(input, source, target),
        duckdb::ListBoundCastData::InitListLocalState);
    },
    /*implicit_cast_cost=*/0);
}

void RegisterTSQueryConstructors(duckdb::ExtensionLoader& loader) {
  // ts_phrase(text [, gap, text, gap, text, ...]) -- tokenises each text
  // pattern via the ambient analyzer and chains them into one
  // irs::ByPhrase. Gap args are bare INTEGERs (exact gap N) or 2-element
  // INTEGER[] / arrays ([min, max] range). See FromPhrase for the full
  // grammar.
  // Each text-arg TSQ constructor accepts VARCHAR and BLOB.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQPhrase}};
    for (auto first_arg :
         {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB}) {
      auto fn = TSQConstructor(duckdb::Identifier{kTSQPhrase}, {first_arg});
      fn.SetVarArgs(duckdb::LogicalType::ANY);
      set.AddFunction(std::move(fn));
    }
    loader.RegisterFunction(std::move(set));
  }

  // NGRAM(text [, threshold]) -- tokenises via ambient analyzer.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQNgram}};
    for (auto first_arg :
         {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB}) {
      set.AddFunction(TSQConstructor({first_arg}));
      set.AddFunction(TSQConstructor({first_arg, duckdb::LogicalType::DOUBLE}));
    }
    loader.RegisterFunction(std::move(set));
  }

  // ts_like(pattern) / PREFIX(text) -- raw, no tokenisation.
  for (auto name : {kTSQLike, kTSQPrefix}) {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
    for (auto first_arg :
         {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB}) {
      set.AddFunction(TSQConstructor({first_arg}));
    }
    loader.RegisterFunction(std::move(set));
  }

  // LESS / LESS_EQ / GREATER / GREATER_EQ -- single-bound range
  // constructors. Each takes one bound value (VARCHAR / numeric /
  // BOOLEAN). The filter builder dispatches per column type: VARCHAR
  // bounds tokenise through the ambient analyzer (single-token
  // requirement) and emit irs::ByRange; numeric bounds emit
  // irs::ByGranularRange; BOOLEAN bounds emit irs::ByRange via
  // BooleanTokenizer. Bound vs column type mismatch is a bind-time
  // error. NULL bound is also a bind-time error -- use
  // RANGE(NULL, ..., ...) for unbounded semantics.
  //
  // SPECIAL_HANDLING is required so DuckDB doesn't constant-fold a
  // NULL bound to NULL before the filter builder sees it.
  for (auto name : {kTSQLess, kTSQLessEq, kTSQGreater, kTSQGreaterEq}) {
    auto fn =
      TSQConstructor(duckdb::Identifier{name}, {duckdb::LogicalType::ANY});
    fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(std::move(fn));
  }

  // REGEXP(pattern [, syntax]) -- raw regex match against indexed
  // terms. `syntax` is 'perl' (default) or 'posix'. No tokenisation;
  // the pattern is matched directly against terms in the field.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQRegexp}};
    for (auto first_arg :
         {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB}) {
      set.AddFunction(TSQConstructor({first_arg}));
      set.AddFunction(
        TSQConstructor({first_arg, duckdb::LogicalType::VARCHAR}));
    }
    loader.RegisterFunction(std::move(set));
  }

  // LEVENSHTEIN(term [, distance [, transpositions [, prefix]]]) -- raw
  // fuzzy match. The optional `prefix` is a literal leading substring
  // that must match exactly; only the suffix after `prefix` participates
  // in edit-distance computation. The 1-arg form `ts_levenshtein('term')`
  // picks the distance automatically from term length.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQLevenshtein}};
    for (auto first_arg :
         {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB}) {
      set.AddFunction(TSQConstructor({first_arg}));
      set.AddFunction(
        TSQConstructor({first_arg, duckdb::LogicalType::INTEGER}));
      set.AddFunction(TSQConstructor({first_arg, duckdb::LogicalType::INTEGER,
                                      duckdb::LogicalType::BOOLEAN}));
      set.AddFunction(TSQConstructor({first_arg, duckdb::LogicalType::INTEGER,
                                      duckdb::LogicalType::BOOLEAN,
                                      duckdb::LogicalType::VARCHAR}));
    }
    loader.RegisterFunction(std::move(set));
  }

  // ANY_OF / ALL_OF -- list-form only (no variadic). Variadic forms
  // were dropped because a (TSQUERY[], INTEGER) overload absorbs them:
  // DuckDB's STRING_LITERAL cast resolver prefers STRING_LITERAL ->
  // INTEGER over STRING_LITERAL -> TSQUERY, making `ANY_OF('a', 'b')`
  // silently bind to (TSQUERY[], INTEGER) and crash at runtime. With
  // list-only call forms (`ANY_OF([...])`, `ANY_OF([...], N)`,
  // `ALL_OF([...])`), there's no ambiguity.
  //
  // Bare-string lists (`['a', 'b']`) are typed VARCHAR[] by the
  // binder; the registered VARCHAR[] -> TSQUERY[] list-cast (above)
  // lifts them to TSQUERY[] at cost 0 per element.
  //
  // Fixed-length ARRAY inputs (e.g. `CAST([...] AS VARCHAR[N])`,
  // `CAST(... AS TSQUERY[N])`) are accepted via parallel unsized-ARRAY
  // overloads. DuckDB matches an unsized ARRAY type against any
  // ARRAY(T, N), and the filter-builder dispatch handles ARRAY children
  // alongside LIST.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQAnyOf}};
    set.AddFunction(
      TSQConstructor({duckdb::LogicalType::LIST(MakeTSQueryType())}));
    set.AddFunction(
      TSQConstructor({duckdb::LogicalType::LIST(MakeTSQueryType()),
                      duckdb::LogicalType::INTEGER}));
    set.AddFunction(TSQConstructor(
      {duckdb::LogicalType::ARRAY(MakeTSQueryType(), duckdb::optional_idx{})}));
    set.AddFunction(TSQConstructor(
      {duckdb::LogicalType::ARRAY(MakeTSQueryType(), duckdb::optional_idx{}),
       duckdb::LogicalType::INTEGER}));
    loader.RegisterFunction(std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQAllOf}};
    set.AddFunction(
      TSQConstructor({duckdb::LogicalType::LIST(MakeTSQueryType())}));
    set.AddFunction(TSQConstructor(
      {duckdb::LogicalType::ARRAY(MakeTSQueryType(), duckdb::optional_idx{})}));
    loader.RegisterFunction(std::move(set));
  }

  // compound(must, must_not, should [, min_should_match]) -- ES-style
  // bool query. Each of the first three args is TSQUERY (single
  // clause), TSQUERY[] (multiple clauses), or NULL (no clauses).
  // Optional 4th INTEGER is min_should_match (default 1). 16 overloads
  // = 2 (3-arg / 4-arg) * 2^3 (TSQUERY vs TSQUERY[] per arg).
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQCompound}};
    const std::array<duckdb::LogicalType, 2> opts{
      MakeTSQueryType(), duckdb::LogicalType::LIST(MakeTSQueryType())};
    auto register_one = [&](std::vector<duckdb::LogicalType> args) {
      auto fn = duckdb::ScalarFunction(std::move(args), MakeTSQueryType(),
                                       TSQueryStubFn);
      // Without SPECIAL_HANDLING, DuckDB folds any call with a NULL
      // arg to NULL at bind time; we'd never see the user's bucket
      // structure (e.g. `compound(list, NULL, NULL)` -> NULL).
      fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      set.AddFunction(std::move(fn));
    };
    for (const auto& a : opts) {
      for (const auto& b : opts) {
        for (const auto& c : opts) {
          register_one({a, b, c});
          register_one({a, b, c, duckdb::LogicalType::INTEGER});
        }
      }
    }
    loader.RegisterFunction(std::move(set));
  }

  // TOKENIZE(text [, analyzer]). 1-arg uses ambient analyzer (same as
  // bare VARCHAR); 2-arg uses the named analyzer (equivalent to
  // text::tokenize(analyzer)).
  //
  // Array overloads TOKENIZE(text_array [, analyzer]) -> TSQUERY[]
  // produce a flattened token list -- each element is tokenised, and
  // every produced token becomes one TSQUERY leaf. Composes naturally
  // with ANY_OF / ALL_OF whose list-arg shapes accept TSQUERY[].
  //
  // Both LIST(VARCHAR) and unsized ARRAY(VARCHAR) input shapes are
  // registered; the filter-builder dispatch handles both.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQTokenize}};
    for (auto first_arg :
         {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB}) {
      set.AddFunction(TSQConstructor({first_arg}));
      set.AddFunction(
        TSQConstructor({first_arg, duckdb::LogicalType::VARCHAR}));
      set.AddFunction(duckdb::ScalarFunction(
        {duckdb::LogicalType::LIST(first_arg)},
        duckdb::LogicalType::LIST(MakeTSQueryType()), TSQueryStubFn));
      set.AddFunction(duckdb::ScalarFunction(
        {duckdb::LogicalType::LIST(first_arg), duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::LIST(MakeTSQueryType()), TSQueryStubFn));
      set.AddFunction(duckdb::ScalarFunction(
        {duckdb::LogicalType::ARRAY(first_arg, duckdb::optional_idx{})},
        duckdb::LogicalType::LIST(MakeTSQueryType()), TSQueryStubFn));
      set.AddFunction(duckdb::ScalarFunction(
        {duckdb::LogicalType::ARRAY(first_arg, duckdb::optional_idx{}),
         duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::LIST(MakeTSQueryType()), TSQueryStubFn));
    }
    loader.RegisterFunction(std::move(set));
  }

  // RANGE(min, max, min_incl, max_incl) -- TSQUERY range constructor.
  // Mirrors SQL BETWEEN with explicit inclusivity. The first two args
  // are constants of the same value class (text / numeric / boolean);
  // either may be NULL to indicate an unbounded side. Picks irs::ByRange
  // for VARCHAR + BOOLEAN columns and irs::ByGranularRange for numeric
  // columns at filter-build time. Bind-time uses LogicalType::ANY for
  // the value args so VARCHAR / numeric / BOOLEAN / NULL all resolve
  // without per-type overload bloat; the filter builder validates
  // value-type-vs-column-type and rejects mixes. SPECIAL_HANDLING is
  // required so DuckDB doesn't constant-fold the call to NULL when a
  // bound is NULL (NULL operands have meaning here -- "unbounded").
  {
    auto fn = TSQConstructor(
      duckdb::Identifier{kTSQBetween},
      {duckdb::LogicalType::ANY, duckdb::LogicalType::ANY,
       duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::BOOLEAN});
    fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(std::move(fn));
  }
}

// PG-compat tsquery parser functions: to_tsquery, plainto_tsquery,
// phraseto_tsquery, websearch_to_tsquery, tsquery_phrase. All
// throwing stubs claimed by the filter builder at bind time.
void RegisterTSQueryParserFunctions(duckdb::ExtensionLoader& loader) {
  // to_tsquery(VARCHAR) -> TSQUERY -- Lucene parser, wiring deferred.
  {
    auto fn = TSQConstructor(duckdb::Identifier{kToTsquery},
                             {duckdb::LogicalType::VARCHAR});
    loader.RegisterFunction(std::move(fn));
  }

  // plainto_tsquery / phraseto_tsquery / websearch_to_tsquery each take
  // one VARCHAR and produce a TSQUERY via their own semantics.
  for (auto name : {kPlainToTsquery, kPhraseToTsquery, kWebsearchToTsquery}) {
    auto fn =
      TSQConstructor(duckdb::Identifier{name}, {duckdb::LogicalType::VARCHAR});
    loader.RegisterFunction(std::move(fn));
  }

  // tsquery_phrase(q1, q2 [, distance]) -- function form of `##`.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTsqueryPhrase}};
    set.AddFunction(TSQConstructor({MakeTSQueryType(), MakeTSQueryType()}));
    set.AddFunction(TSQConstructor(
      {MakeTSQueryType(), MakeTSQueryType(), duckdb::LogicalType::INTEGER}));
    loader.RegisterFunction(std::move(set));
  }
}

void RegisterTSQueryOperators(duckdb::ExtensionLoader& loader) {
  for (auto name : {kTSQueryOr, kTSQueryAnd}) {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
    set.AddFunction(TSQConstructor({MakeTSQueryType(), MakeTSQueryType()}));
    set.AddFunction(
      TSQConstructor({MakeTSQueryType(), duckdb::LogicalType::VARCHAR}));
    set.AddFunction(
      TSQConstructor({duckdb::LogicalType::VARCHAR, MakeTSQueryType()}));
    loader.RegisterFunction(std::move(set));
  }

  {
    duckdb::ScalarFunction fn(duckdb::Identifier{kTSQueryNot},
                              {MakeTSQueryType()}, MakeTSQueryType(),
                              TSQueryStubFn);
    loader.RegisterFunction(std::move(fn));
  }

  loader.RegisterFunction(
    duckdb::ScalarFunction(duckdb::Identifier{kTSQueryBoost},
                           {MakeTSQueryType(), duckdb::LogicalType::DOUBLE},
                           MakeTSQueryType(), TSQueryBoostFn));

  // Phrase sequence `a ## b` (strictly adjacent), `a ## N ## b` (gap N),
  // `a ## [lo, hi] ## b` (interval).
  //
  // Overload set kept minimal to dodge two DuckDB binder quirks:
  //   1. STRING_LITERAL has special cast preferences (VARCHAR=1,
  //      numeric=19, other incl. aliased VARCHAR like TSQUERY=20).
  //      So `'a' ## 'b'` would prefer (TSQUERY, INTEGER) over
  //      (TSQUERY, TSQUERY) without explicit VARCHAR mirrors.
  //   2. (TSQUERY, INTEGER) ties with (VARCHAR, INTEGER) for bare-
  //      string LHS like `'a' ## 1` (V->T cost 0 vs V exact).
  //
  //   (TSQUERY, TSQUERY)        canonical adjacent form.
  //   (VARCHAR, VARCHAR)        bare-string adjacent (`'a' ## 'b'`).
  //                             Wins over (VARCHAR, INTEGER) which
  //                             would otherwise cast 'b' to INT.
  //   (VARCHAR, INTEGER)        gap. VARCHAR-LHS only avoids the
  //                             aforementioned (TSQUERY, INTEGER)
  //                             tie. Composite TSQUERY-LHS gap calls
  //                             reach via T->V cast (cost 5).
  //   (VARCHAR, INTEGER[])      interval-gap counterpart.
  //
  // The walker treats VARCHAR-typed children identically to TSQUERY
  // ones (both unwrap via UnwrapTSQueryCast).
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQueryPhraseSeq}};
    set.AddFunction(TSQConstructor({MakeTSQueryType(), MakeTSQueryType()}));
    set.AddFunction(TSQConstructor(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}));
    set.AddFunction(TSQConstructor(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::INTEGER}));
    set.AddFunction(TSQConstructor(
      {duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER)}));
    loader.RegisterFunction(std::move(set));
  }

  loader.RegisterFunction(
    duckdb::ScalarFunction(duckdb::Identifier{kTSQueryMatch},
                           {duckdb::LogicalType::ANY, MakeTSQueryType()},
                           duckdb::LogicalType::BOOLEAN, TSQueryStubFn));
}

// Stub registrations for sugar predicates -- the filter builder
// rewrites each `<name>(col, args...)` call to `col @@ ts_*(args...)`
// at bind time, so these never execute as scalar functions.
void RegisterPredicateFunctions(duckdb::ExtensionLoader& loader) {
  {
    duckdb::ScalarFunction fn(
      duckdb::Identifier{kPhraseMatches},
      {duckdb::LogicalType::ANY, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn);
    fn.SetVarArgs(duckdb::LogicalType::ANY);
    loader.RegisterFunction(std::move(fn));
  }

  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kNgramMatches}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ANY, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ANY, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    loader.RegisterFunction(std::move(set));
  }

  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kLevenshteinMatches}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ANY, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::INTEGER},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ANY, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ANY, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    loader.RegisterFunction(std::move(set));
  }

  loader.RegisterFunction(duckdb::ScalarFunction(
    duckdb::Identifier{kHasAllTokens},
    {duckdb::LogicalType::ANY,
     duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
    duckdb::LogicalType::BOOLEAN, SearchStubFn));

  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kHasAnyTokens}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ANY,
       duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ANY,
       duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
       duckdb::LogicalType::INTEGER},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ANY, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ANY, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::INTEGER},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    loader.RegisterFunction(std::move(set));
  }
}

void RegisterTSQueryConstantFolding(duckdb::ExtensionLoader& loader) {
  duckdb::OptimizerExtension fold;
  fold.pre_optimize_function = FoldTSQueryConstants;
  duckdb::OptimizerExtension::Register(loader.GetDatabaseInstance().config,
                                       std::move(fold));
}

// All scorers share `name(BIGINT tableoid [, params...]) -> FLOAT`; the
// iresearch_plan rule claims each call at compile time and threads the
// scorer into bind_data -- the stub fires only if a call escapes the rule.

}  // namespace

duckdb::LogicalType MakeTSQueryType() {
  return MakeTSQueryStructType(kTSQueryTypeName);
}

void RegisterTSQueryFunctions(duckdb::ExtensionLoader& loader) {
  RegisterTSQueryTypes(loader);
  RegisterTSQueryAliasCasts(loader);
  RegisterTSQueryBoolCasts(loader);
  RegisterTSQueryListCast(loader);
  RegisterTSQueryConstructors(loader);
  RegisterTSQueryParserFunctions(loader);
  RegisterTSQueryOperators(loader);
  RegisterPredicateFunctions(loader);
  RegisterTSQueryConstantFolding(loader);
}

}  // namespace sdb::connector
