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

#include "connector/functions/search.h"

#include <duckdb/common/exception.hpp>
#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/cast/bound_cast_data.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/utils/utf8_utils.hpp>

#include "catalog/scorer_options.h"
#include "catalog/tokenizer.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/split_by_non_alpha.h"
#include "connector/functions/ts_highlight.h"
#include "connector/functions/ts_lexize.h"
#include "connector/functions/ts_offsets.h"
#include "connector/functions/vector.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::connector {
namespace {

duckdb::LogicalType MakeTokenizedTSQueryType() {
  auto type = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
  type.SetAlias(std::string{kTokenizedTSQueryTypeName});
  return type;
}

duckdb::LogicalType MakeBoostedTSQueryType() {
  auto type = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
  type.SetAlias(std::string{kBoostedTSQueryTypeName});
  return type;
}

void SearchStubFn(duckdb::DataChunk& /*args*/,
                  duckdb::ExpressionState& /*state*/,
                  duckdb::Vector& /*result*/) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("Inverted index function called outside inverted index context. "
            "Use in WHERE clause on a table with an inverted index."));
}

void TSQueryStubFn(duckdb::DataChunk& /*args*/,
                   duckdb::ExpressionState& /*state*/,
                   duckdb::Vector& /*result*/) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("TSQUERY expression evaluated outside an `@@` match against an "
            "inverted-indexed column."));
}

void ScorerStubFn(duckdb::DataChunk& /*args*/, duckdb::ExpressionState& state,
                  duckdb::Vector& /*result*/) {
  const auto& fn_name =
    state.expr.Cast<duckdb::BoundFunctionExpression>().Function().GetName();
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG(fn_name.GetIdentifierName(),
                          "() requires an inverted index scan in the same "
                          "sub-query"));
}

// Each stub needs distinct callback pointers: DuckDB compares
// AggregateFunctions by callbacks only (ignoring name), so sharing one stub set
// would let the binder dedup two term-dict aggregates with the same return type
// (e.g. ts_dict_min/ts_dict_max) into one before the optimizer can claim them.
template<int Tag>
struct TsDictStub {
  static duckdb::idx_t StateSize(const duckdb::BoundAggregateFunction&) {
    return 1;
  }
  static void Init(const duckdb::BoundAggregateFunction&, duckdb::data_ptr_t) {}
  static void Update(duckdb::Vector[], duckdb::AggregateInputData&,
                     duckdb::idx_t, duckdb::Vector&, duckdb::idx_t) {
    Throw();
  }
  static void Combine(duckdb::Vector&, duckdb::Vector&,
                      duckdb::AggregateInputData&, duckdb::idx_t) {
    Throw();
  }
  static void Finalize(duckdb::Vector&, duckdb::AggregateFinalizeInputData&,
                       duckdb::Vector&, duckdb::idx_t, duckdb::idx_t) {
    Throw();
  }

 private:
  [[noreturn]] static void Throw() {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_dict_agg() family requires an inverted index scan in the "
              "same sub-query (call it over an inverted-indexed column)."));
  }
};

template<int Tag>
void RegisterTsDictStub(duckdb::ExtensionLoader& loader, std::string_view name,
                        const duckdb::LogicalType& ret) {
  duckdb::AggregateFunction fn(
    duckdb::Identifier{std::string{name}}, {duckdb::LogicalType::ANY}, ret,
    TsDictStub<Tag>::StateSize, TsDictStub<Tag>::Init, TsDictStub<Tag>::Update,
    TsDictStub<Tag>::Combine, TsDictStub<Tag>::Finalize,
    duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING);
  loader.RegisterFunction(std::move(fn));
}

void RegisterTsDictFunctions(duckdb::ExtensionLoader& loader) {
  const auto list = [](const duckdb::LogicalType& el) {
    return duckdb::LogicalType::LIST(el);
  };
  RegisterTsDictStub<0>(loader, kTsDictAgg, list(duckdb::LogicalType::VARCHAR));
  RegisterTsDictStub<1>(loader, kTsDictRawAgg, list(duckdb::LogicalType::BLOB));
  RegisterTsDictStub<2>(loader, kTsDictCount,
                        list(duckdb::LogicalType::INTEGER));
  RegisterTsDictStub<3>(loader, kTsDictFreq, list(duckdb::LogicalType::BIGINT));
  RegisterTsDictStub<4>(loader, kTsDictScore, list(duckdb::LogicalType::FLOAT));
  RegisterTsDictStub<5>(loader, kTsDictMin, duckdb::LogicalType::VARCHAR);
  RegisterTsDictStub<6>(loader, kTsDictMax, duckdb::LogicalType::VARCHAR);
}

void RegisterTSQueryTypes(duckdb::ExtensionLoader& loader) {
  loader.RegisterType(std::string{kTSQueryTypeName}, MakeTSQueryType());

  // `tokenize(<analyzer-name>)` is a parameterized type registered as
  // a TSQUERY variant. The bind function consumes a single VARCHAR
  // modifier and stores it in ExtensionTypeInfo so the filter builder
  // can route the inner expression through the named analyzer instead
  // of the ambient one. The bound type's alias remains "TSQUERY", so
  // existing VARCHAR<->TSQUERY casts apply unchanged; the modifier just
  // travels with the LogicalType into BoundCastExpression.return_type.
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
      // Return TOKENIZED_TSQUERY (distinct alias from TSQUERY) so a
      // `<TSQ-typed expr>::tokenize(...)` cast doesn't get short-
      // circuited by DuckDB's same-alias cast elision. The implicit
      // casts registered below let TOKENIZED_TSQUERY values flow back
      // into TSQUERY-expecting overloads (`@@`, `||`, `&&`, `##`, ...)
      // transparently.
      auto type = MakeTokenizedTSQueryType();
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
  // a DOUBLE modifier instead of VARCHAR. Different alias keeps the
  // cast wrapper alive so the walker can read the factor.
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
      auto type = MakeBoostedTSQueryType();
      auto info = duckdb::make_uniq<duckdb::ExtensionTypeInfo>();
      info->modifiers.emplace_back(std::move(factor));
      type.SetExtensionInfo(std::move(info));
      return type;
    });
}

void RegisterTSQueryAliasCasts(duckdb::ExtensionLoader& loader) {
  // VARCHAR <-> TSQUERY reinterpret casts. Bare string literals in
  // TSQUERY context get tokenised by the filter builder via the
  // ambient (@@ column) analyzer. Asymmetric costs keep typed TSQUERY
  // values from down-casting into competing VARCHAR overloads:
  // VARCHAR -> TSQUERY is free (cost 0) so bare strings flow into
  // TSQUERY contexts, but TSQUERY -> VARCHAR is expensive (cost 100)
  // so a TSQUERY-typed expression always prefers TSQUERY overloads
  // over VARCHAR mirrors when both exist (e.g. `<TSQ> ## 'b'` picks
  // (TSQUERY, TSQUERY) not (VARCHAR, VARCHAR)).
  loader.RegisterCastFunction(MakeTSQueryType(), duckdb::LogicalType::VARCHAR,
                              duckdb::DefaultCasts::ReinterpretCast, 100);
  loader.RegisterCastFunction(duckdb::LogicalType::VARCHAR, MakeTSQueryType(),
                              duckdb::DefaultCasts::ReinterpretCast, 0);

  // Casts to/from the TOKENIZED_TSQUERY alias. The bind callback for
  // `tokenize(...)` returns this alias (instead of TSQUERY) so that:
  //   - `'foo'::tokenize('x')`     -- VARCHAR -> TOK-mod-x cast survives
  //   - `ts_phrase('y')::tokenize('x')` -- TSQ -> TOK-mod-x cast survives
  // (both are different-alias casts so DuckDB doesn't short-circuit).
  // The TOK->TSQ direction lets a tokenized value flow back into any
  // TSQUERY-expecting overload (`@@`, `||`, `&&`, `##`, ...) without
  // duplicate registrations. TOK->VARCHAR mirrors the asymmetric cost
  // for TSQ->VARCHAR so TOKENIZED values prefer TSQUERY overloads.
  //
  // VARCHAR -> TOK_TSQ at cost 50 (NOT 0): the operator overload set
  // (`||`, `&&`, `!!`, `^`, `##`) is registered against TOK_TSQ, and a
  // free VARCHAR -> TOK_TSQ cast would let our overloads tie with
  // builtin VARCHAR ones for plain VARCHAR operands (e.g. `a::varchar
  // || b::varchar`), with DuckDB silently picking ours and producing a
  // TSQUERY value that fires SearchStubFn at runtime. Cost 50 is high
  // enough that builtin VARCHAR wins for VARCHAR/VARCHAR operands and
  // low enough that STRING_LITERAL (cost 20 to alias) and TSQ-typed
  // (cost 0 via TSQ -> TOK below) operands still prefer ours.
  loader.RegisterCastFunction(duckdb::LogicalType::VARCHAR,
                              MakeTokenizedTSQueryType(),
                              duckdb::DefaultCasts::ReinterpretCast, 50);
  loader.RegisterCastFunction(MakeTSQueryType(), MakeTokenizedTSQueryType(),
                              duckdb::DefaultCasts::ReinterpretCast, 0);
  loader.RegisterCastFunction(MakeTokenizedTSQueryType(), MakeTSQueryType(),
                              duckdb::DefaultCasts::ReinterpretCast, 0);
  loader.RegisterCastFunction(MakeTokenizedTSQueryType(),
                              duckdb::LogicalType::VARCHAR,
                              duckdb::DefaultCasts::ReinterpretCast, 100);

  // BOOSTED <-> {VARCHAR, TSQ, TOK} reinterpret casts. Cost 0 in/out
  // of the TSQ family so BOOSTED args feed the existing TOK-typed
  // operator overloads (`||`, `&&`, `^`, `!!`); 100 down to bare
  // VARCHAR mirrors the TSQ/TOK -> VARCHAR asymmetry. The cross-edges
  // (TOK <-> BOOSTED) are what let `::tokenize(...)` and `::boost(K)`
  // compose in either order on a compound expression.
  //
  // VARCHAR -> BOOSTED at cost 50: same rationale as VARCHAR -> TOK
  // above (keep plain VARCHAR operands out of the TSQ operator set).
  loader.RegisterCastFunction(duckdb::LogicalType::VARCHAR,
                              MakeBoostedTSQueryType(),
                              duckdb::DefaultCasts::ReinterpretCast, 50);
  loader.RegisterCastFunction(MakeTSQueryType(), MakeBoostedTSQueryType(),
                              duckdb::DefaultCasts::ReinterpretCast, 0);
  loader.RegisterCastFunction(MakeTokenizedTSQueryType(),
                              MakeBoostedTSQueryType(),
                              duckdb::DefaultCasts::ReinterpretCast, 0);
  loader.RegisterCastFunction(MakeBoostedTSQueryType(), MakeTSQueryType(),
                              duckdb::DefaultCasts::ReinterpretCast, 0);
  loader.RegisterCastFunction(MakeBoostedTSQueryType(),
                              MakeTokenizedTSQueryType(),
                              duckdb::DefaultCasts::ReinterpretCast, 0);
  loader.RegisterCastFunction(MakeBoostedTSQueryType(),
                              duckdb::LogicalType::VARCHAR,
                              duckdb::DefaultCasts::ReinterpretCast, 100);
}

void RegisterTSQueryBoolCasts(duckdb::ExtensionLoader& loader) {
  // BOOLEAN -> {TSQUERY, TOK}: lets `true` / `false` flow into any
  // TSQUERY position. The runtime function throws -- which makes the
  // cast non-foldable, so the BoundCastExpression survives in the
  // bound tree. The walker intercepts it and emits irs::All /
  // irs::Empty. Both targets registered because DuckDB's binder
  // doesn't chain multi-step implicit casts (so e.g. @@(ANY, TOK)
  // wouldn't pick up a BOOL via TSQ).
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
  loader.RegisterCastFunction(duckdb::LogicalType::BOOLEAN,
                              MakeTokenizedTSQueryType(), bool_cast_bind,
                              /*implicit_cast_cost=*/0);

  // BOOLEAN <-> BOOSTED_TSQUERY: lets `(predicate)::boost(K)` apply
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
  // Cost 50 mirrors VARCHAR -> BOOSTED_TSQUERY (above): keeps plain
  // BOOLEAN operands from sweeping into TSQUERY overloads when no
  // boost was actually requested.
  loader.RegisterCastFunction(duckdb::LogicalType::BOOLEAN,
                              MakeBoostedTSQueryType(), boost_bool_cast_bind,
                              /*implicit_cast_cost=*/50);
  // Cost 0: this is the WHERE-clause coercion DuckDB inserts when a
  // `(predicate)::boost(K)` cast appears at the predicate root (the
  // WhereBinder unconditionally adds a cast to BOOLEAN).
  loader.RegisterCastFunction(MakeBoostedTSQueryType(),
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
      duckdb::ScalarFunction fn(duckdb::Identifier{kTSQPhrase}, {first_arg},
                                MakeTSQueryType(), TSQueryStubFn);
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
      set.AddFunction(
        duckdb::ScalarFunction({first_arg}, MakeTSQueryType(), TSQueryStubFn));
      set.AddFunction(
        duckdb::ScalarFunction({first_arg, duckdb::LogicalType::DOUBLE},
                               MakeTSQueryType(), TSQueryStubFn));
    }
    loader.RegisterFunction(std::move(set));
  }

  // ts_like(pattern) / PREFIX(text) -- raw, no tokenisation.
  for (auto name : {kTSQLike, kTSQPrefix}) {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
    for (auto first_arg :
         {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB}) {
      set.AddFunction(
        duckdb::ScalarFunction({first_arg}, MakeTSQueryType(), TSQueryStubFn));
    }
    loader.RegisterFunction(std::move(set));
  }

  // LESS / LESS_EQ / GREATER / GREATER_EQ -- single-bound range
  // constructors. Each takes one bound value (VARCHAR / numeric /
  // BOOLEAN). The filter builder dispatches per column type: VARCHAR
  // bounds tokenise through the ambient analyzer (single-token
  // requirement) and emit irs::ByRange; numeric bounds emit
  // irs::ByGranularRange; BOOLEAN bounds emit irs::ByRange via the
  // boolean term encoding. Bound vs column type mismatch is a bind-time
  // error. NULL bound is also a bind-time error -- use
  // RANGE(NULL, ..., ...) for unbounded semantics.
  //
  // SPECIAL_HANDLING is required so DuckDB doesn't constant-fold a
  // NULL bound to NULL before the filter builder sees it.
  for (auto name : {kTSQLess, kTSQLessEq, kTSQGreater, kTSQGreaterEq}) {
    duckdb::ScalarFunction fn(duckdb::Identifier{name},
                              {duckdb::LogicalType::ANY}, MakeTSQueryType(),
                              TSQueryStubFn);
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
      set.AddFunction(
        duckdb::ScalarFunction({first_arg}, MakeTSQueryType(), TSQueryStubFn));
      set.AddFunction(
        duckdb::ScalarFunction({first_arg, duckdb::LogicalType::VARCHAR},
                               MakeTSQueryType(), TSQueryStubFn));
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
      set.AddFunction(
        duckdb::ScalarFunction({first_arg}, MakeTSQueryType(), TSQueryStubFn));
      set.AddFunction(
        duckdb::ScalarFunction({first_arg, duckdb::LogicalType::INTEGER},
                               MakeTSQueryType(), TSQueryStubFn));
      set.AddFunction(duckdb::ScalarFunction(
        {first_arg, duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN},
        MakeTSQueryType(), TSQueryStubFn));
      set.AddFunction(duckdb::ScalarFunction(
        {first_arg, duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN,
         duckdb::LogicalType::VARCHAR},
        MakeTSQueryType(), TSQueryStubFn));
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
      duckdb::ScalarFunction({duckdb::LogicalType::LIST(MakeTSQueryType())},
                             MakeTSQueryType(), TSQueryStubFn));
    set.AddFunction(
      duckdb::ScalarFunction({duckdb::LogicalType::LIST(MakeTSQueryType()),
                              duckdb::LogicalType::INTEGER},
                             MakeTSQueryType(), TSQueryStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ARRAY(MakeTSQueryType(), duckdb::optional_idx{})},
      MakeTSQueryType(), TSQueryStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ARRAY(MakeTSQueryType(), duckdb::optional_idx{}),
       duckdb::LogicalType::INTEGER},
      MakeTSQueryType(), TSQueryStubFn));
    loader.RegisterFunction(std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTSQAllOf}};
    set.AddFunction(
      duckdb::ScalarFunction({duckdb::LogicalType::LIST(MakeTSQueryType())},
                             MakeTSQueryType(), TSQueryStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::ARRAY(MakeTSQueryType(), duckdb::optional_idx{})},
      MakeTSQueryType(), TSQueryStubFn));
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

  // ts_is_null() / ts_is_not_null() -- zero-arg existence predicates.
  // Paired with a column via @@; the filter builder emits an
  // irs::ByColumnExistence (negated for IS NULL).
  for (auto name : {kTSQIsNull, kTSQIsNotNull}) {
    loader.RegisterFunction(duckdb::ScalarFunction(
      duckdb::Identifier{name}, {}, MakeTSQueryType(), TSQueryStubFn));
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
      set.AddFunction(
        duckdb::ScalarFunction({first_arg}, MakeTSQueryType(), TSQueryStubFn));
      set.AddFunction(
        duckdb::ScalarFunction({first_arg, duckdb::LogicalType::VARCHAR},
                               MakeTSQueryType(), TSQueryStubFn));
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
    duckdb::ScalarFunction fn(
      duckdb::Identifier{kTSQBetween},
      {duckdb::LogicalType::ANY, duckdb::LogicalType::ANY,
       duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::BOOLEAN},
      MakeTSQueryType(), TSQueryStubFn);
    fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(std::move(fn));
  }
}

// PG-compat tsquery parser functions: to_tsquery, plainto_tsquery,
// phraseto_tsquery, websearch_to_tsquery, tsquery_phrase. All
// throwing stubs claimed by the filter builder at bind time.
void RegisterTSQueryParserFunctions(duckdb::ExtensionLoader& loader) {
  // to_tsquery(VARCHAR) -> TSQUERY -- Lucene parser, wiring deferred.
  loader.RegisterFunction(duckdb::ScalarFunction(
    duckdb::Identifier{kToTsquery}, {duckdb::LogicalType::VARCHAR},
    MakeTSQueryType(), TSQueryStubFn));

  // plainto_tsquery / phraseto_tsquery / websearch_to_tsquery each take
  // one VARCHAR and produce a TSQUERY via their own semantics.
  for (auto name : {kPlainToTsquery, kPhraseToTsquery, kWebsearchToTsquery}) {
    loader.RegisterFunction(duckdb::ScalarFunction(
      duckdb::Identifier{name}, {duckdb::LogicalType::VARCHAR},
      MakeTSQueryType(), TSQueryStubFn));
  }

  // tsquery_phrase(q1, q2 [, distance]) -- function form of `##`.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kTsqueryPhrase}};
    set.AddFunction(
      duckdb::ScalarFunction({MakeTSQueryType(), MakeTSQueryType()},
                             MakeTSQueryType(), TSQueryStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {MakeTSQueryType(), MakeTSQueryType(), duckdb::LogicalType::INTEGER},
      MakeTSQueryType(), TSQueryStubFn));
    loader.RegisterFunction(std::move(set));
  }
}

void RegisterTSQueryOperators(duckdb::ExtensionLoader& loader) {
  // PG-style typed-tsquery binary operators: ||, &&. Registered as
  // (TOKENIZED_TSQUERY, TOKENIZED_TSQUERY) -> TOKENIZED_TSQUERY only.
  // TSQUERY operands are auto-cast to TOK at cost 0; the cast wrapper
  // survives (different aliases). The walker peels modifier-free TOK
  // wrappers via UnwrapTSQueryCast, so existing tree shapes are
  // unchanged. Crucially, per-leg `'a'::tokenize(x) || 'b'::tokenize(x)`
  // resolves directly without a TOK->TSQ auto-cast that would fold the
  // modifier away. Registering both (TSQ, TSQ) and (TOK, TOK) variants
  // would cause "Could not choose a best candidate" bind errors for
  // TSQUERY operands -- DuckDB doesn't break the tie by cast count
  // when aliases share the underlying VARCHAR type.
  for (auto name : {kTSQueryOr, kTSQueryAnd}) {
    loader.RegisterFunction(duckdb::ScalarFunction(
      duckdb::Identifier{name},
      {MakeTokenizedTSQueryType(), MakeTokenizedTSQueryType()},
      MakeTokenizedTSQueryType(), TSQueryStubFn));
  }

  // Unary prefix NOT (!!). Single TOK overload (same reasoning).
  loader.RegisterFunction(duckdb::ScalarFunction(
    duckdb::Identifier{kTSQueryNot}, {MakeTokenizedTSQueryType()},
    MakeTokenizedTSQueryType(), TSQueryStubFn));

  // Boost: TOK ^ DOUBLE -> TSQ. Returns plain TSQUERY so the result
  // composes inside TSQUERY[] contexts (e.g. compound([expr ^ K, ...])).
  // Args stay TOK so per-leg `::tokenize(...)` modifiers on the LHS
  // survive (no TOK->TSQ cast that would fold them away).
  loader.RegisterFunction(duckdb::ScalarFunction(
    duckdb::Identifier{kTSQueryBoost},
    {MakeTokenizedTSQueryType(), duckdb::LogicalType::DOUBLE},
    MakeTSQueryType(), TSQueryStubFn));

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
    set.AddFunction(
      duckdb::ScalarFunction({MakeTSQueryType(), MakeTSQueryType()},
                             MakeTSQueryType(), TSQueryStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      MakeTSQueryType(), TSQueryStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::INTEGER},
      MakeTSQueryType(), TSQueryStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER)},
      MakeTSQueryType(), TSQueryStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // @@(ANY, TOKENIZED_TSQUERY) -> BOOLEAN. Commutative (filter builder
  // inspects both children for column-ref). The second arg is typed as
  // TOKENIZED_TSQUERY (not TSQUERY) so that:
  //   - `b @@ EXPR::tokenize('x')`: the user-written cast targets TOK
  //     directly, sits unwrapped under @@. Walker reads modifier.
  //   - `b @@ ts_phrase('y')` / `b @@ 'foo'`: TSQ / VARCHAR auto-cast to
  //     TOK. The wrapping cast carries no modifier (target alias is
  //     plain TOK), so the walker peels it via UnwrapTSQueryCast and
  //     dispatches the inner TSQ/VARCHAR expression as before.
  // We don't register the (ANY, TSQUERY) overload because that would
  // cause an ambiguous-overload bind error for STRING_LITERAL operands
  // (DuckDB ranks `VARCHAR -> TSQUERY` and `VARCHAR -> TOKENIZED_TSQUERY`
  // identically for literals, regardless of registered cast costs).
  loader.RegisterFunction(duckdb::ScalarFunction(
    duckdb::Identifier{kTSQueryMatch},
    {duckdb::LogicalType::ANY, MakeTokenizedTSQueryType()},
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

void RegisterTSQuerySurface(duckdb::ExtensionLoader& loader) {
  RegisterTSQueryTypes(loader);
  RegisterTSQueryAliasCasts(loader);
  RegisterTSQueryBoolCasts(loader);
  RegisterTSQueryListCast(loader);
  RegisterTSQueryConstructors(loader);
  RegisterTSQueryParserFunctions(loader);
  RegisterTSQueryOperators(loader);
  RegisterPredicateFunctions(loader);
}

// Functions normally executed by inverted indexes. If rejected by an index the
// query fails with the "outside inverted index context" message above.
// Per-row scorer functions: bm25, tfidf, raw_tf, language models,
// DFI. All share the shape `name(BIGINT tableoid [, params...]) ->
// FLOAT` and the ScorerStubFn runtime. The iresearch_plan rule
// claims each call at compile time and threads the scorer into
// bind_data; the stub fires only if the call escapes the rule.
void RegisterScorerFunctions(duckdb::ExtensionLoader& loader) {
  // bm25(tableoid) / bm25(tableoid, k1, b) -> DOUBLE -- emits the BM25
  // score per row for the scan identified by tableoid. Parameters are
  // extracted at compile time by the iresearch_plan rule; defaults
  // follow iresearch's Bm25 (k1 = 1.2, b = 0.75).
  {
    duckdb::ScalarFunctionSet set{
      duckdb::Identifier{catalog::ScorerOptions::Bm25::Owner::type_name()}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, ScorerStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::DOUBLE,
       duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::FLOAT, ScorerStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // tfidf(tableoid) / tfidf(tableoid, with_norms) -> DOUBLE -- emits
  // TF-IDF. `with_norms` toggles length normalisation (default false).
  {
    duckdb::ScalarFunctionSet set{
      duckdb::Identifier{catalog::ScorerOptions::Tfidf::Owner::type_name()}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, ScorerStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::FLOAT, ScorerStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // lm_jm(tableoid) / lm_jm(tableoid, lambda) -> FLOAT.
  // Language model with Jelinek-Mercer (linear interpolation) smoothing.
  // lambda in (0, 1]; iresearch default is 0.1.
  {
    duckdb::ScalarFunctionSet set{
      duckdb::Identifier{catalog::ScorerOptions::LmJm::Owner::type_name()}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, ScorerStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::FLOAT, ScorerStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // lm_dirichlet(tableoid) / lm_dirichlet(tableoid, mu) -> FLOAT.
  // Language model with Bayesian (Dirichlet) smoothing. mu >= 0;
  // iresearch default is 2000.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{
      catalog::ScorerOptions::LmDirichlet::Owner::type_name()}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, ScorerStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::FLOAT, ScorerStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // indri_dirichlet(tableoid) / indri_dirichlet(tableoid, mu) -> FLOAT.
  // Indri-style Dirichlet: same smoothing as lm_dirichlet but without the
  // floor-at-zero clamp, so scores can be negative when tf < mu*P(t|C).
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{
      catalog::ScorerOptions::IndriDirichlet::Owner::type_name()}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, ScorerStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::FLOAT, ScorerStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // dfi(tableoid) / dfi(tableoid, measure) -> FLOAT.
  // Divergence-From-Independence. `measure` selects the independence
  // kernel: 'standardized' (default), 'saturated', or 'chi_squared'.
  {
    duckdb::ScalarFunctionSet set{
      duckdb::Identifier{catalog::ScorerOptions::Dfi::Owner::type_name()}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, ScorerStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::FLOAT, ScorerStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // raw_tf(tableoid) -> FLOAT -- emits raw term frequency per matched doc.
  {
    duckdb::ScalarFunctionSet set{
      duckdb::Identifier{catalog::ScorerOptions::RawBoost::Owner::type_name()}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, ScorerStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // raw_tf(tableoid) -> FLOAT -- emits raw term frequency per matched doc.
  {
    duckdb::ScalarFunctionSet set{
      duckdb::Identifier{catalog::ScorerOptions::RawTf::Owner::type_name()}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, ScorerStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // raw_dl(tableoid) -> FLOAT -- emits raw document length per matched doc.
  {
    duckdb::ScalarFunctionSet set{
      duckdb::Identifier{catalog::ScorerOptions::RawDL::Owner::type_name()}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, ScorerStubFn));
    loader.RegisterFunction(std::move(set));
  }
}

// ts_offsets(col [, limit]) -> INTEGER[] of interleaved start/end pairs.
// Inline form: stub body; iresearch_plan rewrites it into either a
// virtual SearchScan column (stored offsets) or a real OffsetsScalarFn
// call (derived offsets). Standalone form: ts_offsets(dict, body, filter
// [, limit]) -- always runs OffsetsScalarFn against an in-memory
// mini-segment built per chunk.
void RegisterPositionFunctions(duckdb::ExtensionLoader& loader) {
  duckdb::ScalarFunctionSet set{duckdb::Identifier{kOffsets}};
  const auto list_int = duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);

  auto add_inline = [&](duckdb::vector<duckdb::LogicalType> args) {
    duckdb::ScalarFunction fn{std::move(args), list_int, SearchStubFn};
    fn.SetInitStateCallback(InitOffsetsLocalState);
    set.AddFunction(std::move(fn));
  };
  add_inline({duckdb::LogicalType::ANY});
  add_inline({duckdb::LogicalType::ANY, duckdb::LogicalType::INTEGER});

  auto add_standalone = [&](duckdb::vector<duckdb::LogicalType> args) {
    duckdb::ScalarFunction fn{std::move(args), list_int, OffsetsScalarFn,
                              OffsetsStandaloneBind};
    fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    fn.SetInitStateCallback(InitOffsetsLocalState);
    set.AddFunction(std::move(fn));
  };
  add_standalone({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                  MakeTSQueryType()});
  add_standalone({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                  MakeTSQueryType(), duckdb::LogicalType::INTEGER});

  loader.RegisterFunction(std::move(set));
}

// ST_* geo predicates. All are stubs at runtime -- the filter builder claims
// them at bind time and rewrites them into iresearch GeoFilter /
// GeoDistanceFilter calls. Field and centroid each accept VARCHAR
// (GeoJSON-text literal) or GEOMETRY('OGC:CRS84'); the catalog gates JSON-vs-
// GEOMETRY column types separately at CREATE INDEX time.
void RegisterGeoFunctions(duckdb::ExtensionLoader& loader) {
  // Pin GEOMETRY signatures to CRS84 so DuckDB's bind-time geo cast rules
  // apply: matching-CRS values pass through unchanged (CRS metadata
  // preserved), cross-CRS values throw BinderException, bare-GEOMETRY values
  // reinterpret to CRS84. Without the pin, the bind cast to bare GEOMETRY
  // would silently strip CRS metadata before the filter builder can
  // validate it.
  const duckdb::LogicalType geo_field_types[] = {
    duckdb::LogicalType::VARCHAR, duckdb::LogicalType::GEOMETRY("OGC:CRS84")};
  const duckdb::LogicalType geo_centroid_types[] = {
    duckdb::LogicalType::VARCHAR, duckdb::LogicalType::GEOMETRY("OGC:CRS84")};

  // ST_Distance_Between(field, centroid, min_distance, max_distance,
  //                     [include_min, [include_max]]) -> bool
  //
  // field   : JSON column (GeoJSON) or GEOMETRY column.
  // centroid: JSON value (GeoJSON) or GEOMETRY value.
  //
  // Register all 4 type combinations for the (field, centroid) pair across
  // each arity so DuckDB resolves the call without implicit casts.
  {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{kGeoInRange}};
    for (const auto& field_t : geo_field_types) {
      for (const auto& centroid_t : geo_centroid_types) {
        set.AddFunction(duckdb::ScalarFunction(
          {field_t, centroid_t, duckdb::LogicalType::DOUBLE,
           duckdb::LogicalType::DOUBLE},
          duckdb::LogicalType::BOOLEAN, SearchStubFn));
        set.AddFunction(duckdb::ScalarFunction(
          {field_t, centroid_t, duckdb::LogicalType::DOUBLE,
           duckdb::LogicalType::DOUBLE, duckdb::LogicalType::BOOLEAN},
          duckdb::LogicalType::BOOLEAN, SearchStubFn));
        set.AddFunction(duckdb::ScalarFunction(
          {field_t, centroid_t, duckdb::LogicalType::DOUBLE,
           duckdb::LogicalType::DOUBLE, duckdb::LogicalType::BOOLEAN,
           duckdb::LogicalType::BOOLEAN},
          duckdb::LogicalType::BOOLEAN, SearchStubFn));
      }
    }
    loader.RegisterFunction(std::move(set));
  }

  // ST_Distance_Centroid(field, centroid) -> DOUBLE
  //   and its operator-form synonym `field <-> centroid`.
  //
  // Returns the geodesic distance from the indexed value's centroid to the
  // centroid argument. Pseudo-function: outside an inverted-index scan it
  // throws via the stub. The filter builder recognizes
  // `ST_Distance_Centroid(...) OP <const>` (and the `<->` form) and
  // rewrites them into iresearch GeoDistanceFilter range bounds.
  //
  // The `<->` set extends the vector-distance set registered in
  // RegisterVectorFunctions (vector.cpp); DuckDB merges overloads under
  // the same name via OnCreateConflict::ALTER_ON_CONFLICT, so vector
  // (ARRAY(FLOAT/DOUBLE)) and geo (VARCHAR / GEOMETRY) overloads coexist
  // and bind by argument types. IsVectorDistanceFunction(...) in
  // iresearch_plan.cpp keeps the geo overloads off the vector-ANN paths.
  for (auto name : {kGeoDistance, kL2DistanceOp}) {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
    for (const auto& field_t : geo_field_types) {
      for (const auto& centroid_t : geo_centroid_types) {
        set.AddFunction(duckdb::ScalarFunction(
          {field_t, centroid_t}, duckdb::LogicalType::DOUBLE, SearchStubFn));
      }
    }
    loader.RegisterFunction(std::move(set));
  }

  // ST_Intersects(field, shape) -> bool    (commutative; either arg may be
  // the column reference. Builds an iresearch GeoFilter with type=Intersects.)
  // ST_Contains(field, shape)   -> bool    (indexed ⊇ shape, type=IsContained)
  // ST_Contains(shape, field)   -> bool    (shape ⊇ indexed, type=Contains)
  for (auto name : {kGeoIntersects, kGeoContains}) {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
    for (const auto& a : geo_field_types) {
      for (const auto& b : geo_centroid_types) {
        set.AddFunction(duckdb::ScalarFunction(
          {a, b}, duckdb::LogicalType::BOOLEAN, SearchStubFn));
      }
    }
    loader.RegisterFunction(std::move(set));
  }
}

}  // namespace

duckdb::LogicalType MakeTSQueryType() {
  auto type = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
  type.SetAlias(std::string{kTSQueryTypeName});
  return type;
}

catalog::Tokenizer::TokenizerWrapper AcquireTokenizer(
  duckdb::ClientContext& context, std::string_view name) {
  auto entry = ResolveCatalogTokenizer(context, name);
  if (!entry) {
    return {};
  }
  return entry->GetTokenizer();
}

std::shared_ptr<catalog::Tokenizer> ResolveCatalogTokenizer(
  duckdb::ClientContext& context, std::string_view name) {
  auto state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  if (!state) [[unlikely]] {
    return nullptr;
  }
  auto& conn_ctx = state->GetConnectionContext();
  auto db_id = conn_ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();
  auto qualified = pg::ParseObjectName(name, current_schema);
  auto snapshot = conn_ctx.CatalogSnapshot();
  if (!snapshot) {
    return nullptr;
  }
  return snapshot->GetTokenizer(catalog::NoAccessCheck(), db_id,
                                qualified.schema, qualified.relation);
}

void RegisterSearchFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  RegisterScorerFunctions(loader);
  RegisterPositionFunctions(loader);
  RegisterGeoFunctions(loader);
  RegisterTsLexize(loader);
  RegisterSplitByNonAlpha(loader);
  RegisterTsHighlight(loader);
  RegisterTSQuerySurface(loader);
  RegisterTsDictFunctions(loader);
}

}  // namespace sdb::connector
