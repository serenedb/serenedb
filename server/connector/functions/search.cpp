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
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/utils/utf8_utils.hpp>

#include "catalog/scorer_options.h"
#include "catalog/tokenizer.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/split_by_non_alpha.h"
#include "connector/functions/ts_common.hpp"
#include "connector/functions/ts_highlight.h"
#include "connector/functions/ts_lexize.h"
#include "connector/functions/ts_offsets.h"
#include "connector/functions/ts_query.h"
#include "connector/functions/ts_query_codec.h"
#include "connector/functions/vector.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::connector {

void SearchStubFn(duckdb::DataChunk& /*args*/,
                  duckdb::ExpressionState& /*state*/,
                  duckdb::Vector& /*result*/) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("Inverted index function called outside inverted index context. "
            "Use in WHERE clause on a table with an inverted index."));
}

namespace {

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

void RegisterScorerFunctions(duckdb::ExtensionLoader& loader) {
  using S = catalog::ScorerOptions;
  using LT = duckdb::LogicalType;
  struct Scorer {
    std::string_view name;
    duckdb::vector<duckdb::LogicalType> params;
  };
  const Scorer scorers[] = {
    {S::Bm25::Owner::type_name(), {LT::DOUBLE, LT::DOUBLE}},
    {S::Tfidf::Owner::type_name(), {LT::BOOLEAN}},
    {S::LmJm::Owner::type_name(), {LT::DOUBLE}},
    {S::LmDirichlet::Owner::type_name(), {LT::DOUBLE}},
    {S::IndriDirichlet::Owner::type_name(), {LT::DOUBLE}},
    {S::Dfi::Owner::type_name(), {LT::VARCHAR}},
    {S::RawBoost::Owner::type_name(), {}},
    {S::RawTf::Owner::type_name(), {}},
    {S::RawDL::Owner::type_name(), {}},
  };
  for (const auto& [name, params] : scorers) {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
    set.AddFunction(
      duckdb::ScalarFunction({LT::BIGINT}, LT::FLOAT, ScorerStubFn));
    if (!params.empty()) {
      duckdb::vector<duckdb::LogicalType> args{LT::BIGINT};
      args.insert(args.end(), params.begin(), params.end());
      set.AddFunction(
        duckdb::ScalarFunction(std::move(args), LT::FLOAT, ScorerStubFn));
    }
    loader.RegisterFunction(std::move(set));
  }
}

// ts_offsets(col [, limit]) -> INTEGER[] of interleaved start/end pairs.
// Inline form: stub body; iresearch_plan rewrites it into either a
// virtual search-scan column (stored offsets) or a real OffsetsScalarFn
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
  const duckdb::LogicalType geo_arg_types[] = {
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
    for (const auto& field_t : geo_arg_types) {
      for (const auto& centroid_t : geo_arg_types) {
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
    for (const auto& field_t : geo_arg_types) {
      for (const auto& centroid_t : geo_arg_types) {
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
    for (const auto& a : geo_arg_types) {
      for (const auto& b : geo_arg_types) {
        set.AddFunction(duckdb::ScalarFunction(
          {a, b}, duckdb::LogicalType::BOOLEAN, SearchStubFn));
      }
    }
    loader.RegisterFunction(std::move(set));
  }
}

}  // namespace

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
  RegisterTSQueryFunctions(loader);
  RegisterTsDictFunctions(loader);
}

}  // namespace sdb::connector
