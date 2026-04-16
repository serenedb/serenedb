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
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

namespace sdb::connector {
namespace {

void SearchStubFn(duckdb::DataChunk& /*args*/,
                  duckdb::ExpressionState& /*state*/,
                  duckdb::Vector& /*result*/) {
  throw duckdb::InvalidInputException(
    "Inverted index function called outside inverted index context. "
    "Use in WHERE clause on a table with an inverted index.");
}

}  // namespace

// Functions normally executed by inverted indexes. If rejected by an index the
// query fails with the "outside inverted index context" message above.
void RegisterSearchFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  // phrase(field, target) -> bool
  loader.RegisterFunction(duckdb::ScalarFunction(
    std::string{kPhrase},
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN, SearchStubFn));

  // term_eq/lt/lte/gte/gt/like(field, target) -> bool
  for (auto name : {kTermEq, kTermLt, kTermLe, kTermGe, kTermGt, kTermLike}) {
    loader.RegisterFunction(duckdb::ScalarFunction(
      std::string{name},
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
  }

  // term_in(field, values...) -> bool  (variadic: 1 fixed VARCHAR + N VARCHAR)
  {
    duckdb::ScalarFunction fn(std::string{kTermIn},
                              {duckdb::LogicalType::VARCHAR},
                              duckdb::LogicalType::BOOLEAN, SearchStubFn);
    fn.varargs = duckdb::LogicalType::VARCHAR;
    loader.RegisterFunction(std::move(fn));
  }

  // ngram_match(field, target[, threshold]) -> bool
  {
    duckdb::ScalarFunctionSet ngram{std::string{kNgramMatch}};
    ngram.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    ngram.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    loader.RegisterFunction(std::move(ngram));
  }

  // levenshtein_match(field, target, distance[, transpositions[, maxTerms[,
  // prefix]]]) -> bool
  {
    duckdb::ScalarFunctionSet lev{std::string{kLevenshteinMatch}};
    lev.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    lev.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    lev.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN,
       duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    lev.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    loader.RegisterFunction(std::move(lev));
  }

  // boost(expr, boost_value) -> bool
  loader.RegisterFunction(duckdb::ScalarFunction(
    std::string{kBoost},
    {duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::DOUBLE},
    duckdb::LogicalType::BOOLEAN, SearchStubFn));

  // bm25(tableoid) / bm25(tableoid, k1, b) -> DOUBLE -- emits the BM25
  // score per row for the scan identified by tableoid. Parameters are
  // extracted at compile time by the iresearch_plan rule; defaults
  // follow iresearch's Bm25 (k1 = 1.2, b = 0.75).
  {
    duckdb::ScalarFunctionSet set{std::string{kBm25}};
    set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::BIGINT},
                                           duckdb::LogicalType::FLOAT,
                                           SearchStubFn));
    set.AddFunction(
      duckdb::ScalarFunction({duckdb::LogicalType::BIGINT,
                              duckdb::LogicalType::DOUBLE,
                              duckdb::LogicalType::DOUBLE},
                             duckdb::LogicalType::FLOAT, SearchStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // tfidf(tableoid) / tfidf(tableoid, with_norms) -> DOUBLE -- emits
  // TF-IDF. `with_norms` toggles length normalisation (default false).
  {
    duckdb::ScalarFunctionSet set{std::string{kTfidf}};
    set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::BIGINT},
                                           duckdb::LogicalType::FLOAT,
                                           SearchStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::FLOAT, SearchStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // offsets(col) -> BIGINT[] -- emit position pairs (start, end) for
  // matched terms in `col` per row. List elements alternate start/end
  // (so length is 2*N for N positions). Claimed by the iresearch_plan
  // rule, which identifies the scan via the column ref's
  // binding.table_index.
  loader.RegisterFunction(duckdb::ScalarFunction(
    std::string{kOffsets}, {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT), SearchStubFn));
}

}  // namespace sdb::connector
