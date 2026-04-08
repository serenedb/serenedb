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

#include "connector/duckdb_search_functions.h"

#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include "functions/search.h"

namespace sdb::connector {

namespace {

void SearchStubFunction(duckdb::DataChunk& args,
                        duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  throw duckdb::InvalidInputException(
    "Inverted index function called outside inverted index context. "
    "Use in WHERE clause on a table with an inverted index.");
}

}  // namespace

void RegisterSearchFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  loader.RegisterFunction(duckdb::ScalarFunction(
    std::string{functions::kPhrase},
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN, SearchStubFunction));

  loader.RegisterFunction(duckdb::ScalarFunction(
    std::string{functions::kTermEq},
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN, SearchStubFunction));
}

}  // namespace sdb::connector
