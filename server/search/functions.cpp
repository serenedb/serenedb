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
#include "search/functions.hpp"

#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/type/SimpleFunctionApi.h>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/fwd.h"

namespace sdb::search::functions {
namespace {

template<typename T>
struct SearchStubFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template<typename TInput>
  void call(bool& out, const TInput& lhs, const TInput& rhs) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Inverted index function called outside inverted index context");
  }

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<bool>& result, const arg_type<velox::Varchar>& field_arg,
    const arg_type<velox::Variadic<velox::Varchar>>& values_args) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Inverted index function called outside inverted index context");
  }

  // NGRAM_MATCH(path, target, threshold)
  void call(bool& out, const arg_type<velox::Varchar>&,
            const arg_type<velox::Varchar>&, const double&) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Inverted index function called outside inverted index context");
  }

  // LEVENSHTEIN_MATCH overloads (3-6 args)
  void call(bool& out, const arg_type<velox::Varchar>&,
            const arg_type<velox::Varchar>&, const int64_t&) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Inverted index function called outside inverted index context");
  }

  void call(bool& out, const arg_type<velox::Varchar>&,
            const arg_type<velox::Varchar>&, const int64_t&, const bool&) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Inverted index function called outside inverted index context");
  }

  void call(bool& out, const arg_type<velox::Varchar>&,
            const arg_type<velox::Varchar>&, const int64_t&, const bool&,
            const int64_t&) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Inverted index function called outside inverted index context");
  }

  void call(bool& out, const arg_type<velox::Varchar>&,
            const arg_type<velox::Varchar>&, const int64_t&, const bool&,
            const int64_t&, const arg_type<velox::Varchar>&) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Inverted index function called outside inverted index context");
  }
};

}  // namespace

// Functions normally executed by inverted indexes. If rejected by index will
// fail query.
// TODO(Dronplane): maybe add naive implementation to run without index on best
// effort basis?
void registerSearchFunctions() {
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar>({std::string{kPhrase}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar>({std::string{kTermEq}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar>({std::string{kTermLt}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar>({std::string{kTermLe}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar>({std::string{kTermGe}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar>({std::string{kTermGt}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Variadic<velox::Varchar>>(
    {std::string{kTermIn}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar>({std::string{kTermLike}});

  // NGRAM_MATCH(path, target[, threshold])
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar>({std::string{kNgramMatch}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar, double>({std::string{kNgramMatch}});

  // LEVENSHTEIN_MATCH(path, target, distance[, transpositions[, maxTerms[,
  // prefix]]])
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar, int64_t>(
    {std::string{kLevenshteinMatch}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar, int64_t, bool>(
    {std::string{kLevenshteinMatch}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar, int64_t, bool, int64_t>(
    {std::string{kLevenshteinMatch}});
  velox::registerFunction<SearchStubFunction, bool, velox::Varchar,
                          velox::Varchar, int64_t, bool, int64_t,
                          velox::Varchar>({std::string{kLevenshteinMatch}});
}

}  // namespace sdb::search::functions
