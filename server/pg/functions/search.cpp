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
#include "pg/functions/search.hpp"

#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/type/SimpleFunctionApi.h>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/fwd.h"

namespace sdb::pg::functions {

namespace {

template<typename T>
struct SearchStubFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<bool>& result,
    const arg_type<velox::Variadic<velox::Any>>& func_args) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Inverted index function called outside inverted index context");
  }
};

}  // namespace

// Functions normally executed by inverted indexes. If rejected by index will
// fail query.
// TODO(Dronplane): maybe add naive implementation to run without index on best
// effort basis?
void registerSearchFunctions(const std::string& prefix) {
  velox::registerFunction<SearchStubFunction, bool,
                          velox::Variadic<velox::Any>>({prefix + "phrase"});
}

}  // namespace sdb::pg::functions
