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

#include "pg/types/function.h"

#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace sdb::pg {

void RegisterFunctions(duckdb::DatabaseInstance& db, Functions functions) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  for (auto& [_, set] : functions.scalars) {
    if (loader.TryGetFunction(set.name)) {
      loader.AddFunctionOverload(std::move(set));
    } else {
      loader.RegisterFunction(std::move(set));
    }
  }
  for (auto& [_, set] : functions.aggregates) {
    if (loader.TryGetAggregateFunction(set.name)) {
      for (const auto& function : set.functions) {
        loader.AddFunctionOverload(*function);
      }
    } else {
      loader.RegisterFunction(std::move(set));
    }
  }
  for (auto& [_, set] : functions.replacements) {
    duckdb::CreateScalarFunctionInfo info{std::move(set)};
    info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }
}

}  // namespace sdb::pg
