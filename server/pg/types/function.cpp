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

#include <absl/algorithm/container.h>

#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/assert.hpp>

namespace sdb::pg {
namespace {

void ScopeToOwnTypes(duckdb::SimpleFunction& function) {
  auto& signature = function.GetSignature();
  SDB_ASSERT(absl::c_any_of(
    signature.GetParameters(),
    [](const auto& parameter) { return parameter.GetType().HasAlias(); }));
  signature.SetTypeScoped();
}

}  // namespace

void RegisterFunctions(duckdb::DatabaseInstance& db, Functions functions) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  for (auto& [_, set] : functions.scalars) {
    if (loader.TryGetFunction(set.name)) {
      set.ApplyToFunctions(ScopeToOwnTypes);
      loader.AddFunctionOverload(std::move(set));
    } else {
      loader.RegisterFunction(std::move(set));
    }
  }
  for (auto& [_, set] : functions.aggregates) {
    if (loader.TryGetAggregateFunction(set.name)) {
      set.ApplyToFunctions(ScopeToOwnTypes);
      for (const auto& function : set.functions) {
        loader.AddFunctionOverload(*function);
      }
    } else {
      loader.RegisterFunction(std::move(set));
    }
  }
}

}  // namespace sdb::pg
