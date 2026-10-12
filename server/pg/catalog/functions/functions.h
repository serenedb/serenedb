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

#pragma once

#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <initializer_list>
#include <span>
#include <string_view>

#include "pg/catalog/lookup.h"

namespace sdb::pg {

struct SystemMacro {
  std::string_view schema;
  std::string_view name;
  std::string_view macro_definition;
};

extern const std::span<const SystemMacro> kPgCatalogMacros;
extern const std::span<const SystemMacro> kInformationSchemaMacros;

void Register(duckdb::ExtensionLoader& loader, std::string_view schema,
              duckdb::FunctionStability stability,
              duckdb::ScalarFunctionSet set);
void RegisterScalar(duckdb::ExtensionLoader& loader, std::string_view schema,
                    std::string_view name, duckdb::FunctionStability stability,
                    duckdb::init_local_state_t init,
                    std::initializer_list<duckdb::ScalarFunction> functions);

inline void RegisterScalar(
  duckdb::ExtensionLoader& loader, std::string_view schema,
  std::string_view name, duckdb::FunctionStability stability,
  std::initializer_list<duckdb::ScalarFunction> functions) {
  RegisterScalar(loader, schema, name, stability, nullptr, functions);
}

inline duckdb::unique_ptr<duckdb::FunctionLocalState> InitSession(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<SessionState>(&state.GetContext());
}

inline const Session& SessionOf(duckdb::ExpressionState& state) {
  return duckdb::ExecuteFunctionState::GetFunctionState(state)
    ->Cast<SessionState>()
    .session;
}

void RegisterAclFunctions(duckdb::ExtensionLoader& loader);
void RegisterDbsizeFunctions(duckdb::ExtensionLoader& loader);
void RegisterFormatTypeFunctions(duckdb::ExtensionLoader& loader);
void RegisterRegprocFunctions(duckdb::ExtensionLoader& loader);
void RegisterRuleutilsFunctions(duckdb::ExtensionLoader& loader);
void RegisterMiscFunctions(duckdb::ExtensionLoader& loader);
void RegisterSubscriptionFunctions(duckdb::ExtensionLoader& loader);
void RegisterInformationSchemaFunctions(duckdb::ExtensionLoader& loader);

void RegisterFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::pg
