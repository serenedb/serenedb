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

#include "pg/catalog/functions/functions.h"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace sdb::pg {

void Register(duckdb::ExtensionLoader& loader, std::string_view schema,
              duckdb::ScalarFunctionSet set) {
  duckdb::CreateScalarFunctionInfo info{std::move(set)};
  info.SetSchema(duckdb::Identifier{schema});
  info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));
}

void RegisterScalar(duckdb::ExtensionLoader& loader, std::string_view schema,
                    std::string_view name,
                    std::initializer_list<duckdb::ScalarFunction> functions) {
  duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
  for (const auto& function : functions) {
    set.AddFunction(function);
  }
  Register(loader, schema, std::move(set));
}

duckdb::ScalarFunction WithinQuery(duckdb::ScalarFunction function) {
  function.SetStability(duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
  return function;
}

void RegisterFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  RegisterMiscFunctions(loader);
  RegisterAclFunctions(loader);
  RegisterDbsizeFunctions(loader);
  RegisterFormatTypeFunctions(loader);
  RegisterRegprocFunctions(loader);
  RegisterRuleutilsFunctions(loader);
  RegisterInformationSchemaFunctions(loader);
}

}  // namespace sdb::pg
