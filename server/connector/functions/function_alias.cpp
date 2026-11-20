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

#include "connector/functions/function_alias.h"

#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <utility>

namespace sdb::connector {

void RegisterScalarAlias(duckdb::ExtensionLoader& loader,
                         std::string_view source_name, std::string_view alias) {
  auto& source = loader.GetFunction(duckdb::Identifier{source_name});
  auto functions = source.functions;
  functions.SetName(duckdb::Identifier{alias});

  duckdb::CreateScalarFunctionInfo info{std::move(functions)};
  info.descriptions = source.descriptions;
  info.alias_of = source.name;
  info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));
}

}  // namespace sdb::connector
