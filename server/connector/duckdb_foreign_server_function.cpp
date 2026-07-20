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

#include "connector/duckdb_foreign_server_function.h"

#include <duckdb/function/function.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <string>

#include "connector/duckdb_client_state.h"
#include "pg/commands/create_server.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

// PRAGMA create_foreign_server('name', 'fdw_name', if_not_exists,
//                              host := ..., port := ..., database := ..., ...)
void CreateForeignServerPragma(duckdb::ClientContext& context,
                               const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  const auto name = args[0].GetValue<std::string>();
  const auto fdw_name = args[1].GetValue<std::string>();
  const auto if_not_exists = args[2].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  pg::CreateForeignServer(conn_ctx, name, fdw_name, if_not_exists,
                          params.named_parameters);
}

// PRAGMA drop_foreign_server('name', missing_ok, cascade)
void DropForeignServerPragma(duckdb::ClientContext& context,
                             const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  const auto name = args[0].GetValue<std::string>();
  const auto missing_ok = args[1].GetValue<bool>();
  const auto cascade = args[2].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  pg::DropForeignServer(conn_ctx, name, missing_ok, cascade);
}

}  // namespace

void RegisterForeignServerPragma(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  const auto kVarchar = duckdb::LogicalType::VARCHAR;
  const auto kBoolean = duckdb::LogicalType::BOOLEAN;

  auto create = duckdb::PragmaFunction::PragmaCall(
    "create_foreign_server", CreateForeignServerPragma,
    {kVarchar, kVarchar, kBoolean});
  // CREATE takes the connection options as arbitrary named parameters.
  create.accept_arbitrary_named_parameters = true;
  loader.RegisterFunction(create);

  auto drop = duckdb::PragmaFunction::PragmaCall(
    "drop_foreign_server", DropForeignServerPragma,
    {kVarchar, kBoolean, kBoolean});
  loader.RegisterFunction(drop);
}

}  // namespace sdb::connector
