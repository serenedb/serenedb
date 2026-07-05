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
  if (args.size() < 3) {
    throw duckdb::InvalidInputException(
      "create_foreign_server requires name, fdw_name and if_not_exists");
  }
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
  if (args.size() < 3) {
    throw duckdb::InvalidInputException(
      "drop_foreign_server requires name, missing_ok and cascade");
  }
  const auto name = args[0].GetValue<std::string>();
  const auto missing_ok = args[1].GetValue<bool>();
  const auto cascade = args[2].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  pg::DropForeignServer(conn_ctx, name, missing_ok, cascade);
}

// PRAGMA create_user_mapping('user', 'server', if_not_exists, user := ..., ...)
void CreateUserMappingPragma(duckdb::ClientContext& context,
                             const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 3) {
    throw duckdb::InvalidInputException(
      "create_user_mapping requires user, server and if_not_exists");
  }
  const auto user = args[0].GetValue<std::string>();
  const auto server = args[1].GetValue<std::string>();
  const auto if_not_exists = args[2].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  pg::CreateUserMapping(conn_ctx, user, server, if_not_exists,
                        params.named_parameters);
}

// PRAGMA drop_user_mapping('user', 'server', missing_ok)
void DropUserMappingPragma(duckdb::ClientContext& context,
                           const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 3) {
    throw duckdb::InvalidInputException(
      "drop_user_mapping requires user, server and missing_ok");
  }
  const auto user = args[0].GetValue<std::string>();
  const auto server = args[1].GetValue<std::string>();
  const auto missing_ok = args[2].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  pg::DropUserMapping(conn_ctx, user, server, missing_ok);
}

}  // namespace

void RegisterForeignServerPragma(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  auto create_pragma = duckdb::PragmaFunction::PragmaCall(
    "create_foreign_server", CreateForeignServerPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::BOOLEAN});
  // Connection options are passed as arbitrary named parameters.
  create_pragma.accept_arbitrary_named_parameters = true;
  loader.RegisterFunction(create_pragma);

  auto drop_pragma = duckdb::PragmaFunction::PragmaCall(
    "drop_foreign_server", DropForeignServerPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN,
     duckdb::LogicalType::BOOLEAN});
  loader.RegisterFunction(drop_pragma);

  auto create_um = duckdb::PragmaFunction::PragmaCall(
    "create_user_mapping", CreateUserMappingPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::BOOLEAN});
  create_um.accept_arbitrary_named_parameters = true;
  loader.RegisterFunction(create_um);

  auto drop_um = duckdb::PragmaFunction::PragmaCall(
    "drop_user_mapping", DropUserMappingPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::BOOLEAN});
  loader.RegisterFunction(drop_um);
}

}  // namespace sdb::connector
