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

#include "connector/quack.h"

#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/storage/storage_extension.hpp>

#include "auth/role_closure.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

constinit duckdb::attach_function_t gQuackAttach = nullptr;
constinit duckdb::table_function_bind_t gQuackQueryBind = nullptr;

void RequireSuperuser(duckdb::ClientContext& context) {
  if (!auth::ClosureFor(&context, ConnectionContext::Get(context).GetRoleId())
         ->is_superuser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("must be superuser to connect to a Quack server"));
  }
}

duckdb::unique_ptr<duckdb::Catalog> Attach(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::string& name, duckdb::AttachInfo& attach_info,
  duckdb::AttachOptions& options) {
  RequireSuperuser(context);
  return gQuackAttach(info, context, db, name, attach_info, options);
}

duckdb::unique_ptr<duckdb::FunctionData> Bind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<duckdb::Identifier>& names) {
  RequireSuperuser(context);
  return gQuackQueryBind(context, input, types, names);
}

}  // namespace

void RestrictQuackClient(duckdb::DatabaseInstance& db) {
  auto extension = duckdb::StorageExtension::Find(db.config, "quack");
  SDB_ASSERT(extension);
  if (extension->attach != Attach) {
    gQuackAttach = extension->attach;
    extension->attach = Attach;
  }

  duckdb::ExtensionLoader loader{db, "serenedb"};
  auto entry = loader.TryGetTableFunction(duckdb::Identifier{"quack_query"});
  SDB_ASSERT(entry);
  auto functions = entry->Cast<duckdb::TableFunctionCatalogEntry>().functions;
  functions.ApplyToFunctions([](duckdb::TableFunction& function) {
    if (function.bind != Bind) {
      gQuackQueryBind = function.bind;
      function.bind = Bind;
    }
  });
  duckdb::CreateTableFunctionInfo info{std::move(functions)};
  info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));
}

}  // namespace sdb::connector
