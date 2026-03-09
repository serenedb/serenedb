////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {

yaclib::Future<> RemoveTombstone(ExecContext& context, const RangeVar& rel) {
  const auto db = context.GetDatabaseId();
  auto& conn_ctx = basics::downCast<ConnectionContext>(context);
  std::string current_schema = conn_ctx.GetCurrentSchema();
  const std::string_view schema =
    rel.schemaname ? std::string_view{rel.schemaname} : current_schema;
  if (schema.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
                    ERR_MSG("no schema has been selected to create in"));
  }

  SDB_IF_FAILURE("crash_before_remove_tombstone") {
    SDB_IMMEDIATE_ABORT();
  }

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto r = catalog.RemoveTombstone(db, schema, rel.relname);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return {};
}

}  // namespace sdb::pg
