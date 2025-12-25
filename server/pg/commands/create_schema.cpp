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

#include <absl/strings/str_cat.h>

#include <memory>
#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "pg/sql_utils.h"
#include "utils/exec_context.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

yaclib::Future<Result> CreateSchema(ExecContext& context,
                                    const CreateSchemaStmt& stmt) {
  if (stmt.schemaElts) {
    return yaclib::MakeFuture<Result>(
      ERROR_NOT_IMPLEMENTED,
      "CREATE SCHEMA with schema elements is not implemented");
  }

  if (stmt.schemaname == StaticStrings::kPgCatalogSchema ||
      stmt.schemaname == StaticStrings::kInformationSchema) {
    return yaclib::MakeFuture<Result>(ERROR_BAD_PARAMETER,
                                      "unacceptable schema name \"",
                                      stmt.schemaname, "\"");
  }

  SDB_ASSERT(stmt.schemaname);

  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();

  catalog::SchemaOptions options;
  options.name = stmt.schemaname;

  const auto db = context.GetDatabaseId();
  auto r = catalog.CreateSchema(
    db, std::make_shared<catalog::Schema>(db, std::move(options)));
  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && stmt.if_not_exists) {
    r = {};
  }

  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
