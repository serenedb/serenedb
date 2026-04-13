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

#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/view.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_resolver.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "postgres_deparse.h"
#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

std::string DeparseWithAlias(Node* select, const char* table_alias,
                             const List* column_aliases) {
  SDB_ASSERT(select->type == T_SelectStmt);

  std::string body = pg::DeparseStmt(select);
  if (list_length(column_aliases) == 0) {
    return body;
  }

  body =
    absl::StrCat("SELECT * FROM (", std::move(body), ") AS ", table_alias, "(");
  VisitNodes(column_aliases, [&](const Node& node) {
    absl::StrAppend(&body, strVal(&node), ",");
  });

  SDB_ASSERT(!body.empty());
  SDB_ASSERT(body.back() == ',');
  body.back() = ')';

  return body;
}

yaclib::Future<> CreateView(const ExecContext& context, const ViewStmt& stmt) {
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(context);
  const auto db = context.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();
  const std::string_view schema = stmt.view->schemaname
                                    ? std::string_view{stmt.view->schemaname}
                                    : current_schema;

  SDB_ASSERT(stmt.view);
  SDB_ASSERT(stmt.view->relname);

  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();

  std::string_view name = stmt.view->relname;
  auto query = DeparseWithAlias(stmt.query, stmt.view->relname, stmt.aliases);

  auto view = std::make_shared<catalog::PgSqlView>(db, id::kGenerateNew, name,
                                                   std::move(query));

  // Validate the view query
  SDB_ASSERT(view->GetStatement());
  if (view->GetStatement()->stmt->type != T_SelectStmt) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
                    ERR_MSG("views must be based on SELECT statements"));
  }
  {
    auto search_path = conn_ctx.Get<VariableType::PgSearchPath>("search_path");
    pg::Objects objects;
    pg::Disallowed disallowed;
    disallowed.relations.emplace(pg::Objects::ObjectName{{}, name});
    pg::ResolveQueryView(db, search_path, objects, disallowed,
                         view->GetObjects(), conn_ctx);
  }

  auto r = catalog.CreateView(db, schema, view, stmt.replace);

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (stmt.replace) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                      ERR_MSG("\"", stmt.view->relname, "\" is not a view"));
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_TABLE),
        ERR_MSG("relation \"", stmt.view->relname, "\" already exists"));
    }
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return {};
}

std::shared_ptr<catalog::PgSqlView> CreateSystemView(const ViewStmt& stmt) {
  SDB_ASSERT(stmt.view);
  SDB_ASSERT(stmt.view->relname);

  std::string_view name = stmt.view->relname;
  auto query = DeparseWithAlias(stmt.query, stmt.view->relname, stmt.aliases);

  return std::make_shared<catalog::PgSqlView>(id::kSystemDB, id::kGenerateNew,
                                              name, std::move(query));
}

}  // namespace sdb::pg
