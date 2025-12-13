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
#include "catalog/sql_query_view.h"
#include "catalog/view.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

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

  std::string body = pg::Deparse(select);
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

yaclib::Future<Result> CreateView(const ExecContext& context,
                                  const ViewStmt& stmt) {
  // TODO: use correct schema
  const auto db = context.GetDatabaseId();
  auto current_schema =
    basics::downCast<const ConnectionContext>(context).GetCurrentSchema();
  const std::string_view schema = stmt.view->schemaname
                                    ? std::string_view{stmt.view->schemaname}
                                    : current_schema;

  SDB_ASSERT(stmt.view);
  SDB_ASSERT(stmt.view->relname);

  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();

  catalog::ViewOptions options;
  options.meta.name = stmt.view->relname;
  options.meta.type = catalog::ViewType::ViewSqlQuery;

  vpack::Builder builder;
  builder.openObject();
  builder.add("query",
              DeparseWithAlias(stmt.query, stmt.view->relname, stmt.aliases));
  builder.close();
  options.properties = builder.slice();

  std::shared_ptr<catalog::View> view;
  auto r = SqlQueryView::Make(view, db, std::move(options),
                              catalog::ViewContext::User);
  if (!r.ok()) {
    return yaclib::MakeFuture(std::move(r));
  }

  r = catalog.CreateView(db, schema, view);
  // TODO(ISSUE#214): use replace view instead,
  // code below has race condition and ugly
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (!stmt.replace) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_TABLE),
        ERR_MSG("relation \"", stmt.view->relname, "\" already exists"));
    }

    r = catalog.DropView(db, schema, stmt.view->relname);
    if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                      ERR_MSG("\"", stmt.view->relname, "\" is not a view"));
    }
    r = catalog.CreateView(db, schema, std::move(view));
  }

  return yaclib::MakeFuture(std::move(r));
}

std::shared_ptr<catalog::View> CreateSystemView(const ViewStmt& stmt) {
  SDB_ASSERT(stmt.view);
  SDB_ASSERT(stmt.view->relname);

  catalog::ViewOptions options;
  options.meta.name = stmt.view->relname;
  options.meta.type = catalog::ViewType::ViewSqlQuery;

  vpack::Builder builder;
  builder.openObject();
  builder.add("query",
              DeparseWithAlias(stmt.query, stmt.view->relname, stmt.aliases));
  builder.close();
  options.properties = builder.slice();

  std::shared_ptr<catalog::View> view;
  // TODO why ViewContext::Internal and id::kInvalid() does not work?
  auto r = SqlQueryView::Make(view, id::kSystemDB, std::move(options),
                              catalog::ViewContext::User);

  SDB_ASSERT(r.ok(), "Cannot make system view");

  return view;
}

}  // namespace sdb::pg
