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
#include "catalog/catalog.h"
#include "catalog/function.h"
#include "catalog/sql_function_impl.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_analyzer_velox.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/types.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

namespace {

template<typename T>
inline int ExprLocation(const T* node) noexcept {
  return ::exprLocation(reinterpret_cast<const Node*>(node));
}

// Parses options and return function body and catalog options.
// For the pre-PG14 syntax function body is stored in the "as" option.
// Example: CREATE FUNCTION foo() RETURNS int AS $$ SELECT 1; $$ LANGUAGE
// For the PG14+ syntax function body is in the sql_body field of
// CreateFunctionStmt.
// Example: CREATE FUNCTION foo() RETURNS int LANGUAGE SQL BEGIN ATOMIC
std::pair<std::string, catalog::FunctionOptions> ParseOptions(
  const List* pg_options) {
  catalog::FunctionOptions options;

  options.language = catalog::FunctionLanguage::SQL;
  options.state = catalog::FunctionState::Volatile;
  options.parallel = catalog::FunctionParallel::Unsafe;
  options.type = catalog::FunctionType::Compute;
  options.kind = catalog::FunctionKind::Scalar;
  options.internal = false;

  std::string function_body;
  VisitNodes(pg_options, [&](const DefElem& option) {
    std::string_view opt_name = option.defname;

    if (opt_name == "language") {
      std::string_view lang = strVal(option.arg);
      if (lang == "sql") {
        options.language = catalog::FunctionLanguage::SQL;
      } else {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("language \"", lang, "\" does not exist"));
      }
    } else if (opt_name == "volatility") {
      std::string_view vol = strVal(option.arg);
      if (vol == "immutable") {
        options.state = catalog::FunctionState::Immutable;
      } else if (vol == "stable") {
        options.state = catalog::FunctionState::Stable;
      } else if (vol == "volatile") {
        options.state = catalog::FunctionState::Volatile;
      } else {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("invalid volatility: ", vol));
      }
    } else if (opt_name == "parallel") {
      std::string_view par = strVal(option.arg);
      if (par == "safe") {
        options.parallel = catalog::FunctionParallel::Safe;
      } else if (par == "restricted") {
        options.parallel = catalog::FunctionParallel::Restricted;
      } else if (par == "unsafe") {
        options.parallel = catalog::FunctionParallel::Unsafe;
      } else {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("invalid parallel mode: ", par));
      }
    } else if (opt_name == "strict") {
      options.strict = boolVal(option.arg);
    } else if (opt_name == "security") {
      options.security = boolVal(option.arg);
    } else if (opt_name == "cost") {
      options.cost = floatVal(option.arg);
    } else if (opt_name == "rows") {
      options.rows = floatVal(option.arg);
    } else if (opt_name == "window") {
      options.kind = catalog::FunctionKind::Window;
    } else if (opt_name == "leakproof") {
    } else if (opt_name == "as") {
      if (IsA(option.arg, List)) {
        List* list = castNode(List, option.arg);
        function_body = strVal(castNode(Node, list_nth(list, 0)));
      } else if (IsA(option.arg, String)) {
        function_body = strVal(option.arg);
      }
    } else if (opt_name == "transform") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("TRANSFORM option is not supported"));
    } else if (opt_name == "support") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("SUPPORT option is not supported"));
    } else if (opt_name == "set") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("SET option is not supported"));
    } else {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("unknown function option: ", opt_name));
    }
  });

  return {std::move(function_body), std::move(options)};
}

}  // namespace

yaclib::Future<Result> CreateFunction(ExecContext& context,
                                      const CreateFunctionStmt& stmt) {
  // TODO: use correct schema
  const auto db = context.GetDatabaseId();

  SDB_ASSERT(stmt.funcname);

  auto current_schema =
    basics::downCast<const ConnectionContext>(context).GetCurrentSchema();

  auto [schema, function_name] =
    ParseObjectName(stmt.funcname, context.GetDatabase(), current_schema);

  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();

  catalog::FunctionProperties properties;
  properties.name = std::string{function_name};

  std::string function_body;
  std::tie(function_body, properties.options) = ParseOptions(stmt.options);
  SDB_ASSERT(!function_body.empty() == !stmt.sql_body);
  if (stmt.sql_body) {
    // all asserts are guaranteed by sql_analyzer_velox.cpp
    SDB_ASSERT(IsA(stmt.sql_body, List));
    const auto* outer_list = castNode(List, stmt.sql_body);
    SDB_ASSERT(list_length(outer_list) == 1);
    const auto* inner_list_node = list_nth_node(Node, outer_list, 0);
    SDB_ASSERT(IsA(inner_list_node, List));
    const auto* inner_list = castNode(List, inner_list_node);
    SDB_ASSERT(list_length(inner_list) == 1);
    auto* body_stmt = list_nth_node(Node, inner_list, 0);
    function_body = pg::Deparse(body_stmt);
  }

  auto& signature = properties.signature;
  signature = pg::ToSignature(stmt.parameters, stmt.returnType);
  if (stmt.is_procedure) {
    SDB_ASSERT(!signature.return_type);
    signature.MarkAsProcedure();
  }

  auto sql_impl = std::make_unique<pg::FunctionImpl>();
  auto r = sql_impl->Init(db, function_name, std::move(function_body),
                          stmt.is_procedure);
  if (!r.ok()) {
    return yaclib::MakeFuture(std::move(r));
  }
  vpack::Builder builder;
  sql_impl->ToVPack(builder);
  properties.implementation = builder.slice();

  auto function = std::make_shared<catalog::Function>(std::move(properties),
                                                      std::move(sql_impl), db);

  r = catalog.CreateFunction(db, schema, function, stmt.replace);

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    SDB_ASSERT(!stmt.replace);
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                    ERR_MSG("relation \"", function_name, "\" already exists"));
  }

  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
