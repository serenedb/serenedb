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
#include "pg/options_parser.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_analyzer_velox.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/types.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

namespace {

using namespace std::string_view_literals;

constexpr EnumOptionInfo<catalog::FunctionLanguage> kLanguage{
  "language", catalog::FunctionLanguage::SQL,
  "Function language (only SQL is supported)"};
constexpr EnumOptionInfo<catalog::FunctionState> kVolatility{
  "volatility", catalog::FunctionState::Volatile, "Volatility category"};
constexpr EnumOptionInfo<catalog::FunctionParallel> kParallel{
  "parallel", catalog::FunctionParallel::Unsafe, "Parallel safety"};
constexpr OptionInfo kStrict{"strict", false, "Returns NULL on NULL input"};
constexpr OptionInfo kSecurity{"security", false, "Security definer"};
constexpr OptionInfo kCost{"cost", 1.0, "Estimated execution cost"};
constexpr OptionInfo kRows{"rows", 0.0, "Estimated number of rows"};
constexpr OptionInfo kWindow{"window", false, "Window function"};
constexpr OptionInfo kLeakproof{"leakproof", false, "Leakproof"};
constexpr OptionInfo kAs{"as", ""sv, "Function body"};
constexpr OptionInfo kFunctionOptions[] = {
  kLanguage, kVolatility, kParallel, kStrict,    kSecurity,
  kCost,     kRows,       kWindow,   kLeakproof, kAs};
constexpr OptionGroup kFunctionGroup{"Function", kFunctionOptions, {}};
constexpr OptionGroup kFunctionOptionGroups[] = {kFunctionGroup};

// Parses options and return function body and catalog options.
// For the pre-PG14 syntax function body is stored in the "as" option.
// Example: CREATE FUNCTION foo() RETURNS int AS $$ SELECT 1; $$ LANGUAGE
// For the PG14+ syntax function body is in the sql_body field of
// CreateFunctionStmt.
// Example: CREATE FUNCTION foo() RETURNS int LANGUAGE SQL BEGIN ATOMIC
class CreateFunctionOptionsParser : public OptionsParser {
 public:
  CreateFunctionOptionsParser(const List* pg_options)
    : OptionsParser{"CREATE FUNCTION",
                    {},
                    {},
                    MakeOptions(pg_options, {}),
                    kFunctionOptionGroups} {
    Parse();
  }

  auto Result() && {
    return std::pair{std::move(_function_body), std::move(_func_options)};
  }

 private:
  void Parse() {
    _func_options.language = EraseOptionOrDefault<kLanguage>();
    if (_func_options.language != catalog::FunctionLanguage::SQL) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("only SQL language is supported for user functions"));
    }

    _func_options.state = EraseOptionOrDefault<kVolatility>();
    _func_options.parallel = EraseOptionOrDefault<kParallel>();
    _func_options.strict = EraseOptionOrDefault<kStrict>();
    _func_options.security = EraseOptionOrDefault<kSecurity>();
    _func_options.type = catalog::FunctionType::Compute;
    _func_options.internal = false;
    _func_options.cost = EraseOptionOrDefault<kCost>();
    _func_options.rows = EraseOptionOrDefault<kRows>();

    auto window_location = OptionLocation(kWindow);
    if (EraseOptionOrDefault<kWindow>()) {
      THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(window_location)),
                      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("WINDOW functions are not supported"));
    }

    auto leakproof_location = OptionLocation(kLeakproof);
    if (EraseOptionOrDefault<kLeakproof>()) {
      THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(leakproof_location)),
                      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("LEAKPROOF functions are not supported"));
    }

    ParseFunctionBody();
  }

  void ParseFunctionBody() {
    const auto* option = EraseOption(kAs);
    if (!option) {
      return;
    }
    if (IsA(option->arg, List)) {
      List* list = castNode(List, option->arg);
      _function_body = strVal(castNode(Node, list_nth(list, 0)));
    } else if (IsA(option->arg, String)) {
      _function_body = strVal(option->arg);
    }
  }

  std::string _function_body;
  catalog::FunctionOptions _func_options;
};

}  // namespace

std::shared_ptr<catalog::Function> CreateFunctionImpl(
  const Config* config, ObjectId database_id, std::string_view database_name,
  std::string_view current_schema, const CreateFunctionStmt& stmt) {
  SDB_ASSERT(stmt.funcname);

  auto function_name =
    ParseObjectName(stmt.funcname, database_name, current_schema).relation;

  catalog::FunctionProperties properties;
  properties.name = std::string{function_name};

  std::string function_body;
  std::tie(function_body, properties.options) =
    CreateFunctionOptionsParser{stmt.options}.Result();
  SDB_ASSERT(!function_body.empty() == !stmt.sql_body);
  if (stmt.sql_body) {
    // All checks for user functions are guaranteed by sql_analyzer_velox.cpp,
    // but that is not the case for system functions, so throw if something in
    // system functions is not OK.
    SDB_ENSURE(IsA(stmt.sql_body, List), ERROR_INTERNAL);
    const auto* outer_list = castNode(List, stmt.sql_body);
    SDB_ENSURE(list_length(outer_list) == 1, ERROR_INTERNAL);
    const auto* inner_list_node = list_nth_node(Node, outer_list, 0);
    SDB_ENSURE(IsA(inner_list_node, List), ERROR_INTERNAL);
    const auto* inner_list = castNode(List, inner_list_node);
    SDB_ENSURE(list_length(inner_list) == 1, ERROR_INTERNAL);
    auto* body_stmt = list_nth_node(Node, inner_list, 0);
    function_body = pg::DeparseStmt(body_stmt);
  }

  auto& signature = properties.signature;
  signature = pg::ToSignature(stmt.parameters, stmt.returnType);
  if (stmt.is_procedure) {
    SDB_ASSERT(!signature.return_type);
    signature.MarkAsProcedure();
  }

  auto sql_impl = std::make_unique<pg::FunctionImpl>();
  auto r = sql_impl->Init(database_id, function_name, std::move(function_body),
                          stmt.is_procedure, config);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  if (config) {
    // Case for non system views
    vpack::Builder builder;
    sql_impl->ToVPack(builder);
    properties.implementation = builder.slice();
  }

  return std::make_shared<catalog::Function>(std::move(properties),
                                             std::move(sql_impl), database_id);
}

yaclib::Future<Result> CreateFunction(ExecContext& context,
                                      const CreateFunctionStmt& stmt) {
  SDB_ASSERT(stmt.funcname);

  auto database_name = context.GetDatabase();
  const auto database_id = context.GetDatabaseId();

  auto& connection_context = basics::downCast<const ConnectionContext>(context);
  auto current_schema = connection_context.GetCurrentSchema();
  auto schema =
    ParseObjectName(stmt.funcname, database_name, current_schema).schema;

  auto function = CreateFunctionImpl(&connection_context, database_id,
                                     database_name, current_schema, stmt);

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();

  auto r = catalog.CreateFunction(database_id, schema, function, stmt.replace);

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    SDB_ASSERT(!stmt.replace);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_TABLE),
      ERR_MSG("relation \"", function->GetName(), "\" already exists"));
  }

  return yaclib::MakeFuture(std::move(r));
}

std::shared_ptr<catalog::Function> CreateSystemFunction(
  const CreateFunctionStmt& stmt) {
  return CreateFunctionImpl(nullptr, id::kSystemDB, "", "", stmt);
}

}  // namespace sdb::pg
