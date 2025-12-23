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

#include "sql_utils.h"

#include <absl/numeric/bits.h>
#include <absl/strings/str_cat.h>
#include <velox/type/Type.h>

#include "basics/containers/flat_hash_set.h"
#include "basics/utf8_utils.hpp"
#include "catalog/function.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_analyzer_velox.h"
#include "pg/sql_error.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "pg_query.h"
#include "postgres_deparse.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

template<typename T>
inline int ExprLocation(const T* node) noexcept {
  return ::exprLocation(reinterpret_cast<const Node*>(node));
}

std::string GetUnnamedFunctionArgumentName(size_t idx) {
  SDB_ASSERT(idx > 0);
  return absl::StrCat("$", idx);
}

// TODO: use errorPosition in THROW_SQL_ERROR calls
catalog::FunctionSignature ToSignature(const List* pg_parameters,
                                       const TypeName* pg_return_type) {
  catalog::FunctionSignature signature;

  containers::FlatHashSet<std::string_view> unique_names;
  unique_names.reserve(list_length(pg_parameters));
  auto to_sql_parameter =
    [&unique_names](
      size_t idx,
      const ::FunctionParameter& pg_param) -> catalog::FunctionParameter {
    catalog::FunctionParameter param;

    if (pg_param.name) {
      auto [_, emplaced] = unique_names.emplace(pg_param.name);
      if (!emplaced) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        CURSOR_POS(ExprLocation(pg_param.name)),
                        ERR_MSG("parameter name \"", pg_param.name,
                                "\" used more than once"));
      }
      param.name = pg_param.name;
    } else {
      param.name = GetUnnamedFunctionArgumentName(idx + 1);
    }

    if (pg_param.argType) {
      param.type = pg::NameToType(*pg_param.argType);
    }

    if (pg_param.defexpr) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      CURSOR_POS(ExprLocation(pg_param.defexpr)),
                      ERR_MSG("default parameter values are not supported"));
    }

    param.mode = [&] {
      switch (pg_param.mode) {
        using enum catalog::FunctionParameter::Mode;
        case FUNC_PARAM_IN:
        case FUNC_PARAM_DEFAULT:
          return In;
        case FUNC_PARAM_OUT:
          return Out;
        case FUNC_PARAM_INOUT:
          return InOut;
        case FUNC_PARAM_VARIADIC:
          return Variadic;
        case FUNC_PARAM_TABLE:
          SDB_ENSURE(false, ERROR_NOT_IMPLEMENTED);
      }
    }();

    return param;
  };

  std::vector<velox::TypePtr> table_types;
  std::vector<std::string> table_names;
  // ^ RETURNS TABLE

  for (size_t i = 0; i < list_length(pg_parameters); ++i) {
    const auto& param =
      *castNode(::FunctionParameter, list_nth(pg_parameters, i));
    if (param.mode != FUNC_PARAM_TABLE) {
      signature.parameters.emplace_back(to_sql_parameter(i, param));
    } else {
      table_types.emplace_back(pg::NameToType(*param.argType));
      SDB_ASSERT(table_types.back());
      SDB_ASSERT(param.name);
      table_names.emplace_back(param.name);
    }
  }

  SDB_ASSERT(table_types.size() == table_names.size());
  auto& return_type = signature.return_type;
  if (!table_types.empty()) {
    return_type = velox::ROW(std::move(table_names), std::move(table_types));
  } else if (pg_return_type) {
    return_type = pg::NameToType(*pg_return_type);
    SDB_ASSERT(return_type);
  }

  return signature;
}

bool IsExpr(const Node* node) {
  switch (nodeTag(node)) {
    case T_ColumnRef:
    case T_A_Const:
    case T_ParamRef:
    case T_A_Indirection:
    case T_CaseExpr:
    case T_SubLink:
    case T_A_ArrayExpr:
    case T_RowExpr:
    case T_GroupingFunc:
    case T_TypeCast:
    case T_CollateClause:
    case T_A_Expr:
    case T_BoolExpr:
    case T_NullTest:
    case T_BooleanTest:
    case T_JsonIsPredicate:
    case T_SetToDefault:
    case T_MergeSupportFunc:
    case T_JsonParseExpr:
    case T_JsonScalarExpr:
    case T_JsonSerializeExpr:
    case T_JsonFuncExpr:
    case T_FuncCall:
    case T_SQLValueFunction:
    case T_MinMaxExpr:
    case T_CoalesceExpr:
    case T_XmlExpr:
    case T_XmlSerialize:
    case T_JsonObjectAgg:
    case T_JsonArrayAgg:
    case T_JsonObjectConstructor:
    case T_JsonArrayConstructor:
    case T_JsonArrayQueryConstructor:
      return true;
    default:
      return false;
  }
}

std::string DeparseStmt(Node* node) {
  SDB_ASSERT(!IsExpr(node));

  auto ctx = CreateMemoryContext();
  auto scope = EnterMemoryContext(*ctx);
  StringInfoData buf;
  initStringInfo(&buf);
  RawStmt raw_stmt{
    .type = node->type, .stmt = node, .stmt_location = -1, .stmt_len = 0};
  deparseRawStmt(&buf, &raw_stmt);
  std::string query_sql{buf.data, static_cast<size_t>(buf.len)};
  pfree(buf.data);
  return query_sql;
}

std::string DeparseExpr(Node* expr) {
  SDB_ASSERT(IsExpr(expr));

  ResTarget dummy_res_target{};
  dummy_res_target.val = expr;
  dummy_res_target.location = -1;
  dummy_res_target.type = T_ResTarget;

  List* target_list = list_make1(&dummy_res_target);

  SelectStmt dummy_select{};
  dummy_select.targetList = target_list;
  dummy_select.type = T_SelectStmt;

  auto deparsed = DeparseStmt(castNode(Node, &dummy_select));

  static constexpr std::string_view kSelectPrefix = "SELECT ";
  SDB_ASSERT(deparsed.starts_with(kSelectPrefix));
  deparsed = deparsed.substr(kSelectPrefix.size());

  return deparsed;
}

void MemoryContextDeleter::operator()(MemoryContext p) const noexcept {
  SDB_ASSERT(p);
  SDB_ASSERT(MemoryContextIsValid(p));
  SDB_ASSERT(p != TopMemoryContext);
  SDB_ASSERT(p != CurrentMemoryContext);

  MemoryContextDelete(p);
}

MemoryContextPtr CreateMemoryContext() {
  // Note: to make enableFreeListIndex true we need to call
  // pg_query_init before delete
  MemoryContext ctx = AllocSetContextCreateInternal(
    nullptr,  // pretend that we create root memory context,
    "serenedb", ALLOCSET_DEFAULT_SIZES, false);
  SDB_ASSERT(ctx);

  return MemoryContextPtr{ctx};
}

void ResetMemoryContext(MemoryContextData& ctx) noexcept {
  MemoryContextReset(&ctx);
}

void MemoryContextScopeGuard::operator()(MemoryContext p) const noexcept {
  SDB_ASSERT(p);
  SDB_ASSERT(MemoryContextIsValid(p));
  MemoryContextSwitchTo(p);
}

MemoryContextScope EnterMemoryContext(MemoryContextData& ctx) noexcept {
  SDB_ASSERT(MemoryContextIsValid(&ctx));

  pg_query_init();  // Ensure TopMemoryContext is initialized

  auto old = MemoryContextSwitchTo(&ctx);
  return MemoryContextScope{old};
}

int ErrorPosition(const char* source_text, int location) {
  if (location < 0 || !source_text) {
    return 0;
  }

  // TODO(gnusi): We must honor DB encoding
  return irs::utf8_utils::Length(
    {reinterpret_cast<const irs::byte_type*>(source_text),
     static_cast<size_t>(location)});
}

std::tuple<std::string_view, std::string_view, std::string_view>
GetDbSchemaRelation(const List* names) {
  PgStrListWrapper wrapper{names};
  auto it = wrapper.rbegin();
  SDB_ASSERT(it != wrapper.rend());

  std::string_view relation;
  relation = *(it++);

  std::string_view schema;
  if (it != wrapper.rend()) {
    schema = *(it++);
  }

  std::string_view db;
  if (it != wrapper.rend()) {
    db = *(it++);
  }

  SDB_ENSURE(it == wrapper.rend(), ERROR_NOT_IMPLEMENTED,
             "unsupported object with too many dotted names");

  return {db, schema, relation};
}

std::string NameToStr(const List* name) {
  std::string result;
  VisitNodes(name, [&](const String& n) {
    if (!result.empty()) {
      result.append(".");
    }
    result.append(n.sval);
  });
  return result;
}

}  // namespace sdb::pg
