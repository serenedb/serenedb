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
std::optional<T> TryGetImpl(const Node* expr) {
  SDB_ASSERT(expr);

  if constexpr (std::is_same_v<T, int>) {
    if (nodeTag(expr) == T_Integer) {
      return intVal(expr);
    }
  } else if constexpr (std::is_same_v<T, double>) {
    if (nodeTag(expr) == T_Float) {
      return floatVal(expr);
    }
  } else if constexpr (std::is_same_v<T, std::string_view> ||
                       std::is_same_v<T, std::string>) {
    if (nodeTag(expr) == T_String) {
      return strVal(expr);
    }
  } else if constexpr (std::is_same_v<T, char>) {
    if (nodeTag(expr) == T_String) {
      std::string_view str = strVal(expr);
      if (str.size() != 1) {
        return {};
      }
      return str[0];
    }
  } else if constexpr (std::is_same_v<T, bool>) {
    if (auto val = TryGet<std::string_view>(expr)) {
      if (*val == "true" || *val == "on") {
        return true;
      }
      if (*val == "false" || *val == "off") {
        return false;
      }
      return {};
    }

    if (auto val = TryGet<int>(expr)) {
      switch (*val) {
        case 0:
          return false;
        case 1:
          return true;
        default:
          return {};
      }
    }

    return {};
  } else {
    static_assert(false);
  }
  return {};
}

template<typename T>
std::optional<T> TryGet(const Node* expr) {
  if (!expr) {
    return {};
  }

  if (nodeTag(expr) == T_A_Const) {
    const auto& a_const = *castNode(A_Const, expr);
    if (a_const.isnull) {
      return {};
    }
    return TryGetImpl<T>(castNode(Node, &a_const.val));
  }

  return TryGetImpl<T>(expr);
}

template<typename T>
std::optional<T> TryGet(const Node& node) {
  return TryGet<T>(&node);
}

template<typename T>
std::optional<T> TryGet(const List* list, size_t i) {
  if (i < list_length(list)) {
    return TryGet<T>(castNode(Node, list_nth(list, i)));
  }
  return {};
}

#define SDB_DECLARE_TRYGET(T)                       \
  template std::optional<T> TryGet<T>(const Node*); \
  template std::optional<T> TryGet<T>(const Node&); \
  template std::optional<T> TryGet<T>(const List*, size_t)

SDB_DECLARE_TRYGET(int);
SDB_DECLARE_TRYGET(double);
SDB_DECLARE_TRYGET(std::string_view);
SDB_DECLARE_TRYGET(char);
SDB_DECLARE_TRYGET(std::string);
SDB_DECLARE_TRYGET(bool);
#undef SDB_DECLARE_TRYGET

bool IsDistinctAll(const List* distinct_clause) noexcept {
  return list_length(distinct_clause) == 1 &&
         list_nth(distinct_clause, 0) == nullptr;
}

int ExprLocation(const void* node) noexcept {
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

static std::string DeparseStmtImpl(Node* node) {
  SDB_ASSERT(!IsExpr(node));
  StringInfoData buf;
  initStringInfo(&buf);
  RawStmt raw_stmt{
    .type = node->type, .stmt = node, .stmt_location = -1, .stmt_len = 0};
  deparseRawStmt(&buf, &raw_stmt);
  std::string query_sql{buf.data, static_cast<size_t>(buf.len)};
  pfree(buf.data);
  return query_sql;
}

std::string DeparseStmt(Node* node) {
  SDB_ASSERT(!IsExpr(node));

  auto ctx = CreateMemoryContext();
  auto scope = EnterMemoryContext(*ctx);
  return DeparseStmtImpl(node);
}

std::string DeparseValue(Node* expr) {
  switch (nodeTag(expr)) {
    case T_String: {
      return strVal(expr);
    }
    case T_Integer: {
      return absl::StrCat(intVal(expr));
    }
    case T_Float: {
      return absl::StrCat(floatVal(expr));
    }
    case T_Boolean: {
      return boolVal(expr) ? "true" : "false";
    }
    default:
      SDB_ASSERT(false);
      return "";
  }
}

std::string DeparseExpr(Node* expr) {
  SDB_ASSERT(IsExpr(expr));

  ResTarget dummy_res_target{};
  dummy_res_target.val = expr;
  dummy_res_target.location = -1;
  dummy_res_target.type = T_ResTarget;

  auto ctx = CreateMemoryContext();
  auto scope = EnterMemoryContext(*ctx);

  List* target_list = list_make1(&dummy_res_target);

  SelectStmt dummy_select{};
  dummy_select.targetList = target_list;
  dummy_select.type = T_SelectStmt;

  auto deparsed = DeparseStmtImpl(castNode(Node, &dummy_select));

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

int ErrorPosition(std::string_view source_text, int location) {
  if (location < 0 || source_text.size() <= static_cast<size_t>(location)) {
    return 0;
  }

  // TODO(gnusi): We must honor DB encoding
  return irs::utf8_utils::Length(
    {reinterpret_cast<const irs::byte_type*>(source_text.data()),
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
