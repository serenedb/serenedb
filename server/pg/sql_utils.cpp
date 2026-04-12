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

#include <absl/functional/overload.h>
#include <absl/numeric/bits.h>
#include <absl/strings/str_cat.h>

#include <iresearch/utils/utf8_utils.hpp>

#include "basics/containers/flat_hash_set.h"
#include "catalog/function.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_collector.h"
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

std::string_view ToPgObjectTypeName(int pg_object_type) noexcept {
  switch (pg_object_type) {
    case OBJECT_TABLE:
      return "table";
    case OBJECT_INDEX:
      return "index";
    case OBJECT_VIEW:
      return "view";
    case OBJECT_FUNCTION:
      return "function";
    case OBJECT_SCHEMA:
      return "schema";
    case OBJECT_TSDICTIONARY:
      return "text search dictionary";
    default:
      return "object";
  }
}

template<typename Str>
Str DeparseTypeName(const TypeName* type_name) {
  const auto* names = type_name->names;
  if (list_length(names) > 0) {
    return strVal(linitial(names));
  }

  return {};
}

template<typename T>
std::optional<T> TryGetImpl(const Node* expr) {
  SDB_ASSERT(expr);

  if constexpr (std::is_same_v<T, int>) {
    if (nodeTag(expr) == T_Integer) {
      return intVal(expr);
    }
    if (nodeTag(expr) == T_String) {
      const std::string_view str = strVal(expr);
      if (int i = 0; absl::SimpleAtoi(str, &i)) {
        return i;
      }
    }
  } else if constexpr (std::is_same_v<T, double>) {
    if (nodeTag(expr) == T_Float) {
      return floatVal(expr);
    }
    if (nodeTag(expr) == T_Integer) {
      return intVal(expr);
    }
    if (nodeTag(expr) == T_String) {
      const std::string_view str = strVal(expr);
      if (int i = 0; absl::SimpleAtoi(str, &i)) {
        return i;
      }
      if (double d = 0; absl::SimpleAtod(str, &d)) {
        return d;
      }
    }
  } else if constexpr (std::is_same_v<T, std::string_view> ||
                       std::is_same_v<T, std::string>) {
    if (nodeTag(expr) == T_String) {
      return strVal(expr);
    }
    if (nodeTag(expr) == T_TypeName) {
      return DeparseTypeName<T>(castNode(TypeName, expr));
    }
    if (nodeTag(expr) == T_Float) {
      return ((Float*)(expr))->fval;
    }
    if constexpr (std::is_same_v<T, std::string>) {
      if (nodeTag(expr) == T_Integer) {
        return absl::StrCat(intVal(expr));
      }
    }
    if (nodeTag(expr) == T_Boolean) {
      if (boolVal(expr)) {
        return "true";
      } else {
        return "false";
      }
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
    if (nodeTag(expr) == T_Boolean) {
      return boolVal(expr);
    }

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
    case T_TypeName: {
      return DeparseTypeName<std::string>(castNode(TypeName, expr));
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

// Moved from sql_collector.cpp
Objects::ObjectName ParseObjectName(const List* names,
                                    std::string_view database,
                                    std::string_view default_schema) {
  return VisitName(
    names, absl::Overload{
             [&](std::string_view relation) {
               return Objects::ObjectName{default_schema, relation};
             },
             [](std::string_view schema, std::string_view relation) {
               return Objects::ObjectName{schema, relation};
             },
             [&](std::string_view db, std::string_view schema,
                 std::string_view relation) {
               if (database.data() && db != database) {
                 THROW_SQL_ERROR(
                   ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                   ERR_MSG("Cross database queries are not allowed: ", db,
                           " accessed instead of ", database));
               }
               return Objects::ObjectName{schema, relation};
             },
             [&](...) -> Objects::ObjectName {
               THROW_SQL_ERROR(
                 ERR_CODE(ERRCODE_SYNTAX_ERROR),
                 ERR_MSG("improper qualified name (too many dotted names)"));
             },
           });
}

Objects::ObjectName ParseObjectName(std::string_view name,
                                    std::string_view default_schema) {
  const auto pos = name.find('.');
  auto schema_name =
    pos == std::string_view::npos ? default_schema : name.substr(0, pos);
  auto object_name =
    pos == std::string_view::npos ? name : name.substr(pos + 1);
  return {.schema = schema_name, .relation = object_name};
}

}  // namespace sdb::pg
