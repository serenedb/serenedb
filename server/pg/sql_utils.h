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

#pragma once

#include <array>
#include <memory>

#include "basics/assert.h"
#include "catalog/fwd.h"

struct Node;
struct AConst;
struct AExpr;
struct AArrayExpr;
struct BooleanTest;
struct NullTest;
struct SelectStmt;
struct DeleteStmt;
struct UpdateStmt;
struct MergeStmt;
struct InsertStmt;
struct CaseExpr;
struct CoalesceExpr;
struct BoolExpr;
struct FuncCall;
struct ColumnRef;
struct ExplainStmt;
struct WithClause;
struct ParamRef;
struct RangeSubselect;
struct RangeVar;
struct JoinExpr;
struct IntoClause;
struct LockingClause;
struct List;
struct MinMaxExpr;
struct MemoryContextData;
struct RawStmt;
struct RowExpr;
struct TypeCast;
struct JsonArrayConstructor;
struct JsonArrayQueryConstructor;
struct JsonObjectConstructor;
struct SQLValueFunction;
struct SubLink;
struct TypeName;

namespace sdb {
class QueryString;
namespace pg {

std::string GetUnnamedFunctionArgumentName(size_t param_idx);

catalog::FunctionSignature ToSignature(const List* pg_parameters,
                                       const TypeName* pg_return_type);

std::string DeparseStmt(Node* node);

std::string DeparseExpr(Node* expr);

struct MemoryContextDeleter {
  void operator()(MemoryContextData* p) const noexcept;
};

using MemoryContextPtr =
  std::unique_ptr<MemoryContextData, MemoryContextDeleter>;

[[nodiscard]] MemoryContextPtr CreateMemoryContext();
void ResetMemoryContext(MemoryContextData& ctx) noexcept;

struct MemoryContextScopeGuard {
  void operator()(MemoryContextData* p) const noexcept;
};

using MemoryContextScope =
  std::unique_ptr<MemoryContextData, MemoryContextScopeGuard>;

[[nodiscard]] MemoryContextScope EnterMemoryContext(
  MemoryContextData& ctx) noexcept;

static constexpr size_t kSqlStateSize = 5;

// Unpack MAKE_SQLSTATE code.
template<typename T>
void UnpackSqlState(T& buf, int sql_state) {
  if constexpr (requires(T c) { std::size(buf); }) {
    SDB_ASSERT(std::size(buf) >= kSqlStateSize);
  }

  for (size_t i = 0; i < 5; i++) {
    buf[i] = (sql_state & 0x3F) + '0';
    sql_state >>= 6;
  }
}

int ErrorPosition(const char* source_text, int location);

std::tuple<std::string_view, std::string_view, std::string_view>
GetDbSchemaRelation(const List* names);

std::string NameToStr(const List* names);

enum class SqlCommandType : uint32_t {
  Unknown = 0,
  Select,       // select stmt
  Update,       // update stmt
  Insert,       // insert stmt
  Delete,       // delete stmt
  Merge,        // merge stmt
  DDL,          // cmds like create, destroy, copy, vacuum, * etc.
  Explain,      // explain stmt
  Transaction,  // some transaction stmt
  Set,          // set stmt
  Show,         // show stmt
  Nothing,      // dummy command for instead nothing rules with qual
  Call,         // call stmt
};

}  // namespace pg
}  // namespace sdb

// clang-format off

#define LIBPG_QUERY_INCLUDES_BEGIN \
  _Pragma("push_macro(\"LOG\")") \
  _Pragma("push_macro(\"INFO\")") \
  _Pragma("push_macro(\"NOTICE\")") \
  _Pragma("push_macro(\"WARNING\")") \
  _Pragma("push_macro(\"ERROR\")") \
  _Pragma("push_macro(\"FATAL\")") \
  _Pragma("push_macro(\"PANIC\")") \
  _Pragma("push_macro(\"vsnprintf\")") \
  _Pragma("push_macro(\"snprintf\")") \
  _Pragma("push_macro(\"vsprintf\")") \
  _Pragma("push_macro(\"sprintf\")") \
  _Pragma("push_macro(\"vfprintf\")") \
  _Pragma("push_macro(\"fprintf\")") \
  _Pragma("push_macro(\"vprintf\")") \
  _Pragma("push_macro(\"printf\")") \
  _Pragma("push_macro(\"foreach\")") \
  extern "C" {

#define LIBPG_QUERY_INCLUDES_END \
  } /* extern "C" */ \
  _Pragma("pop_macro(\"LOG\")") \
  _Pragma("pop_macro(\"INFO\")") \
  _Pragma("pop_macro(\"NOTICE\")") \
  _Pragma("pop_macro(\"WARNING\")") \
  _Pragma("pop_macro(\"ERROR\")") \
  _Pragma("pop_macro(\"FATAL\")") \
  _Pragma("pop_macro(\"PANIC\")") \
  _Pragma("pop_macro(\"vsnprintf\")") \
  _Pragma("pop_macro(\"snprintf\")") \
  _Pragma("pop_macro(\"vsprintf\")") \
  _Pragma("pop_macro(\"sprintf\")") \
  _Pragma("pop_macro(\"vfprintf\")") \
  _Pragma("pop_macro(\"fprintf\")") \
  _Pragma("pop_macro(\"vprintf\")") \
  _Pragma("pop_macro(\"printf\")") \
  _Pragma("pop_macro(\"foreach\")")

// clang-format on
