////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "pg/isolation_level.h"

#include <absl/strings/match.h>

#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {
namespace {

std::string GetIsolationLevelFromList(List* args) {
  std::string level;
  VisitNodes(args, [&](const DefElem& option) {
    std::string_view opt_name = option.defname;
    if (opt_name == kTransactionIsolation) {
      level = strVal(&castNode(A_Const, option.arg)->val);
      return;
    }
    if (opt_name == "transaction_read_only") {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("transaction READ WRITE | READ ONLY is not supported yet"));
    }
    if (opt_name == "transaction_deferrable") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("transaction DEFERRABLE is not supported yet"));
    }
    SDB_ASSERT(!opt_name.data());
  });
  return level;
}

}  // namespace

std::string GetIsolationLevel(const VariableSetStmt& stmt) {
  if (stmt.kind == VAR_SET_MULTI) {
    return GetIsolationLevelFromList(stmt.args);
  }
  SDB_ASSERT(stmt.kind == VAR_SET_VALUE);
  if (list_length(stmt.args) != 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("SET ", stmt.name, " takes only one argument"));
  }
  const auto& value = *list_nth_node(A_Const, stmt.args, 0);
  switch (nodeTag(&value.val)) {
    case T_Integer:
      return absl::StrCat(intVal(&value.val));
    case T_Float:
      return absl::StrCat(floatVal(&value.val));
    case T_Boolean:
      return absl::StrCat(boolVal(&value.val));
    case T_String:
      return std::string{strVal(&value.val)};
    default:
      SDB_UNREACHABLE();
  }
}

std::string GetIsolationLevel(const TransactionStmt& stmt) {
  SDB_ASSERT(stmt.kind == TRANS_STMT_BEGIN || stmt.kind == TRANS_STMT_START);
  return GetIsolationLevelFromList(stmt.options);
}

void ValidateIsolationLevel(std::string_view isolation_level,
                            std::string_view parameter_name) {
  const bool is_known = IsKnownIsolationLevel(isolation_level);
  const bool is_supported = IsSupportedIsolationLevel(isolation_level);

  static constexpr std::string_view kHint =
    "Available values: repeatable read, read committed.";
  if (is_known && !is_supported) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("transaction isolation level \"", isolation_level,
                            "\" is not supported"),
                    ERR_HINT(kHint));
  }
  if (!is_supported) {
    SDB_ASSERT(!parameter_name.empty());
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid value for parameter \"", parameter_name,
                            "\": \"", isolation_level, "\""),
                    ERR_HINT(kHint));
  }
}

bool IsKnownIsolationLevel(std::string_view data) {
  return absl::EqualsIgnoreCase(data, kReadUncommitted) ||
         absl::EqualsIgnoreCase(data, kReadCommitted) ||
         absl::EqualsIgnoreCase(data, kRepeatableRead) ||
         absl::EqualsIgnoreCase(data, kSerializable);
}

bool IsSupportedIsolationLevel(std::string_view data) {
  return absl::EqualsIgnoreCase(data, kReadCommitted) ||
         absl::EqualsIgnoreCase(data, kRepeatableRead);
}

}  // namespace sdb::pg
