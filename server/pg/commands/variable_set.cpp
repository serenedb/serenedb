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

#include <yaclib/async/make.hpp>

#include "basics/down_cast.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"
#include "query/config.h"

namespace sdb::pg {
namespace {

std::string ProcessValue(const A_Const& value) {
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

bool NeedQuotes(std::string_view str) {
  SDB_ASSERT(str.data() != nullptr);
  if (str.empty()) {
    return true;
  }
  if (str[0] == '_' || absl::ascii_isalpha(str[0])) {
    return absl::c_any_of(str, [](char c) {
      return absl::ascii_isupper(c) || (!absl::ascii_isalnum(c) && c != '_');
    });
  }

  if (absl::ascii_isdigit(str[0])) {
    return !absl::c_all_of(str, absl::ascii_isdigit);
  }
  return true;
}

Result ProcessValues(const List* list, std::string_view name, VariableType type,
                     Config& config, Config::VariableContext context) {
  std::string values;
  if (type == VariableType::PgSearchPath || type == VariableType::String) {
    VisitNodes(list, [&](const A_Const& value) {
      std::string value_str = ProcessValue(value);
      bool need_quotes = NeedQuotes(value_str);
      if (need_quotes) {
        if (!values.empty()) {
          absl::StrAppend(&values, ", \"", std::move(value_str), "\"");
        } else {
          absl::StrAppend(&values, "\"", std::move(value_str), "\"");
        }
      } else {
        if (!values.empty()) {
          absl::StrAppend(&values, ", ", std::move(value_str));
        } else {
          absl::StrAppend(&values, std::move(value_str));
        }
      }
    });
    config.Set(context, name, std::move(values));
    return {};
  }

  SDB_ASSERT(list_length(list) == 1);
  if (list_length(list) != 1) {
    return {ERROR_FAILED, "SET ", name, " takes only one argument"};
  }
  values = ProcessValue(*list_nth_node(A_Const, list, 0));

  if (!ValidateValue(type, values)) {
    return {ERROR_FAILED, "parameter \"", name,
            "\" requires different value type"};
  }
  config.Set(context, name, std::move(values));
  return {};
}

#ifdef SDB_FAULT_INJECTION

yaclib::Future<Result> ProcessFailurePoint(std::string_view stmt_name,
                                           const VariableSetStmt& stmt) {
  SDB_ASSERT(stmt_name.starts_with(kFailPointPrefix));
  stmt_name.remove_prefix(kFailPointPrefix.size());
  if (stmt_name == "s") {
    if (stmt.kind != VAR_RESET) {
      return yaclib::MakeFuture<Result>(ERROR_FAILED,
                                        "only RESET sdb_faults is valid");
    }
    ClearFailurePointsDebugging();
    return {};
  }
  if (!stmt_name.starts_with('_')) {
    return yaclib::MakeFuture<Result>(
      ERROR_FAILED, "failure point configuration parameter must start with '",
      kFailPointPrefix, "_'");
  }
  stmt_name.remove_prefix(1);
  if (stmt.kind == VAR_RESET) {
    if (!RemoveFailurePointDebugging(stmt_name)) {
      return yaclib::MakeFuture<Result>(ERROR_FAILED, "failure point '",
                                        stmt_name,
                                        "' not set so cannot remove");
    }
  } else if (stmt.kind == VAR_SET_DEFAULT) {
    if (!AddFailurePointDebugging(stmt_name)) {
      return yaclib::MakeFuture<Result>(ERROR_FAILED, "failure point '",
                                        stmt_name,
                                        "' already set so cannot add");
    }
  } else {
    return yaclib::MakeFuture<Result>(
      ERROR_FAILED,
      "only SET ... TO DEFAULT and RESET are supported for fail points");
  }
  return {};
}

#endif

std::string GetIsolationLevel(const VariableSetStmt& stmt) {
  if (stmt.kind == VAR_SET_MULTI) {
    std::string level;
    VisitNodes(stmt.args, [&](const DefElem& option) {
      std::string_view opt_name = option.defname;
      if (opt_name == "transaction_isolation") {
        level = strVal(&castNode(A_Const, option.arg)->val);
        if (!ValidateValue(VariableType::SdbTransactionIsolation, level)) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          ERR_MSG("transaction isolation level \"", level,
                                  "\" is not supported"));
        }
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
  SDB_ASSERT(stmt.kind == VAR_SET_VALUE);
  if (list_length(stmt.args) != 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("SET ", stmt.name, " takes only one argument"));
  }
  return ProcessValue(*list_nth_node(A_Const, stmt.args, 0));
};

constexpr std::string_view IsolationLevelName(IsolationLevel isolation_level) {
  switch (isolation_level) {
    case IsolationLevel::ReadCommitted:
      return "read committed";
    case IsolationLevel::RepeatableRead:
      return "repeatable read";
    default:
      SDB_UNREACHABLE();
  }
}

void ValidateIsolationLevel(std::string_view isolation_level) {
  const bool is_unsupported =
    isolation_level == "read uncommitted" || isolation_level == "serializable";
  if (is_unsupported) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("transaction isolation level \"", isolation_level,
                            "\" is not supported"));
  }
  if (!ValidateValue(VariableType::SdbTransactionIsolation, isolation_level)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid value for parameter \"transaction_isolation\": \"",
              isolation_level, "\""));
  }
}

// SET TRANSACTION statement and "transaction_isolation" variable processing
void ProcessTransactionIsolation(sdb::ConnectionContext& conn_ctx,
                                 const VariableSetStmt& stmt) {
  switch (stmt.kind) {
    case VAR_SET_DEFAULT:
      [[fallthrough]];
    case VAR_RESET:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("parameter \"transaction_isolation\" cannot be reset"));
      break;
    case VAR_SET_VALUE:
      [[fallthrough]];
    case VAR_SET_MULTI: {
      if (!conn_ctx.HasTransactionBegin()) {
        if (stmt.kind == VAR_SET_MULTI) {
          conn_ctx.AddNotice(SQL_ERROR_DATA(
            ERR_CODE(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
            ERR_MSG("SET TRANSACTION can only be used in transaction blocks")));
        }
        return;
      }
      auto isolation_level = GetIsolationLevel(stmt);
      SDB_ASSERT(!isolation_level.empty());
      SDB_ASSERT(absl::c_none_of(isolation_level, absl::ascii_isupper));
      ValidateIsolationLevel(isolation_level);

      if ((conn_ctx.HasRocksDBRead() || conn_ctx.HasRocksDBWrite()) &&
          isolation_level != IsolationLevelName(conn_ctx.GetIsolationLevel())) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
          ERR_MSG("SET TRANSACTION ISOLATION LEVEL must be called before "
                  "any query"));
      }

      conn_ctx.Set(Config::VariableContext::Local, "transaction_isolation",
                   std::string{isolation_level});
    } break;
    default:
      SDB_UNREACHABLE();
      break;
  }
}

// SET SESSION CHARACTERISTICS AS TRANSACTION statement and
// "default_transaction_isolation" variable processing
void ProcessDefaultTransactionIsolation(sdb::ConnectionContext& conn_ctx,
                                        const VariableSetStmt& stmt) {
  auto var_ctx = conn_ctx.HasTransactionBegin()
                   ? Config::VariableContext::Transaction
                   : Config::VariableContext::Session;

  switch (stmt.kind) {
    case VAR_SET_DEFAULT: {
      auto description = GetDefaultDescription("default_transaction_isolation");
      SDB_ASSERT(description);
      auto default_value = description->default_value;
      SDB_ASSERT(default_value.data());

      conn_ctx.Set(var_ctx, "default_transaction_isolation",
                   std::string{default_value});
    } break;
    case VAR_RESET:
      conn_ctx.Reset("default_transaction_isolation");
      break;
    case VAR_SET_VALUE:
      [[fallthrough]];
    case VAR_SET_MULTI: {
      auto isolation_level = GetIsolationLevel(stmt);
      SDB_ASSERT(!isolation_level.empty());
      SDB_ASSERT(absl::c_none_of(isolation_level, absl::ascii_isupper));
      ValidateIsolationLevel(isolation_level);

      conn_ctx.Set(var_ctx, "default_transaction_isolation",
                   std::string{isolation_level});
      if (var_ctx == Config::VariableContext::Session) {
        conn_ctx.Set(Config::VariableContext::Session, "transaction_isolation",
                     std::string{isolation_level});
      }
    } break;
    default:
      SDB_UNREACHABLE();
      break;
  }
}

}  // namespace

yaclib::Future<Result> VariableSet(ExecContext& ctx,
                                   const VariableSetStmt& stmt) {
  auto& conn_ctx = basics::downCast<ConnectionContext>(ctx);
  auto context = Config::VariableContext::Session;
  if (stmt.is_local) {
    context = Config::VariableContext::Local;
    if (!conn_ctx.HasTransactionBegin()) {
      return yaclib::MakeFuture<Result>(
        ERROR_QUERY_USER_WARN,
        "SET LOCAL can only be used in transaction blocks");
    }
  } else if (conn_ctx.HasTransactionBegin()) {
    context = Config::VariableContext::Transaction;
  }

  if (stmt.kind == VAR_RESET_ALL) {
    conn_ctx.ResetAll();
    return {};
  }

  std::string_view stmt_name = stmt.name;

  if (stmt.kind == VAR_SET_MULTI) {
    if (stmt_name == "TRANSACTION") {
      ProcessTransactionIsolation(conn_ctx, stmt);
      return {};
    }

    SDB_ASSERT(stmt_name == "SESSION CHARACTERISTICS");
    ProcessDefaultTransactionIsolation(conn_ctx, stmt);
    return {};
  }

#ifdef SDB_FAULT_INJECTION
  if (stmt_name.starts_with(kFailPointPrefix)) {
    return ProcessFailurePoint(stmt_name, stmt);
  }
#endif

  auto value_name = GetOriginalName(stmt_name);
  if (!value_name.data()) {
    return yaclib::MakeFuture<Result>(
      ERROR_FAILED, "unrecognized configuration parameter \"", stmt.name, "\"");
  }

  if (value_name == "transaction_isolation") {
    ProcessTransactionIsolation(conn_ctx, stmt);
    return {};
  }

  if (value_name == "default_transaction_isolation") {
    ProcessDefaultTransactionIsolation(conn_ctx, stmt);
    return {};
  }

  auto description = GetDefaultDescription(value_name);
  SDB_ASSERT(description);
  Result r;
  switch (stmt.kind) {
    case VAR_SET_DEFAULT: {
      auto default_value = description->default_value;
      if (default_value.data()) {
        conn_ctx.Set(context, value_name, std::string{default_value});
      } else {
        r = {ERROR_FAILED, "No default value for variable ", value_name};
      }
    } break;
    case VAR_RESET:
      conn_ctx.Reset(value_name);
      break;
    case VAR_SET_VALUE:
      r = ProcessValues(stmt.args, value_name, description->type, conn_ctx,
                        context);
      break;
    case VAR_SET_CURRENT:
      r = {ERROR_NOT_IMPLEMENTED, "SET ... TO CURRENT is not implemented"};
      break;
    default:
      SDB_UNREACHABLE();
  }
  if (r.ok()) {
    return {};
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
