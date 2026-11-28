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

#include "iresearch/utils/pg/error_mapping.hpp"

#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <fast_float/fast_float.h>

#include <duckdb/common/exception.hpp>
#include <string_view>
#include <system_error>

#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception.hpp"

namespace irs::pg {
namespace {

int DuckExceptionToErrcode(duckdb::ExceptionType type) {
  switch (type) {
    case duckdb::ExceptionType::SYNTAX:
    case duckdb::ExceptionType::PARSER:
    case duckdb::ExceptionType::EXPRESSION:
      return ERRCODE_SYNTAX_ERROR;
    case duckdb::ExceptionType::CATALOG:
      return ERRCODE_UNDEFINED_TABLE;
    case duckdb::ExceptionType::BINDER:
      return ERRCODE_UNDEFINED_COLUMN;
    case duckdb::ExceptionType::CONVERSION:
      return ERRCODE_INVALID_TEXT_REPRESENTATION;
    case duckdb::ExceptionType::MISMATCH_TYPE:
    case duckdb::ExceptionType::INVALID_TYPE:
    case duckdb::ExceptionType::DECIMAL:
      return ERRCODE_DATA_EXCEPTION;
    case duckdb::ExceptionType::OUT_OF_RANGE:
      return ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
    case duckdb::ExceptionType::OBJECT_SIZE:
      return ERRCODE_PROGRAM_LIMIT_EXCEEDED;
    // Bad input value/argument, setting value, or configuration are user
    // errors, not internal_error (XX000) -- e.g. SET with an uncastable value,
    // or an out-of-range option. PG report invalid_parameter_value here.
    case duckdb::ExceptionType::INVALID_INPUT:
    case duckdb::ExceptionType::SETTINGS:
    case duckdb::ExceptionType::INVALID_CONFIGURATION:
      return ERRCODE_INVALID_PARAMETER_VALUE;
    case duckdb::ExceptionType::DIVIDE_BY_ZERO:
      return ERRCODE_DIVISION_BY_ZERO;
    case duckdb::ExceptionType::CONSTRAINT:
      return ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION;
    case duckdb::ExceptionType::DEPENDENCY:
      return ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST;
    case duckdb::ExceptionType::PERMISSION:
      return ERRCODE_INSUFFICIENT_PRIVILEGE;
    case duckdb::ExceptionType::TRANSACTION:
      return ERRCODE_T_R_SERIALIZATION_FAILURE;
    case duckdb::ExceptionType::OUT_OF_MEMORY:
      return ERRCODE_OUT_OF_MEMORY;
    case duckdb::ExceptionType::NOT_IMPLEMENTED:
    case duckdb::ExceptionType::MISSING_EXTENSION:
    case duckdb::ExceptionType::AUTOLOAD:
      return ERRCODE_FEATURE_NOT_SUPPORTED;
    case duckdb::ExceptionType::PARAMETER_NOT_RESOLVED:
    case duckdb::ExceptionType::PARAMETER_NOT_ALLOWED:
      return ERRCODE_INDETERMINATE_DATATYPE;
    case duckdb::ExceptionType::CONNECTION:
    case duckdb::ExceptionType::NETWORK:
    case duckdb::ExceptionType::HTTP:
      return ERRCODE_CONNECTION_EXCEPTION;
    case duckdb::ExceptionType::IO:
      return ERRCODE_IO_ERROR;
    case duckdb::ExceptionType::DATA_CORRUPTION:
      return ERRCODE_DATA_CORRUPTED;
    case duckdb::ExceptionType::INTERRUPT:
      return ERRCODE_QUERY_CANCELED;
    default:
      return ERRCODE_INTERNAL_ERROR;
  }
}

// Refine the SQLSTATE from the structured extra_info DuckDB attaches to
// catalog/binder and I/O errors: the bare ExceptionType collapses
// undefined_table / undefined_column / undefined_function / undefined_schema
// (and the duplicate_* counterparts) into one code each, which PG
// distinguishes, and every file error into io_error.
int DuckExceptionToErrcode(const duckdb::ErrorData& error) {
  const auto& extra = error.ExtraInfo();
  if (const auto sqlstate = extra.find("sqlstate");
      sqlstate != extra.end() && sqlstate->second.size() == 5) {
    const auto& code = sqlstate->second;
    return MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4]);
  }
  if (const auto it = extra.find("errno");
      it != extra.end() && error.Type() == duckdb::ExceptionType::IO) {
    const auto& text = it->second;
    int value = 0;
    if (fast_float::from_chars(text.data(), text.data() + text.size(), value)
          .ec == std::errc{}) {
      return ErrcodeForFileAccess(value);
    }
  }
  const auto subtype_it = extra.find("error_subtype");
  const std::string_view subtype =
    subtype_it != extra.end() ? subtype_it->second : std::string_view{};
  if (subtype == "INVALID_ESCAPE_SEQUENCE") {
    return ERRCODE_INVALID_ESCAPE_SEQUENCE;
  }
  if (subtype == "INVALID_USE_OF_ESCAPE_CHARACTER") {
    return ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER;
  }
  if (subtype == "INVALID_REGULAR_EXPRESSION") {
    return ERRCODE_INVALID_REGULAR_EXPRESSION;
  }
  if (subtype == "NUMERIC_VALUE_OUT_OF_RANGE") {
    return ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
  }
  if (subtype == "INTERVAL_FIELD_OVERFLOW") {
    return ERRCODE_INTERVAL_FIELD_OVERFLOW;
  }
  if (subtype == "FEATURE_NOT_SUPPORTED") {
    return ERRCODE_FEATURE_NOT_SUPPORTED;
  }
  if (subtype == "STATEMENT_TOO_COMPLEX") {
    return ERRCODE_STATEMENT_TOO_COMPLEX;
  }
  if (subtype == "CARDINALITY_VIOLATION") {
    return ERRCODE_CARDINALITY_VIOLATION;
  }
  if (error.Type() == duckdb::ExceptionType::CATALOG) {
    if (subtype == "WRONG_OBJECT_TYPE") {
      return ERRCODE_WRONG_OBJECT_TYPE;
    }
    if (subtype == "UNSUPPORTED") {
      return ERRCODE_FEATURE_NOT_SUPPORTED;
    }
    const auto kind_it = extra.find("type");
    const std::string_view kind =
      kind_it != extra.end() ? kind_it->second : std::string_view{};
    const bool dup = subtype == "ENTRY_ALREADY_EXISTS";
    if (kind == "Schema") {
      return dup ? ERRCODE_DUPLICATE_SCHEMA : ERRCODE_UNDEFINED_SCHEMA;
    }
    if (kind == "Database") {
      return dup ? ERRCODE_DUPLICATE_DATABASE : ERRCODE_UNDEFINED_DATABASE;
    }
    if (kind == "Column") {
      return dup ? ERRCODE_DUPLICATE_COLUMN : ERRCODE_UNDEFINED_COLUMN;
    }
    // "Scalar Function", "Aggregate Function", "Table Function", ...
    if (kind.find("Function") != std::string_view::npos) {
      return dup ? ERRCODE_DUPLICATE_FUNCTION : ERRCODE_UNDEFINED_FUNCTION;
    }
    if (kind == "Type" || kind == "Constraint") {
      return dup ? ERRCODE_DUPLICATE_OBJECT : ERRCODE_UNDEFINED_OBJECT;
    }
    if (kind == "prepared statement") {
      return dup ? ERRCODE_DUPLICATE_PSTATEMENT
                 : ERRCODE_INVALID_SQL_STATEMENT_NAME;
    }
    if (kind == "configuration parameter") {
      // Unknown GUC -- DuckDB throws CatalogException::MissingEntry(
      // "configuration parameter", ...) for SET / RESET / current_setting. PG
      // reports undefined_object (42704), not undefined_table.
      return ERRCODE_UNDEFINED_OBJECT;
    }
    // Table / View / Sequence / Index and the untyped fallback: a relation.
    return dup ? ERRCODE_DUPLICATE_TABLE : ERRCODE_UNDEFINED_TABLE;
  }
  if (error.Type() == duckdb::ExceptionType::BINDER) {
    if (subtype == "NO_MATCHING_FUNCTION") {
      return ERRCODE_UNDEFINED_FUNCTION;
    }
    if (const auto kind_it = extra.find("type");
        kind_it != extra.end() && kind_it->second == "prepared statement") {
      return ERRCODE_INVALID_SQL_STATEMENT_NAME;
    }
    if (subtype == "UNSUPPORTED") {
      return ERRCODE_FEATURE_NOT_SUPPORTED;
    }
    const std::string_view binder_msg = error.RawMessage();
    if (absl::StrContainsIgnoreCase(binder_msg, "ambiguous")) {
      return ERRCODE_AMBIGUOUS_COLUMN;
    }
    if (absl::StrContainsIgnoreCase(binder_msg,
                                    "must appear in the GROUP BY")) {
      return ERRCODE_GROUPING_ERROR;
    }
    if (absl::StrContainsIgnoreCase(binder_msg,
                                    "as conflict target are not referenced")) {
      return ERRCODE_INVALID_COLUMN_REFERENCE;
    }
    return ERRCODE_UNDEFINED_COLUMN;
  }
  if (error.Type() == duckdb::ExceptionType::CONSTRAINT) {
    const std::string_view msg = error.RawMessage();
    if (absl::StrContainsIgnoreCase(msg, "not null")) {
      return ERRCODE_NOT_NULL_VIOLATION;
    }
    if (absl::StrContainsIgnoreCase(msg, "foreign key")) {
      return ERRCODE_FOREIGN_KEY_VIOLATION;
    }
    if (absl::StrContainsIgnoreCase(msg, "check constraint")) {
      return ERRCODE_CHECK_VIOLATION;
    }
    if (absl::StrContainsIgnoreCase(msg, "unique") ||
        absl::StrContainsIgnoreCase(msg, "primary key") ||
        absl::StrContainsIgnoreCase(msg, "duplicate")) {
      return ERRCODE_UNIQUE_VIOLATION;
    }
    return ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION;
  }
  if (error.Type() == duckdb::ExceptionType::SEQUENCE) {
    if (subtype == "NOT_YET_DEFINED") {
      return ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE;
    }
    if (subtype == "LIMIT_REACHED") {
      return ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED;
    }
    if (subtype == "OUT_OF_BOUNDS") {
      return ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
    }
  }
  return DuckExceptionToErrcode(error.Type());
}

SqlErrorData DuckErrorToSqlData(const duckdb::ErrorData& error) {
  // DuckDB reports running a statement in an aborted explicit transaction as a
  // generic TransactionException (40001 by type); postgres flags it 25P02. The
  // message is the stable ErrorManager::INVALIDATED_TRANSACTION text.
  if (error.Type() == duckdb::ExceptionType::TRANSACTION &&
      error.RawMessage().starts_with("current transaction is aborted")) {
    return SqlErrorData{
      .errcode = ERRCODE_IN_FAILED_SQL_TRANSACTION,
      .errmsg = error.RawMessage(),
    };
  }
  if (error.Type() == duckdb::ExceptionType::TRANSACTION) {
    const auto& extra = error.ExtraInfo();
    const auto subtype_it = extra.find("error_subtype");
    const std::string_view subtype =
      subtype_it != extra.end() ? subtype_it->second : std::string_view{};
    if (subtype == "CROSS_DATABASE_WRITE") {
      return SqlErrorData{
        .errcode = ERRCODE_FEATURE_NOT_SUPPORTED,
        .errmsg = error.RawMessage(),
      };
    }
    if (subtype == "READ_ONLY") {
      return SqlErrorData{
        .errcode = ERRCODE_READ_ONLY_SQL_TRANSACTION,
        .errmsg = error.RawMessage(),
      };
    }
    if (subtype == "TRANSACTION_LOCAL_CHANGES") {
      return SqlErrorData{
        .errcode = ERRCODE_ACTIVE_SQL_TRANSACTION,
        .errmsg = error.RawMessage(),
      };
    }
  }
  // An interrupted query is DuckDB "Interrupted!"; report postgres's wording.
  const bool interrupted = error.Type() == duckdb::ExceptionType::INTERRUPT;
  SqlErrorData data{
    .errcode = DuckExceptionToErrcode(error),
    .errmsg = !interrupted ? error.RawMessage()
              : error.RawMessage() == "Query exceeded maximum execution time"
                ? "canceling statement due to statement timeout"
                : "canceling statement due to user request",
  };
  // PG's error Position is a 1-based offset into the query; DuckDB records a
  // 0-based one in extra_info["position"]. This is a byte offset -- exact for
  // ASCII; PG counts characters, so a multibyte query would need the query
  // text to convert it precisely (left as a followup).
  if (auto it = error.ExtraInfo().find("position");
      it != error.ExtraInfo().end()) {
    int pos = 0;
    if (absl::SimpleAtoi(it->second, &pos)) {
      data.cursorpos = pos + 1;
    }
  }
  if (auto it = error.ExtraInfo().find("detail");
      it != error.ExtraInfo().end()) {
    data.errdetail = it->second;
  }
  if (auto it = error.ExtraInfo().find("hint"); it != error.ExtraInfo().end()) {
    data.errhint = it->second;
  }
  if (auto it = error.ExtraInfo().find("context");
      it != error.ExtraInfo().end()) {
    data.context = it->second;
  }
  return data;
}

}  // namespace

SqlErrorData ToSqlError(const std::exception& exception) {
  if (const auto* sql = dynamic_cast<const irs::SqlException*>(&exception)) {
    return sql->error();
  }
  if (const auto* duck = dynamic_cast<const duckdb::Exception*>(&exception)) {
    return DuckErrorToSqlData(duckdb::ErrorData{*duck});
  }
  return SqlErrorData{
    .errcode = ERRCODE_INTERNAL_ERROR,
    .errmsg = exception.what(),
  };
}

SqlErrorData ToSqlError(const duckdb::ErrorData& error) {
  try {
    error.Throw();
  } catch (const irs::SqlException& e) {
    return e.error();
  } catch (...) {
  }
  return DuckErrorToSqlData(error);
}

void ThrowWithContext(std::string_view context, const std::exception& exception,
                      std::source_location location) {
  auto error = ToSqlError(exception);
  error.errmsg = absl::StrCat(context, ": ", error.errmsg);
  error.cursorpos = -1;
  throw irs::SqlException{std::move(error), location};
}

}  // namespace irs::pg
