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

#include "pg/constraint_errors.h"

#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <string>
#include <string_view>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {
namespace {

std::string_view Get(const duckdb::ErrorData& error, std::string_view key) {
  const auto& extra = error.ExtraInfo();
  auto it = extra.find(key);
  return it == extra.end() ? std::string_view{} : std::string_view{it->second};
}

// Store-table names are the full pg path as one identifier
// ("<database>.<schema>.<table>"); for error rendering the bare table name
// is the part after the second dot. Cosmetic only -- identity never derives
// from it; names whose db/schema segments contain literal dots degrade to
// the full string.
std::string_view RelationName(std::string_view store_name) {
  auto first = store_name.find('.');
  if (first == std::string_view::npos) {
    return store_name;
  }
  auto second = store_name.find('.', first + 1);
  if (second == std::string_view::npos) {
    return store_name;
  }
  return store_name.substr(second + 1);
}

std::string_view FirstKeyColumn(std::string_view key_columns) {
  auto pos = key_columns.find(',');
  return pos == std::string_view::npos ? key_columns
                                       : key_columns.substr(0, pos);
}

}  // namespace

void ThrowTranslatedConstraintError(const duckdb::ErrorData& err) {
  if (err.Type() != duckdb::ExceptionType::CONSTRAINT) {
    return;
  }
  const std::string_view msg = err.RawMessage();
  const auto relation = RelationName(Get(err, "table_name"));
  const auto column_name = Get(err, "column_name");
  const auto check_expression = Get(err, "check_expression");
  const auto kind = Get(err, "constraint_kind");
  const auto key_columns = Get(err, "key_columns");
  const auto key_values = Get(err, "key_values");

  if (msg.starts_with("NOT NULL constraint failed")) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NOT_NULL_VIOLATION),
                    ERR_MSG("null value in column \"", column_name,
                            "\" of relation \"", relation,
                            "\" violates not-null constraint"));
  }
  if (msg.starts_with("CHECK constraint failed")) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_CHECK_VIOLATION),
                    ERR_MSG("new row for relation \"", relation,
                            "\" violates check constraint"),
                    ERR_DETAIL("Failing expression: ", check_expression, "."));
  }
  if (msg.starts_with("Duplicate key") ||
      msg.starts_with("PRIMARY KEY or UNIQUE constraint violation")) {
    std::string constraint;
    if (kind == "primary") {
      constraint = absl::StrCat(relation, "_pkey");
    } else {
      constraint =
        absl::StrCat(relation, "_", FirstKeyColumn(key_columns), "_key");
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
                    ERR_MSG("duplicate key value violates unique constraint \"",
                            constraint, "\""),
                    ERR_DETAIL("Key (", key_columns, ")=(", key_values,
                               ") already exists."));
  }
  if (msg.starts_with("Violates foreign key constraint")) {
    if (kind == "fk_delete") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FOREIGN_KEY_VIOLATION),
                      ERR_MSG("update or delete on table \"", relation,
                              "\" violates foreign key constraint"),
                      ERR_DETAIL("Key (", key_columns, ")=(", key_values,
                                 ") is still referenced from another table."));
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FOREIGN_KEY_VIOLATION),
                    ERR_MSG("insert or update on table \"", relation,
                            "\" violates foreign key constraint"),
                    ERR_DETAIL("Key (", key_columns, ")=(", key_values,
                               ") is not present in the referenced table."));
  }
}

}  // namespace sdb::pg
