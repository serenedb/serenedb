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

#include "pg/command_tag.h"

#include <absl/strings/str_cat.h>

#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/alter_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>
#include <duckdb/parser/statement/drop_statement.hpp>
#include <duckdb/parser/statement/execute_statement.hpp>
#include <duckdb/parser/statement/transaction_statement.hpp>

namespace sdb::pg {

namespace {

std::string_view CatalogObjectTag(duckdb::CatalogType type) {
  using duckdb::CatalogType;
  switch (type) {
    case CatalogType::TABLE_ENTRY:
      return "TABLE";
    case CatalogType::VIEW_ENTRY:
      return "VIEW";
    case CatalogType::INDEX_ENTRY:
      return "INDEX";
    case CatalogType::SCHEMA_ENTRY:
      return "SCHEMA";
    case CatalogType::SEQUENCE_ENTRY:
      return "SEQUENCE";
    case CatalogType::TYPE_ENTRY:
      return "TYPE";
    case CatalogType::MACRO_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
      return "FUNCTION";
    case CatalogType::DATABASE_ENTRY:
      return "DATABASE";
    default:
      return {};
  }
}

// `EXECUTE name` reports the underlying statement's tag in PG (e.g. a
// SELECT-backed prepared statement yields "SELECT N"). Look the referenced
// statement up in DuckDB's client-local prepared-statement catalog.
CommandTag ExecuteTagForPrepared(const duckdb::PreparedStatement& prepared) {
  auto* unbound = prepared.data->unbound_statement.get();
  if (!unbound) {
    return {"EXECUTE", duckdb::StatementType::EXECUTE_STATEMENT};
  }
  auto& exec_stmt = unbound->Cast<duckdb::ExecuteStatement>();
  auto& client_data = duckdb::ClientData::Get(*prepared.context);
  auto it = client_data.prepared_statements.find(exec_stmt.name);
  if (it == client_data.prepared_statements.end() || !it->second) {
    return {"EXECUTE", duckdb::StatementType::EXECUTE_STATEMENT};
  }
  using duckdb::StatementType;
  const auto inner = it->second->statement_type;
  switch (inner) {
    case StatementType::SELECT_STATEMENT:
      return {"SELECT", inner};
    case StatementType::INSERT_STATEMENT:
      return {"INSERT", inner};
    case StatementType::UPDATE_STATEMENT:
      return {"UPDATE", inner};
    case StatementType::DELETE_STATEMENT:
      return {"DELETE", inner};
    default:
      return {duckdb::StatementTypeToString(inner), inner};
  }
}

}  // namespace

CommandTag BuildCommandTag(const duckdb::PreparedStatement& prepared) {
  const auto stmt_type = prepared.data->statement_type;
  const auto* unbound = prepared.data->unbound_statement.get();
  auto make = [&](std::string s) -> CommandTag {
    return {std::move(s), stmt_type};
  };
  using duckdb::StatementType;
  switch (stmt_type) {
    case StatementType::SELECT_STATEMENT:
      return make("SELECT");
    case StatementType::INSERT_STATEMENT:
      return make("INSERT");
    case StatementType::UPDATE_STATEMENT:
      return make("UPDATE");
    case StatementType::DELETE_STATEMENT:
      if (unbound->Cast<duckdb::DeleteStatement>().node->is_truncate) {
        return make("TRUNCATE TABLE");
      }
      return make("DELETE");
    case StatementType::COPY_STATEMENT:
      return make("COPY");
    case StatementType::MERGE_INTO_STATEMENT:
      return make("MERGE");
    case StatementType::PREPARE_STATEMENT:
      return make("PREPARE");
    case StatementType::EXECUTE_STATEMENT:
      return ExecuteTagForPrepared(prepared);
    case StatementType::EXPLAIN_STATEMENT:
      return make("EXPLAIN");
    case StatementType::VACUUM_STATEMENT:
      return make("VACUUM");
    case StatementType::ANALYZE_STATEMENT:
      return make("ANALYZE");
    case StatementType::ATTACH_STATEMENT:
      return make("ATTACH");
    case StatementType::DETACH_STATEMENT:
      return make("DETACH");
    case StatementType::SET_STATEMENT:
    case StatementType::VARIABLE_SET_STATEMENT:
      return make("SET");
    case StatementType::LOAD_STATEMENT:
      return make("LOAD");
    case StatementType::CALL_STATEMENT:
      return make("CALL");
    case StatementType::CREATE_STATEMENT:
    case StatementType::CREATE_FUNC_STATEMENT: {
      if (unbound) {
        const auto& create_stmt = unbound->Cast<duckdb::CreateStatement>();
        if (create_stmt.info) {
          auto obj = CatalogObjectTag(create_stmt.info->type);
          if (!obj.empty()) {
            return make(absl::StrCat("CREATE ", obj));
          }
        }
      }
      return make("CREATE");
    }
    case StatementType::DROP_STATEMENT: {
      if (unbound) {
        const auto& drop_stmt = unbound->Cast<duckdb::DropStatement>();
        if (drop_stmt.info) {
          if (drop_stmt.info->type == duckdb::CatalogType::PREPARED_STATEMENT) {
            return make(drop_stmt.info->name.empty() ? "DEALLOCATE ALL"
                                                     : "DEALLOCATE");
          }
          auto obj = CatalogObjectTag(drop_stmt.info->type);
          if (!obj.empty()) {
            return make(absl::StrCat("DROP ", obj));
          }
        }
      }
      return make("DROP");
    }
    case StatementType::ALTER_STATEMENT: {
      if (unbound) {
        const auto& alter_stmt = unbound->Cast<duckdb::AlterStatement>();
        if (alter_stmt.info) {
          switch (alter_stmt.info->type) {
            case duckdb::AlterType::ALTER_TABLE:
              return make("ALTER TABLE");
            case duckdb::AlterType::ALTER_VIEW:
              return make("ALTER VIEW");
            case duckdb::AlterType::ALTER_SEQUENCE:
              return make("ALTER SEQUENCE");
            case duckdb::AlterType::ALTER_DATABASE:
              return make("ALTER DATABASE");
            default:
              break;
          }
        }
      }
      return make("ALTER");
    }
    case StatementType::TRANSACTION_STATEMENT: {
      if (unbound) {
        const auto& tx = unbound->Cast<duckdb::TransactionStatement>();
        if (tx.info) {
          switch (tx.info->type) {
            case duckdb::TransactionType::BEGIN_TRANSACTION:
              return make("BEGIN");
            case duckdb::TransactionType::COMMIT:
              return make("COMMIT");
            case duckdb::TransactionType::ROLLBACK:
              return make("ROLLBACK");
            default:
              break;
          }
        }
      }
      return make("TRANSACTION");
    }
    default:
      return make(duckdb::StatementTypeToString(stmt_type));
  }
}

std::string FormatCommandTag(const duckdb::PreparedStatement& prepared,
                             duckdb::StatementReturnType return_type,
                             uint64_t rows) {
  auto tag = BuildCommandTag(prepared);
  if (return_type == duckdb::StatementReturnType::CHANGED_ROWS) {
    if (tag.effective_type == duckdb::StatementType::INSERT_STATEMENT) {
      absl::StrAppend(&tag.tag, " 0 ", rows);
    } else {
      absl::StrAppend(&tag.tag, " ", rows);
    }
  } else if (return_type == duckdb::StatementReturnType::QUERY_RESULT) {
    absl::StrAppend(&tag.tag, " ", rows);
  }
  return std::move(tag.tag);
}

}  // namespace sdb::pg
