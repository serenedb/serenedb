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

#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/enums/set_type.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/alter_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>
#include <duckdb/parser/statement/drop_statement.hpp>
#include <duckdb/parser/statement/execute_statement.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/parser/statement/transaction_statement.hpp>

namespace sdb::pg {
namespace {

std::string_view CreateObjectTag(duckdb::CatalogType type) {
  using duckdb::CatalogType;
  switch (type) {
    case CatalogType::TABLE_ENTRY:
      return "CREATE TABLE";
    case CatalogType::VIEW_ENTRY:
      return "CREATE VIEW";
    case CatalogType::INDEX_ENTRY:
      return "CREATE INDEX";
    case CatalogType::SCHEMA_ENTRY:
      return "CREATE SCHEMA";
    case CatalogType::SEQUENCE_ENTRY:
      return "CREATE SEQUENCE";
    case CatalogType::TYPE_ENTRY:
      return "CREATE TYPE";
    case CatalogType::MACRO_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
      return "CREATE FUNCTION";
    case CatalogType::DATABASE_ENTRY:
      return "CREATE DATABASE";
    default:
      return "CREATE";
  }
}

std::string_view DropObjectTag(duckdb::CatalogType type) {
  using duckdb::CatalogType;
  switch (type) {
    case CatalogType::TABLE_ENTRY:
      return "DROP TABLE";
    case CatalogType::VIEW_ENTRY:
      return "DROP VIEW";
    case CatalogType::INDEX_ENTRY:
      return "DROP INDEX";
    case CatalogType::SCHEMA_ENTRY:
      return "DROP SCHEMA";
    case CatalogType::SEQUENCE_ENTRY:
      return "DROP SEQUENCE";
    case CatalogType::TYPE_ENTRY:
      return "DROP TYPE";
    case CatalogType::MACRO_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
      return "DROP FUNCTION";
    case CatalogType::DATABASE_ENTRY:
      return "DROP DATABASE";
    default:
      return "DROP";
  }
}

// `EXECUTE name` reports the underlying statement's tag in PG (e.g. a
// SELECT-backed prepared statement yields "SELECT N"). Look the referenced
// statement up in DuckDB's client-local prepared-statement catalog.
CommandTag ExecuteTag(const duckdb::SQLStatement* unbound,
                      duckdb::ClientContext* context) {
  if (!unbound || !context) {
    return {"EXECUTE", duckdb::StatementType::EXECUTE_STATEMENT};
  }
  auto& exec_stmt = unbound->Cast<duckdb::ExecuteStatement>();
  auto& client_data = duckdb::ClientData::Get(*context);
  auto it = client_data.prepared_statements.find(exec_stmt.name);
  if (it == client_data.prepared_statements.end() || !it->second) {
    return {"EXECUTE", duckdb::StatementType::EXECUTE_STATEMENT};
  }
  using duckdb::StatementType;
  const auto inner = it->second->statement_type;
  switch (inner) {
    case StatementType::SELECT_STATEMENT:
      return {"SELECT", inner, true};
    case StatementType::INSERT_STATEMENT:
      return {"INSERT", inner, true};
    case StatementType::UPDATE_STATEMENT:
      return {"UPDATE", inner, true};
    case StatementType::DELETE_STATEMENT:
      return {"DELETE", inner, true};
    default:
      return {duckdb::StatementTypeToString(inner), inner};
  }
}

}  // namespace
namespace {

CommandTag BuildCommandTagImpl(duckdb::StatementType stmt_type,
                               const duckdb::SQLStatement* unbound,
                               duckdb::ClientContext* context) {
  auto make = [&](std::string_view s, bool rowcount = false) -> CommandTag {
    return {s, stmt_type, rowcount};
  };
  using duckdb::StatementType;
  switch (stmt_type) {
    case StatementType::SELECT_STATEMENT:
      return make("SELECT", true);
    case StatementType::INSERT_STATEMENT:
      return make("INSERT", true);
    case StatementType::UPDATE_STATEMENT:
      return make("UPDATE", true);
    case StatementType::DELETE_STATEMENT:
      if (unbound->Cast<duckdb::DeleteStatement>().node->is_truncate) {
        return make("TRUNCATE TABLE");
      }
      return make("DELETE", true);
    case StatementType::COPY_STATEMENT:
      return make("COPY", true);
    case StatementType::MERGE_INTO_STATEMENT:
      return make("MERGE", true);
    case StatementType::PREPARE_STATEMENT:
      return make("PREPARE");
    case StatementType::EXECUTE_STATEMENT:
      return ExecuteTag(unbound, context);
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
      if (unbound && unbound->Cast<duckdb::SetStatement>().set_type ==
                       duckdb::SetType::RESET) {
        return make("RESET");
      }
      return make("SET");
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
          // CREATE TABLE AS / SELECT INTO complete as "SELECT n" in PG, not
          // "CREATE TABLE n" (createas.c uses CMDTAG_SELECT). DuckDB models
          // CTAS as a CreateTableInfo carrying a non-null query; the
          // CHANGED_ROWS count it returns is the rows produced, matching PG's
          // es_processed.
          if (create_stmt.info->type == duckdb::CatalogType::TABLE_ENTRY &&
              create_stmt.info->Cast<duckdb::CreateTableInfo>().query) {
            return make("SELECT", true);
          }
          return make(CreateObjectTag(create_stmt.info->type));
        }
      }
      return make("CREATE");
    }
    case StatementType::DROP_STATEMENT: {
      if (unbound) {
        const auto& drop_stmt = unbound->Cast<duckdb::DropStatement>();
        if (drop_stmt.info) {
          if (drop_stmt.info->type == duckdb::CatalogType::PREPARED_STATEMENT) {
            return make(drop_stmt.info->GetQualifiedName().Name().empty()
                          ? "DEALLOCATE ALL"
                          : "DEALLOCATE");
          }
          return make(DropObjectTag(drop_stmt.info->type));
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
            case duckdb::AlterType::SET_COMMENT:
            case duckdb::AlterType::SET_COLUMN_COMMENT:
              return make("COMMENT");
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

}  // namespace

CommandTag BuildCommandTag(const duckdb::PreparedStatement& prepared) {
  return BuildCommandTagImpl(prepared.data->statement_type,
                             prepared.data->unbound_statement.get(),
                             prepared.context.get());
}

CommandTag BuildCommandTag(const duckdb::SQLStatement& statement,
                           duckdb::ClientContext& context) {
  return BuildCommandTagImpl(statement.type, &statement, &context);
}

}  // namespace sdb::pg
