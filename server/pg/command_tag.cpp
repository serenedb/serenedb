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

#include <absl/strings/match.h>

#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/enums/set_type.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/alter_statement.hpp>
#include <duckdb/parser/statement/attach_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>
#include <duckdb/parser/statement/drop_statement.hpp>
#include <duckdb/parser/statement/execute_statement.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/parser/statement/transaction_statement.hpp>
#include <duckdb/parser/statement/vacuum_statement.hpp>
#include <iresearch/utils/system_compiler.hpp>

#include "catalog/catalog.h"

namespace sdb::pg {
namespace {

struct ObjectTags {
  std::string_view create;
  std::string_view drop;
  std::string_view alter;
};

ObjectTags TagsOf(duckdb::CatalogType type, bool procedure) {
  using enum duckdb::CatalogType;
  switch (type) {
    case TABLE_ENTRY:
      return {"CREATE TABLE", "DROP TABLE", "ALTER TABLE"};
    case VIEW_ENTRY:
      return {"CREATE VIEW", "DROP VIEW", "ALTER VIEW"};
    case INDEX_ENTRY:
      return {"CREATE INDEX", "DROP INDEX", "ALTER INDEX"};
    case SCHEMA_ENTRY:
      return {"CREATE SCHEMA", "DROP SCHEMA", "ALTER SCHEMA"};
    case SEQUENCE_ENTRY:
      return {"CREATE SEQUENCE", "DROP SEQUENCE", "ALTER SEQUENCE"};
    case TYPE_ENTRY:
      return {"CREATE TYPE", "DROP TYPE", "ALTER TYPE"};
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      if (procedure) {
        return {"CREATE PROCEDURE", "DROP PROCEDURE", "ALTER PROCEDURE"};
      }
      return {"CREATE FUNCTION", "DROP FUNCTION", "ALTER FUNCTION"};
    case DATABASE_ENTRY:
      return {"CREATE DATABASE", "DROP DATABASE", "ALTER DATABASE"};
    case TRIGGER_ENTRY:
      return {"CREATE TRIGGER", "DROP TRIGGER", "ALTER TRIGGER"};
    case ROLE_ENTRY:
      return {"CREATE ROLE", "DROP ROLE", "ALTER ROLE"};
    case FOREIGN_SERVER_ENTRY:
      return {"CREATE SERVER", "DROP SERVER", "ALTER SERVER"};
    case TOKENIZER_ENTRY:
      return {"CREATE TEXT SEARCH DICTIONARY", "DROP TEXT SEARCH DICTIONARY",
              "ALTER TEXT SEARCH DICTIONARY"};
    case JOB_ENTRY:
      return {"CREATE JOB", "DROP JOB", "ALTER JOB"};
    case SUBSCRIPTION_ENTRY:
      return {"CREATE SUBSCRIPTION", "DROP SUBSCRIPTION", "ALTER SUBSCRIPTION"};
    case INVALID:
    case PREPARED_STATEMENT:
    case COLLATION_ENTRY:
    case COORDINATE_SYSTEM_ENTRY:
    case TABLE_FUNCTION_ENTRY:
    case SCALAR_FUNCTION_ENTRY:
    case AGGREGATE_FUNCTION_ENTRY:
    case PRAGMA_FUNCTION_ENTRY:
    case COPY_FUNCTION_ENTRY:
    case WINDOW_FUNCTION_ENTRY:
    case DELETED_ENTRY:
    case RENAMED_ENTRY:
    case SECRET_ENTRY:
    case SECRET_TYPE_ENTRY:
    case SECRET_FUNCTION_ENTRY:
    case REPLICATION_ORIGIN_ENTRY:
    case DEPENDENCY_ENTRY:
      return {"CREATE", "DROP", "ALTER"};
  }
  SDB_UNREACHABLE();
}

std::string_view AlterTag(const duckdb::AlterInfo& info) {
  using enum duckdb::AlterType;
  switch (info.type) {
    case ALTER_TABLE:
      return "ALTER TABLE";
    case ALTER_VIEW:
      return "ALTER VIEW";
    case ALTER_SEQUENCE:
    case CHANGE_OWNERSHIP:
      return "ALTER SEQUENCE";
    case ALTER_SCALAR_FUNCTION:
    case ALTER_TABLE_FUNCTION:
      return "ALTER FUNCTION";
    case SET_COMMENT:
    case SET_COLUMN_COMMENT:
      return "COMMENT";
    case ALTER_DATABASE:
      return "ALTER DATABASE";
    case ALTER_SCHEMA:
      return "ALTER SCHEMA";
    case ALTER_INDEX:
      return "ALTER INDEX";
    case ALTER_JOB:
      return "ALTER JOB";
    case ALTER_ROLE: {
      const auto& role = info.Cast<duckdb::AlterRoleInfo>();
      if (role.grant_role.empty()) {
        return "ALTER ROLE";
      }
      return role.revoke ? "REVOKE ROLE" : "GRANT ROLE";
    }
    case ALTER_PERMISSIONS: {
      const auto& permissions = info.Cast<duckdb::AlterPermissionsInfo>();
      if (permissions.default_objtype != duckdb::CatalogType::INVALID) {
        return "ALTER DEFAULT PRIVILEGES";
      }
      if (!permissions.new_owner.empty()) {
        return TagsOf(permissions.entry_catalog_type, false).alter;
      }
      return permissions.revoke ? "REVOKE" : "GRANT";
    }
    case INVALID:
    case REPLACE_DEFINITION:
      return "ALTER";
  }
  SDB_UNREACHABLE();
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
      if (unbound) {
        const auto& options =
          unbound->Cast<duckdb::VacuumStatement>().info->options;
        if (options.analyze && !options.vacuum) {
          return make("ANALYZE");
        }
      }
      return make("VACUUM");
    case StatementType::ANALYZE_STATEMENT:
      return make("ANALYZE");
    case StatementType::ATTACH_STATEMENT:
      if (unbound) {
        const auto& options =
          unbound->Cast<duckdb::AttachStatement>().info->options;
        const auto type = options.find("type");
        if (type != options.end() &&
            absl::EqualsIgnoreCase(type->second.ToString(),
                                   catalog::SereneDBCatalog::kStorageType)) {
          return make("CREATE DATABASE");
        }
      }
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
          // CTAS as a duckdb::CreateTableInfo carrying a non-null query; the
          // CHANGED_ROWS count it returns is the rows produced, matching PG's
          // es_processed.
          if (create_stmt.info->type == duckdb::CatalogType::TABLE_ENTRY &&
              create_stmt.info->Cast<duckdb::CreateTableInfo>().query) {
            return make("SELECT", true);
          }
          const auto& info = *create_stmt.info;
          const bool procedure =
            (info.type == duckdb::CatalogType::MACRO_ENTRY ||
             info.type == duckdb::CatalogType::TABLE_MACRO_ENTRY) &&
            info.Cast<duckdb::CreateMacroInfo>().is_procedure;
          return make(TagsOf(info.type, procedure).create);
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
          return make(
            TagsOf(drop_stmt.info->type, drop_stmt.info->is_procedure).drop);
        }
      }
      return make("DROP");
    }
    case StatementType::ALTER_STATEMENT: {
      if (unbound) {
        const auto& alter_stmt = unbound->Cast<duckdb::AlterStatement>();
        if (alter_stmt.info) {
          return make(AlterTag(*alter_stmt.info));
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
  return BuildCommandTagImpl(prepared.GetStatementType(),
                             UnboundStatement(prepared),
                             prepared.TryGetContext().get());
}

const duckdb::SQLStatement* UnboundStatement(
  const duckdb::PreparedStatement& prepared) {
  const auto context = prepared.TryGetContext();
  if (!context) {
    return nullptr;
  }
  const auto& statements =
    duckdb::ClientData::Get(*context).prepared_statements;
  const auto it = statements.find(duckdb::Identifier{prepared.GetName()});
  return it != statements.end() && it->second
           ? it->second->unbound_statement.get()
           : nullptr;
}

CommandTag BuildCommandTag(const duckdb::SQLStatement& statement,
                           duckdb::ClientContext& context) {
  return BuildCommandTagImpl(statement.type, &statement, &context);
}

}  // namespace sdb::pg
