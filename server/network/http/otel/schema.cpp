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

#include "network/http/otel/schema.h"

#include <absl/status/status.h>
#include <absl/strings/str_cat.h>

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/cluster.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/otel.h"
#include "network/http/common.h"
#include "otel/schema_sql.h"
#include "pg/connection_context.h"
#include "pg/types.h"

namespace sdb::otel {
namespace {

class Creator {
 public:
  Creator(std::string_view database, duckdb::idx_t database_id,
          std::string_view schema = "public")
    : _conn{connector::MakeSystemConnection(irs::StaticStrings::kDefaultUser,
                                            pg::kRootUser, database,
                                            database_id)
              .conn},
      _schema{schema} {}

  // A search table cannot gain an index once it holds rows, so re-running the
  // DDL over an existing schema is not just wasteful, it fails. Presence of
  // the logs table is the marker that the schema is already there.
  bool Exists() {
    auto result = _conn->Query(
      absl::StrCat("SELECT 1 FROM pg_tables WHERE schemaname = ",
                   network::http::SqlLiteral(_schema), " AND tablename = ",
                   network::http::SqlLiteral(connector::kOtelLogsTable)));
    return !result->HasError() && result->RowCount() == 1;
  }

  bool Run(std::string_view sql) {
    auto result = _conn->Query(sql);
    if (result->HasError()) {
      SDB_WARN(STARTUP, "OpenTelemetry schema: ", result->GetError());
      return false;
    }
    return true;
  }

  bool Create() {
    const auto schema = network::http::SqlIdentifier(_schema);
    if (!Run(absl::StrCat("CREATE SCHEMA IF NOT EXISTS ", schema)) ||
        !Run(absl::StrCat("SET search_path TO ", schema))) {
      return false;
    }
    for (const auto statement : kSchemaStatements) {
      if (!Run(statement)) {
        return false;
      }
    }
    return true;
  }

  absl::Status Check() {
    std::vector<std::string> inserts{
      InsertSql(_schema, connector::kOtelLogsTable,
                connector::kOtelSourceLogsFunction),
      InsertSql(_schema, connector::kOtelTracesTable,
                connector::kOtelSourceTracesFunction),
    };
    for (size_t i = 0; i < connector::kOtelMetricTables.size(); ++i) {
      inserts.push_back(InsertSql(_schema, connector::kOtelMetricTables[i],
                                  connector::kOtelSourceMetricsFunctions[i]));
    }
    for (const auto& insert : inserts) {
      auto prepared = _conn->Prepare(insert);
      if (prepared->HasError()) {
        return absl::FailedPreconditionError(
          prepared->GetErrorObject().RawMessage());
      }
    }
    return absl::OkStatus();
  }

 private:
  std::unique_ptr<duckdb::Connection> _conn;
  std::string _schema;
};

}  // namespace

std::string InsertSql(std::string_view schema, std::string_view table,
                      std::string_view source) {
  return absl::StrCat("INSERT INTO ", network::http::SqlIdentifier(schema), ".",
                      network::http::SqlIdentifier(table), " SELECT * FROM ",
                      source, "(", network::http::SqlLiteral(schema), ")");
}

absl::Status EnsureSchema(std::string_view database, std::string_view schema) {
  std::string name;
  duckdb::idx_t oid = 0;
  const auto read = [&](const catalog::DatabaseCatalogEntry& entry) {
    name = entry.name.GetIdentifierName();
    oid = entry.oid;
  };
  if (!catalog::ReadDatabase(database, read)) {
    // CREATE DATABASE has to run somewhere: the default database always
    // exists and every role may connect to it.
    if (!catalog::ReadDatabase(irs::StaticStrings::kDefaultDatabase, read)) {
      return absl::NotFoundError("default database not found");
    }
    Creator bootstrap{name, oid};
    if (!bootstrap.Run(absl::StrCat("CREATE DATABASE ",
                                    network::http::SqlIdentifier(database)))) {
      return absl::InternalError(
        absl::StrCat("cannot create database '", database, "'"));
    }
    if (!catalog::ReadDatabase(database, read)) {
      return absl::InternalError(absl::StrCat(
        "database '", database, "' not visible after CREATE DATABASE"));
    }
    SDB_INFO(STARTUP, "OpenTelemetry database created: ", database);
  }
  Creator creator{name, oid, schema};
  if (creator.Exists()) {
    SDB_INFO(STARTUP, "OpenTelemetry schema already present in ", database, ".",
             schema);
  } else if (creator.Create()) {
    SDB_INFO(STARTUP, "OpenTelemetry schema created in ", database, ".",
             schema);
  }
  auto status = creator.Check();
  if (status.ok()) {
    return status;
  }
  return absl::FailedPreconditionError(absl::StrCat(
    "database '", database,
    "' does not match the built-in schema: ", status.message(),
    ". Fix the table, or start with the built-in schema elsewhere: set "
    "db=<new database> or schema=<new schema> on the ?api=otel listener, e.g. "
    "--listen='http://0.0.0.0:4318?api=otel&db=otel'"));
}

}  // namespace sdb::otel
