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

#include "network/http/otlp/schema.h"

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/otlp.h"
#include "otel/schema_sql.h"
#include "pg/connection_context.h"

namespace sdb::network::http::otlp {
namespace {

class Creator {
 public:
  Creator(std::string_view database, ObjectId database_id)
    : _conn{irs::DuckDBEngine::Instance().CreateConnection()} {
    // The catalog layer reaches the role, database and transaction through
    // this; a bare DuckDB connection cannot resolve a SereneDB relation.
    auto ctx = std::make_shared<ConnectionContext>(
      *_conn->context, irs::StaticStrings::kDefaultUser, id::kRootUser,
      database, database_id, nullptr, 0, nullptr);
    ctx->MarkSystemWriter();
    connector::SereneDBClientState::Register(*_conn->context, std::move(ctx));
    _conn->context->session_user =
      std::string{irs::StaticStrings::kDefaultUser};
    std::vector<duckdb::CatalogSearchEntry> paths{
      duckdb::CatalogSearchEntry{duckdb::Identifier{std::string{database}},
                                 duckdb::Identifier{"$user"}},
      duckdb::CatalogSearchEntry{duckdb::Identifier{std::string{database}},
                                 duckdb::Identifier{"public"}},
    };
    _conn->context->client_data->catalog_search_path->SetDefaultPaths(
      std::vector{paths});
    _conn->context->client_data->catalog_search_path->Set(
      std::move(paths), duckdb::CatalogSetPathType::SET_DIRECTLY);
  }

  // A search table cannot gain an index once it holds rows, so re-running the
  // DDL over an existing schema is not just wasteful, it fails. Presence of
  // the logs table is the marker that the schema is already there.
  bool Exists() {
    auto result = _conn->Query(absl::StrCat(
      "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '",
      connector::kOtelLogsTable, "'"));
    return !result->HasError() && result->RowCount() == 1;
  }

  bool Create() {
    for (const auto statement : otel::kSchemaStatements) {
      auto result = _conn->Query(std::string{statement});
      if (result->HasError()) {
        SDB_WARN(STARTUP, "OpenTelemetry schema: ", result->GetError());
        return false;
      }
    }
    return true;
  }

 private:
  std::unique_ptr<duckdb::Connection> _conn;
};

}  // namespace

void EnsureSchema() {
  const auto* database =
    catalog::FindDatabase(nullptr, irs::StaticStrings::kDefaultDatabase);
  if (database == nullptr) {
    SDB_WARN(STARTUP, "OpenTelemetry schema: default database not found");
    return;
  }
  Creator creator{database->name.GetIdentifierName(), catalog::IdOf(*database)};
  if (creator.Exists()) {
    SDB_INFO(STARTUP, "OpenTelemetry schema already present");
    return;
  }
  if (creator.Create()) {
    SDB_INFO(STARTUP, "OpenTelemetry schema created");
  }
}

}  // namespace sdb::network::http::otlp
