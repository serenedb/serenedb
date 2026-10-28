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

#include "network/http/sql_connection.h"

#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <string>

#include "catalog/cluster.h"
#include "connector/duckdb_client_state.h"

namespace sdb::network::http {

SqlConnection OpenSqlConnection(
  std::string_view user, std::string_view database, uint64_t cancel_key,
  CancelRegistry* cancel,
  const duckdb::shared_ptr<duckdb::SchedulerAccount>& account) {
  const std::string_view dbname =
    database.empty() ? irs::StaticStrings::kDefaultDatabase : database;
  duckdb::idx_t database_id = 0;
  duckdb::Permissions permissions;
  if (!catalog::ReadDatabase(dbname,
                             [&](const catalog::DatabaseCatalogEntry& entry) {
                               database_id = entry.oid;
                               permissions = entry.permissions;
                             })) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CATALOG_NAME),
                    ERR_MSG("database \"", dbname, "\" does not exist"));
  }
  const std::string_view login_user =
    user.empty() ? irs::StaticStrings::kDefaultUser : user;
  auto login = sdb::pg::RequireLoginRole(login_user, dbname, permissions);
  if (!login.role) {
    THROW_SQL_ERROR_FROM_DATA(std::move(login.error));
  }
  SqlConnection result{.conn =
                         irs::DuckDBEngine::Instance().CreateConnection()};
  result.conn->context->SetSchedulerAccount(account);
  result.context = std::make_shared<ConnectionContext>(
    *result.conn->context, login_user, login.role, dbname, database_id, nullptr,
    static_cast<int32_t>(cancel_key >> 32), cancel);
  result.progress =
    sdb::pg::ProgressRegistration{connector::SereneDBClientState::Register(
                                    *result.conn->context, result.context)
                                    .progress_source};
  connector::SetDefaultSearchPath(*result.conn->context, dbname);
  result.context->SetSetting("session_authorization", std::string{login_user},
                             false);
  result.context->SetSetting("is_superuser", login.superuser ? "on" : "off",
                             false);
  return result;
}

}  // namespace sdb::network::http
