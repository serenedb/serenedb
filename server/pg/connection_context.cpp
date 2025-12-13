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

#include "pg/connection_context.h"

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "rest_server/serened.h"

namespace sdb {

ConnectionContext::ConnectionContext(std::string_view user,
                                     std::string_view dbname,
                                     ObjectId database_id)
  : ExecContext{user, dbname, database_id} {}

std::string ConnectionContext::GetCurrentSchema() const {
  auto database_id = ExecContext::GetDatabaseId();
  auto search_path = Config::Get<VariableType::PgSearchPath>("search_path");
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto it = absl::c_find_if(search_path, [&](const std::string& schema_name) {
    return catalog.GetSchema(database_id, schema_name);
  });

  return it != search_path.end() ? *it : "";
}
}  // namespace sdb
