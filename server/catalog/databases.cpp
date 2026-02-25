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

#include "databases.h"

#include "app/app_server.h"
#include "app/name_validator.h"
#include "catalog/catalog.h"
#include "rest_server/serened.h"

namespace sdb::catalog {

Result CreateDatabase(const ExecContext& exec,
                      catalog::DatabaseOptions options) {
  if (auto r = DatabaseNameValidator::validateName(/*allowSystem*/ false,
                                                   options.name);
      r.fail()) {
    return r;
  }

  if (ServerState::instance()->IsSingle()) {
    // TODO(gnusi): fail if wrong?
    options.replicationFactor = 1;
    options.writeConcern = 1;
  }

  auto database = std::make_shared<catalog::Database>(std::move(options));

  return SerenedServer::Instance()
    .getFeature<catalog::CatalogFeature>()
    .Global()
    .CreateDatabase(std::move(database));
}

Result DropDatabase(const ExecContext& exec, std::string_view db_name) {
  if (exec.systemAuthLevel() != auth::Level::RW) {
    return {ERROR_FORBIDDEN};
  }

  return SerenedServer::Instance()
    .getFeature<catalog::CatalogFeature>()
    .Global()
    .DropDatabase(db_name, nullptr);
}

}  // namespace sdb::catalog
