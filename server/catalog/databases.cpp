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
#include "catalog/database.h"

namespace sdb::catalog {

Result CreateDatabase(const ExecContext& exec, std::string_view name) {
  if (auto r = DatabaseNameValidator::validateName(/*allowSystem*/ false, name);
      r.fail()) {
    return r;
  }

  auto database = std::make_shared<catalog::Database>(
    ObjectId{}, catalog::DatabaseOptions{std::string{name}});

  return catalog::CatalogFeature::instance().Global().CreateDatabase(
    std::move(database));
}

Result DropDatabase(const ExecContext& exec, std::string_view db_name,
                    duckdb::shared_ptr<void> keep_alive) {
  if (exec.systemAuthLevel() != auth::Level::RW) {
    return {ERROR_FORBIDDEN};
  }

  return catalog::CatalogFeature::instance().Global().DropDatabase(
    db_name, std::move(keep_alive));
}

}  // namespace sdb::catalog
