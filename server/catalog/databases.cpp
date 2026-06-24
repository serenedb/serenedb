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
#include "catalog/catalog.h"
#include "catalog/database.h"

namespace sdb::catalog {

Result CreateDatabase(std::string_view name) {
  auto database = std::make_shared<catalog::Database>(
    ObjectId{}, catalog::DatabaseOptions{std::string{name}});

  return catalog::GetCatalog().CreateDatabase(std::move(database));
}

Result DropDatabase(std::string_view db_name,
                    duckdb::shared_ptr<void> keep_alive) {
  return catalog::GetCatalog().DropDatabase(db_name, std::move(keep_alive));
}

}  // namespace sdb::catalog
