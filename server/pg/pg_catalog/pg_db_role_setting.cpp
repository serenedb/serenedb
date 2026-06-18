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

#include "pg/pg_catalog/pg_db_role_setting.h"

#include <deque>

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/role.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgDbRoleSetting>::GetTableData() {
  auto catalog = _config.EnsureCatalogSnapshot();

  // setconfig is text[]: a span of string_view into the role's stored config
  // strings. Hold the role shared_ptrs for the whole call so the strings the
  // views point at stay alive through WriteData; the per-row view vectors are
  // owned by `views` (a deque so the spans stay valid across pushes).
  auto roles = catalog->GetRoles();
  std::vector<PgDbRoleSetting> values;
  std::deque<std::vector<std::string_view>> views;
  for (const auto& role : roles) {
    auto config = role->Config();
    if (config.empty()) {
      continue;  // PG inserts a pg_db_role_setting row only when a GUC is set.
    }
    auto& view = views.emplace_back();
    view.reserve(config.size());
    for (const auto& entry : config) {
      view.push_back(entry);
    }
    values.push_back(PgDbRoleSetting{
      .setdatabase = 0,  // role-wide (all databases) -> pg_roles.rolconfig join
      .setrole = role->GetId().id(),
      .setconfig = view,
    });
  }

  auto result = CreateColumns<PgDbRoleSetting>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, *_config.EnsureCatalogSnapshot());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
