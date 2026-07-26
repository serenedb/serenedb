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

#include "pg/pg_catalog/pg_auth_members.h"

#include "app/app_server.h"
#include "auth/role_closure.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/role.h"
#include "connector/duckdb_catalog_sets.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgAuthMembers>::GetTableData() {
  std::vector<PgAuthMembers> values;
  uint64_t oid = 1;
  connector::VisitRoles(&_config.GetClientContext(),
                        [&](const catalog::CreateRoleInfo& role) {
                          for (const auto& edge : role.MemberOf()) {
                            values.push_back(PgAuthMembers{
                              .oid = oid++,
                              .roleid = edge.role.id(),
                              .member = role.GetId().id(),
                              .grantor = id::kRootUser.id(),
                              .admin_option = edge.admin_option,
                              .inherit_option = edge.inherit_option,
                              .set_option = edge.set_option,
                            });
                          }
                        });

  auto result = CreateColumns<PgAuthMembers>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row,
              *sdb::auth::RolesOf(&_config.GetClientContext()));
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
