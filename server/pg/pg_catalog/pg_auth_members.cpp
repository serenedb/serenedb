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
#include "basics/down_cast.h"
#include "catalog1/cluster.h"
#include "catalog1/entry/role.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"

namespace sdb::pg {

template<>
MaterializedData SystemTableSnapshot<PgAuthMembers>::GetTableData() {
  std::vector<PgAuthMembers> values;
  uint64_t oid = 1;
  auto& context = _context;
  auto& cluster = catalog::ClusterOf(context);
  cluster.ScanRoles(
    cluster.GetCatalogTransaction(context), [&](duckdb::CatalogEntry& entry) {
      const auto& role = entry.Cast<catalog::RoleCatalogEntry>();
      for (const auto& edge : role.MemberOf()) {
        values.push_back(PgAuthMembers{
          .oid = oid++,
          .roleid = edge.role,
          .member = role.oid,
          .grantor = pg::kRootUser,
          .admin_option = edge.admin_option,
          .inherit_option = edge.inherit_option,
          .set_option = edge.set_option,
        });
      }
    });

  auto result = CreateColumns<PgAuthMembers>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
