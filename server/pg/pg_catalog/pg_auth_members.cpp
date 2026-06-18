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
#include "catalog/catalog.h"
#include "catalog/role.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgAuthMembers>::GetTableData() {
  auto catalog = _config.EnsureCatalogSnapshot();

  // Synthetic, order-dependent oid for an (member, roleid) edge. XOR collides
  // for distinct pairs (1^2 == 4^7); PG requires pg_auth_members.oid unique, so
  // mix the two ids non-commutatively instead.
  const auto edge_oid = [](uint64_t member, uint64_t roleid) -> uint64_t {
    uint64_t h = member * 0x9E3779B97F4A7C15ull;
    h ^= roleid + 0x9E3779B97F4A7C15ull + (h << 6) + (h >> 2);
    return h;
  };

  std::vector<PgAuthMembers> values;
  for (const auto& role : catalog->GetRoles()) {
    for (const auto& edge : role->MemberOf()) {
      values.push_back(PgAuthMembers{
        .oid = edge_oid(role->GetId().id(), edge.role.id()),
        .roleid = edge.role.id(),
        .member = role->GetId().id(),
        .grantor = id::kRootUser.id(),
        .admin_option = edge.admin_option,
        .inherit_option = edge.inherit_option,
        .set_option = edge.set_option,
      });
    }
  }

  auto result = CreateColumns<PgAuthMembers>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, *_config.EnsureCatalogSnapshot());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
