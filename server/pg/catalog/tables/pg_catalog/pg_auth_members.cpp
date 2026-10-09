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

#include "catalog/entry/role.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct AuthMember {
  const catalog::RoleCatalogEntry& role;
  const duckdb::Membership& edge;
  int64_t oid;
};

class PgAuthMembers final : public SystemTableScan<kPgAuthMembersSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::ROLE_ENTRY, {}}};

  static constexpr auto kMember = Shape<kSql, const AuthMember>(
    Col<"oid">(&AuthMember::oid),
    Col<"roleid">([](const auto& row) { return row.edge.role; }),
    Col<"member">([](const auto& row) { return row.role.oid; }),
    Col<"grantor">([](const auto& row) { return row.edge.grantor; }),
    Col<"admin_option">([](const auto& row) { return row.edge.admin_option; }),
    Col<"inherit_option">(
      [](const auto& row) { return row.edge.inherit_option; }),
    Col<"set_option">([](const auto& row) { return row.edge.set_option; }));

  void Row(const catalog::RoleCatalogEntry& role) {
    if (&role != _role) {
      _role = &role;
      _first = _next;
      _next += static_cast<int64_t>(role.MemberOf().size());
    }
    auto oid = _first;
    for (const auto& edge : role.MemberOf()) {
      Emit<kMember>({role, edge, oid++});
    }
  }

 private:
  const catalog::RoleCatalogEntry* _role = nullptr;
  int64_t _first = 1;
  int64_t _next = 1;
};

}  // namespace

SystemTable gPgAuthMembers = SystemTableOf<PgAuthMembers>();

}  // namespace sdb::pg
