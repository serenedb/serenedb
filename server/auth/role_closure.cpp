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

#include "auth/role_closure.h"

#include <algorithm>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/main/client_context.hpp>
#include <memory>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "catalog1/cluster.h"
#include "catalog1/entry/role.h"
#include "pg/pg_types.h"

namespace sdb::auth {
namespace {

using duckdb::AclItem;
using duckdb::AclMode;
using RoleIdSpan = std::span<const duckdb::idx_t>;

bool RolesContain(RoleIdSpan roles, duckdb::idx_t id) {
  return std::ranges::binary_search(roles, id);
}

bool Reaches(const AclItem& item, RoleIdSpan roles) {
  return item.grantee == pg::kPublicGrantee ||
         RolesContain(roles, item.grantee);
}

AclMode Held(std::span<const duckdb::AclItem> acl, RoleIdSpan roles,
             AclMode AclItem::* field) {
  AclMode held = AclMode::NoRights;
  for (const auto& item : acl) {
    if (Reaches(item, roles)) {
      held |= item.*field;
    }
  }
  return held;
}

bool Allows(std::span<const duckdb::AclItem> stored, duckdb::CatalogType type,
            duckdb::idx_t owner, RoleIdSpan roles, AclMode need, bool any) {
  SDB_ASSERT(std::ranges::is_sorted(roles),
             "Allows requires an ascending-sorted roles span");
  if (need == AclMode::NoRights) {
    return false;
  }
  const auto done = [&](AclMode have) {
    return any ? (have & need) != AclMode::NoRights : (have & need) == need;
  };
  AclMode have = AclMode::NoRights;
  if (RolesContain(roles, owner)) {
    have |= duckdb::Permissions::AllPrivileges(type);
    if (done(have)) {
      return true;
    }
  }
  if (stored.empty()) {
    have |= duckdb::Permissions::PublicPrivileges(type);
    return done(have);
  }
  for (const auto& item : stored) {
    if (!Reaches(item, roles)) {
      continue;
    }
    have |= item.privs;
    if (done(have)) {
      return true;
    }
  }
  return false;
}

bool ColumnGrants(std::span<const duckdb::AclItem> acl, duckdb::idx_t owner,
                  RoleIdSpan closure, AclMode need) {
  return Allows(acl, duckdb::CatalogType::TABLE_ENTRY, owner, closure, need,
                false);
}

std::vector<duckdb::idx_t> Reachable(const RoleGraph& graph, duckdb::idx_t role,
                                     bool catalog::Membership::* option) {
  containers::FlatHashSet<duckdb::idx_t> seen{role};
  std::vector<duckdb::idx_t> work{role};
  while (!work.empty()) {
    const auto* node = graph.Find(work.back());
    work.pop_back();
    if (node == nullptr) {
      continue;
    }
    for (const auto& edge : node->member_of) {
      if ((option == nullptr || edge.*option) && graph.Find(edge.role) &&
          seen.insert(edge.role).second) {
        work.push_back(edge.role);
      }
    }
  }
  std::vector<duckdb::idx_t> out(seen.begin(), seen.end());
  std::ranges::sort(out);
  return out;
}

std::shared_ptr<const RoleGraph> BuildRoleGraph(
  catalog::ClusterCatalog& cluster, duckdb::CatalogTransaction transaction) {
  auto graph = std::make_shared<RoleGraph>();
  cluster.ScanRoles(transaction, [&](duckdb::CatalogEntry& entry) {
    const auto& role = entry.Cast<catalog::RoleCatalogEntry>();
    auto& node = graph->nodes[role.oid];
    node.name = role.name.GetIdentifierName();
    node.member_of = role.MemberOf();
    node.options = role.Options();
    node.is_superuser = role.IsSuperuser();
  });
  return graph;
}

}  // namespace

RoleClosure ComputeRoleClosure(const RoleGraph& graph, duckdb::idx_t role) {
  RoleClosure out;
  if (role == pg::kInvalidOid) {
    return out;
  }
  out.closure = Reachable(graph, role, &catalog::Membership::inherit_option);
  out.members = Reachable(graph, role, nullptr);
  out.settable = Reachable(graph, role, &catalog::Membership::set_option);
  for (const auto member : out.members) {
    const auto* node = graph.Find(member);
    if (node == nullptr) {
      continue;
    }
    for (const auto& edge : node->member_of) {
      if (edge.admin_option) {
        out.admin.push_back(edge.role);
      }
    }
  }
  std::ranges::sort(out.admin);
  if (const auto* node = graph.Find(role)) {
    out.options = node->options;
    out.is_superuser = node->is_superuser;
  }
  return out;
}

std::shared_ptr<const RoleGraph> RolesOf(duckdb::ClientContext* context) {
  if (context == nullptr) {
    auto& cluster = catalog::ClusterOf();
    return BuildRoleGraph(cluster, cluster.LoginTransaction());
  }
  auto& cluster = catalog::ClusterOf(*context);
  return BuildRoleGraph(cluster, cluster.GetCatalogTransaction(*context));
}

std::shared_ptr<const RoleClosure> ClosureFor(duckdb::ClientContext* context,
                                              duckdb::idx_t role) {
  return std::make_shared<const RoleClosure>(
    ComputeRoleClosure(*RolesOf(context), role));
}

bool RoleClosure::Can(duckdb::CatalogType type, const duckdb::Permissions& perm,
                      duckdb::AclMode need) const {
  if (Owns(perm.owner)) {
    return true;
  }
  return Allows(perm.acl, type, perm.owner, closure, need, false);
}

bool RoleClosure::CanAny(duckdb::CatalogType type,
                         const duckdb::Permissions& perm,
                         duckdb::AclMode need) const {
  if (Owns(perm.owner)) {
    return true;
  }
  return Allows(perm.acl, type, perm.owner, closure, need, true);
}

duckdb::AclMode RoleClosure::HeldModes(
  std::span<const duckdb::AclItem> acl) const {
  return Held(acl, closure, &AclItem::privs);
}

duckdb::AclMode RoleClosure::GrantableModes(
  std::span<const duckdb::AclItem> acl) const {
  return Held(acl, closure, &AclItem::grant_option);
}

bool RoleClosure::CanColumns(
  const duckdb::Permissions& perm, duckdb::AclMode need,
  std::span<const std::span<const duckdb::AclItem>> acls) const {
  if (Can(duckdb::CatalogType::TABLE_ENTRY, perm, need)) {
    return true;
  }
  return !acls.empty() &&
         std::ranges::all_of(acls, [&](std::span<const duckdb::AclItem> acl) {
           return ColumnGrants(acl, perm.owner, closure, need);
         });
}

bool RoleClosure::CanAnyColumn(
  const duckdb::Permissions& perm, duckdb::AclMode need,
  std::span<const std::span<const duckdb::AclItem>> acls) const {
  if (Can(duckdb::CatalogType::TABLE_ENTRY, perm, need)) {
    return true;
  }
  return std::ranges::any_of(acls, [&](std::span<const duckdb::AclItem> acl) {
    return ColumnGrants(acl, perm.owner, closure, need);
  });
}

}  // namespace sdb::auth
