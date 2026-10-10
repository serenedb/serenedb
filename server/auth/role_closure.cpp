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
#include <duckdb/main/database.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "catalog/cluster.h"
#include "catalog/entry/role.h"
#include "pg/catalog/oids.h"

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
                                     bool duckdb::Membership::* option) {
  irs::containers::FlatHashSet<duckdb::idx_t> seen{role};
  std::vector<duckdb::idx_t> work{role};
  while (!work.empty()) {
    const auto* node = graph.Find(work.back());
    work.pop_back();
    if (!node) {
      continue;
    }
    for (const auto& edge : node->member_of) {
      if ((!option || edge.*option) && graph.Find(edge.role) &&
          seen.insert(edge.role).second) {
        work.emplace_back(edge.role);
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
  cluster.GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
    .Scan(transaction, [&](duckdb::CatalogEntry& entry) {
      const auto& role = entry.Cast<catalog::RoleCatalogEntry>();
      auto& node = graph->nodes[role.oid];
      node.name = role.name.GetIdentifierName();
      node.member_of = role.MemberOf();
      node.options = role.Options();
      node.is_superuser = role.IsSuperuser();
    });
  return graph;
}

struct RoleCache final : duckdb::ObjectCacheEntry {
  static std::string ObjectType() { return "sdb_role_cache"; }
  std::string GetObjectType() final { return ObjectType(); }
  duckdb::optional_idx GetEstimatedCacheMemory() const final { return {}; }

  std::mutex mutex;
  duckdb::idx_t version = 0;
  std::shared_ptr<const RoleGraph> roles;
  irs::containers::FlatHashMap<duckdb::idx_t,
                               std::shared_ptr<const RoleClosure>>
    closures;
};

duckdb::shared_ptr<RoleCache> RoleCacheOf(catalog::ClusterCatalog& cluster) {
  return cluster.GetDatabase().GetObjectCache().GetOrCreate<RoleCache>(
    RoleCache::ObjectType());
}

duckdb::idx_t CommittedVersion(catalog::ClusterCatalog& cluster) {
  return duckdb::DuckTransactionManager::Get(cluster.GetAttached())
    .GetLastCommittedCatalogVersion();
}

std::shared_ptr<const RoleGraph> CommittedRoles(
  catalog::ClusterCatalog& cluster) {
  auto cache = RoleCacheOf(cluster);
  const auto version = CommittedVersion(cluster);
  {
    std::lock_guard guard{cache->mutex};
    if (cache->roles && cache->version == version) {
      return cache->roles;
    }
  }
  auto roles = BuildRoleGraph(cluster, cluster.LoginTransaction());
  std::lock_guard guard{cache->mutex};
  if (!cache->roles || version > cache->version) {
    cache->version = version;
    cache->roles = roles;
    cache->closures.clear();
  }
  return roles;
}

bool WritesCatalog(duckdb::ClientContext& context,
                   catalog::ClusterCatalog& cluster) {
  if (!context.transaction.HasActiveTransaction()) {
    return false;
  }
  auto transaction = context.transaction.ActiveTransaction().TryGetTransaction(
    cluster.GetAttached());
  return transaction &&
         transaction->Cast<duckdb::DuckTransaction>().catalog_version >=
           duckdb::TRANSACTION_ID_START;
}

}  // namespace

RoleClosure ComputeRoleClosure(const RoleGraph& graph, duckdb::idx_t role) {
  RoleClosure out;
  if (role == pg::kInvalidOid) {
    return out;
  }
  out.closure = Reachable(graph, role, &duckdb::Membership::inherit_option);
  out.members = Reachable(graph, role, nullptr);
  out.settable = Reachable(graph, role, &duckdb::Membership::set_option);
  for (const auto member : out.members) {
    const auto* node = graph.Find(member);
    if (!node) {
      continue;
    }
    for (const auto& edge : node->member_of) {
      if (edge.admin_option) {
        out.admin.emplace_back(edge.role);
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
  if (!context) {
    return CommittedRoles(catalog::ClusterOf());
  }
  auto& cluster = catalog::ClusterOf(*context->db);
  if (WritesCatalog(*context, cluster)) {
    return BuildRoleGraph(cluster, cluster.GetCatalogTransaction(*context));
  }
  return CommittedRoles(cluster);
}

std::shared_ptr<const RoleClosure> ClosureFor(duckdb::ClientContext* context,
                                              duckdb::idx_t role) {
  auto& cluster =
    context ? catalog::ClusterOf(*context->db) : catalog::ClusterOf();
  if (context && WritesCatalog(*context, cluster)) {
    return std::make_shared<const RoleClosure>(ComputeRoleClosure(
      *BuildRoleGraph(cluster, cluster.GetCatalogTransaction(*context)), role));
  }
  auto cache = RoleCacheOf(cluster);
  const auto version = CommittedVersion(cluster);
  {
    std::lock_guard guard{cache->mutex};
    if (cache->roles && cache->version == version) {
      if (const auto it = cache->closures.find(role);
          it != cache->closures.end()) {
        return it->second;
      }
    }
  }
  auto closure = std::make_shared<const RoleClosure>(
    ComputeRoleClosure(*CommittedRoles(cluster), role));
  std::lock_guard guard{cache->mutex};
  if (cache->version == version) {
    cache->closures.try_emplace(role, closure);
  }
  return closure;
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
