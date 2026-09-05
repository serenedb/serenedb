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
#include <atomic>
#include <duckdb/main/client_context.hpp>
#include <memory>
#include <vector>

#include "auth/acl.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/role.h"

namespace sdb::auth {
namespace {

bool ColumnGrants(catalog::AclView acl, ObjectId owner, RoleIdSpan closure,
                  catalog::AclMode need) {
  return AclCheckSorted(acl, duckdb::CatalogType::TABLE_ENTRY, owner, closure,
                        need, PrivMatch::All);
}

enum class EdgeFilter {
  All,
  Inherit,
  Set,
};

bool EdgePasses(const catalog::Membership& edge, EdgeFilter filter) {
  switch (filter) {
    case EdgeFilter::All:
      return true;
    case EdgeFilter::Inherit:
      return edge.inherit_option;
    case EdgeFilter::Set:
      return edge.set_option;
  }
  return false;
}

RoleIdSet ComputeClosure(const RoleGraph& graph, ObjectId role,
                         EdgeFilter filter) {
  RoleIdSet out;
  if (!role.isSet()) {
    return out;
  }
  out.insert(role);
  std::vector<ObjectId> work{role};
  while (!work.empty()) {
    auto cur = work.back();
    work.pop_back();
    const auto* node = graph.Find(cur);
    if (node == nullptr) {
      continue;
    }
    for (const auto& edge : node->member_of) {
      if (!EdgePasses(edge, filter)) {
        continue;
      }
      if (out.contains(edge.role) || graph.Find(edge.role) == nullptr) {
        continue;
      }
      out.insert(edge.role);
      work.push_back(edge.role);
    }
  }
  return out;
}

std::atomic_uint64_t gRoleGeneration{1};

// One generation's worth of answers, published whole and replaced whole. The
// closures accumulate as they are asked for, so a role nobody checks costs
// nothing, and a generation that never moves is never rebuilt.
struct RoleCache {
  uint64_t generation = 0;
  std::shared_ptr<const RoleGraph> graph;
  containers::FlatHashMap<ObjectId, std::shared_ptr<const RoleClosure>>
    closures;
};

std::shared_ptr<const RoleCache> gRoleCache =
  std::make_shared<const RoleCache>();

std::shared_ptr<const RoleGraph> LoadRoleGraph(duckdb::ClientContext* context) {
  auto graph = std::make_shared<RoleGraph>();
  catalog::VisitRoles(context, [&](const catalog::SereneDBRoleEntry& role) {
    auto& node = graph->nodes[role.GetId()];
    node.name = role.GetName();
    node.member_of.assign(role.MemberOf().begin(), role.MemberOf().end());
    node.is_superuser = role.IsSuperuser();
  });
  return graph;
}

// A transaction reads its own uncommitted roles, so it can neither use the
// shared cache nor publish into it.
bool ReadsOwnRoles(duckdb::ClientContext* context) {
  return context != nullptr && catalog::HasUncommittedRoles(*context);
}

}  // namespace

uint64_t RoleGeneration() noexcept {
  return gRoleGeneration.load(std::memory_order_relaxed);
}

void BumpRoleGeneration() noexcept {
  gRoleGeneration.fetch_add(1, std::memory_order_relaxed);
}

RoleIdSet ComputeMembershipClosure(const RoleGraph& graph, ObjectId role) {
  return ComputeClosure(graph, role, EdgeFilter::All);
}

RoleIdSet ComputeSetRoleClosure(const RoleGraph& graph, ObjectId role) {
  return ComputeClosure(graph, role, EdgeFilter::Set);
}

RoleClosure ComputeRoleClosure(const RoleGraph& graph, ObjectId role) {
  RoleClosure out;
  if (!role.isSet()) {
    return out;
  }
  // The membership set comes from the one canonical inherit-closure BFS: it
  // seeds with `role` even when that id names no role -- the PUBLIC
  // pseudo-grantee and dangling grantee ids must still reach the ACL walk,
  // where PUBLIC entries are matched.
  const RoleIdSet set = ComputeClosure(graph, role, EdgeFilter::Inherit);
  out.closure.assign(set.begin(), set.end());
  std::ranges::sort(out.closure);
  // The superuser bit is the start role's own attribute -- never inherited.
  if (const auto* node = graph.Find(role)) {
    out.is_superuser = node->is_superuser;
  }
  return out;
}

std::shared_ptr<const RoleGraph> RolesOf(duckdb::ClientContext* context) {
  if (ReadsOwnRoles(context)) {
    return LoadRoleGraph(context);
  }
  const auto generation = RoleGeneration();
  auto cached = std::atomic_load(&gRoleCache);
  if (cached->generation == generation && cached->graph) {
    return cached->graph;
  }
  auto fresh = std::make_shared<RoleCache>();
  fresh->generation = generation;
  // Committed, not the caller's read view: a transaction that started before a
  // CREATE ROLE committed would otherwise publish a graph without that role
  // under the current generation, and every later reader would take it.
  fresh->graph = LoadRoleGraph(nullptr);
  auto graph = fresh->graph;
  std::atomic_store(&gRoleCache, std::shared_ptr<const RoleCache>{fresh});
  return graph;
}

std::shared_ptr<const RoleClosure> ClosureFor(duckdb::ClientContext* context,
                                              ObjectId role) {
  if (ReadsOwnRoles(context)) {
    return std::make_shared<const RoleClosure>(
      ComputeRoleClosure(*LoadRoleGraph(context), role));
  }
  const auto generation = RoleGeneration();
  auto cached = std::atomic_load(&gRoleCache);
  if (cached->generation == generation) {
    if (auto it = cached->closures.find(role); it != cached->closures.end()) {
      return it->second;
    }
  }
  // A miss: either the generation moved (role DDL happened) or this role has
  // not been checked yet. Both are rare, and both produce a new cache built on
  // whatever the current one still holds.
  auto fresh = std::make_shared<RoleCache>();
  fresh->generation = generation;
  fresh->graph = cached->generation == generation && cached->graph
                   ? cached->graph
                   : LoadRoleGraph(nullptr);
  if (cached->generation == generation) {
    fresh->closures = cached->closures;
  }
  auto closure = std::make_shared<const RoleClosure>(
    ComputeRoleClosure(*fresh->graph, role));
  fresh->closures.emplace(role, closure);
  std::atomic_store(&gRoleCache, std::shared_ptr<const RoleCache>{fresh});
  return closure;
}

bool HasAdminOption(const RoleGraph& graph, ObjectId member, ObjectId target) {
  for (ObjectId r : ComputeMembershipClosure(graph, member)) {
    const auto* node = graph.Find(r);
    if (node == nullptr) {
      continue;
    }
    for (const auto& edge : node->member_of) {
      if (edge.role == target && edge.admin_option) {
        return true;
      }
    }
  }
  return false;
}

bool RoleClosure::Can(duckdb::CatalogType type,
                      const catalog::Permissions& perm,
                      catalog::AclMode need) const {
  // The owner (and a superuser, who owns everything) holds every privilege.
  if (Owns(ObjectId{perm.owner})) {
    return true;
  }
  return AclCheckSorted(perm.acl, type, ObjectId{perm.owner}, closure, need,
                        PrivMatch::All);
}

bool RoleClosure::CanAny(duckdb::CatalogType type,
                         const catalog::Permissions& perm,
                         catalog::AclMode need) const {
  if (Owns(ObjectId{perm.owner})) {
    return true;
  }
  return AclCheckSorted(perm.acl, type, ObjectId{perm.owner}, closure, need,
                        PrivMatch::Any);
}

catalog::AclMode RoleClosure::HeldModes(catalog::AclView acl) const {
  return AclPrivsHeld(acl, closure);
}

catalog::AclMode RoleClosure::GrantableModes(catalog::AclView acl) const {
  return AclGrantOptionHeld(acl, closure);
}

bool RoleClosure::CanColumns(const catalog::Permissions& perm,
                             catalog::AclMode need,
                             std::span<const catalog::AclView> acls) const {
  if (Can(duckdb::CatalogType::TABLE_ENTRY, perm, need)) {
    return true;
  }
  return !acls.empty() && std::ranges::all_of(acls, [&](catalog::AclView acl) {
    return ColumnGrants(acl, ObjectId{perm.owner}, closure, need);
  });
}

bool RoleClosure::CanAnyColumn(const catalog::Permissions& perm,
                               catalog::AclMode need,
                               std::span<const catalog::AclView> acls) const {
  if (Can(duckdb::CatalogType::TABLE_ENTRY, perm, need)) {
    return true;
  }
  return std::ranges::any_of(acls, [&](catalog::AclView acl) {
    return ColumnGrants(acl, ObjectId{perm.owner}, closure, need);
  });
}

}  // namespace sdb::auth
