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

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <vector>

#include "catalog/cluster.h"
#include "catalog/entry/role.h"
#include "pg/catalog/oids.h"

namespace sdb::auth {
namespace {

std::vector<duckdb::idx_t> Reachable(const RoleNodes& nodes, duckdb::idx_t role,
                                     bool duckdb::Membership::* option) {
  irs::containers::FlatHashSet<duckdb::idx_t> seen{role};
  std::vector<duckdb::idx_t> work{role};
  while (!work.empty()) {
    const auto it = nodes.find(work.back());
    work.pop_back();
    if (it == nodes.end()) {
      continue;
    }
    for (const auto& edge : it->second.member_of) {
      if ((!option || edge.*option) && nodes.contains(edge.role) &&
          seen.insert(edge.role).second) {
        work.emplace_back(edge.role);
      }
    }
  }
  std::vector<duckdb::idx_t> out(seen.begin(), seen.end());
  absl::c_sort(out);
  return out;
}

duckdb::RoleCapability CapabilitiesOf(duckdb::idx_t role,
                                      std::span<const duckdb::idx_t> closure) {
  auto capabilities = duckdb::RoleCapability::None;
  for (const auto& predefined : kPredefinedRoles) {
    if (predefined.oid == role ||
        absl::c_binary_search(closure, predefined.oid)) {
      capabilities |= predefined.capability;
    }
  }
  return capabilities;
}

duckdb::vector<duckdb::SecuritySnapshot::Role> InheritClosures(
  const RoleNodes& nodes) {
  duckdb::vector<duckdb::SecuritySnapshot::Role> closures;
  closures.reserve(nodes.size());
  for (const auto& [oid, node] : nodes) {
    auto closure = Reachable(nodes, oid, &duckdb::Membership::inherit_option);
    const auto capabilities = CapabilitiesOf(oid, closure);
    closures.push_back(
      {.role = oid,
       .options = node.options,
       .capabilities = capabilities,
       .closure = std::move(closure),
       .settable = Reachable(nodes, oid, &duckdb::Membership::set_option)});
  }
  return closures;
}

RoleGraphPtr Publish(catalog::ClusterCatalog& cluster, RoleCache& cache) {
  const auto durable =
    duckdb::DuckTransactionManager::Get(cluster.GetAttached())
      .GetDurableSnapshot();
  {
    absl::MutexLock lock{&cache.mutex};
    if (cache.roles && cache.version >= durable.catalog_version) {
      return cache.roles;
    }
  }
  auto roles = BuildRoleGraph(
    cluster, duckdb::CatalogTransaction{cluster.GetDatabase(),
                                        duckdb::TRANSACTION_ID_START - 1,
                                        durable.visibility_bound});
  absl::MutexLock lock{&cache.mutex};
  if (!cache.roles || durable.catalog_version > cache.version) {
    cache.version = durable.catalog_version;
    cache.roles = std::move(roles);
    cluster.GetDatabase().PublishSecurity(cache.roles);
  }
  return cache.roles;
}

RoleGraphPtr PublishedRoles(catalog::ClusterCatalog& cluster,
                            RoleCache& cache) {
  {
    absl::MutexLock lock{&cache.mutex};
    if (cache.roles) {
      return cache.roles;
    }
  }
  return Publish(cluster, cache);
}

}  // namespace

RoleGraph::RoleGraph(RoleNodes nodes_p, duckdb::vector<Role> closures)
  : duckdb::SecuritySnapshot{std::move(closures)}, nodes{std::move(nodes_p)} {
  for (const auto& [oid, node] : nodes) {
    by_name.emplace(node.name, oid);
  }
}

RoleGraphPtr BuildRoleGraph(catalog::ClusterCatalog& cluster,
                            duckdb::CatalogTransaction transaction) {
  RoleNodes nodes;
  cluster.GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
    .Scan(transaction, [&](duckdb::CatalogEntry& entry) {
      const auto& role = entry.Cast<catalog::RoleCatalogEntry>();
      auto& node = nodes[role.oid];
      node.name = role.name.GetIdentifierName();
      node.member_of = role.MemberOf();
      node.options = role.Options();
    });
  auto closures = InheritClosures(nodes);
  return duckdb::make_shared_ptr<const RoleGraph>(std::move(nodes),
                                                  std::move(closures));
}

void PublishRoles(catalog::ClusterCatalog& cluster) {
  Publish(cluster, cluster.Roles());
}

RoleGraphPtr RolesOf(duckdb::ClientContext* context) {
  if (context && context->security) {
    return duckdb::shared_ptr_cast<const duckdb::SecuritySnapshot,
                                   const RoleGraph>(context->security);
  }
  auto& cluster =
    context ? catalog::ClusterOf(*context->db) : catalog::ClusterOf();
  return PublishedRoles(cluster, cluster.Roles());
}

RoleAccessRef ClosureFor(duckdb::ClientContext* context, duckdb::idx_t role) {
  auto graph = RolesOf(context);
  if (const auto access = graph->Access(role)) {
    return {std::move(graph), *access};
  }
  RoleClosure missing;
  missing.role = role;
  return {std::move(graph), missing};
}

RoleMemberships MembershipsOf(const RoleGraph& graph, duckdb::idx_t role) {
  RoleMemberships out;
  if (role == pg::kInvalidOid) {
    return out;
  }
  out.members = Reachable(graph.nodes, role, nullptr);
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
  absl::c_sort(out.admin);
  return out;
}

}  // namespace sdb::auth
