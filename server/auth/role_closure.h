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

#pragma once

#include <absl/algorithm/container.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/entry/role.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

class ClusterCatalog;

}  // namespace sdb::catalog
namespace sdb::auth {

using RoleClosure = duckdb::RoleAccess;

struct RoleNode {
  std::string name;
  std::vector<duckdb::Membership> member_of;
  catalog::RoleOption options = catalog::RoleOption::None;
};

using RoleNodes = irs::containers::FlatHashMap<duckdb::idx_t, RoleNode>;

class RoleGraph final : public duckdb::SecuritySnapshot {
 public:
  RoleGraph(RoleNodes nodes, duckdb::vector<Role> closures);

  const RoleNode* Find(duckdb::idx_t role) const {
    auto it = nodes.find(role);
    return it == nodes.end() ? nullptr : &it->second;
  }

  const duckdb::idx_t* FindByName(std::string_view name) const {
    const auto it = by_name.find(name);
    return it == by_name.end() ? nullptr : &it->second;
  }

  std::string_view NameOf(duckdb::idx_t role) const {
    const auto* node = Find(role);
    return node ? std::string_view{node->name} : std::string_view{};
  }

  const RoleNodes nodes;
  irs::containers::FlatHashMap<std::string_view, duckdb::idx_t> by_name;
};

using RoleGraphPtr = duckdb::shared_ptr<const RoleGraph>;

class RoleAccessRef {
 public:
  RoleAccessRef() = default;
  RoleAccessRef(RoleGraphPtr graph, const RoleClosure& access)
    : _graph{std::move(graph)}, _access{access} {}

  explicit operator bool() const noexcept { return static_cast<bool>(_graph); }
  const RoleClosure* operator->() const noexcept { return &_access; }
  const RoleClosure& operator*() const noexcept { return _access; }

 private:
  RoleGraphPtr _graph;
  RoleClosure _access;
};

struct RoleMemberships {
  std::vector<duckdb::idx_t> members;
  std::vector<duckdb::idx_t> admin;

  bool IsMember(duckdb::idx_t r) const {
    return absl::c_binary_search(members, r);
  }
  bool IsAdminOf(duckdb::idx_t r) const {
    return absl::c_binary_search(admin, r);
  }
};

struct RoleCache {
  absl::Mutex mutex;
  duckdb::idx_t version ABSL_GUARDED_BY(mutex) = 0;
  RoleGraphPtr roles ABSL_GUARDED_BY(mutex);
};

RoleGraphPtr RolesOf(duckdb::ClientContext* context);

RoleAccessRef ClosureFor(duckdb::ClientContext* context, duckdb::idx_t role);

RoleAccessRef SessionClosure(duckdb::ClientContext& context);

RoleMemberships MembershipsOf(const RoleGraph& graph, duckdb::idx_t role);

RoleGraphPtr BuildRoleGraph(catalog::ClusterCatalog& cluster,
                            duckdb::CatalogTransaction transaction);

void PublishRoles(catalog::ClusterCatalog& cluster);

}  // namespace sdb::auth
