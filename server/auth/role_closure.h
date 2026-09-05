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

#include <absl/functional/function_ref.h>

#include <algorithm>
#include <duckdb/catalog/catalog_entry.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog1/entry/role.h"
#include "catalog1/permissions.h"
namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::auth {

using RoleIdSet = containers::FlatHashSet<duckdb::idx_t>;

struct RoleGraph {
  struct Node {
    std::string name;
    std::vector<catalog::Membership> member_of;
    bool is_superuser = false;
  };

  containers::FlatHashMap<duckdb::idx_t, Node> nodes;

  const Node* Find(duckdb::idx_t role) const {
    auto it = nodes.find(role);
    return it == nodes.end() ? nullptr : &it->second;
  }

  std::string_view NameOf(duckdb::idx_t role) const {
    const auto* node = Find(role);
    return node == nullptr ? std::string_view{} : std::string_view{node->name};
  }
};

RoleIdSet ComputeMembershipClosure(const RoleGraph& graph, duckdb::idx_t role);

RoleIdSet ComputeSetRoleClosure(const RoleGraph& graph, duckdb::idx_t role);

bool HasAdminOption(const RoleGraph& graph, duckdb::idx_t member,
                    duckdb::idx_t target);

struct RoleClosure {
  std::vector<duckdb::idx_t> closure;

  bool is_superuser = false;

  bool MemberOf(duckdb::idx_t r) const {
    return is_superuser || std::ranges::binary_search(closure, r);
  }

  bool Owns(duckdb::idx_t owner) const { return MemberOf(owner); }

  bool Can(duckdb::CatalogType type, const catalog::Permissions& perm,
           catalog::AclMode need) const;

  bool CanAny(duckdb::CatalogType type, const catalog::Permissions& perm,
              catalog::AclMode need) const;

  bool CanColumns(const catalog::Permissions& perm, catalog::AclMode need,
                  std::span<const catalog::AclView> acls) const;

  bool CanAnyColumn(const catalog::Permissions& perm, catalog::AclMode need,
                    std::span<const catalog::AclView> acls) const;

  catalog::AclMode HeldModes(catalog::AclView acl) const;
  catalog::AclMode GrantableModes(catalog::AclView acl) const;
};

std::shared_ptr<const RoleGraph> RolesOf(duckdb::ClientContext* context);

std::shared_ptr<const RoleClosure> ClosureFor(duckdb::ClientContext* context,
                                              duckdb::idx_t role);

RoleClosure ComputeRoleClosure(const RoleGraph& graph, duckdb::idx_t role);

}  // namespace sdb::auth
