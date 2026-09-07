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

#include <algorithm>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog1/entry/role.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::auth {

struct RoleGraph {
  struct Node {
    std::string name;
    std::vector<catalog::Membership> member_of;
    catalog::RoleOption options = catalog::RoleOption::None;
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

struct RoleClosure {
  std::vector<duckdb::idx_t> closure;
  std::vector<duckdb::idx_t> members;
  std::vector<duckdb::idx_t> settable;
  std::vector<duckdb::idx_t> admin;

  catalog::RoleOption options = catalog::RoleOption::None;

  bool is_superuser = false;

  bool Has(catalog::RoleOption option) const {
    return catalog::HasOption(options, option);
  }

  bool MemberOf(duckdb::idx_t r) const {
    return is_superuser || std::ranges::binary_search(closure, r);
  }

  bool Owns(duckdb::idx_t owner) const { return MemberOf(owner); }

  bool IsMember(duckdb::idx_t r) const {
    return std::ranges::binary_search(members, r);
  }

  bool CanSet(duckdb::idx_t r) const {
    return std::ranges::binary_search(settable, r);
  }

  bool IsAdminOf(duckdb::idx_t r) const {
    return std::ranges::binary_search(admin, r);
  }

  bool Can(duckdb::CatalogType type, const duckdb::Permissions& perm,
           duckdb::AclMode need) const;

  bool CanAny(duckdb::CatalogType type, const duckdb::Permissions& perm,
              duckdb::AclMode need) const;

  bool CanColumns(const duckdb::Permissions& perm, duckdb::AclMode need,
                  std::span<const std::span<const duckdb::AclItem>> acls) const;

  bool CanAnyColumn(
    const duckdb::Permissions& perm, duckdb::AclMode need,
    std::span<const std::span<const duckdb::AclItem>> acls) const;

  duckdb::AclMode HeldModes(std::span<const duckdb::AclItem> acl) const;
  duckdb::AclMode GrantableModes(std::span<const duckdb::AclItem> acl) const;
};

std::shared_ptr<const RoleGraph> RolesOf(duckdb::ClientContext* context);

std::shared_ptr<const RoleClosure> ClosureFor(duckdb::ClientContext* context,
                                              duckdb::idx_t role);

RoleClosure ComputeRoleClosure(const RoleGraph& graph, duckdb::idx_t role);

}  // namespace sdb::auth
