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
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/role.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {}  // namespace sdb::catalog
namespace sdb::auth {

using RoleIdSet = containers::FlatHashSet<ObjectId>;

// The role graph, flattened out of the ROLE_ENTRY set: for each role, who it is
// a member of and whether it is a superuser. Loaded once per generation (see
// RoleGeneration) and shared by every reader -- a workload that does no role
// DDL never builds a second one.
struct RoleGraph {
  struct Node {
    std::string name;
    std::vector<catalog::Membership> member_of;
    bool is_superuser = false;
  };

  containers::FlatHashMap<ObjectId, Node> nodes;

  const Node* Find(ObjectId role) const {
    auto it = nodes.find(role);
    return it == nodes.end() ? nullptr : &it->second;
  }

  // The name of `role`, or empty when no role carries that id. Callers
  // rendering pg_catalog fall back to the raw oid.
  std::string_view NameOf(ObjectId role) const {
    const auto* node = Find(role);
    return node == nullptr ? std::string_view{} : std::string_view{node->name};
  }
};

// Bumped by every mutation that can change the role graph: CREATE / ALTER /
// DROP ROLE and GRANT / REVOKE of a membership. Nothing else invalidates a
// cached closure, which is what makes a DDL-free workload pay one relaxed load
// and a hash lookup per check.
uint64_t RoleGeneration() noexcept;
void BumpRoleGeneration() noexcept;

RoleIdSet ComputeMembershipClosure(const RoleGraph& graph, ObjectId role);

RoleIdSet ComputeSetRoleClosure(const RoleGraph& graph, ObjectId role);

// Whether `member` (in)directly holds ADMIN OPTION on role `target`. A graph
// query (it inspects admin_option edges, which the flattened RoleClosure does
// not carry), so it is a free function, not a RoleClosure method.
bool HasAdminOption(const RoleGraph& graph, ObjectId member, ObjectId target);

struct RoleClosure {
  // The set of roles this principal acts as (its own membership closure),
  // kept sorted for binary search.
  std::vector<ObjectId> closure;
  // A superuser bypasses every membership / ownership / privilege check. It is
  // folded into the predicates below so callers never test it by hand -- read
  // it directly only for non-authz needs (e.g. the is_superuser GUC, or the
  // permission-to-SET-ROLE decision, which is about the actor, not an object).
  bool is_superuser = false;

  // Does this principal act as role `r`? (member-of-or-equals; superuser: all.)
  bool MemberOf(ObjectId r) const {
    return is_superuser || std::ranges::binary_search(closure, r);
  }

  // Does this principal own an object owned by `owner`? A superuser owns
  // everything, and ownership is membership in the owning role, so this is
  // exactly MemberOf(owner).
  bool Owns(ObjectId owner) const { return MemberOf(owner); }

  // Does this principal hold ALL of `need` on `object`? (A single-bit `need` --
  // the common case -- is simply "holds that privilege".) Owners, and
  // superusers who own everything, hold every privilege. The primary check.
  //
  // `perm` is what the bound catalog entry carries.
  bool Can(duckdb::CatalogType type, const catalog::Permissions& perm,
           catalog::AclMode need) const;

  // Does this principal hold ANY of the `need` bits on `object`? For has_*_
  // privilege('SELECT,INSERT') style checks, which PG answers true if the role
  // holds at least one of the listed privileges.
  bool CanAny(duckdb::CatalogType type, const catalog::Permissions& perm,
              catalog::AclMode need) const;

  // Holds `need` on EVERY column whose own grants are listed in `acls` -- a
  // table-level grant of `need` satisfies it outright, otherwise every column
  // has to carry it. An empty list is false: nothing was selected, so nothing
  // gives the right. Covers the planner's referenced-column check and the
  // has_column_privilege column-list form.
  //
  // The caller spells the column set, because what a column is differs by
  // reader: a bound entry's ColumnList keyed by the id its ColumnAcls are keyed
  // on, or a definition's own columns. Neither carries the hidden generated
  // primary key, which never holds a grant. Postgres gives a column no owner of
  // its own, so `perm.owner` answers for all of them.
  bool CanColumns(const catalog::Permissions& perm, catalog::AclMode need,
                  std::span<const catalog::AclView> acls) const;

  // Holds `need` on ANY of `acls` (or table-wide) -- the "no specific columns
  // referenced" case, e.g. a bare table read that touched no column.
  bool CanAnyColumn(const catalog::Permissions& perm, catalog::AclMode need,
                    std::span<const catalog::AclView> acls) const;

  // The subset of `acl`'s privileges this principal holds -- HeldModes at all,
  // GrantableModes only those carrying WITH GRANT OPTION. For deciding what a
  // non-owner grantor may pass on / how much a REVOKE removes.
  catalog::AclMode HeldModes(catalog::AclView acl) const;
  catalog::AclMode GrantableModes(catalog::AclView acl) const;
};

// The role graph as `context`'s transaction sees it. A transaction that has
// written a role reads its own uncommitted version out of the MVCC'd
// ROLE_ENTRY set and gets a graph of its own, which is never published;
// everybody else shares the one cached for the current generation.
std::shared_ptr<const RoleGraph> RolesOf(duckdb::ClientContext* context);

// The closure of `role`, cached across queries and thrown away only when the
// generation moves. The common path -- no role DDL since the last call -- is a
// hash lookup with no BFS behind it.
std::shared_ptr<const RoleClosure> ClosureFor(duckdb::ClientContext* context,
                                              ObjectId role);

// The closure computed straight out of `graph`, for the callers that already
// hold one (a mutator checking several roles under one lock).
RoleClosure ComputeRoleClosure(const RoleGraph& graph, ObjectId role);

}  // namespace sdb::auth
