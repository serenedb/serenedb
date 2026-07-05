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
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"

namespace sdb::catalog {

struct Snapshot;
class Table;
class Column;

}  // namespace sdb::catalog
namespace sdb::auth {

using RoleIdSet = containers::FlatHashSet<ObjectId>;

RoleIdSet ComputeMembershipClosure(const catalog::Snapshot& snapshot,
                                   ObjectId role);

RoleIdSet ComputeSetRoleClosure(const catalog::Snapshot& snapshot,
                                ObjectId role);

// Whether `member` (in)directly holds ADMIN OPTION on role `target`. A
// snapshot-level graph query (it inspects admin_option edges, which the
// flattened RoleClosure does not carry), so it is a free function, not a
// RoleClosure method.
bool HasAdminOption(const catalog::Snapshot& snapshot, ObjectId member,
                    ObjectId target);

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
  bool Owns(const catalog::Object& object) const {
    return Owns(object.GetOwner());
  }

  // Does this principal hold ALL of `need` on `object`? (A single-bit `need` --
  // the common case -- is simply "holds that privilege".) Owners, and
  // superusers who own everything, hold every privilege. The primary check.
  bool Can(const catalog::Object& object, catalog::AclMode need) const;

  // Does this principal hold ANY of the `need` bits on `object`? For has_*_
  // privilege('SELECT,INSERT') style checks, which PG answers true if the role
  // holds at least one of the listed privileges.
  bool CanAny(const catalog::Object& object, catalog::AclMode need) const;

  // Holds `need` on EVERY column picked by `selected` -- a table-level grant of
  // `need` satisfies it outright, otherwise the per-column ACLs are consulted.
  // `selected` receives each visible column's logical index and the column
  // itself (callers match by index or by identity); false if it selects none.
  // Hidden generated PKs are skipped. Covers the planner's referenced-column
  // check and the has_column_privilege column-list form.
  bool CanColumns(
    const catalog::Table& table, catalog::AclMode need,
    absl::FunctionRef<bool(uint64_t, const catalog::Column&)> selected) const;

  // Holds `need` on ANY visible column (or table-wide) -- the "no specific
  // columns referenced" case, e.g. a bare table read that touched no column.
  bool CanAnyColumn(const catalog::Table& table, catalog::AclMode need) const;

  // The subset of `acl`'s privileges this principal holds -- HeldModes at all,
  // GrantableModes only those carrying WITH GRANT OPTION. For deciding what a
  // non-owner grantor may pass on / how much a REVOKE removes.
  catalog::AclMode HeldModes(catalog::AclView acl) const;
  catalog::AclMode GrantableModes(catalog::AclView acl) const;
};

class RoleClosureMap {
 public:
  void Build(const catalog::Snapshot& snapshot);

  const RoleClosure* Find(ObjectId role) const {
    auto it = _by_role.find(role);
    return it == _by_role.end() ? nullptr : &it->second;
  }

 private:
  containers::FlatHashMap<ObjectId, RoleClosure> _by_role;
};

}  // namespace sdb::auth
