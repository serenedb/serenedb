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
#include <vector>

#include "auth/acl.h"
#include "catalog/catalog.h"
#include "catalog/role.h"
#include "catalog/table.h"

namespace sdb::auth {
namespace {

// Hidden generated primary keys never carry ACL rows and are not part of the
// user-visible column set; exclude them from every column-privilege scan.
bool IsHidden(const catalog::Column& column) {
  return column.GetId() == catalog::Column::kGeneratedPKId;
}

bool ColumnGrants(const catalog::Column& column, ObjectId owner,
                  RoleIdSpan closure, catalog::AclMode need) {
  return AclCheckSorted(column.GetAcl(), catalog::ObjectType::Table, owner,
                        closure, need, PrivMatch::All);
}

enum class EdgeFilter { All, Inherit, Set };

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

RoleIdSet ComputeClosure(const catalog::Snapshot& snapshot, ObjectId role,
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
    auto obj = snapshot.GetObject<catalog::Role>(cur);
    if (!obj) {
      continue;
    }
    for (const auto& edge : obj->MemberOf()) {
      if (!EdgePasses(edge, filter)) {
        continue;
      }
      if (out.contains(edge.role) ||
          !snapshot.GetObject<catalog::Role>(edge.role)) {
        continue;
      }
      out.insert(edge.role);
      work.push_back(edge.role);
    }
  }
  return out;
}

RoleIdSet ComputeEffectiveRoles(const catalog::Snapshot& snapshot,
                                ObjectId role) {
  return ComputeClosure(snapshot, role, EdgeFilter::Inherit);
}

RoleClosure ComputeRoleClosure(const catalog::Snapshot& snapshot,
                               ObjectId role) {
  RoleClosure out;
  if (!role.isSet()) {
    return out;
  }
  // The membership set comes from the one canonical inherit-closure BFS
  // (ComputeEffectiveRoles): it seeds with `role` even when that id does not
  // resolve to a real Role -- the PUBLIC pseudo-grantee and dangling grantee
  // ids must still reach the ACL walk, where PUBLIC entries are matched.
  const RoleIdSet set = ComputeEffectiveRoles(snapshot, role);
  out.closure.assign(set.begin(), set.end());
  std::ranges::sort(out.closure);
  // The superuser bit is the start role's own attribute -- never inherited.
  if (auto start = snapshot.GetObject<catalog::Role>(role)) {
    out.is_superuser = start->IsSuperuser();
  }
  return out;
}

}  // namespace

RoleIdSet ComputeMembershipClosure(const catalog::Snapshot& snapshot,
                                   ObjectId role) {
  return ComputeClosure(snapshot, role, EdgeFilter::All);
}

RoleIdSet ComputeSetRoleClosure(const catalog::Snapshot& snapshot,
                                ObjectId role) {
  return ComputeClosure(snapshot, role, EdgeFilter::Set);
}

void RoleClosureMap::Build(const catalog::Snapshot& snapshot) {
  // Computed once at publish; reads afterward are lock-free lookups.
  auto roles = snapshot.GetRoles();
  _by_role.clear();
  _by_role.reserve(roles.size());
  for (const auto& role : roles) {
    _by_role.emplace(role->GetId(),
                     ComputeRoleClosure(snapshot, role->GetId()));
  }
}

bool HasAdminOption(const catalog::Snapshot& snapshot, ObjectId member,
                    ObjectId target) {
  for (ObjectId r : ComputeMembershipClosure(snapshot, member)) {
    auto obj = snapshot.GetObject<catalog::Role>(r);
    if (!obj) {
      continue;
    }
    for (const auto& edge : obj->MemberOf()) {
      if (edge.role == target && edge.admin_option) {
        return true;
      }
    }
  }
  return false;
}

bool RoleClosure::Can(const catalog::Object& object,
                      catalog::AclMode need) const {
  // The owner (and a superuser, who owns everything) holds every privilege.
  if (Owns(object)) {
    return true;
  }
  return AclCheckSorted(object.GetAcl(), object.GetType(), object.GetOwner(),
                        closure, need, PrivMatch::All);
}

bool RoleClosure::CanAny(const catalog::Object& object,
                         catalog::AclMode need) const {
  if (Owns(object)) {
    return true;
  }
  return AclCheckSorted(object.GetAcl(), object.GetType(), object.GetOwner(),
                        closure, need, PrivMatch::Any);
}

catalog::AclMode RoleClosure::HeldModes(catalog::AclView acl) const {
  return AclPrivsHeld(acl, closure);
}

catalog::AclMode RoleClosure::GrantableModes(catalog::AclView acl) const {
  return AclGrantOptionHeld(acl, closure);
}

bool RoleClosure::CanColumns(
  const catalog::Table& table, catalog::AclMode need,
  absl::FunctionRef<bool(uint64_t, const catalog::Column&)> selected) const {
  if (Can(table, need)) {
    return true;
  }
  const auto owner = table.GetOwner();

  bool saw_selected = false;
  uint64_t idx = 0;
  for (const auto& col : table.Columns()) {
    if (IsHidden(col)) {
      continue;
    }
    if (!selected(idx++, col)) {
      continue;
    }
    saw_selected = true;
    if (!ColumnGrants(col, owner, closure, need)) {
      return false;
    }
  }
  return saw_selected;
}

bool RoleClosure::CanAnyColumn(const catalog::Table& table,
                               catalog::AclMode need) const {
  if (Can(table, need)) {
    return true;
  }
  const auto owner = table.GetOwner();
  return std::ranges::any_of(table.Columns(), [&](const catalog::Column& c) {
    return !IsHidden(c) && ColumnGrants(c, owner, closure, need);
  });
}

}  // namespace sdb::auth
