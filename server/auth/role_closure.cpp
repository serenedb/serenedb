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

#include "catalog/catalog.h"
#include "catalog/role.h"

namespace sdb::auth {
namespace {

// Which per-edge option gates traversal: All = every edge (is_member_of),
// Inherit = inherit_option edges (has_privs_of_role), Set = set_option edges
// (member_can_set_role).
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

// BFS over membership edges, following only edges that pass `filter`. Dangling
// edges (DROP ROLE leaves them behind) are skipped.
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

RoleIdSet ComputeEffectiveRoles(const catalog::Snapshot& snapshot,
                                ObjectId role) {
  return ComputeClosure(snapshot, role, EdgeFilter::Inherit);
}

RoleIdSet ComputeMembershipClosure(const catalog::Snapshot& snapshot,
                                   ObjectId role) {
  return ComputeClosure(snapshot, role, EdgeFilter::All);
}

RoleIdSet ComputeSetRoleClosure(const catalog::Snapshot& snapshot,
                                ObjectId role) {
  return ComputeClosure(snapshot, role, EdgeFilter::Set);
}

void RoleClosureMap::Build(const catalog::Snapshot& snapshot) {
  // Eager: compute every role's inherit-closure once, at snapshot publish.
  // Reads afterward are lock-free lookups into this frozen map. Rebuild cost is
  // paid only on the (rare) DDL/GRANT publish, never on the (hot) privilege
  // check.
  auto roles = snapshot.GetRoles();
  _by_role.clear();
  _by_role.reserve(roles.size());
  for (const auto& role : roles) {
    _by_role.emplace(role->GetId(),
                     ComputeRoleClosure(snapshot, role->GetId()));
  }
}

}  // namespace sdb::auth
