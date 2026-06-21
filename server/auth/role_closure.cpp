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

#include "auth/privilege.h"
#include "catalog/catalog.h"
#include "catalog/role.h"

namespace sdb::auth {
namespace {

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

const RoleClosure& RoleClosureCache::Get(const catalog::Snapshot& snapshot,
                                         ObjectId role) const {
  // Hot path: every privilege check hits this. The closure for a (snapshot,
  // role) is computed once and never changes (the snapshot is immutable COW),
  // and NodeHashMap nodes are reference-stable across inserts, so a cache hit
  // only needs a shared read lock -- no writer contention. The exclusive lock
  // is taken solely to compute+insert on the first miss (double-checked).
  {
    absl::ReaderMutexLock lock{&_mu};
    if (auto it = _by_role.find(role); it != _by_role.end()) {
      return it->second;
    }
  }
  absl::WriterMutexLock lock{&_mu};
  auto it = _by_role.find(role);
  if (it == _by_role.end()) {
    it = _by_role.emplace(role, ComputeRoleClosure(snapshot, role)).first;
  }
  return it->second;
}

}  // namespace sdb::auth
