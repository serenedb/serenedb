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

#include <algorithm>

#include "catalog/catalog.h"
#include "catalog/role.h"

namespace sdb::auth {

namespace {

RoleClosure Compute(const catalog::Snapshot& snapshot, ObjectId role) {
  RoleClosure out;
  if (!role.isSet()) {
    return out;
  }
  // Seed the closure with the queried id even when it does not resolve to a
  // real Role: the PUBLIC pseudo-grantee (kPublicGrantee) and dangling grantee
  // ids must still reach the ACL walk, where PUBLIC entries are matched. Only
  // a real Role contributes a superuser bit / inherit edges.
  out.closure.push_back(role);
  auto start = snapshot.GetObject<catalog::Role>(role);
  if (!start) {
    return out;
  }
  out.is_superuser = start->IsSuperuser();
  for (std::size_t i = 0; i < out.closure.size(); ++i) {
    auto cur = snapshot.GetObject<catalog::Role>(out.closure[i]);
    if (!cur) {
      continue;
    }
    for (const auto& edge : cur->MemberOf()) {
      if (!edge.inherit_option) {
        continue;
      }
      const ObjectId parent = edge.role;
      if (absl::c_contains(out.closure, parent)) {
        continue;
      }
      if (snapshot.GetObject<catalog::Role>(parent)) {
        out.closure.push_back(parent);
      }
    }
  }
  std::ranges::sort(out.closure);
  return out;
}

}  // namespace

const RoleClosure& RoleClosureCache::Get(const catalog::Snapshot& snapshot,
                                         ObjectId role) const {
  absl::MutexLock lock{&_mu};
  auto it = _by_role.find(role);
  if (it == _by_role.end()) {
    it = _by_role.emplace(role, Compute(snapshot, role)).first;
  }
  return it->second;
}

}  // namespace sdb::auth
