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

#include <vector>

#include "auth/acl.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

struct Snapshot;

}  // namespace sdb::catalog
namespace sdb::auth {

RoleIdSet ComputeMembershipClosure(const catalog::Snapshot& snapshot,
                                   ObjectId role);

RoleIdSet ComputeSetRoleClosure(const catalog::Snapshot& snapshot,
                                ObjectId role);

struct RoleClosure {
  std::vector<ObjectId> closure;
  bool is_superuser = false;
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
