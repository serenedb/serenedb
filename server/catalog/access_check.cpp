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

#include "catalog/access_check.h"

#include <algorithm>

#include "auth/privilege.h"
#include "auth/role_closure.h"
#include "basics/errors.h"
#include "catalog/catalog.h"

namespace sdb::catalog {

Result CheckAccess(const Snapshot& snapshot, ObjectId role,
                   const Object& object, AclMode need) {
  if (auth::HasPrivilege(snapshot, role, object, need)) {
    return {};
  }
  return Result{ERROR_FORBIDDEN};
}

Result CheckAnyAccess(const Snapshot& snapshot, ObjectId role,
                      const Object& object, AclMode need) {
  if (auth::HasAnyPrivilege(snapshot, role, object, need)) {
    return {};
  }
  return Result{ERROR_FORBIDDEN};
}

Result CheckOwnership(const Snapshot& snapshot, ObjectId role, ObjectId owner) {
  if (!owner.isSet()) {
    return {};
  }
  const auto& rc = snapshot.EffectiveRoleClosure(role);
  if (rc.is_superuser || std::ranges::binary_search(rc.closure, owner)) {
    return {};
  }
  return Result{ERROR_FORBIDDEN};
}

}  // namespace sdb::catalog
