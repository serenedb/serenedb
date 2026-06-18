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

#include "basics/result.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"

namespace sdb::catalog {

struct Snapshot;

struct AccessContext {
  ObjectId role;
  AclMode need = AclMode::NoRights;
};

inline AccessContext RequireAccess(ObjectId role, AclMode need) {
  return {role, need};
}

inline AccessContext NoAccessCheck() { return {id::kRootUser}; }

Result CheckAccess(const Snapshot& snapshot, ObjectId role,
                   const Object& object, AclMode need);

Result CheckAnyAccess(const Snapshot& snapshot, ObjectId role,
                      const Object& object, AclMode need);

Result CheckOwnership(const Snapshot& snapshot, ObjectId role, ObjectId owner);

}  // namespace sdb::catalog
