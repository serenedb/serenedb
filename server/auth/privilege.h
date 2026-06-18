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

#include <memory>

#include "auth/acl.h"
#include "catalog/catalog.h"
#include "catalog/object.h"

namespace sdb::auth {

// Inherit-closure of `role` (PG has_privs_of_role): {role} + roles reachable
// via edges whose inherit_option is set. This is the privilege closure --
// PUBLIC is matched in AclCheck, not inserted here.
RoleIdSet ComputeEffectiveRoles(const catalog::Snapshot& snapshot,
                                ObjectId role);

// Membership-closure of `role` (PG is_member_of): {role} + all roles reachable
// via memberships regardless of inherit_option. Used for cycle detection and
// membership questions, NOT for privilege checks.
RoleIdSet ComputeMembershipClosure(const catalog::Snapshot& snapshot,
                                   ObjectId role);

// SET-closure of `role` (PG member_can_set_role): {role} + roles reachable via
// edges whose set_option is set. Used for "can SET ROLE to X" checks, e.g.
// ALTER ... OWNER TO requires the caller to be able to SET ROLE to the new
// owner.
RoleIdSet ComputeSetRoleClosure(const catalog::Snapshot& snapshot,
                                ObjectId role);

// True if `member` holds ADMIN OPTION on `target` (PG: WITH ADMIN OPTION,
// propagated through membership). Used by pg_has_role and GRANT-role authority.
bool HasAdminOption(const catalog::Snapshot& snapshot, ObjectId member,
                    ObjectId target);

// superuser -> ownercheck -> ACL walk (NULL-acl expanded). False if role/object
// cannot be resolved. The privilege class is `object.GetType()`.
bool HasPrivilege(const catalog::Snapshot& snapshot, ObjectId role,
                  const catalog::Object& object, catalog::AclMode need);

// True if `role` holds AT LEAST ONE bit in `need` (has_*_privilege comma-list).
bool HasAnyPrivilege(const catalog::Snapshot& snapshot, ObjectId role,
                     const catalog::Object& object, catalog::AclMode need);

}  // namespace sdb::auth
