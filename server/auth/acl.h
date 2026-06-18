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

#include <span>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_set.h"
#include "catalog/object.h"

namespace sdb::auth {

using RoleIdSet = containers::FlatHashSet<ObjectId>;
// Sorted ascending; membership tested with binary_search.
using RoleIdSpan = std::span<const ObjectId>;

catalog::Acl AclDefault(catalog::ObjectType type, ObjectId owner);

catalog::Acl AclEffective(catalog::AclView stored, catalog::ObjectType type,
                          ObjectId owner);

bool AclCheckSorted(catalog::AclView stored, catalog::ObjectType type,
                    ObjectId owner, RoleIdSpan roles, catalog::AclMode need,
                    bool any_of);

// Accumulated grant-option bits held by `roles` (plus PUBLIC) over `acl` -- the
// privileges those roles may re-grant. `acl` should be the effective ACL.
catalog::AclMode AclGrantOptionHeld(catalog::AclView acl,
                                    const RoleIdSet& roles);

// Privileges those roles hold on the object (not the grant option).
catalog::AclMode AclPrivsHeld(catalog::AclView acl, const RoleIdSet& roles);

// Items are keyed by (grantee, grantor).
void AclGrant(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
              catalog::AclMode privs,
              catalog::AclMode grant_option = catalog::AclMode::NoRights);

// A non-matching (grantee, grantor) is a silent no-op (PG). RESTRICT only.
void AclRevoke(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
               catalog::AclMode privs);

// REVOKE GRANT OPTION FOR: clears the grant-option bits for `privs` on the
// (grantee, grantor) item but keeps the underlying privilege. Silent no-op if
// no item matches.
void AclRemoveGrantOption(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                          catalog::AclMode privs);

// Privileges in `acl` that depend on `grantee` being able to re-grant `privs`:
// the union of `privs`-bits carried by items whose grantor is `grantee`. Empty
// (NoRights) means a REVOKE RESTRICT can proceed without breaking dependents.
catalog::AclMode AclDependentPrivs(catalog::AclView acl, ObjectId grantee,
                                   catalog::AclMode privs);

// REVOKE ... CASCADE: revoke `privs` from (grantee, grantor) and recursively
// revoke any privileges that `grantee` re-granted (items with grantor ==
// grantee), to a fixpoint. Mirrors PG's dependent-grant teardown.
void AclRevokeCascade(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                      catalog::AclMode privs);

// "SELECT, INSERT" -> mask; throws on a bad or class-invalid name.
catalog::AclMode ParseAclMode(std::string_view privileges,
                              catalog::ObjectType type);

// PG aclitemout: "grantee=privs/grantor", PUBLIC grantee empty, '*' = grant
// option.
std::string AclItemToText(
  const catalog::AclItem& item,
  absl::FunctionRef<std::string_view(ObjectId)> name_of);

}  // namespace sdb::auth
