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

#include <expected>
#include <span>
#include <string>
#include <string_view>

#include "basics/containers/flat_hash_set.h"
#include "catalog/object.h"

namespace sdb::auth {

using RoleIdSet = containers::FlatHashSet<ObjectId>;
// Sorted ascending; membership tested with binary_search.
using RoleIdSpan = std::span<const ObjectId>;

catalog::Acl AclDefault(catalog::ObjectType type, ObjectId owner);

// Produce the ACL to persist: drop the owner's derived self-grant (owner
// privileges come from ownership at check time) and, on first touch, seed the
// PUBLIC default so a later REVOKE FROM PUBLIC has a row to subtract from --
// mirroring PG's acldefault() materialization, minus the owner row.
catalog::Acl AclForStorage(catalog::AclView stored, catalog::ObjectType type,
                           ObjectId owner);

bool AclCheckSorted(catalog::AclView stored, catalog::ObjectType type,
                    ObjectId owner, RoleIdSpan roles, catalog::AclMode need,
                    bool any_of);

catalog::AclMode AclGrantOptionHeld(catalog::AclView acl,
                                    const RoleIdSet& roles);

catalog::AclMode AclPrivsHeld(catalog::AclView acl, const RoleIdSet& roles);

// Items are keyed by (grantee, grantor).
void AclGrant(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
              catalog::AclMode privs,
              catalog::AclMode grant_option = catalog::AclMode::NoRights);

void AclRevoke(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
               catalog::AclMode privs);

void AclRemoveGrantOption(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                          catalog::AclMode privs);

catalog::AclMode AclDependentPrivs(catalog::AclView acl, ObjectId grantee,
                                   catalog::AclMode privs);

void AclRevokeCascade(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                      catalog::AclMode privs);

enum class AclKeywordError { Unrecognized, WrongClass };

std::expected<catalog::AclMode, AclKeywordError> TryParseAclKeyword(
  std::string_view keyword, catalog::ObjectType type);

// Render one aclitem to PG's text form ("grantee=privchars/grantor", "" grantee
// for PUBLIC). name_of resolves a role id to its name.
std::string AclItemToText(
  const catalog::AclItem& item,
  absl::FunctionRef<std::string_view(ObjectId)> name_of);

}  // namespace sdb::auth
