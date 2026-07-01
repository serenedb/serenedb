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

#include <optional>
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

catalog::Acl AclForStorage(catalog::AclView stored, catalog::ObjectType type,
                           ObjectId owner);

bool AclCheckSorted(catalog::AclView stored, catalog::ObjectType type,
                    ObjectId owner, RoleIdSpan roles, catalog::AclMode need,
                    bool any_of);

catalog::AclMode AclGrantOptionHeld(catalog::AclView acl, RoleIdSpan roles);

std::optional<catalog::AclMode> TryParseAclKeyword(std::string_view keyword,
                                                   catalog::ObjectType type);

std::string AclItemToText(
  const catalog::AclItem& item,
  absl::FunctionRef<std::string_view(ObjectId)> name_of);

}  // namespace sdb::auth
