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

#include <optional>
#include <span>
#include <string_view>

#include "catalog/object.h"

namespace sdb::auth {

// Sorted ascending; membership tested with binary_search.
using RoleIdSpan = std::span<const ObjectId>;

enum class PrivMatch { All, Any };

catalog::Acl AclDefault(catalog::ObjectType type, ObjectId owner);

catalog::Acl AclForStorage(catalog::AclView stored, catalog::ObjectType type,
                           ObjectId owner);

std::optional<catalog::AclMode> TryParseAclKeyword(std::string_view keyword,
                                                   catalog::ObjectType type);

bool AclCheckSorted(catalog::AclView stored, catalog::ObjectType type,
                    ObjectId owner, RoleIdSpan roles, catalog::AclMode need,
                    PrivMatch match);

catalog::AclMode AclGrantOptionHeld(catalog::AclView acl, RoleIdSpan roles);

catalog::AclMode AclPrivsHeld(catalog::AclView acl, RoleIdSpan roles);

}  // namespace sdb::auth
