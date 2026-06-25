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

#include <span>

#include "auth/acl.h"
#include "catalog/fwd.h"
#include "catalog/object.h"

namespace sdb::auth {

bool HasAdminOption(const catalog::Snapshot& snapshot, ObjectId member,
                    ObjectId target);

bool HasPrivilege(const catalog::Snapshot& snapshot, ObjectId role,
                  const catalog::Object& object, catalog::AclMode need);

bool HasAnyPrivilege(const catalog::Snapshot& snapshot, ObjectId role,
                     const catalog::Object& object, catalog::AclMode need);

bool HasColumnPrivilege(const catalog::Snapshot& snapshot, ObjectId role,
                        const catalog::Table& table, catalog::AclMode need,
                        std::span<const catalog::Column* const> columns);

}  // namespace sdb::auth
