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

#include "auth/privilege.h"

#include <algorithm>
#include <span>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/role.h"
#include "catalog/table.h"

namespace sdb::auth {

bool HasAdminOption(const catalog::Snapshot& snapshot, ObjectId member,
                    ObjectId target) {
  for (ObjectId r : ComputeMembershipClosure(snapshot, member)) {
    auto obj = snapshot.GetObject<catalog::Role>(r);
    if (!obj) {
      continue;
    }
    for (const auto& edge : obj->MemberOf()) {
      if (edge.role == target && edge.admin_option) {
        return true;
      }
    }
  }
  return false;
}

bool IsOwner(const RoleClosure& closure, const catalog::Object& object) {
  return closure.is_superuser ||
         std::ranges::binary_search(closure.closure, object.GetOwner());
}

bool HasPrivilege(const RoleClosure& closure, const catalog::Object& object,
                  catalog::AclMode need, PrivMatch match) {
  if (IsOwner(closure, object)) {
    return true;
  }
  return AclCheckSorted(object.GetAcl(), object.GetType(), object.GetOwner(),
                        closure.closure, need, match == PrivMatch::Any);
}

bool HasPrivilege(const catalog::Snapshot& snapshot, ObjectId role,
                  const catalog::Object& object, catalog::AclMode need,
                  PrivMatch match) {
  return HasPrivilege(snapshot.EffectiveRoleClosure(role), object, need, match);
}

namespace {

bool ColumnGrants(const catalog::Column& column, ObjectId owner,
                  RoleIdSpan closure, catalog::AclMode need) {
  return AclCheckSorted(column.GetAcl(), catalog::ObjectType::Table, owner,
                        closure, need, /*any_of=*/false);
}

}  // namespace

bool HasColumnPrivilege(const RoleClosure& rc, const catalog::Table& table,
                        catalog::AclMode need, bool any_referenced,
                        const std::function<bool(uint64_t)>& referenced) {
  if (rc.is_superuser) {
    return true;
  }
  const auto owner = table.GetOwner();

  if (std::ranges::binary_search(rc.closure, owner) ||
      AclCheckSorted(table.GetAcl(), catalog::ObjectType::Table, owner,
                     rc.closure, need, /*any_of=*/false)) {
    return true;
  }

  bool saw_referenced = false;
  uint64_t visible = 0;
  for (const auto& col : table.Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    const uint64_t idx = visible++;
    if (!any_referenced) {
      if (ColumnGrants(col, owner, rc.closure, need)) {
        return true;
      }
    } else if (referenced(idx)) {
      saw_referenced = true;
      if (!ColumnGrants(col, owner, rc.closure, need)) {
        return false;
      }
    }
  }
  return any_referenced && saw_referenced;
}

bool HasColumnPrivilege(const catalog::Snapshot& snapshot, ObjectId role,
                        const catalog::Table& table, catalog::AclMode need,
                        std::span<const catalog::Column* const> columns) {
  const auto& rc = snapshot.EffectiveRoleClosure(role);
  if (rc.is_superuser) {
    return true;
  }
  const auto owner = table.GetOwner();

  if (std::ranges::binary_search(rc.closure, owner) ||
      AclCheckSorted(table.GetAcl(), catalog::ObjectType::Table, owner,
                     rc.closure, need, /*any_of=*/false)) {
    return true;
  }

  if (columns.empty()) {
    return std::ranges::any_of(table.Columns(), [&](const catalog::Column& c) {
      return ColumnGrants(c, owner, rc.closure, need);
    });
  }

  return std::ranges::all_of(columns, [&](const catalog::Column* c) {
    return c != nullptr && ColumnGrants(*c, owner, rc.closure, need);
  });
}

}  // namespace sdb::auth
