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
  // PG: `member` holds ADMIN OPTION on `target` if any role it is effectively a
  // member of holds a direct admin_option grant of `target` (admin propagates
  // through plain membership, so use the full membership closure).
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

namespace {

// `any_of` = has_*_privilege comma-list semantics (>=1 bit) vs enforcement's
// all-of (every bit in `need`). superuser/owner imply all privileges. The
// privilege class is `object.GetType()`.
bool CheckPrivilege(const RoleClosure& rc, const catalog::Object& object,
                    catalog::AclMode need, bool any_of) {
  if (rc.is_superuser) {
    return true;
  }
  // Note: a non-resolvable id (PUBLIC pseudo-grantee, dangling grantee) still
  // runs the ACL walk below -- its closure is seeded with the id itself, so
  // PUBLIC entries match. Only a real Role contributes a superuser bit or
  // inherit edges to the closure.

  // Ownercheck short-circuits the ACL. An unset owner (e.g. an index, which
  // derives ownership from its table) is in no role's closure and grants no
  // implicit access -- the correct default, so it is passed through as-is.
  const auto owner = object.GetOwner();
  if (std::ranges::binary_search(rc.closure, owner)) {
    return true;
  }

  return AclCheckSorted(object.GetAcl(), object.GetType(), owner, rc.closure,
                        need, any_of);
}

}  // namespace

bool HasPrivilege(const RoleClosure& closure, const catalog::Object& object,
                  catalog::AclMode need) {
  return CheckPrivilege(closure, object, need, /*any_of=*/false);
}

bool HasAnyPrivilege(const RoleClosure& closure, const catalog::Object& object,
                     catalog::AclMode need) {
  return CheckPrivilege(closure, object, need, /*any_of=*/true);
}

bool HasPrivilege(const catalog::Snapshot& snapshot, ObjectId role,
                  const catalog::Object& object, catalog::AclMode need) {
  return CheckPrivilege(snapshot.EffectiveRoleClosure(role), object, need,
                        /*any_of=*/false);
}

bool HasAnyPrivilege(const catalog::Snapshot& snapshot, ObjectId role,
                     const catalog::Object& object, catalog::AclMode need) {
  return CheckPrivilege(snapshot.EffectiveRoleClosure(role), object, need,
                        /*any_of=*/true);
}

namespace {

// A column carries the privilege if the table owner reaches it (the owner holds
// every column privilege) or the column's own ACL grants it. Column privileges
// use the Table privilege class (SELECT/INSERT/UPDATE share the relation bits).
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

  // Relation-level grant satisfies the privilege for every column.
  if (std::ranges::binary_search(rc.closure, owner) ||
      AclCheckSorted(table.GetAcl(), catalog::ObjectType::Table, owner,
                     rc.closure, need, /*any_of=*/false)) {
    return true;
  }

  // Walk the table's columns once: a referenced column must carry the privilege
  // (all-of); with no referenced column, ANY one column suffices (PG count(*)).
  bool saw_referenced = false;
  uint64_t visible = 0;
  for (const auto& col : table.Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    const uint64_t idx = visible++;
    if (!any_referenced) {
      if (ColumnGrants(col, owner, rc.closure, need)) {
        return true;  // ANY-column: first granted column passes.
      }
    } else if (referenced(idx)) {
      saw_referenced = true;
      if (!ColumnGrants(col, owner, rc.closure, need)) {
        return false;  // all-of: a referenced column lacks the privilege.
      }
    }
  }
  // ANY-column found none; all-of: pass iff every referenced column was
  // granted.
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

  // Relation-level grant satisfies the privilege for every column.
  if (std::ranges::binary_search(rc.closure, owner) ||
      AclCheckSorted(table.GetAcl(), catalog::ObjectType::Table, owner,
                     rc.closure, need, /*any_of=*/false)) {
    return true;
  }

  // No column referenced (e.g. SELECT count(*)): PG requires the privilege on
  // ANY one column of the relation.
  if (columns.empty()) {
    return std::ranges::any_of(table.Columns(), [&](const catalog::Column& c) {
      return ColumnGrants(c, owner, rc.closure, need);
    });
  }

  // Otherwise EVERY accessed column must carry the privilege at column level.
  return std::ranges::all_of(columns, [&](const catalog::Column* c) {
    return c != nullptr && ColumnGrants(*c, owner, rc.closure, need);
  });
}

}  // namespace sdb::auth
