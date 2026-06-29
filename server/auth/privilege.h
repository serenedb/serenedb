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

#include <functional>
#include <span>

#include "auth/acl.h"
#include "auth/role_closure.h"
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

// Closure-based overloads: the caller resolves the role closure ONCE (it is the
// only snapshot dependency of the ACL math) and reuses it across many checks,
// avoiding a per-check closure lookup. Behaviour is identical to the snapshot
// overloads above.
bool HasPrivilege(const RoleClosure& closure, const catalog::Object& object,
                  catalog::AclMode need);

bool HasAnyPrivilege(const RoleClosure& closure, const catalog::Object& object,
                     catalog::AclMode need);

// Column-privilege core over a resolved closure. `referenced(visible_index)`
// reports whether the logical column at that PK-excluded position was touched;
// `any_referenced` is false when the query named no specific column (count(*)
// semantics: ANY one granted column suffices). Walks the table's columns once,
// no intermediate Column* vector. The templated overload below adapts any set
// with `.empty()`/`.contains()` (FlatHashSet, duckdb::unordered_set) to this.
bool HasColumnPrivilege(const RoleClosure& closure, const catalog::Table& table,
                        catalog::AclMode need, bool any_referenced,
                        const std::function<bool(uint64_t)>& referenced);

template<typename SetT>
bool HasColumnPrivilege(const RoleClosure& closure, const catalog::Table& table,
                        catalog::AclMode need, const SetT& logical) {
  return HasColumnPrivilege(closure, table, need, !logical.empty(),
                            [&](uint64_t i) { return logical.contains(i); });
}

}  // namespace sdb::auth
