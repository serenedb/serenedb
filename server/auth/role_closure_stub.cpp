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

// SDB_RBAC_DISABLED. The real role_closure.cpp is out of the build until the
// RBAC phase; its header stays because seventeen translation units include it.
// Every check here answers "allowed" and the graph is always empty, which is
// the same answer catalog1/permissions.h gives. Deleting this file and
// restoring role_closure.cpp is the whole switch-back.

#include <memory>

#include "auth/role_closure.h"

namespace sdb::auth {
namespace {

const RoleGraph& EmptyGraph() {
  static const RoleGraph kEmpty;
  return kEmpty;
}

}  // namespace

// Nothing caches a closure while RBAC is disabled, so the generation never
// has to move; the DDL paths still call Bump and are left untouched.
uint64_t RoleGeneration() noexcept { return 0; }

void BumpRoleGeneration() noexcept {}

RoleIdSet ComputeMembershipClosure(const RoleGraph&, duckdb::idx_t role) {
  return RoleIdSet{role};
}

RoleIdSet ComputeSetRoleClosure(const RoleGraph&, duckdb::idx_t role) {
  return RoleIdSet{role};
}

bool HasAdminOption(const RoleGraph&, duckdb::idx_t, duckdb::idx_t) {
  return true;
}

bool RoleClosure::Can(duckdb::CatalogType, const catalog::Permissions&,
                      catalog::AclMode) const {
  return true;
}

bool RoleClosure::CanAny(duckdb::CatalogType, const catalog::Permissions&,
                         catalog::AclMode) const {
  return true;
}

bool RoleClosure::CanColumns(const catalog::Permissions&, catalog::AclMode,
                             std::span<const catalog::AclView>) const {
  return true;
}

bool RoleClosure::CanAnyColumn(const catalog::Permissions&, catalog::AclMode,
                               std::span<const catalog::AclView>) const {
  return true;
}

catalog::AclMode RoleClosure::HeldModes(catalog::AclView) const {
  return catalog::AclMode::NoRights;
}

catalog::AclMode RoleClosure::GrantableModes(catalog::AclView) const {
  return catalog::AclMode::NoRights;
}

std::shared_ptr<const RoleGraph> RolesOf(duckdb::ClientContext*) {
  return {std::shared_ptr<void>{}, &EmptyGraph()};
}

std::shared_ptr<const RoleClosure> ClosureFor(duckdb::ClientContext*,
                                              duckdb::idx_t role) {
  return std::make_shared<const RoleClosure>(
    ComputeRoleClosure(EmptyGraph(), role));
}

RoleClosure ComputeRoleClosure(const RoleGraph&, duckdb::idx_t role) {
  // Superuser: every predicate above already answers true, and this keeps the
  // non-authz readers (the is_superuser GUC, the SET ROLE decision) consistent
  // with them.
  return RoleClosure{.closure = {role}, .is_superuser = true};
}

}  // namespace sdb::auth
