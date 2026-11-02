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

#include "auth/role_closure.h"

#include <absl/algorithm/container.h>

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>

#include "catalog/cluster.h"

namespace sdb::auth {

duckdb::RoleCapability PredefinedCapability(duckdb::idx_t oid) {
  const auto it = absl::c_find_if(
    kPredefinedRoles, [&](const auto& role) { return role.oid == oid; });
  return it == std::end(kPredefinedRoles) ? duckdb::RoleCapability::None
                                          : it->capability;
}

duckdb::shared_ptr<const duckdb::SecuritySnapshot> RolesOf(
  duckdb::ClientContext* context) {
  if (context && context->security) {
    return context->security;
  }
  auto& cluster =
    context ? catalog::ClusterOf(*context->db) : catalog::ClusterOf();
  auto& db = cluster.GetDatabase();
  if (auto roles = db.GetSecurity()) {
    return roles;
  }
  cluster.PublishSecurity();
  return db.GetSecurity();
}

}  // namespace sdb::auth
