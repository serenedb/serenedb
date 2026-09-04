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

#include <duckdb/catalog/catalog_permissions.hpp>
#include <span>

namespace sdb::catalog {

// The privilege types live on duckdb::CatalogEntry so that every kind -- table,
// view, macro, database, role -- has one home for them, which is what the
// pg_catalog acl columns and the access-control layer both read.
using AclMode = duckdb::AclMode;
using AclItem = duckdb::AclItem;
using Acl = duckdb::vector<AclItem>;
using AclView = std::span<const AclItem>;
using ColumnAcls = duckdb::vector<Acl>;
using Permissions = duckdb::CatalogPermissions;

// SDB_RBAC_DISABLED. Enforcement is deferred to the RBAC phase: every acl is
// empty and every check below answers "allowed". Ownership is real -- the
// system suite asserts pg_class.relowner. Grep this marker for every inert
// site before turning enforcement on.
constexpr bool Can(const Permissions&, AclMode) noexcept { return true; }

constexpr bool CanColumn(const Permissions&, duckdb::idx_t, AclMode) noexcept {
  return true;
}

// SDB_RBAC_DISABLED. No column carries an acl until the RBAC phase, so what
// pg_attribute.attacl renders is empty rather than absent.
constexpr AclView ColumnAclOf(const Permissions&, duckdb::idx_t) noexcept {
  return {};
}

}  // namespace sdb::catalog
