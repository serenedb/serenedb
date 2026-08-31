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

#include <absl/functional/any_invocable.h>

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <optional>
#include <span>
#include <string_view>

#include "catalog1/permissions.h"
namespace sdb::auth {

// Sorted ascending; membership tested with binary_search.
using RoleIdSpan = std::span<const duckdb::idx_t>;

// Owning, because it runs inside the mutation, against the ACL of the version
// it is about to record.
using AclMutator = absl::AnyInvocable<void(duckdb::idx_t owner, catalog::Acl&)>;

enum class PrivMatch {
  All,
  Any,
};

catalog::Acl AclDefault(duckdb::CatalogType type, duckdb::idx_t owner);

catalog::Acl AclForStorage(catalog::AclView stored, duckdb::CatalogType type,
                           duckdb::idx_t owner);

std::optional<catalog::AclMode> TryParseAclKeyword(std::string_view keyword,
                                                   duckdb::CatalogType type);

bool AclCheckSorted(catalog::AclView stored, duckdb::CatalogType type,
                    duckdb::idx_t owner, RoleIdSpan roles,
                    catalog::AclMode need, PrivMatch match);

catalog::AclMode AclGrantOptionHeld(catalog::AclView acl, RoleIdSpan roles);

catalog::AclMode AclPrivsHeld(catalog::AclView acl, RoleIdSpan roles);

// PG ownership transfer: drop the old owner's implicit self-grant and rewrite
// grantor old->new on the rows it had granted, then install the new owner.
// Operates on an already-cloned object (the COW analogue of writing a new
// catalog tuple).
catalog::Permissions TransferredOwner(catalog::Permissions perm,
                                      duckdb::idx_t new_owner);

// The ACL a GRANT or REVOKE leaves behind. The stored form holds only
// non-owner grants: the owner's privileges are derived from ownership at check
// time and synthesized at render time.
catalog::Permissions MutatedAcl(const catalog::Permissions& perm,
                                duckdb::CatalogType type, AclMutator& mutate);

}  // namespace sdb::auth
