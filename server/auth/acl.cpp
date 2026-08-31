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

#include "auth/acl.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <optional>
#include <string>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/system-compiler.h"
#include "pg/pg_types.h"

namespace sdb::auth {
namespace {

using catalog::AclItem;
using catalog::AclMode;
using duckdb::CatalogType;

// Lowercase keyword -> AclMode; callers lowercase the input before lookup.
const containers::FlatHashMap<std::string_view, AclMode> kPrivNames{
  {"select", AclMode::Select},     {"insert", AclMode::Insert},
  {"update", AclMode::Update},     {"delete", AclMode::Delete},
  {"truncate", AclMode::Truncate}, {"references", AclMode::References},
  {"trigger", AclMode::Trigger},   {"maintain", AclMode::Maintain},
  {"execute", AclMode::Execute},   {"usage", AclMode::Usage},
  {"create", AclMode::Create},     {"temporary", AclMode::CreateTemp},
  {"temp", AclMode::CreateTemp},   {"connect", AclMode::Connect},
};

AclMode ClassPrivs(CatalogType type) noexcept {
  switch (type) {
    case CatalogType::TABLE_ENTRY:
      return AclMode::Select | AclMode::Insert | AclMode::Update |
             AclMode::Delete | AclMode::Truncate | AclMode::References |
             AclMode::Trigger | AclMode::Maintain;
    case CatalogType::SEQUENCE_ENTRY:
      return AclMode::Select | AclMode::Update | AclMode::Usage;
    case CatalogType::DATABASE_ENTRY:
      return AclMode::Create | AclMode::CreateTemp | AclMode::Connect;
    case CatalogType::SCHEMA_ENTRY:
      return AclMode::Usage | AclMode::Create;
    case CatalogType::MACRO_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
      return AclMode::Execute;
    case CatalogType::TYPE_ENTRY:
      return AclMode::Usage;
    // A FOREIGN SERVER carries USAGE, as in postgres.
    case CatalogType::FOREIGN_SERVER_ENTRY:
      return AclMode::Usage;
    default:
      return AclMode::NoRights;
  }
}

bool Has(AclMode have, AclMode need) noexcept {
  return (have & need) == need && need != AclMode::NoRights;
}

AclMode PublicDefaultPrivs(CatalogType type) noexcept {
  switch (type) {
    case CatalogType::DATABASE_ENTRY:
      return AclMode::Connect | AclMode::CreateTemp;
    case CatalogType::MACRO_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
      return AclMode::Execute;
    case CatalogType::TYPE_ENTRY:
      return AclMode::Usage;
    default:
      return AclMode::NoRights;
  }
}

bool RolesContain(RoleIdSpan roles, duckdb::idx_t id) noexcept {
  return std::ranges::binary_search(roles, id);
}

bool IsGranteeInRoles(duckdb::idx_t grantee, RoleIdSpan roles) {
  return grantee == pg::kPublicGrantee || RolesContain(roles, grantee);
}

bool IsGranteeInRoles(const AclItem& item, RoleIdSpan roles) {
  return IsGranteeInRoles(item.grantee, roles);
}

AclMode AclModeHeld(catalog::AclView acl, RoleIdSpan roles,
                    AclMode AclItem::* field) {
  AclMode held = AclMode::NoRights;
  for (const auto& item : acl) {
    if (IsGranteeInRoles(item, roles)) {
      held |= item.*field;
    }
  }
  return held;
}

}  // namespace

catalog::Acl AclDefault(CatalogType type, duckdb::idx_t owner) {
  catalog::Acl acl;
  const AclMode owner_privs = ClassPrivs(type);
  if (owner_privs == AclMode::NoRights) {
    return acl;
  }
  acl.push_back(AclItem{
    .grantee = owner,
    .grantor = owner,
    .privs = owner_privs,
  });

  const AclMode public_privs = PublicDefaultPrivs(type);
  if (public_privs != AclMode::NoRights) {
    acl.push_back(AclItem{
      .grantee = pg::kPublicGrantee,
      .grantor = owner,
      .privs = public_privs,
    });
  }
  return acl;
}

catalog::Acl AclForStorage(catalog::AclView stored, CatalogType type,
                           duckdb::idx_t owner) {
  if (stored.empty()) {
    return AclDefault(type, owner);
  }
  return catalog::Acl{stored.begin(), stored.end()};
}

bool AclCheckSorted(catalog::AclView stored, CatalogType type,
                    duckdb::idx_t owner, RoleIdSpan roles, AclMode need,
                    PrivMatch match) {
  SDB_ASSERT(std::ranges::is_sorted(roles),
             "AclCheckSorted requires an ascending-sorted roles span");
  if (need == AclMode::NoRights) {
    return false;
  }
  const auto done = [&](AclMode have) {
    return match == PrivMatch::Any ? (have & need) != AclMode::NoRights
                                   : Has(have, need);
  };

  AclMode have = AclMode::NoRights;
  if (RolesContain(roles, owner)) {
    have |= ClassPrivs(type);
    if (done(have)) {
      return true;
    }
  }

  if (stored.empty()) {
    have |= PublicDefaultPrivs(type);
    return done(have);
  }

  for (const auto& item : stored) {
    if (!IsGranteeInRoles(item, roles)) {
      continue;
    }
    have |= item.privs;
    if (done(have)) {
      return true;
    }
  }
  return false;
}

AclMode AclGrantOptionHeld(catalog::AclView acl, RoleIdSpan roles) {
  return AclModeHeld(acl, roles, &AclItem::grant_option);
}

AclMode AclPrivsHeld(catalog::AclView acl, RoleIdSpan roles) {
  return AclModeHeld(acl, roles, &AclItem::privs);
}

catalog::Permissions TransferredOwner(catalog::Permissions perm,
                                      duckdb::idx_t new_owner) {
  const duckdb::idx_t old_owner = perm.owner;
  std::erase_if(perm.acl, [&](const AclItem& item) {
    return item.grantee == old_owner && item.grantor == old_owner;
  });
  for (auto& item : perm.acl) {
    if (item.grantor == old_owner) {
      item.grantor = new_owner;
    }
  }
  perm.owner = new_owner;
  return perm;
}

catalog::Permissions MutatedAcl(const catalog::Permissions& perm,
                                CatalogType type, AclMutator& mutate) {
  const auto owner = perm.owner;
  auto acl = AclForStorage(perm.acl, type, owner);
  mutate(owner, acl);
  return catalog::Permissions{owner, std::move(acl), perm.column_acl};
}

std::optional<AclMode> TryParseAclKeyword(std::string_view keyword,
                                          CatalogType type) {
  const AclMode allowed = ClassPrivs(type);
  if (absl::EqualsIgnoreCase(keyword, "ALL")) {
    return allowed;
  }
  std::string lowered{keyword};
  absl::AsciiStrToLower(&lowered);
  const auto it = kPrivNames.find(lowered);
  if (it == kPrivNames.end() || (allowed & it->second) != it->second) {
    return std::nullopt;
  }
  return it->second;
}

}  // namespace sdb::auth
